package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"time"

	"feishu-botd/internal/feishu"
	"feishu-botd/internal/notify"
)

// maxDeliveryImages bounds how much of the bot's tenant-side image store one
// inbound message can spend. It is a refusal, not an eviction: dropping the
// oldest entry would silently re-mint a key a provider still believes it holds,
// and a run that wants a seventeenth picture is a run worth interrupting.
const maxDeliveryImages = 16

// maxConversationImages is the same ceiling, applied to one conversation
// grant rather than one inbound delivery. A later message that shows a QR
// code should not be able to mint unbounded tenant-side images just because
// it has no delivery to pin them to.
const maxConversationImages = 16

// AgentImageUploadInput is one complete image, already reassembled from its
// transport frames. The service layer never sees a partial upload.
//
// Exactly one of DeliveryID or ConversationID must be set. A live inbound
// delivery is the card path; a conversation is the later-message path, and
// it is authorized the same way SendAgentFollowUp is.
type AgentImageUploadInput struct {
	Provider       string
	DeliveryID     string
	ConversationID string
	OperationID    string
	Data           []byte
}

type AgentImageUploadResult struct {
	ImageKey  string
	MediaType string
	Duplicate bool
}

// uploadedImage is what a delivery remembers about an upload so that replaying
// its operation id returns the original key instead of minting another.
type uploadedImage struct {
	imageKey    string
	mediaType   string
	fingerprint string
}

// imageUploadTarget is the app and ledger one authorized upload writes through.
type imageUploadTarget struct {
	appAlias string
	label    string
	lookup   func(operationID, fingerprint string) (*uploadedImage, *notify.APIError)
	remember func(operationID string, image uploadedImage) uploadedImage
}

// UploadAgentImage puts provider-supplied bytes into Feishu under the app that
// delivered the event, and returns the image_key a response markdown embeds as
// ![alt](image_key). Cards and ordinary follow-up posts both render images only
// from such a key, so this is the only way an agent answer can contain a picture.
func (s *Service) UploadAgentImage(ctx context.Context, in AgentImageUploadInput) (AgentImageUploadResult, *notify.APIError) {
	provider := strings.TrimSpace(in.Provider)
	deliveryID := strings.TrimSpace(in.DeliveryID)
	conversationID := strings.TrimSpace(in.ConversationID)
	operationID := strings.TrimSpace(in.OperationID)
	switch {
	case provider == "":
		return AgentImageUploadResult{}, notify.BadRequest("missing_provider", "provider is required")
	case deliveryID != "" && conversationID != "":
		return AgentImageUploadResult{}, notify.BadRequest("ambiguous_image_scope", "set delivery_id or conversation_id, not both")
	case deliveryID == "" && conversationID == "":
		return AgentImageUploadResult{}, notify.BadRequest("missing_image_scope", "delivery_id or conversation_id is required")
	case operationID == "":
		return AgentImageUploadResult{}, notify.BadRequest("missing_operation_id", "operation_id is required")
	case len(provider) > 64 || len(deliveryID) > 160 || len(conversationID) > 160 || len(operationID) > 160:
		return AgentImageUploadResult{}, notify.BadRequest("field_too_large", "one or more fields are too large")
	case len(in.Data) == 0:
		return AgentImageUploadResult{}, notify.BadRequest("missing_image", "image bytes are required")
	}
	if !s.cfg.ProviderAllowsImageUpload(provider) {
		return AgentImageUploadResult{}, notify.NewAPIError(403, "provider_scope_denied", "provider capability is not allowed", false)
	}

	now := time.Now()
	target, apiErr := s.resolveImageUploadTarget(provider, deliveryID, conversationID, operationID, now)
	if apiErr != nil {
		return AgentImageUploadResult{}, apiErr
	}
	backend, ok := s.backendForApp(target.appAlias)
	uploader, capable := backend.images, false
	if ok && uploader != nil {
		capable = true
	}
	if !capable {
		return AgentImageUploadResult{}, notify.NewAPIError(501, "image_upload_unsupported", "this app cannot upload images", false)
	}

	fingerprint := imageFingerprint(in.Data)
	if existing, apiErr := target.lookup(operationID, fingerprint); apiErr != nil {
		return AgentImageUploadResult{}, apiErr
	} else if existing != nil {
		return AgentImageUploadResult{
			ImageKey: existing.imageKey, MediaType: existing.mediaType, Duplicate: true,
		}, nil
	}

	callCtx, cancel := context.WithTimeout(ctx, s.cfg.SendTimeout)
	defer cancel()
	imageKey, mediaType, err := uploader.UploadImage(callCtx, in.Data)
	if err != nil {
		var rejected *feishu.ImageRejectedError
		if errors.As(err, &rejected) {
			// A refusal botd decided. Different bytes may succeed; the same bytes
			// never will, so this is not retryable.
			return AgentImageUploadResult{}, notify.NewAPIError(400, "invalid_image", rejected.Reason, false)
		}
		s.logAgentCardFailure("image upload", target.label, err)
		return AgentImageUploadResult{}, agentCardCallError(err, "Feishu image upload failed")
	}

	stored := target.remember(operationID, uploadedImage{
		imageKey: imageKey, mediaType: mediaType, fingerprint: fingerprint,
	})
	return AgentImageUploadResult{ImageKey: stored.imageKey, MediaType: stored.mediaType}, nil
}

func (s *Service) resolveImageUploadTarget(
	provider, deliveryID, conversationID, operationID string, now time.Time,
) (imageUploadTarget, *notify.APIError) {
	if deliveryID != "" {
		delivery, ok := s.lookupAndPinAgentDelivery(provider, deliveryID, now, now.Add(s.agentBroker.ttl))
		if !ok {
			return imageUploadTarget{}, notify.NewAPIError(404, "unknown_delivery", "unknown delivery", false)
		}
		return imageUploadTarget{
			appAlias: delivery.appAlias,
			label:    deliveryID,
			lookup:   delivery.lookupUploadedImage,
			remember: delivery.rememberUploadedImage,
		}, nil
	}

	route, appAlias, ok := s.agentBroker.lookupAndPinConversationImage(
		conversationID,
		provider,
		operationID,
		func(resolvedApp string) bool { return s.appAllowed(provider, resolvedApp) },
		now,
		s.cfg.SendTimeout,
	)
	if !ok {
		return imageUploadTarget{}, unknownConversationError()
	}
	return imageUploadTarget{
		appAlias: appAlias,
		label:    conversationID,
		lookup:   route.lookupUploadedImage,
		remember: route.rememberUploadedImage,
	}, nil
}

// lookupUploadedImage returns a prior upload for this operation id, or nil when
// the caller should perform the upload. The delivery lock is deliberately not
// held across the Feishu call: an upload is slow, and a response Start on the
// same delivery must not queue behind it.
func (d *agentDelivery) lookupUploadedImage(operationID, fingerprint string) (*uploadedImage, *notify.APIError) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if existing, ok := d.uploadedImages[operationID]; ok {
		if existing.fingerprint != fingerprint {
			return nil, notify.NewAPIError(409, "operation_conflict", "operation id reused with different content", false)
		}
		return &existing, nil
	}
	if len(d.uploadedImages) >= maxDeliveryImages {
		return nil, notify.NewAPIError(429, "too_many_images", "this delivery has uploaded too many images", false)
	}
	return nil, nil
}

// rememberUploadedImage records an upload and returns the entry that won. A
// concurrent replay of the same operation id can mint a second key on Feishu's
// side; keeping the first one means every caller is handed the same key.
func (d *agentDelivery) rememberUploadedImage(operationID string, image uploadedImage) uploadedImage {
	d.mu.Lock()
	defer d.mu.Unlock()
	if existing, ok := d.uploadedImages[operationID]; ok {
		return existing
	}
	if d.uploadedImages == nil {
		d.uploadedImages = make(map[string]uploadedImage, 1)
	}
	d.uploadedImages[operationID] = image
	return image
}

func (r *agentConversationRoute) lookupUploadedImage(operationID, fingerprint string) (*uploadedImage, *notify.APIError) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if existing, ok := r.uploadedImages[operationID]; ok {
		if existing.fingerprint != fingerprint {
			return nil, notify.NewAPIError(409, "operation_conflict", "operation id reused with different content", false)
		}
		return &existing, nil
	}
	if len(r.uploadedImages) >= maxConversationImages {
		return nil, notify.NewAPIError(429, "too_many_images", "this conversation has uploaded too many images", false)
	}
	return nil, nil
}

func (r *agentConversationRoute) rememberUploadedImage(operationID string, image uploadedImage) uploadedImage {
	r.mu.Lock()
	defer r.mu.Unlock()
	if existing, ok := r.uploadedImages[operationID]; ok {
		return existing
	}
	if r.uploadedImages == nil {
		r.uploadedImages = make(map[string]uploadedImage, 1)
	}
	r.uploadedImages[operationID] = image
	return image
}

func imageFingerprint(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
