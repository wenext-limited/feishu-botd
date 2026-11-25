package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"

	"feishu-botd/internal/config"
	"feishu-botd/internal/dedupe"
	"feishu-botd/internal/feishu"
	"feishu-botd/internal/notify"
)

// SendNotification validates, deduplicates, and sends a structured
// notification. It is the shared core for both POST /v1/notify and the gRPC
// NotificationService.SendNotification. Deduplication is always enabled, so the
// caller-supplied source + dedupe_key make the call idempotent.
func (s *Service) SendNotification(ctx context.Context, req notify.Request) (notify.Response, *notify.APIError) {
	if strings.TrimSpace(req.Target.UserEmail) != "" {
		// Never fall back to the default channel for a request addressed to a
		// person: that would post their message somewhere public.
		return notify.Response{}, notify.BadRequest("unsupported_target", "target.user_email is only supported by SendMessage")
	}
	req.Target.Channel = s.resolveChannel(req.Target.Channel, req.Source)
	if apiErr := req.Validate(s.cfg().Channels); apiErr != nil {
		return notify.Response{}, apiErr
	}
	if _, _, ok := s.cfg().ResolveChannel(req.Target.Channel); !ok {
		return notify.Response{}, notify.NewAPIError(404, "unknown_channel", "unknown channel", false)
	}
	// SendNotification remains the stable markdown notification contract. The
	// lower-level SendMessage path owns card delivery and reply-threading, even
	// if a caller includes an additive card_json or reply_to_message_id field.
	req.CardJSON = ""
	req.ReplyToMessageID = ""
	result, apiErr := s.deliver(ctx, req, true)
	if apiErr != nil {
		return notify.Response{}, apiErr
	}
	return notify.Response{
		Status:    "sent",
		Provider:  result.Provider,
		DedupeKey: req.DedupeKey,
		MessageID: result.MessageID,
		Duplicate: result.Duplicate,
	}, nil
}

// MessageInput is the lower-level, transport-agnostic input for SendMessage.
// v1 supports markdown and raw Feishu interactive-card JSON. Other content
// kinds are rejected by the transport adapter before reaching the service.
type MessageInput struct {
	Channel string
	// UserEmail targets one person by Feishu email instead of a channel. It is
	// gated by config.DirectMessages (recipient domain and source).
	UserEmail        string
	Source           string
	DedupeKey        string
	Title            string
	Markdown         string
	CardJSON         string
	ReplyToMessageID string
}

// SendMessage is the lower-level send path. Deduplication applies only when a
// dedupe key is supplied; otherwise every call sends.
func (s *Service) SendMessage(ctx context.Context, in MessageInput) (SendResult, *notify.APIError) {
	var target notify.Target
	if strings.TrimSpace(in.UserEmail) != "" {
		if strings.TrimSpace(in.Channel) != "" {
			return SendResult{}, notify.BadRequest("invalid_target", "exactly one of target.channel and target.user_email is allowed")
		}
		if strings.TrimSpace(in.ReplyToMessageID) != "" {
			return SendResult{}, notify.BadRequest("invalid_target", "a reply cannot target target.user_email")
		}
		email, apiErr := s.directMessageEmail(in.Source, in.UserEmail)
		if apiErr != nil {
			return SendResult{}, apiErr
		}
		target.UserEmail = email
	} else {
		channel := s.resolveChannel(in.Channel, in.Source)
		if channel == "" {
			return SendResult{}, notify.BadRequest("missing_channel", "target.channel is required")
		}
		if _, _, ok := s.cfg().ResolveChannel(channel); !ok {
			return SendResult{}, notify.NewAPIError(404, "unknown_channel", "unknown channel", false)
		}
		target.Channel = channel
	}
	markdown := strings.TrimSpace(in.Markdown)
	cardJSON := strings.TrimSpace(in.CardJSON)
	hasMarkdown := markdown != ""
	hasCard := cardJSON != ""
	switch {
	case hasMarkdown && hasCard:
		return SendResult{}, notify.BadRequest("invalid_content", "exactly one message content is required")
	case !hasMarkdown && !hasCard:
		return SendResult{}, notify.BadRequest("missing_content", "message content is required")
	case hasCard:
		if apiErr := validateCardJSON(cardJSON); apiErr != nil {
			return SendResult{}, apiErr
		}
	}
	// Bound the same fields SendNotification caps, since both paths write to the
	// shared dedupe store keyed by source + dedupe_key.
	if len(in.Markdown) > 8000 || len(in.Title) > 200 || len(in.Source) > 64 || len(in.DedupeKey) > 240 || len(in.CardJSON) > 64*1024 || len(in.ReplyToMessageID) > 160 {
		return SendResult{}, notify.BadRequest("field_too_large", "one or more fields are too large")
	}

	req := notify.Request{
		Source:           in.Source,
		DedupeKey:        in.DedupeKey,
		Title:            in.Title,
		Markdown:         in.Markdown,
		CardJSON:         in.CardJSON,
		Target:           target,
		ReplyToMessageID: in.ReplyToMessageID,
	}
	return s.deliver(ctx, req, strings.TrimSpace(in.DedupeKey) != "")
}

// directMessageEmail validates a user_email target and checks it against the
// direct_messages gate. The domain is lowercased (domains are
// case-insensitive); the local part is kept as given.
func (s *Service) directMessageEmail(source, raw string) (string, *notify.APIError) {
	email := strings.TrimSpace(raw)
	at := strings.IndexByte(email, '@')
	if len(email) > 254 || at <= 0 || at == len(email)-1 || strings.Count(email, "@") != 1 ||
		strings.IndexFunc(email, func(r rune) bool { return r <= ' ' || r == 0x7f }) >= 0 {
		return "", notify.BadRequest("invalid_user_email", "target.user_email is not a valid email address")
	}
	email = email[:at+1] + strings.ToLower(email[at+1:])
	gate := s.cfg().DirectMessages
	if !gate.Enabled() || !gate.AllowsSource(source) || !gate.AllowsEmail(email) {
		return "", notify.NewAPIError(403, "direct_message_denied", "direct messages to this address are not allowed for this source", false)
	}
	return email, nil
}

func (s *Service) resolveChannel(channel, source string) string {
	channel = strings.TrimSpace(channel)
	if channel != "" {
		return channel
	}
	if svc, ok := s.cfg().Services[strings.TrimSpace(source)]; ok {
		return svc.DefaultChannel
	}
	return s.cfg().DefaultChannel
}

func validateCardJSON(cardJSON string) *notify.APIError {
	var obj map[string]json.RawMessage
	if err := json.Unmarshal([]byte(cardJSON), &obj); err != nil || len(obj) == 0 {
		return notify.BadRequest("invalid_card_json", "card_json must be a JSON object")
	}
	return nil
}

// deliver runs the shared dedupe -> send -> commit/abort flow. When
// dedupeEnabled is false (SendMessage without a key) the store is never
// touched. The channel alias in req.Target is assumed to already be validated
// against config by the caller.
func (s *Service) deliver(ctx context.Context, req notify.Request, dedupeEnabled bool) (SendResult, *notify.APIError) {
	var appAlias, chatID string
	if email := req.Target.UserEmail; email != "" {
		// SendMessage already checked the gate; the email is the receive id.
		appAlias, chatID = s.cfg().DirectMessages.AppAlias(), email
	} else {
		var ok bool
		appAlias, chatID, ok = s.cfg().ResolveChannel(req.Target.Channel)
		if !ok {
			return SendResult{}, notify.NewAPIError(404, "unknown_channel", "unknown channel", false)
		}
	}
	backend, ok := s.backendForApp(appAlias)
	if !ok {
		return SendResult{}, notify.NewAPIError(502, "feishu_unavailable", "Feishu sender is unavailable", true)
	}
	dedupeSource := appScopedDedupeSource(appAlias, req.Source)
	if dedupeEnabled {
		fingerprint := req.Fingerprint()
		reserved := s.store.Reserve(dedupeSource, req.DedupeKey, fingerprint)
		switch {
		case reserved.Conflict:
			return SendResult{}, notify.NewAPIError(409, "dedupe_conflict", "dedupe key reused with different content", false)
		case reserved.InFlight:
			return SendResult{}, notify.NewAPIError(409, "dedupe_in_flight", "dedupe key is already being sent", true)
		case reserved.Duplicate:
			return SendResult{Provider: reserved.Result.Provider, MessageID: reserved.Result.MessageID, Duplicate: true}, nil
		}
	}

	sendCtx, cancel := context.WithTimeout(ctx, s.cfg().SendTimeout)
	defer cancel()

	messageID, err := backend.sender.Send(sendCtx, chatID, req)
	if err != nil {
		if dedupeEnabled {
			s.store.Abort(dedupeSource, req.DedupeKey)
		}
		s.logFeishuFailure("notify", "notification", notificationLogIdentity(req), err)
		var sendErr *feishu.MessageSendError
		if req.Target.UserEmail != "" && errors.As(err, &sendErr) && sendErr.Class == "api_rejected" && !sendErr.Retryable {
			// Feishu refused this recipient (unknown email, outside the app's
			// availability, …): retrying the same request cannot succeed.
			return SendResult{}, notify.NewAPIError(422, "direct_message_rejected", "Feishu rejected the direct message recipient", false)
		}
		return SendResult{}, notify.NewAPIError(502, "feishu_unavailable", "Feishu send failed", true)
	}

	result := dedupe.Result{Provider: Provider, MessageID: messageID}
	if dedupeEnabled {
		s.store.Commit(dedupeSource, req.DedupeKey, result)
	}
	s.logger.Info("send ok",
		"correlation", opaqueLogCorrelationID("notification", notificationLogIdentity(req)),
		"severity", req.Severity,
	)
	return SendResult{Provider: result.Provider, MessageID: result.MessageID, Duplicate: false}, nil
}

func appScopedDedupeSource(appAlias, source string) string {
	if effectiveAppAlias(appAlias) == config.DefaultAppAlias {
		return source
	}
	sum := sha256.Sum256([]byte(
		"feishu-botd/app-dedupe/v2\x00" + effectiveAppAlias(appAlias) + "\x00" + source,
	))
	// The fixed prefix plus a full hex digest is longer than the maximum
	// caller-controlled source (64 bytes), so a default-app caller cannot forge
	// a named app's internal store namespace.
	return "feishu-botd/internal-app-dedupe/v2:" + hex.EncodeToString(sum[:])
}

func notificationLogIdentity(req notify.Request) string {
	return req.Source + "\x00" + req.SourceEventID + "\x00" + req.DedupeKey
}
