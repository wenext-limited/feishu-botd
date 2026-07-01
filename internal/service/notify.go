package service

import (
	"context"
	"encoding/json"
	"strings"

	"feishu-botd/internal/dedupe"
	"feishu-botd/internal/notify"
)

// SendNotification validates, deduplicates, and sends a structured
// notification. It is the shared core for both POST /v1/notify and the gRPC
// NotificationService.SendNotification. Deduplication is always enabled, so the
// caller-supplied source + dedupe_key make the call idempotent.
func (s *Service) SendNotification(ctx context.Context, req notify.Request) (notify.Response, *notify.APIError) {
	req.Target.Channel = s.resolveChannel(req.Target.Channel, req.Source)
	if apiErr := req.Validate(s.cfg.Channels); apiErr != nil {
		return notify.Response{}, apiErr
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
	Channel          string
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
	channel := s.resolveChannel(in.Channel, in.Source)
	if channel == "" {
		return SendResult{}, notify.BadRequest("missing_channel", "target.channel is required")
	}
	if _, ok := s.cfg.Channels[channel]; !ok {
		return SendResult{}, notify.NewAPIError(404, "unknown_channel", "unknown channel", false)
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
		Target:           notify.Target{Channel: channel},
		ReplyToMessageID: in.ReplyToMessageID,
	}
	return s.deliver(ctx, req, strings.TrimSpace(in.DedupeKey) != "")
}

func (s *Service) resolveChannel(channel, source string) string {
	channel = strings.TrimSpace(channel)
	if channel != "" {
		return channel
	}
	if svc, ok := s.cfg.Services[strings.TrimSpace(source)]; ok {
		return svc.DefaultChannel
	}
	return s.cfg.DefaultChannel
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
	if dedupeEnabled {
		fingerprint := req.Fingerprint()
		reserved := s.store.Reserve(req.Source, req.DedupeKey, fingerprint)
		switch {
		case reserved.Conflict:
			return SendResult{}, notify.NewAPIError(409, "dedupe_conflict", "dedupe key reused with different content", false)
		case reserved.InFlight:
			return SendResult{}, notify.NewAPIError(409, "dedupe_in_flight", "dedupe key is already being sent", true)
		case reserved.Duplicate:
			return SendResult{Provider: reserved.Result.Provider, MessageID: reserved.Result.MessageID, Duplicate: true}, nil
		}
	}

	chatID := s.cfg.Channels[req.Target.Channel]
	sendCtx, cancel := context.WithTimeout(ctx, s.cfg.SendTimeout)
	defer cancel()

	messageID, err := s.sender.Send(sendCtx, chatID, req)
	if err != nil {
		if dedupeEnabled {
			s.store.Abort(req.Source, req.DedupeKey)
		}
		s.logger.Warn("send failed",
			"source", req.Source,
			"event", req.SourceEventID,
			"channel", req.Target.Channel,
			"error", s.redactor.redact(err),
		)
		return SendResult{}, notify.NewAPIError(502, "feishu_unavailable", "Feishu send failed", true)
	}

	result := dedupe.Result{Provider: Provider, MessageID: messageID}
	if dedupeEnabled {
		s.store.Commit(req.Source, req.DedupeKey, result)
	}
	s.logger.Info("send ok",
		"source", req.Source,
		"event", req.SourceEventID,
		"channel", req.Target.Channel,
		"severity", req.Severity,
	)
	return SendResult{Provider: result.Provider, MessageID: result.MessageID, Duplicate: false}, nil
}
