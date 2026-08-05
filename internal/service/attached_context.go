package service

import (
	"context"
	"errors"
	"strings"
	"time"

	"feishu-botd/internal/feishu"
	"feishu-botd/internal/notify"
)

// AgentAttachedContextInput addresses only an exact inbound delivery. The
// authenticated transport separately proves Provider matches its bearer.
type AgentAttachedContextInput struct {
	Provider   string
	DeliveryID string
}

// GetAgentAttachedContext lazily resolves the topic snapshot attached to an
// exact provider-owned inbound event. It never chooses a latest delivery or a
// conversation-level fallback.
func (s *Service) GetAgentAttachedContext(ctx context.Context, in AgentAttachedContextInput) (feishu.AttachedContext, *notify.APIError) {
	provider := strings.TrimSpace(in.Provider)
	deliveryID := strings.TrimSpace(in.DeliveryID)
	if provider == "" {
		return feishu.AttachedContext{}, notify.BadRequest("missing_provider", "provider is required")
	}
	if deliveryID == "" {
		return feishu.AttachedContext{}, notify.BadRequest("missing_delivery_id", "delivery_id is required")
	}
	if len(provider) > 64 || len(deliveryID) > 160 {
		return feishu.AttachedContext{}, notify.BadRequest("field_too_large", "one or more fields are too large")
	}
	if !s.cfg.ProviderAllowsAttachedContext(provider) {
		return feishu.AttachedContext{}, notify.NewAPIError(403, "provider_scope_denied", "provider capability is not allowed", false)
	}

	appAlias, request, ok := s.agentBroker.attachedContextRequest(
		provider,
		deliveryID,
		time.Now(),
		func(alias string) bool { return s.appAllowed(provider, alias) },
	)
	if !ok {
		return feishu.AttachedContext{}, notify.NewAPIError(404, "unknown_delivery", "unknown delivery", false)
	}
	backend, ok := s.backendForApp(appAlias)
	if !ok || backend.attachedContext == nil {
		return unreadableAgentAttachedContext(), nil
	}

	result, err := backend.attachedContext.LookupAttachedContext(ctx, request)
	if err != nil {
		s.logger.Warn("attached context lookup failed",
			"correlation", opaqueLogCorrelationID("attached-context", deliveryID),
			"error_class", attachedContextErrorClass(err),
		)
		return unreadableAgentAttachedContext(), nil
	}
	return result, nil
}

func (b *agentBroker) attachedContextRequest(
	provider, deliveryID string,
	now time.Time,
	appAllowed func(string) bool,
) (string, feishu.AttachedContextRequest, bool) {
	b.mu.Lock()
	b.pruneLocked(now)
	delivery, ok := b.deliveries[deliveryID]
	if !ok {
		b.mu.Unlock()
		return "", feishu.AttachedContextRequest{}, false
	}
	if _, received := delivery.allowedProviders[provider]; !received ||
		appAllowed != nil && !appAllowed(delivery.appAlias) {
		b.mu.Unlock()
		return "", feishu.AttachedContextRequest{}, false
	}
	b.mu.Unlock()

	delivery.mu.Lock()
	defer delivery.mu.Unlock()
	if delivery.attachedContextExpiresAt.IsZero() || now.After(delivery.attachedContextExpiresAt) {
		return "", feishu.AttachedContextRequest{}, false
	}
	request := feishu.AttachedContextRequest{
		ThreadID:          strings.TrimSpace(delivery.input.Metadata["thread_id"]),
		TriggerMessageID:  strings.TrimSpace(delivery.input.Metadata["message_id"]),
		TriggerCreateTime: strings.TrimSpace(delivery.input.Metadata["create_time"]),
	}
	return delivery.appAlias, request, true
}

func unreadableAgentAttachedContext() feishu.AttachedContext {
	return feishu.AttachedContext{
		Status: feishu.AttachedContextUnreadable,
		Issues: []feishu.AttachedContextIssue{{
			Code: feishu.AttachedContextIssueHistoryUnreadable, Count: 1,
		}},
	}
}

func attachedContextErrorClass(err error) string {
	switch {
	case errors.Is(err, context.Canceled):
		return "context_canceled"
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline_exceeded"
	default:
		return "lookup_error"
	}
}
