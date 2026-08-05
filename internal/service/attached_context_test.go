package service

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"feishu-botd/internal/config"
	"feishu-botd/internal/dedupe"
	"feishu-botd/internal/feishu"
)

type attachedContextTestBackend struct {
	*fakeSender
	result   feishu.AttachedContext
	err      error
	calls    int
	requests []feishu.AttachedContextRequest
}

func (b *attachedContextTestBackend) LookupAttachedContext(_ context.Context, in feishu.AttachedContextRequest) (feishu.AttachedContext, error) {
	b.calls++
	b.requests = append(b.requests, in)
	return b.result, b.err
}

func newAttachedContextTestService(backend *attachedContextTestBackend) *Service {
	cfg := config.Config{
		AppID: "cli_test", AppSecret: "secret",
		Channels:    map[string]string{"ops": "oc_ops"},
		DedupeTTL:   time.Hour,
		SendTimeout: time.Second,
		AgentProviders: map[string]config.AgentProviderConfig{
			"nous":  {AllowAttachedContext: true},
			"other": {AllowAttachedContext: true},
		},
	}
	return NewService(cfg, backend, dedupe.NewMemoryStore(time.Hour), slog.Default())
}

func TestGetAgentAttachedContextRequiresExplicitProviderGrant(t *testing.T) {
	backend := &attachedContextTestBackend{fakeSender: &fakeSender{messageID: "om_answer"}}
	svc := newAttachedContextTestService(backend)
	svc.cfg.AgentProviders["nous"] = config.AgentProviderConfig{}
	seedAttachedContextDelivery(t, svc, "nous", "delivery_topic")

	if _, apiErr := svc.GetAgentAttachedContext(context.Background(), AgentAttachedContextInput{
		Provider: "nous", DeliveryID: "delivery_topic",
	}); apiErr == nil || apiErr.Code != "provider_scope_denied" {
		t.Fatalf("scope error = %v", apiErr)
	}
	if backend.calls != 0 {
		t.Fatalf("denied lookup reached Feishu backend: calls=%d", backend.calls)
	}
}

func seedAttachedContextDelivery(t *testing.T, svc *Service, provider, deliveryID string) {
	t.Helper()
	sub := mustSubscribeAgent(t, svc, AgentSubscribeOptions{
		Provider: provider, IncludeUnmatchedMessages: true,
	})
	t.Cleanup(sub.Close)
	mustDispatchAgentPrompt(t, svc, CommandInput{
		DeliveryID: deliveryID, Command: "看看这个问题", Prompt: "看看这个问题",
		ConversationID: "conv_topic", ChatAlias: "ops", SenderID: "ou_sender",
		Metadata: map[string]string{
			"chat_type": "topic_group", "message_id": "om_guide",
			"thread_id": "omt_topic", "create_time": "1754380800123",
		},
	})
	_ = receiveAgentEvent(t, sub)
}

func TestGetAgentAttachedContextUsesExactAuthorizedDeliveryLazily(t *testing.T) {
	backend := &attachedContextTestBackend{
		fakeSender: &fakeSender{messageID: "om_answer"},
		result: feishu.AttachedContext{
			Status: feishu.AttachedContextFound,
			Messages: []feishu.AttachedContextMessage{{
				AuthorLabel: "participant-1", AuthorType: "user", Text: "the crash report",
			}},
		},
	}
	svc := newAttachedContextTestService(backend)
	seedAttachedContextDelivery(t, svc, "nous", "delivery_topic")
	if backend.calls != 0 {
		t.Fatalf("dispatch eagerly fetched attached context: calls=%d", backend.calls)
	}

	got, apiErr := svc.GetAgentAttachedContext(context.Background(), AgentAttachedContextInput{
		Provider: "nous", DeliveryID: "delivery_topic",
	})
	if apiErr != nil {
		t.Fatalf("get attached context: %v", apiErr)
	}
	if got.Status != feishu.AttachedContextFound || len(got.Messages) != 1 || backend.calls != 1 {
		t.Fatalf("result=%#v calls=%d", got, backend.calls)
	}
	if len(backend.requests) != 1 || backend.requests[0] != (feishu.AttachedContextRequest{
		ThreadID: "omt_topic", TriggerMessageID: "om_guide", TriggerCreateTime: "1754380800123",
	}) {
		t.Fatalf("private lookup request = %#v", backend.requests)
	}
}

func TestGetAgentAttachedContextDoesNotCrossProviderOrDeliveryOwnership(t *testing.T) {
	backend := &attachedContextTestBackend{fakeSender: &fakeSender{messageID: "om_answer"}}
	svc := newAttachedContextTestService(backend)
	seedAttachedContextDelivery(t, svc, "nous", "delivery_topic")

	for _, input := range []AgentAttachedContextInput{
		{Provider: "other", DeliveryID: "delivery_topic"},
		{Provider: "nous", DeliveryID: "delivery_unknown"},
	} {
		if _, apiErr := svc.GetAgentAttachedContext(context.Background(), input); apiErr == nil || apiErr.Code != "unknown_delivery" {
			t.Fatalf("input=%#v error=%v, want unknown_delivery", input, apiErr)
		}
	}
	if backend.calls != 0 {
		t.Fatalf("unauthorized lookup reached Feishu backend: calls=%d", backend.calls)
	}
}

func TestGetAgentAttachedContextCapabilityExpiresWithoutFallback(t *testing.T) {
	backend := &attachedContextTestBackend{fakeSender: &fakeSender{messageID: "om_answer"}}
	svc := newAttachedContextTestService(backend)
	seedAttachedContextDelivery(t, svc, "nous", "delivery_topic")

	svc.agentBroker.mu.Lock()
	delivery := svc.agentBroker.deliveries["delivery_topic"]
	svc.agentBroker.mu.Unlock()
	if delivery == nil {
		t.Fatal("seeded delivery is missing")
	}
	delivery.mu.Lock()
	delivery.attachedContextExpiresAt = time.Now().Add(-time.Second)
	delivery.mu.Unlock()

	if _, apiErr := svc.GetAgentAttachedContext(context.Background(), AgentAttachedContextInput{
		Provider: "nous", DeliveryID: "delivery_topic",
	}); apiErr == nil || apiErr.Code != "unknown_delivery" {
		t.Fatalf("expired capability error = %v", apiErr)
	}
	if backend.calls != 0 {
		t.Fatalf("expired lookup reached Feishu backend: calls=%d", backend.calls)
	}
}

func TestGetAgentAttachedContextMapsBackendFailureToTypedUnreadable(t *testing.T) {
	backend := &attachedContextTestBackend{
		fakeSender: &fakeSender{messageID: "om_answer"},
		err:        context.DeadlineExceeded,
	}
	svc := newAttachedContextTestService(backend)
	seedAttachedContextDelivery(t, svc, "nous", "delivery_topic")

	got, apiErr := svc.GetAgentAttachedContext(context.Background(), AgentAttachedContextInput{
		Provider: "nous", DeliveryID: "delivery_topic",
	})
	if apiErr != nil {
		t.Fatalf("get attached context: %v", apiErr)
	}
	if got.Status != feishu.AttachedContextUnreadable || len(got.Issues) != 1 ||
		got.Issues[0].Code != feishu.AttachedContextIssueHistoryUnreadable {
		t.Fatalf("result = %#v", got)
	}
}
