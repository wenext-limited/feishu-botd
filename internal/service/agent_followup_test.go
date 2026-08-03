package service

import (
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"feishu-botd/internal/config"
	"feishu-botd/internal/dedupe"
	"feishu-botd/internal/feishu"
	"feishu-botd/internal/notify"
)

type followUpSend struct {
	chatID  string
	request notify.Request
}

// fakeFollowUpBackend reuses the CardKit double and records the ordinary
// message path a follow-up takes. sendErrs is consumed one entry per call so a
// test can fail the first attempt and let the retry through.
type fakeFollowUpBackend struct {
	fakeAgentBackend

	mu       sync.Mutex
	sends    []followUpSend
	sendErrs []error
}

func newFakeFollowUpBackend(sendErrs ...error) *fakeFollowUpBackend {
	return &fakeFollowUpBackend{fakeAgentBackend: *newFakeAgentBackend(), sendErrs: sendErrs}
}

func (f *fakeFollowUpBackend) Send(_ context.Context, chatID string, req notify.Request) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sends = append(f.sends, followUpSend{chatID: chatID, request: req})
	if len(f.sendErrs) > 0 {
		err := f.sendErrs[0]
		f.sendErrs = f.sendErrs[1:]
		if err != nil {
			return "", err
		}
	}
	return "om_follow_up", nil
}

func (f *fakeFollowUpBackend) snapshot() []followUpSend {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]followUpSend(nil), f.sends...)
}

func newFollowUpTestService(backend *fakeFollowUpBackend) *Service {
	cfg := config.Config{
		AppID:       "cli_test",
		AppSecret:   "secret",
		Channels:    map[string]string{"ops": "oc_test", "ci": "oc_ci"},
		DedupeTTL:   time.Hour,
		SendTimeout: time.Second,
	}
	return NewService(cfg, backend, dedupe.NewMemoryStore(time.Hour), slog.Default())
}

func groupFollowUpPrompt() CommandInput {
	return CommandInput{
		DeliveryID:     "evt_follow_up",
		Command:        "ask",
		Text:           "what changed?",
		Prompt:         "ask what changed?",
		ConversationID: "conv_follow_up",
		ChatAlias:      "ops",
		SenderID:       "ou_sender",
		ChatID:         "oc_ingress_route",
		Metadata:       map[string]string{"chat_type": "group", "message_id": "om_inbound"},
	}
}

func seedFollowUpConversation(t *testing.T, svc *Service, provider string, in CommandInput) *AgentSubscription {
	t.Helper()
	sub := mustSubscribeAgent(t, svc, AgentSubscribeOptions{Provider: provider, IncludeUnmatchedMessages: true})
	mustDispatchAgentPrompt(t, svc, in)
	_ = receiveAgentEvent(t, sub)
	return sub
}

func sendFollowUp(t *testing.T, svc *Service, provider, conversationID, operationID, markdown string) (AgentFollowUpReceipt, *notify.APIError) {
	t.Helper()
	return svc.SendAgentFollowUp(context.Background(), SendAgentFollowUpInput{
		Provider:       provider,
		ConversationID: conversationID,
		OperationID:    operationID,
		Markdown:       markdown,
		Summary:        "Research finished",
	})
}

func followUpRoute(t *testing.T, svc *Service, conversationID string) *agentConversationRoute {
	t.Helper()
	route := svc.agentBroker.lookupConversation(conversationID, time.Now())
	if route == nil {
		t.Fatalf("conversation %q has no recorded route", conversationID)
	}
	return route
}

func TestAgentFollowUpDeliversToTheRecordedConversation(t *testing.T) {
	backend := newFakeFollowUpBackend()
	svc := newFollowUpTestService(backend)
	seedFollowUpConversation(t, svc, "agent", groupFollowUpPrompt())

	receipt, apiErr := sendFollowUp(t, svc, "agent", "conv_follow_up", "op-1", "Research finished.")
	if apiErr != nil {
		t.Fatalf("send follow-up: %v", apiErr)
	}
	if !strings.HasPrefix(receipt.FollowUpID, "fup_") || receipt.Duplicate {
		t.Fatalf("follow-up receipt = %#v", receipt)
	}

	sends := backend.snapshot()
	if len(sends) != 1 {
		t.Fatalf("send count = %d, want 1", len(sends))
	}
	if sends[0].chatID != "oc_test" {
		t.Fatalf("follow-up chat = %q, want the configured alias route", sends[0].chatID)
	}
	request := sends[0].request
	if request.Markdown != "Research finished." || request.Title != "Research finished" {
		t.Fatalf("follow-up content = %#v", request)
	}
	// A flat chat gets a fresh top-level message, not a reply threaded under a
	// prompt the user scrolled past hours ago.
	if request.ReplyToMessageID != "" {
		t.Fatalf("flat-chat follow-up replied to %q, want a top-level message", request.ReplyToMessageID)
	}
	if request.Source != agentFollowUpSource || request.DedupeKey != receipt.FollowUpID {
		t.Fatalf("follow-up message uuid seed = %q/%q", request.Source, request.DedupeKey)
	}
}

func TestAgentFollowUpThreadsIntoAThreadScopedConversation(t *testing.T) {
	backend := newFakeFollowUpBackend()
	svc := newFollowUpTestService(backend)
	prompt := groupFollowUpPrompt()
	prompt.Metadata = map[string]string{
		"chat_type": "group", "message_id": "om_inbound",
		"thread_id": "omt_thread", "root_id": "om_thread_root",
	}
	seedFollowUpConversation(t, svc, "agent", prompt)

	if _, apiErr := sendFollowUp(t, svc, "agent", "conv_follow_up", "op-1", "Still working."); apiErr != nil {
		t.Fatalf("send follow-up: %v", apiErr)
	}
	sends := backend.snapshot()
	if len(sends) != 1 || sends[0].request.ReplyToMessageID != "om_thread_root" {
		t.Fatalf("thread-scoped follow-up = %#v", sends)
	}
}

func TestAgentFollowUpUsesThePrivateDirectMessageRoute(t *testing.T) {
	backend := newFakeFollowUpBackend()
	svc := newFollowUpTestService(backend)
	prompt := groupFollowUpPrompt()
	prompt.ChatAlias = "direct"
	prompt.ChatID = "oc_direct_route"
	prompt.Metadata = map[string]string{"chat_type": "p2p"}
	seedFollowUpConversation(t, svc, "agent", prompt)

	receipt, apiErr := sendFollowUp(t, svc, "agent", "conv_follow_up", "op-1", "Done.")
	if apiErr != nil {
		t.Fatalf("send follow-up: %v", apiErr)
	}
	sends := backend.snapshot()
	if len(sends) != 1 || sends[0].chatID != "oc_direct_route" {
		t.Fatalf("direct follow-up route = %#v", sends)
	}
	visible, err := json.Marshal(receipt)
	if err != nil {
		t.Fatalf("encode follow-up receipt: %v", err)
	}
	if strings.Contains(string(visible), "oc_direct_route") {
		t.Fatalf("follow-up receipt leaked the private route: %s", visible)
	}
}

func TestAgentFollowUpUsesThePrivateUnconfiguredGroupRoute(t *testing.T) {
	backend := newFakeFollowUpBackend()
	svc := newFollowUpTestService(backend)
	svc.cfg.Commands.AllowUnconfiguredGroupChats = true
	prompt := groupFollowUpPrompt()
	prompt.DeliveryID = "evt_unconfigured_follow_up"
	prompt.ConversationID = "conv_unconfigured_follow_up"
	prompt.ChatAlias = "unconfigured-group-opaque"
	prompt.ChatID = "oc_private_unconfigured"
	prompt.UnconfiguredGroup = true
	seedFollowUpConversation(t, svc, "agent", prompt)

	if _, apiErr := sendFollowUp(t, svc, "agent", prompt.ConversationID, "op-1", "Done."); apiErr != nil {
		t.Fatalf("send unconfigured group follow-up: %v", apiErr)
	}
	sends := backend.snapshot()
	if len(sends) != 1 || sends[0].chatID != prompt.ChatID {
		t.Fatalf("unconfigured group follow-up route = %#v", sends)
	}
}

func TestAgentFollowUpRejectsUnknownConversation(t *testing.T) {
	backend := newFakeFollowUpBackend()
	svc := newFollowUpTestService(backend)
	seedFollowUpConversation(t, svc, "agent", groupFollowUpPrompt())

	_, apiErr := sendFollowUp(t, svc, "agent", "conv_never_seen", "op-1", "Hello.")
	if apiErr == nil || apiErr.Code != "unknown_conversation" || apiErr.Status != 404 {
		t.Fatalf("unknown conversation error = %v", apiErr)
	}
	if len(backend.snapshot()) != 0 {
		t.Fatalf("unknown conversation reached Feishu: %#v", backend.snapshot())
	}
}

func TestAgentFollowUpRejectsProviderThatNeverReceivedTheConversation(t *testing.T) {
	backend := newFakeFollowUpBackend()
	svc := newFollowUpTestService(backend)
	// The exact-command subscriber wins the message, so the fallback provider
	// never receives it and never earns a follow-up grant.
	mustSubscribeAgent(t, svc, AgentSubscribeOptions{Provider: "owner", Commands: []string{"ask"}})
	bystander := mustSubscribeAgent(t, svc, AgentSubscribeOptions{Provider: "bystander", IncludeUnmatchedMessages: true})
	mustDispatchAgentPrompt(t, svc, groupFollowUpPrompt())
	assertNoAgentEvent(t, bystander)

	_, apiErr := sendFollowUp(t, svc, "bystander", "conv_follow_up", "op-1", "Hello.")
	if apiErr == nil || apiErr.Code != "unknown_conversation" {
		t.Fatalf("unscoped provider error = %v", apiErr)
	}
	if len(backend.snapshot()) != 0 {
		t.Fatalf("unscoped provider reached Feishu: %#v", backend.snapshot())
	}
	if _, apiErr := sendFollowUp(t, svc, "owner", "conv_follow_up", "op-1", "Hello."); apiErr != nil {
		t.Fatalf("owner follow-up: %v", apiErr)
	}
}

func TestAgentFollowUpRetryReturnsTheRecordedReceipt(t *testing.T) {
	backend := newFakeFollowUpBackend()
	svc := newFollowUpTestService(backend)
	seedFollowUpConversation(t, svc, "agent", groupFollowUpPrompt())

	first, apiErr := sendFollowUp(t, svc, "agent", "conv_follow_up", "op-1", "Research finished.")
	if apiErr != nil {
		t.Fatalf("send follow-up: %v", apiErr)
	}
	replay, apiErr := sendFollowUp(t, svc, "agent", "conv_follow_up", "op-1", "Research finished.")
	if apiErr != nil {
		t.Fatalf("retry follow-up: %v", apiErr)
	}
	if !replay.Duplicate || replay.FollowUpID != first.FollowUpID {
		t.Fatalf("retry receipt = %#v, want duplicate of %q", replay, first.FollowUpID)
	}
	if len(backend.snapshot()) != 1 {
		t.Fatalf("retry sent again: %#v", backend.snapshot())
	}
}

func TestAgentFollowUpRejectsOperationIDReuseWithDifferentContent(t *testing.T) {
	backend := newFakeFollowUpBackend()
	svc := newFollowUpTestService(backend)
	seedFollowUpConversation(t, svc, "agent", groupFollowUpPrompt())

	if _, apiErr := sendFollowUp(t, svc, "agent", "conv_follow_up", "op-1", "First answer."); apiErr != nil {
		t.Fatalf("send follow-up: %v", apiErr)
	}
	_, apiErr := sendFollowUp(t, svc, "agent", "conv_follow_up", "op-1", "Different answer.")
	if apiErr == nil || apiErr.Code != "operation_conflict" || apiErr.Retryable {
		t.Fatalf("conflicting reuse error = %v", apiErr)
	}
	if len(backend.snapshot()) != 1 {
		t.Fatalf("conflicting reuse reached Feishu: %#v", backend.snapshot())
	}
}

func TestAgentFollowUpRejectsExpiredProviderGrant(t *testing.T) {
	backend := newFakeFollowUpBackend()
	svc := newFollowUpTestService(backend)
	seedFollowUpConversation(t, svc, "agent", groupFollowUpPrompt())

	route := followUpRoute(t, svc, "conv_follow_up")
	route.mu.Lock()
	route.providers["agent"] = time.Now().Add(-time.Minute)
	route.mu.Unlock()

	_, apiErr := sendFollowUp(t, svc, "agent", "conv_follow_up", "op-1", "Too late.")
	if apiErr == nil || apiErr.Code != "unknown_conversation" {
		t.Fatalf("expired grant error = %v", apiErr)
	}
	if len(backend.snapshot()) != 0 {
		t.Fatalf("expired grant reached Feishu: %#v", backend.snapshot())
	}
}

func TestAgentFollowUpDropsTheRouteAfterTTL(t *testing.T) {
	backend := newFakeFollowUpBackend()
	svc := newFollowUpTestService(backend)
	seedFollowUpConversation(t, svc, "agent", groupFollowUpPrompt())

	route := followUpRoute(t, svc, "conv_follow_up")
	route.mu.Lock()
	route.expiresAt = time.Now().Add(-time.Minute)
	route.mu.Unlock()

	if _, apiErr := sendFollowUp(t, svc, "agent", "conv_follow_up", "op-1", "Too late."); apiErr == nil || apiErr.Code != "unknown_conversation" {
		t.Fatalf("expired route error = %v", apiErr)
	}
	svc.agentBroker.mu.Lock()
	remaining := len(svc.agentBroker.conversations)
	svc.agentBroker.mu.Unlock()
	if remaining != 0 {
		t.Fatalf("expired conversation routes retained = %d", remaining)
	}
}

func TestAgentFollowUpAmbiguousFailureRetriesWithTheSameMessageIdentity(t *testing.T) {
	backend := newFakeFollowUpBackend(&feishu.MessageSendError{
		Operation: "message_create", Class: "transport", Retryable: true,
	})
	svc := newFollowUpTestService(backend)
	seedFollowUpConversation(t, svc, "agent", groupFollowUpPrompt())

	_, apiErr := sendFollowUp(t, svc, "agent", "conv_follow_up", "op-1", "Research finished.")
	if apiErr == nil || apiErr.Code != "feishu_unavailable" || !apiErr.Retryable {
		t.Fatalf("ambiguous failure error = %v", apiErr)
	}
	receipt, apiErr := sendFollowUp(t, svc, "agent", "conv_follow_up", "op-1", "Research finished.")
	if apiErr != nil {
		t.Fatalf("retry after ambiguous failure: %v", apiErr)
	}
	if receipt.Duplicate {
		t.Fatalf("retry receipt = %#v, want a first successful delivery", receipt)
	}

	sends := backend.snapshot()
	if len(sends) != 2 {
		t.Fatalf("attempt count = %d, want 2", len(sends))
	}
	// Reusing the follow-up handle reuses the Feishu message UUID, so an
	// attempt Feishu silently accepted cannot be posted twice.
	if sends[0].request.DedupeKey != sends[1].request.DedupeKey || sends[1].request.DedupeKey != receipt.FollowUpID {
		t.Fatalf("retry changed the message identity: %q then %q", sends[0].request.DedupeKey, sends[1].request.DedupeKey)
	}
}

func TestAgentFollowUpDefinitiveRejectionReleasesTheOperationID(t *testing.T) {
	backend := newFakeFollowUpBackend(&feishu.MessageSendError{
		Operation: "message_create", Class: "api_rejected", Code: 230001, HTTPStatus: 400,
	})
	svc := newFollowUpTestService(backend)
	seedFollowUpConversation(t, svc, "agent", groupFollowUpPrompt())

	_, apiErr := sendFollowUp(t, svc, "agent", "conv_follow_up", "op-1", "Malformed.")
	if apiErr == nil || apiErr.Code != "feishu_rejected" || apiErr.Retryable {
		t.Fatalf("definitive rejection error = %v", apiErr)
	}
	// Nothing was delivered and nothing is in doubt, so the same operation id
	// can carry corrected content.
	if _, apiErr := sendFollowUp(t, svc, "agent", "conv_follow_up", "op-1", "Corrected."); apiErr != nil {
		t.Fatalf("corrected follow-up: %v", apiErr)
	}
	sends := backend.snapshot()
	if len(sends) != 2 || sends[1].request.Markdown != "Corrected." {
		t.Fatalf("corrected attempt = %#v", sends)
	}
}

func TestAgentFollowUpTemporaryRejectionRetainsTheOperationIdentity(t *testing.T) {
	backend := newFakeFollowUpBackend(&feishu.MessageSendError{
		Operation: "message_create", Class: "api_rejected", HTTPStatus: 429, Retryable: true,
	})
	svc := newFollowUpTestService(backend)
	seedFollowUpConversation(t, svc, "agent", groupFollowUpPrompt())

	_, apiErr := sendFollowUp(t, svc, "agent", "conv_follow_up", "op-1", "Research finished.")
	if apiErr == nil || apiErr.Code != "feishu_unavailable" || !apiErr.Retryable {
		t.Fatalf("rate-limit error = %v", apiErr)
	}
	// An explicit rate limit is a definitive temporary rejection: it never
	// commits, so the retry window clock stays unstarted.
	route := followUpRoute(t, svc, "conv_follow_up")
	route.mu.Lock()
	op := route.operations["op-1"]
	ambiguous := op != nil && !op.ambiguousAt.IsZero()
	route.mu.Unlock()
	if op == nil || ambiguous {
		t.Fatalf("rate-limited operation state = %#v", op)
	}
	if _, apiErr := sendFollowUp(t, svc, "agent", "conv_follow_up", "op-1", "Research finished."); apiErr != nil {
		t.Fatalf("retry after rate limit: %v", apiErr)
	}
}

func TestAgentFollowUpStopsBeforeTheMessageUUIDWindowExpires(t *testing.T) {
	backend := newFakeFollowUpBackend(&feishu.MessageSendError{
		Operation: "message_create", Class: "transport", Retryable: true,
	})
	svc := newFollowUpTestService(backend)
	seedFollowUpConversation(t, svc, "agent", groupFollowUpPrompt())

	if _, apiErr := sendFollowUp(t, svc, "agent", "conv_follow_up", "op-1", "Research finished."); apiErr == nil {
		t.Fatal("expected the first attempt to fail")
	}
	route := followUpRoute(t, svc, "conv_follow_up")
	route.mu.Lock()
	route.operations["op-1"].ambiguousAt = time.Now().Add(-messageUUIDDedupeWindow)
	route.mu.Unlock()

	_, apiErr := sendFollowUp(t, svc, "agent", "conv_follow_up", "op-1", "Research finished.")
	if apiErr == nil || apiErr.Code != "send_retry_expired" || apiErr.Retryable {
		t.Fatalf("expired retry window error = %v", apiErr)
	}
	if len(backend.snapshot()) != 1 {
		t.Fatalf("expired retry window made another Feishu call: %#v", backend.snapshot())
	}
}

func TestAgentFollowUpRejectsInvalidRequests(t *testing.T) {
	backend := newFakeFollowUpBackend()
	svc := newFollowUpTestService(backend)
	seedFollowUpConversation(t, svc, "agent", groupFollowUpPrompt())

	cases := []struct {
		name  string
		input SendAgentFollowUpInput
		code  string
	}{
		{
			name:  "missing provider",
			input: SendAgentFollowUpInput{ConversationID: "conv_follow_up", OperationID: "op", Markdown: "hi"},
			code:  "missing_provider",
		},
		{
			name:  "missing conversation",
			input: SendAgentFollowUpInput{Provider: "agent", OperationID: "op", Markdown: "hi"},
			code:  "missing_conversation_id",
		},
		{
			name:  "missing operation id",
			input: SendAgentFollowUpInput{Provider: "agent", ConversationID: "conv_follow_up", Markdown: "hi"},
			code:  "missing_operation_id",
		},
		{
			name:  "missing markdown",
			input: SendAgentFollowUpInput{Provider: "agent", ConversationID: "conv_follow_up", OperationID: "op", Markdown: "   "},
			code:  "missing_markdown",
		},
		{
			name: "oversized markdown",
			input: SendAgentFollowUpInput{
				Provider: "agent", ConversationID: "conv_follow_up", OperationID: "op",
				Markdown: strings.Repeat("x", maxAgentCardBytes+1),
			},
			code: "field_too_large",
		},
		{
			name: "oversized summary",
			input: SendAgentFollowUpInput{
				Provider: "agent", ConversationID: "conv_follow_up", OperationID: "op",
				Markdown: "hi", Summary: strings.Repeat("x", 201),
			},
			code: "field_too_large",
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			_, apiErr := svc.SendAgentFollowUp(context.Background(), testCase.input)
			if apiErr == nil || apiErr.Code != testCase.code {
				t.Fatalf("error = %v, want %s", apiErr, testCase.code)
			}
		})
	}
	if len(backend.snapshot()) != 0 {
		t.Fatalf("invalid follow-up reached Feishu: %#v", backend.snapshot())
	}
}

func TestAgentFollowUpConcurrentAttemptOnOneOperationIsRejected(t *testing.T) {
	backend := newFakeFollowUpBackend()
	svc := newFollowUpTestService(backend)
	seedFollowUpConversation(t, svc, "agent", groupFollowUpPrompt())

	route := followUpRoute(t, svc, "conv_follow_up")
	route.mu.Lock()
	route.operations["op-1"] = &agentFollowUpOperation{
		fingerprint: hashJSON(struct {
			Conversation string
			Markdown     string
			Summary      string
		}{"conv_follow_up", "Research finished.", "Research finished"}),
		followUpID: "fup_in_flight",
		pending:    true,
	}
	route.mu.Unlock()

	_, apiErr := sendFollowUp(t, svc, "agent", "conv_follow_up", "op-1", "Research finished.")
	if apiErr == nil || apiErr.Code != "operation_in_flight" || !apiErr.Retryable {
		t.Fatalf("in-flight error = %v", apiErr)
	}
	if len(backend.snapshot()) != 0 {
		t.Fatalf("in-flight duplicate reached Feishu: %#v", backend.snapshot())
	}
}

func TestAgentFollowUpRouteKeepsRawIdentifiersPrivate(t *testing.T) {
	backend := newFakeFollowUpBackend()
	svc := newFollowUpTestService(backend)
	sub := mustSubscribeAgent(t, svc, AgentSubscribeOptions{Provider: "agent", IncludeUnmatchedMessages: true})
	prompt := groupFollowUpPrompt()
	prompt.Metadata = map[string]string{
		"chat_type": "group", "message_id": "om_inbound", "root_id": "om_thread_root",
	}
	mustDispatchAgentPrompt(t, svc, prompt)

	event := receiveAgentEvent(t, sub)
	visible, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("encode agent event: %v", err)
	}
	for _, raw := range []string{"oc_ingress_route", "om_inbound", "om_thread_root"} {
		if strings.Contains(string(visible), raw) {
			t.Fatalf("agent event leaked %q: %s", raw, visible)
		}
	}
	if event.ConversationID != "conv_follow_up" {
		t.Fatalf("conversation identity = %q", event.ConversationID)
	}
}
