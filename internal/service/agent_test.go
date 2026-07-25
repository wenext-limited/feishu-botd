package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"feishu-botd/internal/config"
	"feishu-botd/internal/dedupe"
	"feishu-botd/internal/feishu"
	"feishu-botd/internal/notify"
)

type fakeAgentBackend struct {
	cardID    string
	messageID string

	createErr   error
	sendErr     error
	contentErr  error
	settingsErr error
	batchErr    error

	createdCards    []string
	sentCards       []feishu.CardSendRequest
	contentUpdates  []feishu.CardContentUpdate
	settingsUpdates []feishu.CardSettingsUpdate
	batchUpdates    []feishu.CardBatchUpdate
}

func newFakeAgentBackend() *fakeAgentBackend {
	return &fakeAgentBackend{cardID: "card_agent_1", messageID: "om_agent_1"}
}

func (f *fakeAgentBackend) Ready(context.Context) error { return nil }

func (f *fakeAgentBackend) Send(context.Context, string, notify.Request) (string, error) {
	return "om_legacy", nil
}

func (f *fakeAgentBackend) CreateCard(_ context.Context, cardJSON string) (string, error) {
	f.createdCards = append(f.createdCards, cardJSON)
	if f.createErr != nil {
		return "", f.createErr
	}
	return f.cardID, nil
}

func (f *fakeAgentBackend) SendCard(_ context.Context, req feishu.CardSendRequest) (string, error) {
	f.sentCards = append(f.sentCards, req)
	if f.sendErr != nil {
		return "", f.sendErr
	}
	return f.messageID, nil
}

func (f *fakeAgentBackend) UpdateContent(_ context.Context, req feishu.CardContentUpdate) error {
	f.contentUpdates = append(f.contentUpdates, req)
	return f.contentErr
}

func (f *fakeAgentBackend) UpdateSettings(_ context.Context, req feishu.CardSettingsUpdate) error {
	f.settingsUpdates = append(f.settingsUpdates, req)
	return f.settingsErr
}

func (f *fakeAgentBackend) BatchUpdate(_ context.Context, req feishu.CardBatchUpdate) error {
	f.batchUpdates = append(f.batchUpdates, req)
	return f.batchErr
}

func newAgentTestService(backend *fakeAgentBackend) *Service {
	cfg := config.Config{
		AppID:       "cli_test",
		AppSecret:   "secret",
		Channels:    map[string]string{"ops": "oc_test", "ci": "oc_ci"},
		DedupeTTL:   time.Hour,
		SendTimeout: time.Second,
	}
	return NewService(cfg, backend, dedupe.NewMemoryStore(time.Hour), slog.Default())
}

func mustSubscribeAgent(t *testing.T, svc *Service, options AgentSubscribeOptions) *AgentSubscription {
	t.Helper()
	sub, apiErr := svc.SubscribeAgentEvents(context.Background(), options)
	if apiErr != nil {
		t.Fatalf("subscribe agent events: %v", apiErr)
	}
	t.Cleanup(sub.Close)
	return sub
}

func mustDispatchAgentPrompt(t *testing.T, svc *Service, in CommandInput) {
	t.Helper()
	if _, apiErr := svc.DispatchCommand(context.Background(), in); apiErr != nil {
		t.Fatalf("dispatch agent prompt: %v", apiErr)
	}
}

func receiveAgentEvent(t *testing.T, sub *AgentSubscription) AgentEvent {
	t.Helper()
	select {
	case event := <-sub.C:
		return event
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for agent event")
		return AgentEvent{}
	}
}

func assertNoAgentEvent(t *testing.T, sub *AgentSubscription) {
	t.Helper()
	select {
	case event := <-sub.C:
		t.Fatalf("unexpected agent event: %#v", event)
	case <-time.After(40 * time.Millisecond):
	}
}

func seedAgentDelivery(t *testing.T, svc *Service, provider, deliveryID string, includeActions bool) (*AgentSubscription, AgentEvent) {
	t.Helper()
	sub := mustSubscribeAgent(t, svc, AgentSubscribeOptions{
		Provider:           provider,
		Commands:           []string{"ask"},
		IncludeCardActions: includeActions,
	})
	mustDispatchAgentPrompt(t, svc, CommandInput{
		DeliveryID:     deliveryID,
		Command:        "ask",
		Text:           "what changed?",
		Prompt:         "ask what changed?",
		ConversationID: "conv_test",
		ChatAlias:      "ops",
		SenderID:       "ou_sender",
		ChatID:         "oc_private_route",
		Metadata:       map[string]string{"message_id": "om_inbound"},
	})
	return sub, receiveAgentEvent(t, sub)
}

func startAgentResponse(t *testing.T, svc *Service, provider, deliveryID string, content AgentResponseContent) AgentResponseReceipt {
	t.Helper()
	receipt, apiErr := svc.StartAgentResponse(context.Background(), StartAgentResponseInput{
		Provider: provider, DeliveryID: deliveryID, OperationID: "start-1", Content: content,
	})
	if apiErr != nil {
		t.Fatalf("start agent response: %v", apiErr)
	}
	return receipt
}

func TestAgentUnmatchedMessageDeliversCompletePrompt(t *testing.T) {
	svc := newAgentTestService(newFakeAgentBackend())
	sub := mustSubscribeAgent(t, svc, AgentSubscribeOptions{
		Provider:                 "chat-agent",
		IncludeUnmatchedMessages: true,
	})
	prompt := "Explain this failure:\n first line\n  second   line"
	mustDispatchAgentPrompt(t, svc, CommandInput{
		DeliveryID: "evt_prompt", Command: "explain", Text: "this failure",
		Prompt: prompt, ConversationID: "conv_prompt", ChatAlias: "ops", SenderID: "ou_sender",
	})

	event := receiveAgentEvent(t, sub)
	if event.Message == nil {
		t.Fatalf("event has no message: %#v", event)
	}
	if event.Message.Text != prompt {
		t.Fatalf("full prompt = %q, want %q", event.Message.Text, prompt)
	}
	if event.Message.Command != "explain" || event.Message.CommandText != "this failure" {
		t.Fatalf("parsed command fields = %#v", event.Message)
	}
	if event.ConversationID != "conv_prompt" || event.ChatAlias != "ops" {
		t.Fatalf("event routing = %#v", event)
	}
}

func TestAgentExactCommandWinsOverFallback(t *testing.T) {
	svc := newAgentTestService(newFakeAgentBackend())
	exact := mustSubscribeAgent(t, svc, AgentSubscribeOptions{Provider: "exact", Commands: []string{"ask"}})
	fallback := mustSubscribeAgent(t, svc, AgentSubscribeOptions{Provider: "fallback", IncludeUnmatchedMessages: true})

	mustDispatchAgentPrompt(t, svc, CommandInput{
		DeliveryID: "evt_exact", Command: "ask", Text: "one", Prompt: "ask one", ChatAlias: "ops",
	})
	if event := receiveAgentEvent(t, exact); event.DeliveryID != "evt_exact" {
		t.Fatalf("exact event = %#v", event)
	}
	assertNoAgentEvent(t, fallback)

	mustDispatchAgentPrompt(t, svc, CommandInput{
		DeliveryID: "evt_fallback", Command: "explain", Text: "two", Prompt: "explain two", ChatAlias: "ops",
	})
	if event := receiveAgentEvent(t, fallback); event.DeliveryID != "evt_fallback" {
		t.Fatalf("fallback event = %#v", event)
	}
	assertNoAgentEvent(t, exact)
}

func TestAgentDeduplicatesMessageRetriesByMessageID(t *testing.T) {
	svc := newAgentTestService(newFakeAgentBackend())
	sub := mustSubscribeAgent(t, svc, AgentSubscribeOptions{Provider: "agent", IncludeUnmatchedMessages: true})
	base := CommandInput{
		Command: "ask", Prompt: "ask once", ChatAlias: "ops",
		Metadata: map[string]string{"message_id": "om_same_message"},
	}
	first := base
	first.DeliveryID = "evt_first_attempt"
	mustDispatchAgentPrompt(t, svc, first)
	_ = receiveAgentEvent(t, sub)
	second := base
	second.DeliveryID = "evt_retry_attempt"
	mustDispatchAgentPrompt(t, svc, second)
	assertNoAgentEvent(t, sub)
}

func TestAgentStartCreatesSchemaTwoCardAndSendsReply(t *testing.T) {
	backend := newFakeAgentBackend()
	svc := newAgentTestService(backend)
	_, _ = seedAgentDelivery(t, svc, "agent", "evt_start", false)

	receipt := startAgentResponse(t, svc, "agent", "evt_start", AgentResponseContent{
		Title: "Agent answer", Markdown: "Working…",
		Actions: []AgentResponseAction{{
			ActionID: "stop", Label: "Stop", PayloadJSON: `{"reason":"user"}`, Style: AgentResponseActionStyleDanger,
		}},
	})
	if receipt.ResponseID == "" || receipt.Revision != 1 || receipt.Phase != AgentResponsePhaseStreaming || receipt.Duplicate {
		t.Fatalf("start receipt = %#v", receipt)
	}
	if len(backend.createdCards) != 1 || len(backend.sentCards) != 1 {
		t.Fatalf("create/send counts = %d/%d, want 1/1", len(backend.createdCards), len(backend.sentCards))
	}

	var card struct {
		Schema string `json:"schema"`
		Config struct {
			StreamingMode bool `json:"streaming_mode"`
			UpdateMulti   bool `json:"update_multi"`
		} `json:"config"`
		Header struct {
			Title struct {
				Content string `json:"content"`
			} `json:"title"`
		} `json:"header"`
		Body struct {
			Elements []struct {
				Tag       string `json:"tag"`
				ElementID string `json:"element_id"`
				Content   string `json:"content"`
				Type      string `json:"type"`
				Text      struct {
					Content string `json:"content"`
				} `json:"text"`
				Behaviors []struct {
					Type  string `json:"type"`
					Value struct {
						ActionID    string `json:"action_id"`
						PayloadJSON string `json:"payload_json"`
					} `json:"value"`
				} `json:"behaviors"`
			} `json:"elements"`
		} `json:"body"`
	}
	if err := json.Unmarshal([]byte(backend.createdCards[0]), &card); err != nil {
		t.Fatalf("decode created card: %v", err)
	}
	if card.Schema != "2.0" || !card.Config.StreamingMode || !card.Config.UpdateMulti {
		t.Fatalf("card streaming schema/config = %#v", card)
	}
	if card.Header.Title.Content != "Agent answer" || len(card.Body.Elements) != 2 {
		t.Fatalf("card header/elements = %#v", card)
	}
	answer := card.Body.Elements[0]
	if answer.Tag != "markdown" || answer.ElementID != agentContentElementID || answer.Content != "Working…" {
		t.Fatalf("answer element = %#v", answer)
	}
	action := card.Body.Elements[1]
	if action.Tag != "button" || action.ElementID != "agent_action_1" || action.Type != "danger" || action.Text.Content != "Stop" || len(action.Behaviors) != 1 {
		t.Fatalf("action button = %#v", action)
	}
	behavior := action.Behaviors[0]
	if behavior.Type != "callback" || behavior.Value.ActionID != "stop" || behavior.Value.PayloadJSON != `{"reason":"user"}` {
		t.Fatalf("action callback behavior = %#v", behavior)
	}

	send := backend.sentCards[0]
	if send.ChatID != "" || send.ReplyToMessageID != "om_inbound" || send.CardID != backend.cardID {
		t.Fatalf("card send = %#v", send)
	}
	if send.UUID == "" || len(send.UUID) > 50 {
		t.Fatalf("message UUID = %q", send.UUID)
	}
}

func TestAgentStartRejectsUnknownActionStyle(t *testing.T) {
	backend := newFakeAgentBackend()
	svc := newAgentTestService(backend)
	_, _ = seedAgentDelivery(t, svc, "agent", "evt_invalid_style", false)

	_, apiErr := svc.StartAgentResponse(context.Background(), StartAgentResponseInput{
		Provider: "agent", DeliveryID: "evt_invalid_style", OperationID: "start-invalid-style",
		Content: AgentResponseContent{Markdown: "Working", Actions: []AgentResponseAction{{
			ActionID: "stop", Label: "Stop", Style: AgentResponseActionStyle(99),
		}}},
	})
	if apiErr == nil || apiErr.Code != "invalid_action_style" {
		t.Fatalf("unknown style error = %v", apiErr)
	}
	if len(backend.createdCards) != 0 || len(backend.sentCards) != 0 {
		t.Fatalf("invalid action reached CardKit: create=%d send=%d", len(backend.createdCards), len(backend.sentCards))
	}
}

func TestAgentDirectMessageWithoutReplyTargetUsesPrivateIngressRoute(t *testing.T) {
	backend := newFakeAgentBackend()
	svc := newAgentTestService(backend)
	sub := mustSubscribeAgent(t, svc, AgentSubscribeOptions{Provider: "agent", IncludeUnmatchedMessages: true})
	mustDispatchAgentPrompt(t, svc, CommandInput{
		DeliveryID: "evt_direct", Command: "hello", Prompt: "hello agent",
		ConversationID: "conv_direct", ChatAlias: "direct", ChatID: "oc_direct_route",
		Metadata: map[string]string{"chat_type": "p2p"},
	})
	event := receiveAgentEvent(t, sub)
	visible, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("encode direct agent event: %v", err)
	}
	if strings.Contains(string(visible), "oc_direct_route") {
		t.Fatalf("direct agent event leaked private route: %s", visible)
	}
	_ = startAgentResponse(t, svc, "agent", "evt_direct", AgentResponseContent{Markdown: "Hello."})
	if len(backend.sentCards) != 1 || backend.sentCards[0].ChatID != "oc_direct_route" || backend.sentCards[0].ReplyToMessageID != "" {
		t.Fatalf("direct response route = %#v", backend.sentCards)
	}
}

func TestAgentStartRetryReusesCardAndMessageUUID(t *testing.T) {
	backend := newFakeAgentBackend()
	backend.sendErr = errors.New("ambiguous send failure")
	svc := newAgentTestService(backend)
	_, _ = seedAgentDelivery(t, svc, "agent", "evt_retry", false)
	in := StartAgentResponseInput{
		Provider: "agent", DeliveryID: "evt_retry", OperationID: "start-retry",
		Content: AgentResponseContent{Title: "Answer", Markdown: "Thinking…"},
	}

	if _, apiErr := svc.StartAgentResponse(context.Background(), in); apiErr == nil || apiErr.Code != "feishu_unavailable" || !apiErr.Retryable {
		t.Fatalf("first start error = %v", apiErr)
	}
	if len(backend.createdCards) != 1 || len(backend.sentCards) != 1 {
		t.Fatalf("first create/send counts = %d/%d", len(backend.createdCards), len(backend.sentCards))
	}
	firstSend := backend.sentCards[0]

	backend.sendErr = nil
	receipt, apiErr := svc.StartAgentResponse(context.Background(), in)
	if apiErr != nil {
		t.Fatalf("retry start: %v", apiErr)
	}
	if len(backend.createdCards) != 1 || len(backend.sentCards) != 2 {
		t.Fatalf("retry create/send counts = %d/%d, want 1/2", len(backend.createdCards), len(backend.sentCards))
	}
	secondSend := backend.sentCards[1]
	if secondSend.CardID != firstSend.CardID || secondSend.UUID != firstSend.UUID {
		t.Fatalf("retry changed card identity: first=%#v second=%#v", firstSend, secondSend)
	}
	if receipt.Duplicate || receipt.Revision != 1 {
		t.Fatalf("retry receipt = %#v", receipt)
	}

	replay, apiErr := svc.StartAgentResponse(context.Background(), in)
	if apiErr != nil {
		t.Fatalf("completed start replay: %v", apiErr)
	}
	if !replay.Duplicate || replay.ResponseID != receipt.ResponseID || replay.Revision != receipt.Revision {
		t.Fatalf("completed start replay = %#v, first = %#v", replay, receipt)
	}
	if len(backend.createdCards) != 1 || len(backend.sentCards) != 2 {
		t.Fatalf("completed replay performed I/O: create/send=%d/%d", len(backend.createdCards), len(backend.sentCards))
	}
}

func TestAgentStartDefinitiveRejectionReleasesDeliveryClaim(t *testing.T) {
	backend := newFakeAgentBackend()
	backend.sendErr = &feishu.DynamicCardAPIError{Operation: "card send", HTTPStatus: 400, Code: 230001, Message: "rejected"}
	svc := newAgentTestService(backend)
	_, _ = seedAgentDelivery(t, svc, "agent", "evt_start_rejected", false)

	_, apiErr := svc.StartAgentResponse(context.Background(), StartAgentResponseInput{
		Provider: "agent", DeliveryID: "evt_start_rejected", OperationID: "start-rejected",
		Content: AgentResponseContent{Markdown: "First attempt"},
	})
	if apiErr == nil || apiErr.Code != "feishu_rejected" || apiErr.Retryable {
		t.Fatalf("definitive start error = %v", apiErr)
	}

	backend.sendErr = nil
	receipt, apiErr := svc.StartAgentResponse(context.Background(), StartAgentResponseInput{
		Provider: "agent", DeliveryID: "evt_start_rejected", OperationID: "start-corrected",
		Content: AgentResponseContent{Markdown: "Corrected attempt"},
	})
	if apiErr != nil {
		t.Fatalf("corrected start after rejection: %v", apiErr)
	}
	if receipt.Revision != 1 || receipt.Phase != AgentResponsePhaseStreaming {
		t.Fatalf("corrected start receipt = %#v", receipt)
	}
	if len(backend.createdCards) != 2 || len(backend.sentCards) != 2 {
		t.Fatalf("corrected start calls: create=%d send=%d", len(backend.createdCards), len(backend.sentCards))
	}
}

func TestAgentStartFeishuFrequencyLimitRetainsRetryIdentity(t *testing.T) {
	backend := newFakeAgentBackend()
	backend.sendErr = &feishu.DynamicCardAPIError{
		Operation: "card send", HTTPStatus: 400, Code: 230020,
		Message: "frequency limit",
	}
	svc := newAgentTestService(backend)
	_, _ = seedAgentDelivery(t, svc, "agent", "evt_start_rate_limited", false)
	in := StartAgentResponseInput{
		Provider: "agent", DeliveryID: "evt_start_rate_limited", OperationID: "start-rate-limited",
		Content: AgentResponseContent{Markdown: "Thinking"},
	}

	if _, apiErr := svc.StartAgentResponse(context.Background(), in); apiErr == nil || apiErr.Code != "feishu_unavailable" || !apiErr.Retryable {
		t.Fatalf("frequency-limit start error = %v", apiErr)
	}
	first := backend.sentCards[0]
	backend.sendErr = nil
	receipt, apiErr := svc.StartAgentResponse(context.Background(), in)
	if apiErr != nil {
		t.Fatalf("frequency-limit retry: %v", apiErr)
	}
	if receipt.Revision != 1 || len(backend.createdCards) != 1 || len(backend.sentCards) != 2 {
		t.Fatalf("frequency-limit retry result=%#v create=%d send=%d", receipt, len(backend.createdCards), len(backend.sentCards))
	}
	second := backend.sentCards[1]
	if second.CardID != first.CardID || second.UUID != first.UUID {
		t.Fatalf("frequency-limit retry changed identity: first=%#v second=%#v", first, second)
	}
}

func TestAgentStartAmbiguousThenRejectedFailsClosed(t *testing.T) {
	for _, firstErr := range []error{
		errors.New("response lost after send"),
		&feishu.DynamicCardAPIError{Operation: "card send", HTTPStatus: 503, Code: 300503, Message: "upstream unavailable"},
	} {
		t.Run(fmt.Sprintf("%T", firstErr), func(t *testing.T) {
			backend := newFakeAgentBackend()
			backend.sendErr = firstErr
			svc := newAgentTestService(backend)
			_, _ = seedAgentDelivery(t, svc, "agent", "evt_start_unknown", false)
			in := StartAgentResponseInput{
				Provider: "agent", DeliveryID: "evt_start_unknown", OperationID: "start-unknown",
				Content: AgentResponseContent{Markdown: "Thinking"},
			}

			if _, apiErr := svc.StartAgentResponse(context.Background(), in); apiErr == nil || apiErr.Code != "feishu_unavailable" || !apiErr.Retryable {
				t.Fatalf("ambiguous start error = %v", apiErr)
			}
			if len(backend.sentCards) != 1 {
				t.Fatalf("ambiguous send calls = %d", len(backend.sentCards))
			}
			first := backend.sentCards[0]
			backend.sendErr = &feishu.DynamicCardAPIError{
				Operation: "card send", HTTPStatus: 400, Code: 300400,
				Message: "duplicate or non-increasing operation",
			}
			if _, apiErr := svc.StartAgentResponse(context.Background(), in); apiErr == nil || apiErr.Code != "send_state_unknown" || apiErr.Retryable {
				t.Fatalf("ambiguous then rejected error = %v", apiErr)
			}
			if len(backend.sentCards) != 2 {
				t.Fatalf("reconciliation attempt calls = %d", len(backend.sentCards))
			}
			second := backend.sentCards[1]
			if second.CardID != first.CardID || second.UUID != first.UUID {
				t.Fatalf("ambiguous retry changed identity: first=%#v second=%#v", first, second)
			}

			backend.sendErr = nil
			if _, apiErr := svc.StartAgentResponse(context.Background(), in); apiErr == nil || apiErr.Code != "send_state_unknown" || apiErr.Retryable {
				t.Fatalf("closed retry error = %v", apiErr)
			}
			corrected := in
			corrected.OperationID = "start-replacement"
			corrected.Content.Markdown = "Replacement"
			if _, apiErr := svc.StartAgentResponse(context.Background(), corrected); apiErr == nil || apiErr.Code != "already_responded" || apiErr.Retryable {
				t.Fatalf("replacement error = %v", apiErr)
			}
			if len(backend.createdCards) != 1 || len(backend.sentCards) != 2 {
				t.Fatalf("fail-closed path performed extra I/O: create=%d send=%d", len(backend.createdCards), len(backend.sentCards))
			}

			svc.agentBroker.mu.Lock()
			delivery := svc.agentBroker.deliveries[in.DeliveryID]
			svc.agentBroker.mu.Unlock()
			if delivery == nil {
				t.Fatal("delivery claim was released")
			}
			delivery.mu.Lock()
			defer delivery.mu.Unlock()
			if delivery.operationID != in.OperationID || delivery.cardID != first.CardID || delivery.messageUUID != first.UUID || !delivery.sendRetryClosed {
				t.Fatalf("ambiguous delivery identity was not retained: %#v", delivery)
			}
		})
	}
}

func TestAgentStartAmbiguousRetryExpiresBeforeUUIDDedupeWindow(t *testing.T) {
	backend := newFakeAgentBackend()
	backend.sendErr = errors.New("response lost after send")
	svc := newAgentTestService(backend)
	_, _ = seedAgentDelivery(t, svc, "agent", "evt_start_expired", false)
	in := StartAgentResponseInput{
		Provider: "agent", DeliveryID: "evt_start_expired", OperationID: "start-expired",
		Content: AgentResponseContent{Markdown: "Thinking"},
	}

	if _, apiErr := svc.StartAgentResponse(context.Background(), in); apiErr == nil || apiErr.Code != "feishu_unavailable" {
		t.Fatalf("ambiguous start error = %v", apiErr)
	}
	first := backend.sentCards[0]
	svc.agentBroker.mu.Lock()
	delivery := svc.agentBroker.deliveries[in.DeliveryID]
	svc.agentBroker.mu.Unlock()
	if delivery == nil {
		t.Fatal("delivery missing after ambiguous send")
	}
	delivery.mu.Lock()
	delivery.sendAmbiguousAt = time.Now().Add(-messageUUIDDedupeWindow)
	delivery.mu.Unlock()

	backend.sendErr = nil
	if _, apiErr := svc.StartAgentResponse(context.Background(), in); apiErr == nil || apiErr.Code != "send_retry_expired" || apiErr.Retryable {
		t.Fatalf("expired retry error = %v", apiErr)
	}
	if len(backend.sentCards) != 1 {
		t.Fatalf("expired retry performed send: %#v", backend.sentCards)
	}
	delivery.mu.Lock()
	defer delivery.mu.Unlock()
	if delivery.operationID != in.OperationID || delivery.cardID != first.CardID || delivery.messageUUID != first.UUID || !delivery.sendRetryClosed {
		t.Fatalf("expired delivery identity was not retained: %#v", delivery)
	}
}

func TestAgentStartPinsDeliveryBeforeWaitingForDeliveryLock(t *testing.T) {
	backend := newFakeAgentBackend()
	svc := newAgentTestService(backend)
	_, _ = seedAgentDelivery(t, svc, "agent", "evt_start_pin", false)

	svc.agentBroker.mu.Lock()
	delivery := svc.agentBroker.deliveries["evt_start_pin"]
	svc.agentBroker.mu.Unlock()
	if delivery == nil {
		t.Fatal("seeded delivery is missing")
	}
	delivery.mu.Lock()
	locked := true
	defer func() {
		if locked {
			delivery.mu.Unlock()
		}
	}()
	oldExpiry := time.Now().Add(5 * time.Second)
	svc.agentBroker.mu.Lock()
	delivery.expiresAt = oldExpiry
	if delivery.messageDedupeKey == "" || strings.Contains(delivery.messageDedupeKey, "om_inbound") {
		svc.agentBroker.mu.Unlock()
		t.Fatal("delivery did not retain an opaque message dedupe key")
	}
	svc.agentBroker.seenMessages[delivery.messageDedupeKey] = oldExpiry
	svc.agentBroker.mu.Unlock()

	type startResult struct {
		receipt AgentResponseReceipt
		err     *notify.APIError
	}
	result := make(chan startResult, 1)
	go func() {
		receipt, apiErr := svc.StartAgentResponse(context.Background(), StartAgentResponseInput{
			Provider: "agent", DeliveryID: "evt_start_pin", OperationID: "start-pin",
			Content: AgentResponseContent{Markdown: "Pinned"},
		})
		result <- startResult{receipt: receipt, err: apiErr}
	}()

	deadline := time.Now().Add(time.Second)
	pinned := false
	pinnedExpiry := time.Time{}
	for time.Now().Before(deadline) {
		svc.agentBroker.mu.Lock()
		pinned = delivery.expiresAt.After(oldExpiry.Add(30*time.Minute)) &&
			svc.agentBroker.seenMessages[delivery.messageDedupeKey].Equal(delivery.expiresAt)
		pinnedExpiry = delivery.expiresAt
		svc.agentBroker.mu.Unlock()
		if pinned {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if !pinned {
		t.Fatal("Start did not atomically pin delivery and message dedupe state before waiting for the delivery lock")
	}
	svc.inboundRoutes.mu.Lock()
	routeExpiry := svc.inboundRoutes.deliveries["evt_start_pin"]
	messageRouteExpiry := svc.inboundRoutes.messages[delivery.messageDedupeKey]
	svc.inboundRoutes.mu.Unlock()
	if routeExpiry.Before(pinnedExpiry) || messageRouteExpiry.Before(pinnedExpiry) {
		t.Fatal("Start did not pin the central delivery/message route through the safe retry horizon")
	}
	svc.agentBroker.mu.Lock()
	svc.agentBroker.pruneLocked(oldExpiry.Add(time.Second))
	retained := svc.agentBroker.deliveries["evt_start_pin"] == delivery
	svc.agentBroker.mu.Unlock()
	if !retained {
		t.Fatal("prune removed a pinned delivery before Start could perform I/O")
	}

	delivery.mu.Unlock()
	locked = false
	select {
	case got := <-result:
		if got.err != nil || got.receipt.Revision != 1 {
			t.Fatalf("pinned start result = %#v, %v", got.receipt, got.err)
		}
	case <-time.After(time.Second):
		t.Fatal("pinned Start did not complete")
	}
	if len(backend.createdCards) != 1 || len(backend.sentCards) != 1 {
		t.Fatalf("pinned Start calls: create=%d send=%d", len(backend.createdCards), len(backend.sentCards))
	}
}

func TestAgentResponseOwnerIsIsolated(t *testing.T) {
	backend := newFakeAgentBackend()
	svc := newAgentTestService(backend)
	owner := mustSubscribeAgent(t, svc, AgentSubscribeOptions{Provider: "owner", Commands: []string{"ask"}})
	intruder := mustSubscribeAgent(t, svc, AgentSubscribeOptions{Provider: "intruder", Commands: []string{"ask"}})
	mustDispatchAgentPrompt(t, svc, CommandInput{
		DeliveryID: "evt_owner", Command: "ask", Prompt: "ask secret", ChatAlias: "ops",
	})
	_ = receiveAgentEvent(t, owner)
	_ = receiveAgentEvent(t, intruder)
	receipt := startAgentResponse(t, svc, "owner", "evt_owner", AgentResponseContent{Markdown: "answer"})

	if _, apiErr := svc.StartAgentResponse(context.Background(), StartAgentResponseInput{
		Provider: "intruder", DeliveryID: "evt_owner", OperationID: "steal", Content: AgentResponseContent{Markdown: "stolen"},
	}); apiErr == nil || apiErr.Code != "already_responded" {
		t.Fatalf("competing start error = %v", apiErr)
	}
	if _, apiErr := svc.StartAgentResponse(context.Background(), StartAgentResponseInput{
		Provider: "not-subscribed", DeliveryID: "evt_owner", OperationID: "steal-2", Content: AgentResponseContent{Markdown: "stolen"},
	}); apiErr == nil || apiErr.Code != "unknown_delivery" {
		t.Fatalf("unsubscribed start error = %v", apiErr)
	}
	if _, apiErr := svc.UpdateAgentResponse(context.Background(), UpdateAgentResponseInput{
		Provider: "intruder", ResponseID: receipt.ResponseID, OperationID: "steal-update", ExpectedRevision: 1, Markdown: "stolen",
	}); apiErr == nil || apiErr.Code != "unknown_response" {
		t.Fatalf("wrong-owner update error = %v", apiErr)
	}
}

func TestAgentUpdateRevisionReplayConflictAndGap(t *testing.T) {
	backend := newFakeAgentBackend()
	svc := newAgentTestService(backend)
	_, _ = seedAgentDelivery(t, svc, "agent", "evt_update", false)
	start := startAgentResponse(t, svc, "agent", "evt_update", AgentResponseContent{Markdown: "A"})
	in := UpdateAgentResponseInput{
		Provider: "agent", ResponseID: start.ResponseID, OperationID: "update-1", ExpectedRevision: 1, Markdown: "AB",
	}

	updated, apiErr := svc.UpdateAgentResponse(context.Background(), in)
	if apiErr != nil {
		t.Fatalf("update: %v", apiErr)
	}
	if updated.Revision != 2 || updated.Phase != AgentResponsePhaseStreaming || updated.Duplicate {
		t.Fatalf("update receipt = %#v", updated)
	}
	if len(backend.contentUpdates) != 1 || backend.contentUpdates[0].Content != "AB" || backend.contentUpdates[0].Sequence != 1 {
		t.Fatalf("content updates = %#v", backend.contentUpdates)
	}

	replay, apiErr := svc.UpdateAgentResponse(context.Background(), in)
	if apiErr != nil {
		t.Fatalf("update replay: %v", apiErr)
	}
	if !replay.Duplicate || replay.Revision != 2 || len(backend.contentUpdates) != 1 {
		t.Fatalf("update replay = %#v, calls=%d", replay, len(backend.contentUpdates))
	}

	changed := in
	changed.Markdown = "different"
	if _, apiErr := svc.UpdateAgentResponse(context.Background(), changed); apiErr == nil || apiErr.Code != "operation_conflict" || apiErr.Retryable {
		t.Fatalf("operation conflict = %v", apiErr)
	}
	if _, apiErr := svc.UpdateAgentResponse(context.Background(), UpdateAgentResponseInput{
		Provider: "agent", ResponseID: start.ResponseID, OperationID: "update-gap", ExpectedRevision: 4, Markdown: "gap",
	}); apiErr == nil || apiErr.Code != "revision_conflict" || !apiErr.Retryable {
		t.Fatalf("revision gap = %v", apiErr)
	}
	if len(backend.contentUpdates) != 1 {
		t.Fatalf("rejected updates reached CardKit: %#v", backend.contentUpdates)
	}

	next, apiErr := svc.UpdateAgentResponse(context.Background(), UpdateAgentResponseInput{
		Provider: "agent", ResponseID: start.ResponseID, OperationID: "update-2", ExpectedRevision: 2, Markdown: "ABC",
	})
	if apiErr != nil {
		t.Fatalf("next update: %v", apiErr)
	}
	if next.Revision != 3 || len(backend.contentUpdates) != 2 || backend.contentUpdates[1].Sequence != 2 {
		t.Fatalf("next update = %#v, CardKit=%#v", next, backend.contentUpdates)
	}
}

func TestAgentUpdateDefinitiveRejectionAllowsCorrectedOperation(t *testing.T) {
	backend := newFakeAgentBackend()
	svc := newAgentTestService(backend)
	_, _ = seedAgentDelivery(t, svc, "agent", "evt_update_rejected", false)
	start := startAgentResponse(t, svc, "agent", "evt_update_rejected", AgentResponseContent{Markdown: "A"})
	backend.contentErr = &feishu.DynamicCardAPIError{Operation: "card content update", HTTPStatus: 400, Code: 300100, Message: "rejected"}

	_, apiErr := svc.UpdateAgentResponse(context.Background(), UpdateAgentResponseInput{
		Provider: "agent", ResponseID: start.ResponseID, OperationID: "update-rejected",
		ExpectedRevision: 1, Markdown: "AB",
	})
	if apiErr == nil || apiErr.Code != "feishu_rejected" || apiErr.Retryable {
		t.Fatalf("definitive update error = %v", apiErr)
	}

	backend.contentErr = nil
	updated, apiErr := svc.UpdateAgentResponse(context.Background(), UpdateAgentResponseInput{
		Provider: "agent", ResponseID: start.ResponseID, OperationID: "update-corrected",
		ExpectedRevision: 1, Markdown: "AC",
	})
	if apiErr != nil {
		t.Fatalf("corrected update after rejection: %v", apiErr)
	}
	if updated.Revision != 2 || len(backend.contentUpdates) != 2 || backend.contentUpdates[1].Sequence != 1 {
		t.Fatalf("corrected update = %#v, calls=%#v", updated, backend.contentUpdates)
	}
}

func TestAgentUpdateTransientAPIRejectionRetainsRetryIdentity(t *testing.T) {
	backend := newFakeAgentBackend()
	svc := newAgentTestService(backend)
	_, _ = seedAgentDelivery(t, svc, "agent", "evt_update_throttled", false)
	start := startAgentResponse(t, svc, "agent", "evt_update_throttled", AgentResponseContent{Markdown: "A"})
	backend.contentErr = &feishu.DynamicCardAPIError{Operation: "card content update", HTTPStatus: 429, Code: 300999, Message: "rate limited"}
	in := UpdateAgentResponseInput{
		Provider: "agent", ResponseID: start.ResponseID, OperationID: "update-throttled",
		ExpectedRevision: 1, Markdown: "AB",
	}

	_, apiErr := svc.UpdateAgentResponse(context.Background(), in)
	if apiErr == nil || apiErr.Code != "feishu_unavailable" || !apiErr.Retryable {
		t.Fatalf("transient API error = %v", apiErr)
	}
	if len(backend.contentUpdates) != 1 {
		t.Fatalf("first throttled call count = %d", len(backend.contentUpdates))
	}
	first := backend.contentUpdates[0]

	backend.contentErr = nil
	updated, apiErr := svc.UpdateAgentResponse(context.Background(), in)
	if apiErr != nil {
		t.Fatalf("retry throttled operation: %v", apiErr)
	}
	if updated.Revision != 2 || len(backend.contentUpdates) != 2 {
		t.Fatalf("retry result = %#v, calls=%d", updated, len(backend.contentUpdates))
	}
	second := backend.contentUpdates[1]
	if second.UUID != first.UUID || second.Sequence != first.Sequence || second.Content != first.Content {
		t.Fatalf("transient retry changed identity: first=%#v second=%#v", first, second)
	}
}

func TestAgentUpdateInteractionConflictRetainsRetryIdentity(t *testing.T) {
	backend := newFakeAgentBackend()
	svc := newAgentTestService(backend)
	_, _ = seedAgentDelivery(t, svc, "agent", "evt_update_interacting", false)
	start := startAgentResponse(t, svc, "agent", "evt_update_interacting", AgentResponseContent{Markdown: "A"})
	backend.contentErr = &feishu.DynamicCardAPIError{
		Operation: "card content update", HTTPStatus: 400, Code: 200810,
		Message: "card is being interacted with",
	}
	in := UpdateAgentResponseInput{
		Provider: "agent", ResponseID: start.ResponseID, OperationID: "update-interacting",
		ExpectedRevision: 1, Markdown: "AB",
	}

	_, apiErr := svc.UpdateAgentResponse(context.Background(), in)
	if apiErr == nil || apiErr.Code != "feishu_unavailable" || !apiErr.Retryable {
		t.Fatalf("interaction conflict = %v", apiErr)
	}
	if len(backend.contentUpdates) != 1 {
		t.Fatalf("first interaction-conflict call count = %d", len(backend.contentUpdates))
	}
	first := backend.contentUpdates[0]
	response := svc.agentBroker.lookupResponse("agent", start.ResponseID)
	response.mu.Lock()
	response.lastMutationAt = time.Time{}
	response.mu.Unlock()

	backend.contentErr = nil
	updated, apiErr := svc.UpdateAgentResponse(context.Background(), in)
	if apiErr != nil {
		t.Fatalf("retry interaction-conflict update: %v", apiErr)
	}
	if updated.Revision != 2 || len(backend.contentUpdates) != 2 {
		t.Fatalf("retry result = %#v, calls=%d", updated, len(backend.contentUpdates))
	}
	second := backend.contentUpdates[1]
	if second.UUID != first.UUID || second.Sequence != first.Sequence || second.Content != first.Content {
		t.Fatalf("interaction retry changed identity: first=%#v second=%#v", first, second)
	}
}

func TestAgentUpdateAmbiguousThenRejectedBlocksReplacement(t *testing.T) {
	for _, firstErr := range []error{
		errors.New("content response lost"),
		&feishu.DynamicCardAPIError{Operation: "card content update", HTTPStatus: 502, Code: 300502, Message: "upstream failed"},
		&feishu.DynamicCardAPIError{Operation: "card content update", HTTPStatus: 400, Code: 300120, Message: "server internal error"},
	} {
		t.Run(fmt.Sprintf("%T", firstErr), func(t *testing.T) {
			backend := newFakeAgentBackend()
			svc := newAgentTestService(backend)
			_, _ = seedAgentDelivery(t, svc, "agent", "evt_update_unknown", false)
			start := startAgentResponse(t, svc, "agent", "evt_update_unknown", AgentResponseContent{Markdown: "A"})
			in := UpdateAgentResponseInput{
				Provider: "agent", ResponseID: start.ResponseID, OperationID: "update-unknown",
				ExpectedRevision: 1, Markdown: "AB",
			}
			backend.contentErr = firstErr
			if _, apiErr := svc.UpdateAgentResponse(context.Background(), in); apiErr == nil || apiErr.Code != "feishu_unavailable" || !apiErr.Retryable {
				t.Fatalf("ambiguous update error = %v", apiErr)
			}
			first := backend.contentUpdates[0]
			response := svc.agentBroker.lookupResponse("agent", start.ResponseID)
			response.mu.Lock()
			response.lastMutationAt = time.Time{}
			response.mu.Unlock()

			backend.contentErr = &feishu.DynamicCardAPIError{
				Operation: "card content update", HTTPStatus: 400, Code: 300400,
				Message: "duplicate or non-increasing operation",
			}
			if _, apiErr := svc.UpdateAgentResponse(context.Background(), in); apiErr == nil || apiErr.Code != "operation_state_unknown" || apiErr.Retryable {
				t.Fatalf("ambiguous then rejected update error = %v", apiErr)
			}
			if len(backend.contentUpdates) != 2 {
				t.Fatalf("update attempts = %d", len(backend.contentUpdates))
			}
			second := backend.contentUpdates[1]
			if second.UUID != first.UUID || second.Sequence != first.Sequence || second.Content != first.Content {
				t.Fatalf("ambiguous update changed identity: first=%#v second=%#v", first, second)
			}

			backend.contentErr = nil
			if _, apiErr := svc.UpdateAgentResponse(context.Background(), in); apiErr == nil || apiErr.Code != "operation_state_unknown" || apiErr.Retryable {
				t.Fatalf("closed update retry error = %v", apiErr)
			}
			if _, apiErr := svc.UpdateAgentResponse(context.Background(), UpdateAgentResponseInput{
				Provider: "agent", ResponseID: start.ResponseID, OperationID: "update-replacement",
				ExpectedRevision: 1, Markdown: "AC",
			}); apiErr == nil || apiErr.Code != "operation_state_unknown" || apiErr.Retryable {
				t.Fatalf("replacement update error = %v", apiErr)
			}
			if len(backend.contentUpdates) != 2 {
				t.Fatalf("fail-closed update performed extra I/O: %d", len(backend.contentUpdates))
			}
			response.mu.Lock()
			defer response.mu.Unlock()
			op := response.operations[in.OperationID]
			if op == nil || !op.contentAmbiguous || !op.contentClosed || op.contentUUID != first.UUID || op.contentSeq != first.Sequence || op.content != first.Content {
				t.Fatalf("ambiguous update identity was not retained: %#v", op)
			}
		})
	}
}

func TestAgentConcurrentUpdatesCommitExactlyOneRevision(t *testing.T) {
	backend := newFakeAgentBackend()
	svc := newAgentTestService(backend)
	_, _ = seedAgentDelivery(t, svc, "agent", "evt_concurrent", false)
	start := startAgentResponse(t, svc, "agent", "evt_concurrent", AgentResponseContent{Markdown: "A"})

	type result struct {
		receipt AgentResponseReceipt
		err     *notify.APIError
	}
	results := make(chan result, 2)
	for index, markdown := range []string{"AB", "AC"} {
		go func(index int, markdown string) {
			receipt, apiErr := svc.UpdateAgentResponse(context.Background(), UpdateAgentResponseInput{
				Provider: "agent", ResponseID: start.ResponseID,
				OperationID: "concurrent-" + string(rune('a'+index)), ExpectedRevision: 1, Markdown: markdown,
			})
			results <- result{receipt: receipt, err: apiErr}
		}(index, markdown)
	}
	successes := 0
	conflicts := 0
	for range 2 {
		result := <-results
		switch {
		case result.err == nil && result.receipt.Revision == 2:
			successes++
		case result.err != nil && result.err.Code == "revision_conflict":
			conflicts++
		default:
			t.Fatalf("unexpected concurrent update result: receipt=%#v error=%v", result.receipt, result.err)
		}
	}
	if successes != 1 || conflicts != 1 || len(backend.contentUpdates) != 1 {
		t.Fatalf("concurrent results: successes=%d conflicts=%d updates=%d", successes, conflicts, len(backend.contentUpdates))
	}
}

func TestAgentFinishFlushesContentDisablesStreamingAndClosesResponse(t *testing.T) {
	backend := newFakeAgentBackend()
	svc := newAgentTestService(backend)
	_, _ = seedAgentDelivery(t, svc, "agent", "evt_finish", false)
	start := startAgentResponse(t, svc, "agent", "evt_finish", AgentResponseContent{Markdown: "Working"})

	finished, apiErr := svc.FinishAgentResponse(context.Background(), FinishAgentResponseInput{
		Provider: "agent", ResponseID: start.ResponseID, OperationID: "finish-1", ExpectedRevision: 1,
		Outcome: AgentResponseOutcomeCompleted, Markdown: "Final answer", Summary: "Completed",
	})
	if apiErr != nil {
		t.Fatalf("finish: %v", apiErr)
	}
	if finished.Revision != 2 || finished.Phase != AgentResponsePhaseCompleted || finished.Duplicate {
		t.Fatalf("finish receipt = %#v", finished)
	}
	if len(backend.contentUpdates) != 1 || backend.contentUpdates[0].Content != "Final answer" || backend.contentUpdates[0].Sequence != 1 {
		t.Fatalf("finish content flush = %#v", backend.contentUpdates)
	}
	if len(backend.settingsUpdates) != 1 || backend.settingsUpdates[0].Sequence != 2 {
		t.Fatalf("finish settings = %#v", backend.settingsUpdates)
	}
	var settings struct {
		Config struct {
			StreamingMode bool `json:"streaming_mode"`
			Summary       struct {
				Content string `json:"content"`
			} `json:"summary"`
		} `json:"config"`
	}
	if err := json.Unmarshal([]byte(backend.settingsUpdates[0].SettingsJSON), &settings); err != nil {
		t.Fatalf("decode finish settings: %v", err)
	}
	if settings.Config.StreamingMode || settings.Config.Summary.Content != "Completed" {
		t.Fatalf("finish settings JSON = %#v", settings)
	}

	if _, apiErr := svc.UpdateAgentResponse(context.Background(), UpdateAgentResponseInput{
		Provider: "agent", ResponseID: start.ResponseID, OperationID: "too-late", ExpectedRevision: 2, Markdown: "late",
	}); apiErr == nil || apiErr.Code != "response_closed" || apiErr.Retryable {
		t.Fatalf("post-finish update error = %v", apiErr)
	}
}

func TestAgentFinishRetryAfterSettingsFailureDoesNotRepeatContent(t *testing.T) {
	backend := newFakeAgentBackend()
	backend.settingsErr = errors.New("settings timeout")
	svc := newAgentTestService(backend)
	_, _ = seedAgentDelivery(t, svc, "agent", "evt_finish_retry", false)
	start := startAgentResponse(t, svc, "agent", "evt_finish_retry", AgentResponseContent{Markdown: "Working"})
	in := FinishAgentResponseInput{
		Provider: "agent", ResponseID: start.ResponseID, OperationID: "finish-retry", ExpectedRevision: 1,
		Outcome: AgentResponseOutcomeCompleted, Markdown: "Final", Summary: "Done",
	}

	if _, apiErr := svc.FinishAgentResponse(context.Background(), in); apiErr == nil || apiErr.Code != "feishu_unavailable" || !apiErr.Retryable {
		t.Fatalf("first finish error = %v", apiErr)
	}
	if len(backend.contentUpdates) != 1 || len(backend.settingsUpdates) != 1 {
		t.Fatalf("first finish calls: content=%d settings=%d", len(backend.contentUpdates), len(backend.settingsUpdates))
	}
	firstSettings := backend.settingsUpdates[0]

	backend.settingsErr = nil
	finished, apiErr := svc.FinishAgentResponse(context.Background(), in)
	if apiErr != nil {
		t.Fatalf("finish retry: %v", apiErr)
	}
	if finished.Revision != 2 || finished.Phase != AgentResponsePhaseCompleted || finished.Duplicate {
		t.Fatalf("finish retry receipt = %#v", finished)
	}
	if len(backend.contentUpdates) != 1 {
		t.Fatalf("finish retry repeated content: %#v", backend.contentUpdates)
	}
	if len(backend.settingsUpdates) != 2 {
		t.Fatalf("finish retry settings calls = %d", len(backend.settingsUpdates))
	}
	secondSettings := backend.settingsUpdates[1]
	if secondSettings.Sequence != firstSettings.Sequence || secondSettings.UUID != firstSettings.UUID || secondSettings.SettingsJSON != firstSettings.SettingsJSON {
		t.Fatalf("finish retry changed idempotency identity: first=%#v second=%#v", firstSettings, secondSettings)
	}

	replay, apiErr := svc.FinishAgentResponse(context.Background(), in)
	if apiErr != nil {
		t.Fatalf("finished replay: %v", apiErr)
	}
	if !replay.Duplicate || replay.Revision != 2 || len(backend.settingsUpdates) != 2 {
		t.Fatalf("finished replay = %#v, settings calls=%d", replay, len(backend.settingsUpdates))
	}
}

func TestAgentFinishInteractionConflictRetainsSettingsIdentity(t *testing.T) {
	backend := newFakeAgentBackend()
	backend.settingsErr = &feishu.DynamicCardAPIError{
		Operation: "card settings update", HTTPStatus: 400, Code: 200810,
		Message: "card is being interacted with",
	}
	svc := newAgentTestService(backend)
	_, _ = seedAgentDelivery(t, svc, "agent", "evt_finish_interacting", false)
	start := startAgentResponse(t, svc, "agent", "evt_finish_interacting", AgentResponseContent{Markdown: "Working"})
	in := FinishAgentResponseInput{
		Provider: "agent", ResponseID: start.ResponseID, OperationID: "finish-interacting", ExpectedRevision: 1,
		Outcome: AgentResponseOutcomeCompleted, Markdown: "Working", Summary: "Done",
	}

	if _, apiErr := svc.FinishAgentResponse(context.Background(), in); apiErr == nil || apiErr.Code != "feishu_unavailable" || !apiErr.Retryable {
		t.Fatalf("interaction-conflict finish error = %v", apiErr)
	}
	if len(backend.contentUpdates) != 0 || len(backend.settingsUpdates) != 1 {
		t.Fatalf("first finish calls: content=%d settings=%d", len(backend.contentUpdates), len(backend.settingsUpdates))
	}
	first := backend.settingsUpdates[0]
	response := svc.agentBroker.lookupResponse("agent", start.ResponseID)
	response.mu.Lock()
	response.lastMutationAt = time.Time{}
	response.mu.Unlock()

	backend.settingsErr = nil
	finished, apiErr := svc.FinishAgentResponse(context.Background(), in)
	if apiErr != nil {
		t.Fatalf("retry interaction-conflict finish: %v", apiErr)
	}
	if finished.Revision != 2 || finished.Phase != AgentResponsePhaseCompleted || len(backend.settingsUpdates) != 2 {
		t.Fatalf("finish retry = %#v, settings calls=%d", finished, len(backend.settingsUpdates))
	}
	second := backend.settingsUpdates[1]
	if second.UUID != first.UUID || second.Sequence != first.Sequence || second.SettingsJSON != first.SettingsJSON {
		t.Fatalf("interaction retry changed settings identity: first=%#v second=%#v", first, second)
	}
}

func TestAgentFinishContentAmbiguousThenRejectedBlocksFinalization(t *testing.T) {
	for _, firstErr := range []error{
		errors.New("content response lost"),
		&feishu.DynamicCardAPIError{Operation: "card content update", HTTPStatus: 503, Code: 300503, Message: "upstream failed"},
		&feishu.DynamicCardAPIError{Operation: "card content update", HTTPStatus: 400, Code: 300120, Message: "server internal error"},
	} {
		t.Run(fmt.Sprintf("%T", firstErr), func(t *testing.T) {
			backend := newFakeAgentBackend()
			svc := newAgentTestService(backend)
			_, _ = seedAgentDelivery(t, svc, "agent", "evt_finish_content_unknown", false)
			start := startAgentResponse(t, svc, "agent", "evt_finish_content_unknown", AgentResponseContent{Markdown: "Working"})
			in := FinishAgentResponseInput{
				Provider: "agent", ResponseID: start.ResponseID, OperationID: "finish-content-unknown",
				ExpectedRevision: 1, Outcome: AgentResponseOutcomeCompleted, Markdown: "Final", Summary: "Done",
			}
			backend.contentErr = firstErr
			if _, apiErr := svc.FinishAgentResponse(context.Background(), in); apiErr == nil || apiErr.Code != "feishu_unavailable" || !apiErr.Retryable {
				t.Fatalf("ambiguous finish content error = %v", apiErr)
			}
			first := backend.contentUpdates[0]
			response := svc.agentBroker.lookupResponse("agent", start.ResponseID)
			response.mu.Lock()
			response.lastMutationAt = time.Time{}
			response.mu.Unlock()
			backend.contentErr = &feishu.DynamicCardAPIError{
				Operation: "card content update", HTTPStatus: 400, Code: 300400,
				Message: "duplicate or non-increasing operation",
			}
			if _, apiErr := svc.FinishAgentResponse(context.Background(), in); apiErr == nil || apiErr.Code != "operation_state_unknown" || apiErr.Retryable {
				t.Fatalf("ambiguous then rejected content error = %v", apiErr)
			}
			if len(backend.contentUpdates) != 2 || len(backend.settingsUpdates) != 0 {
				t.Fatalf("finish content/settings calls = %d/%d", len(backend.contentUpdates), len(backend.settingsUpdates))
			}
			second := backend.contentUpdates[1]
			if second.UUID != first.UUID || second.Sequence != first.Sequence || second.Content != first.Content {
				t.Fatalf("finish content changed identity: first=%#v second=%#v", first, second)
			}
			backend.contentErr = nil
			if _, apiErr := svc.FinishAgentResponse(context.Background(), FinishAgentResponseInput{
				Provider: "agent", ResponseID: start.ResponseID, OperationID: "finish-content-replacement",
				ExpectedRevision: 1, Outcome: AgentResponseOutcomeFailed, Markdown: "Fallback", Summary: "Failed",
			}); apiErr == nil || apiErr.Code != "operation_state_unknown" || apiErr.Retryable {
				t.Fatalf("replacement finish error = %v", apiErr)
			}
			if len(backend.contentUpdates) != 2 || len(backend.settingsUpdates) != 0 {
				t.Fatalf("closed finish performed extra I/O: content=%d settings=%d", len(backend.contentUpdates), len(backend.settingsUpdates))
			}
			response.mu.Lock()
			defer response.mu.Unlock()
			op := response.operations[in.OperationID]
			if op == nil || !op.contentAmbiguous || !op.contentClosed || op.contentUUID != first.UUID || op.contentSeq != first.Sequence || op.content != first.Content {
				t.Fatalf("ambiguous finish content identity was not retained: %#v", op)
			}
		})
	}
}

func TestAgentFinishSettingsAmbiguousThenRejectedBlocksFinalization(t *testing.T) {
	for _, firstErr := range []error{
		errors.New("settings response lost"),
		&feishu.DynamicCardAPIError{Operation: "card settings update", HTTPStatus: 503, Code: 300503, Message: "upstream failed"},
	} {
		t.Run(fmt.Sprintf("%T", firstErr), func(t *testing.T) {
			backend := newFakeAgentBackend()
			backend.settingsErr = firstErr
			svc := newAgentTestService(backend)
			_, _ = seedAgentDelivery(t, svc, "agent", "evt_finish_settings_unknown", false)
			start := startAgentResponse(t, svc, "agent", "evt_finish_settings_unknown", AgentResponseContent{Markdown: "Working"})
			in := FinishAgentResponseInput{
				Provider: "agent", ResponseID: start.ResponseID, OperationID: "finish-settings-unknown",
				ExpectedRevision: 1, Outcome: AgentResponseOutcomeCompleted, Markdown: "Final", Summary: "Done",
			}
			if _, apiErr := svc.FinishAgentResponse(context.Background(), in); apiErr == nil || apiErr.Code != "feishu_unavailable" || !apiErr.Retryable {
				t.Fatalf("ambiguous finish settings error = %v", apiErr)
			}
			if len(backend.contentUpdates) != 1 || len(backend.settingsUpdates) != 1 {
				t.Fatalf("first finish calls: content=%d settings=%d", len(backend.contentUpdates), len(backend.settingsUpdates))
			}
			first := backend.settingsUpdates[0]
			response := svc.agentBroker.lookupResponse("agent", start.ResponseID)
			response.mu.Lock()
			response.lastMutationAt = time.Time{}
			response.mu.Unlock()
			backend.settingsErr = &feishu.DynamicCardAPIError{
				Operation: "card settings update", HTTPStatus: 400, Code: 300400,
				Message: "duplicate or non-increasing operation",
			}
			if _, apiErr := svc.FinishAgentResponse(context.Background(), in); apiErr == nil || apiErr.Code != "operation_state_unknown" || apiErr.Retryable {
				t.Fatalf("ambiguous then rejected settings error = %v", apiErr)
			}
			if len(backend.contentUpdates) != 1 || len(backend.settingsUpdates) != 2 {
				t.Fatalf("retry repeated content or missed settings: content=%d settings=%d", len(backend.contentUpdates), len(backend.settingsUpdates))
			}
			second := backend.settingsUpdates[1]
			if second.UUID != first.UUID || second.Sequence != first.Sequence || second.SettingsJSON != first.SettingsJSON {
				t.Fatalf("finish settings changed identity: first=%#v second=%#v", first, second)
			}
			backend.settingsErr = nil
			if _, apiErr := svc.FinishAgentResponse(context.Background(), FinishAgentResponseInput{
				Provider: "agent", ResponseID: start.ResponseID, OperationID: "finish-settings-replacement",
				ExpectedRevision: 1, Outcome: AgentResponseOutcomeFailed, Markdown: "Fallback", Summary: "Failed",
			}); apiErr == nil || apiErr.Code != "operation_state_unknown" || apiErr.Retryable {
				t.Fatalf("replacement finish error = %v", apiErr)
			}
			if len(backend.contentUpdates) != 1 || len(backend.settingsUpdates) != 2 {
				t.Fatalf("closed settings performed extra I/O: content=%d settings=%d", len(backend.contentUpdates), len(backend.settingsUpdates))
			}
			response.mu.Lock()
			defer response.mu.Unlock()
			op := response.operations[in.OperationID]
			if op == nil || !op.settingsAmbiguous || !op.settingsClosed || op.settingsUUID != first.UUID || op.settingsSeq != first.Sequence || op.settingsJSON != first.SettingsJSON {
				t.Fatalf("ambiguous finish settings identity was not retained: %#v", op)
			}
		})
	}
}

func TestAgentFinishDefinitiveSettingsRejectionAllowsNewFinish(t *testing.T) {
	backend := newFakeAgentBackend()
	backend.settingsErr = &feishu.DynamicCardAPIError{Operation: "card settings update", HTTPStatus: 400, Code: 300200, Message: "rejected"}
	svc := newAgentTestService(backend)
	_, _ = seedAgentDelivery(t, svc, "agent", "evt_finish_rejected", false)
	start := startAgentResponse(t, svc, "agent", "evt_finish_rejected", AgentResponseContent{Markdown: "Working"})

	_, apiErr := svc.FinishAgentResponse(context.Background(), FinishAgentResponseInput{
		Provider: "agent", ResponseID: start.ResponseID, OperationID: "finish-rejected", ExpectedRevision: 1,
		Outcome: AgentResponseOutcomeCompleted, Markdown: "Final", Summary: "Done",
	})
	if apiErr == nil || apiErr.Code != "feishu_rejected" || apiErr.Retryable {
		t.Fatalf("definitive finish error = %v", apiErr)
	}
	if len(backend.contentUpdates) != 1 || len(backend.settingsUpdates) != 1 {
		t.Fatalf("rejected finish calls: content=%d settings=%d", len(backend.contentUpdates), len(backend.settingsUpdates))
	}

	backend.settingsErr = nil
	finished, apiErr := svc.FinishAgentResponse(context.Background(), FinishAgentResponseInput{
		Provider: "agent", ResponseID: start.ResponseID, OperationID: "finish-corrected", ExpectedRevision: 1,
		Outcome: AgentResponseOutcomeCompleted, Markdown: "Final", Summary: "Done",
	})
	if apiErr != nil {
		t.Fatalf("corrected finish after rejection: %v", apiErr)
	}
	if finished.Revision != 2 || finished.Phase != AgentResponsePhaseCompleted {
		t.Fatalf("corrected finish receipt = %#v", finished)
	}
	if len(backend.contentUpdates) != 1 {
		t.Fatalf("corrected finish repeated committed content: %#v", backend.contentUpdates)
	}
	if len(backend.settingsUpdates) != 2 || backend.settingsUpdates[1].Sequence != 2 {
		t.Fatalf("corrected finish settings = %#v", backend.settingsUpdates)
	}
}

func TestAgentCompletedOperationsDropCumulativePayloads(t *testing.T) {
	backend := newFakeAgentBackend()
	svc := newAgentTestService(backend)
	_, _ = seedAgentDelivery(t, svc, "agent", "evt_compaction", false)
	start := startAgentResponse(t, svc, "agent", "evt_compaction", AgentResponseContent{Markdown: "A"})
	response := svc.agentBroker.lookupResponse("agent", start.ResponseID)
	if response == nil {
		t.Fatal("started response was not retained")
	}

	revision := start.Revision
	finalMarkdown := ""
	for index := 1; index <= 32; index++ {
		// This test targets retention rather than wall-clock pacing. Reset only the
		// rate clock; the production sequence/revision path remains unchanged.
		response.mu.Lock()
		response.lastMutationAt = time.Time{}
		response.mu.Unlock()
		finalMarkdown = strings.Repeat("x", index*900)
		updated, apiErr := svc.UpdateAgentResponse(context.Background(), UpdateAgentResponseInput{
			Provider: "agent", ResponseID: start.ResponseID,
			OperationID: "compact-update-" + fmt.Sprint(index), ExpectedRevision: revision,
			Markdown: finalMarkdown,
		})
		if apiErr != nil {
			t.Fatalf("update %d: %v", index, apiErr)
		}
		revision = updated.Revision
	}

	response.mu.Lock()
	response.lastMutationAt = time.Time{}
	response.mu.Unlock()
	if len(response.operations) != 32 {
		t.Fatalf("operation receipt count = %d, want 32", len(response.operations))
	}
	assertCompactedOperations(t, response.operations)

	finished, apiErr := svc.FinishAgentResponse(context.Background(), FinishAgentResponseInput{
		Provider: "agent", ResponseID: start.ResponseID, OperationID: "compact-finish",
		ExpectedRevision: revision, Outcome: AgentResponseOutcomeCompleted,
		Markdown: finalMarkdown, Summary: "Done",
	})
	if apiErr != nil {
		t.Fatalf("finish compacted response: %v", apiErr)
	}
	if finished.Revision != revision+1 {
		t.Fatalf("finish receipt = %#v", finished)
	}

	assertCompactedOperations(t, response.operations)
	if response.markdown != "" {
		t.Fatalf("terminal response retained final markdown (%d bytes)", len(response.markdown))
	}

	svc.agentBroker.mu.Lock()
	delivery := svc.agentBroker.deliveries["evt_compaction"]
	svc.agentBroker.mu.Unlock()
	if delivery == nil {
		t.Fatal("delivery receipt was not retained")
	}
	delivery.mu.Lock()
	defer delivery.mu.Unlock()
	if delivery.input.Prompt != "" || delivery.input.ChatID != "" || delivery.cardJSON != "" || delivery.messageUUID != "" {
		t.Fatalf("completed start retained request payload/private route: %#v", delivery)
	}
}

func assertCompactedOperations(t *testing.T, operations map[string]*agentOperation) {
	t.Helper()
	for operationID, op := range operations {
		if !op.complete {
			t.Fatalf("operation %q is unexpectedly incomplete", operationID)
		}
		if op.content != "" || op.contentUUID != "" || op.contentSeq != 0 || op.contentDone ||
			op.contentAmbiguous || op.contentClosed ||
			op.settingsJSON != "" || op.settingsUUID != "" || op.settingsSeq != 0 || op.settingsDone ||
			op.settingsAmbiguous || op.settingsClosed {
			t.Fatalf("operation %q retained mutation payload: %#v", operationID, op)
		}
	}
}

func TestAgentCardFailureLogsOnlySafeStructuredFields(t *testing.T) {
	const (
		responseCapability = "resp_capability_must_not_be_logged"
		userContent        = "PRIVATE_USER_CONTENT_IN_FEISHU_MESSAGE"
		transportContent   = "PRIVATE_TRANSPORT_ERROR_DETAIL"
	)
	var logs bytes.Buffer
	backend := newFakeAgentBackend()
	svc := newAgentTestService(backend)
	svc.logger = slog.New(slog.NewJSONHandler(&logs, nil))

	svc.logAgentCardFailure("update", responseCapability, &feishu.DynamicCardAPIError{
		Operation: "card content update", HTTPStatus: 400, Code: 300317,
		Message: userContent, RequestID: "req_public_support_handle",
	})
	svc.logAgentCardFailure("send", responseCapability, fmt.Errorf("%s: %w", transportContent, context.DeadlineExceeded))

	output := logs.String()
	for _, forbidden := range []string{responseCapability, userContent, transportContent, "card content update"} {
		if strings.Contains(output, forbidden) {
			t.Fatalf("agent failure log leaked %q: %s", forbidden, output)
		}
	}
	for _, required := range []string{
		`"operation":"update"`, `"operation":"send"`,
		`"http_status":400`, `"code":300317`, `"request_id":"req_public_support_handle"`,
		`"error_class":"deadline_exceeded"`, `"correlation":"agent_`,
	} {
		if !strings.Contains(output, required) {
			t.Fatalf("agent failure log is missing %q: %s", required, output)
		}
	}
}

func TestAgentCardActionRoutesOnlyToOwnerWithoutRawIDs(t *testing.T) {
	backend := newFakeAgentBackend()
	backend.messageID = "om_secret_agent_message"
	svc := newAgentTestService(backend)
	owner := mustSubscribeAgent(t, svc, AgentSubscribeOptions{
		Provider: "owner", Commands: []string{"ask"}, IncludeCardActions: true,
	})
	other := mustSubscribeAgent(t, svc, AgentSubscribeOptions{
		Provider: "other", Commands: []string{"ask"}, IncludeCardActions: true,
	})
	mustDispatchAgentPrompt(t, svc, CommandInput{
		DeliveryID: "evt_action_source", Command: "ask", Prompt: "ask stop",
		ConversationID: "conv_action", ChatAlias: "ops", ChatID: "oc_secret_chat",
		Metadata: map[string]string{"message_id": "om_inbound_secret"},
	})
	_ = receiveAgentEvent(t, owner)
	_ = receiveAgentEvent(t, other)
	start := startAgentResponse(t, svc, "owner", "evt_action_source", AgentResponseContent{
		Markdown: "Working",
		Actions: []AgentResponseAction{{
			ActionID: "stop", Label: "Stop", PayloadJSON: `{"reason":"user","attempt":1}`,
		}},
	})

	if apiErr := svc.DispatchAgentCardAction(context.Background(), AgentCardActionInput{
		DeliveryID: "evt_action", MessageID: backend.messageID, SenderID: "ou_actor",
		ActionID: "stop", PayloadJSON: `{"reason":"user","attempt":1}`,
		ActionPayloadJSON: `{"reason":"user","attempt":1}`,
	}); apiErr != nil {
		t.Fatalf("dispatch card action: %v", apiErr)
	}
	event := receiveAgentEvent(t, owner)
	if event.CardAction == nil {
		t.Fatalf("owner event has no card action: %#v", event)
	}
	if event.DeliveryID != "evt_action" || event.ConversationID != "conv_action" || event.ChatAlias != "ops" || event.SenderID != "ou_actor" {
		t.Fatalf("action event envelope = %#v", event)
	}
	if event.CardAction.ResponseID != start.ResponseID || event.CardAction.ActionID != "stop" || event.CardAction.PayloadJSON != `{"reason":"user","attempt":1}` {
		t.Fatalf("action event payload = %#v", event.CardAction)
	}
	assertNoAgentEvent(t, other)

	encoded, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("encode provider-visible event: %v", err)
	}
	visible := string(encoded)
	for _, rawID := range []string{backend.messageID, "om_inbound_secret", "oc_secret_chat", backend.cardID} {
		if strings.Contains(visible, rawID) {
			t.Fatalf("provider-visible action leaked raw id %q: %s", rawID, visible)
		}
	}
	for name, input := range map[string]AgentCardActionInput{
		"unknown id": {
			DeliveryID: "evt_unknown_action", MessageID: backend.messageID, SenderID: "ou_actor",
			ActionID: "approve", PayloadJSON: `{}`, ActionPayloadJSON: `{}`,
		},
		"conflicting payload": {
			DeliveryID: "evt_conflicting_payload", MessageID: backend.messageID, SenderID: "ou_actor",
			ActionID: "stop", PayloadJSON: `{}`, ActionPayloadJSON: `{"reason":"other","attempt":1}`,
		},
		"missing actor": {
			DeliveryID: "evt_missing_actor", MessageID: backend.messageID,
			ActionID: "stop", PayloadJSON: `{}`, ActionPayloadJSON: `{"reason":"user","attempt":1}`,
		},
	} {
		t.Run(name, func(t *testing.T) {
			apiErr := svc.DispatchAgentCardAction(context.Background(), input)
			if apiErr == nil || apiErr.Retryable {
				t.Fatalf("unsafe action error = %v", apiErr)
			}
		})
	}
	assertNoAgentEvent(t, owner)

	if apiErr := svc.DispatchInboundCardAction(context.Background(), feishu.InboundCardAction{
		DeliveryID: "evt_v2_callback", MessageID: backend.messageID, SenderID: "ou_actor",
		Tag: "button", Name: "independent-form-component", ValueJSON: `{"action_id":"stop","payload_json":"{\"reason\":\"user\",\"attempt\":1}"}`, FormValueJSON: `{}`,
	}); apiErr != nil {
		t.Fatalf("dispatch JSON 2.0 callback: %v", apiErr)
	}
	v2Event := receiveAgentEvent(t, owner)
	if v2Event.CardAction == nil || v2Event.CardAction.ActionID != "stop" {
		t.Fatalf("JSON 2.0 callback event = %#v", v2Event)
	}
	var normalized map[string]any
	if err := json.Unmarshal([]byte(v2Event.CardAction.PayloadJSON), &normalized); err != nil {
		t.Fatalf("decode normalized JSON 2.0 callback: %v", err)
	}
	value, _ := normalized["value"].(map[string]any)
	payload, _ := value["payload"].(map[string]any)
	if value["action_id"] != "stop" || payload["reason"] != "user" || payload["attempt"] != float64(1) {
		t.Fatalf("normalized callback payload = %#v", normalized)
	}
	if normalized["name"] != "independent-form-component" {
		t.Fatalf("normalized callback lost independent action name: %#v", normalized)
	}
}

func TestAgentActionPayloadPreservesLargeJSONInteger(t *testing.T) {
	const actionPayload = `{"id":9007199254740993}`
	backend := newFakeAgentBackend()
	svc := newAgentTestService(backend)
	sub, _ := seedAgentDelivery(t, svc, "agent", "evt_precise_action", true)
	_ = startAgentResponse(t, svc, "agent", "evt_precise_action", AgentResponseContent{
		Markdown: "Choose",
		Actions: []AgentResponseAction{{
			ActionID: "select", Label: "Select", PayloadJSON: actionPayload,
		}},
	})

	var card struct {
		Body struct {
			Elements []struct {
				Behaviors []struct {
					Value struct {
						PayloadJSON string `json:"payload_json"`
					} `json:"value"`
				} `json:"behaviors"`
			} `json:"elements"`
		} `json:"body"`
	}
	if err := json.Unmarshal([]byte(backend.createdCards[0]), &card); err != nil {
		t.Fatalf("decode precise action card: %v", err)
	}
	if got := card.Body.Elements[1].Behaviors[0].Value.PayloadJSON; got != actionPayload {
		t.Fatalf("card action payload = %q, want exact %q", got, actionPayload)
	}
	callbackValue, err := json.Marshal(map[string]string{
		"action_id": "select", "payload_json": actionPayload,
	})
	if err != nil {
		t.Fatalf("encode callback fixture: %v", err)
	}
	if apiErr := svc.DispatchInboundCardAction(context.Background(), feishu.InboundCardAction{
		DeliveryID: "evt_precise_callback", MessageID: backend.messageID, SenderID: "ou_actor",
		Tag: "button", ValueJSON: string(callbackValue), FormValueJSON: `{}`,
	}); apiErr != nil {
		t.Fatalf("dispatch precise callback: %v", apiErr)
	}
	event := receiveAgentEvent(t, sub)
	if event.CardAction == nil || !strings.Contains(event.CardAction.PayloadJSON, "9007199254740993") {
		t.Fatalf("provider callback lost large integer: %#v", event.CardAction)
	}
	var normalized struct {
		Value struct {
			Payload struct {
				ID json.Number `json:"id"`
			} `json:"payload"`
		} `json:"value"`
	}
	decoder := json.NewDecoder(strings.NewReader(event.CardAction.PayloadJSON))
	decoder.UseNumber()
	if err := decoder.Decode(&normalized); err != nil {
		t.Fatalf("decode precise provider callback: %v", err)
	}
	if normalized.Value.Payload.ID.String() != "9007199254740993" {
		t.Fatalf("large integer = %q", normalized.Value.Payload.ID.String())
	}
}

var _ feishu.DynamicCards = (*fakeAgentBackend)(nil)
var _ feishu.Sender = (*fakeAgentBackend)(nil)
