package service

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestCommandDispatchAndRespond(t *testing.T) {
	sender := &fakeSender{messageID: "om_reply"}
	svc := newTestService(sender)

	sub, apiErr := svc.SubscribeCommands(context.Background(), "example-service", []string{"status"})
	if apiErr != nil {
		t.Fatalf("subscribe: %v", apiErr)
	}
	defer sub.Close()

	delivered, apiErr := svc.DispatchCommand(context.Background(), CommandInput{
		DeliveryID: "evt_1",
		Command:    "STATUS",
		Text:       "now",
		ChatAlias:  "ops",
		SenderID:   "ou_sender",
		ChatID:     "oc_private_route",
		Metadata: map[string]string{
			"chat_type": "group", "message_type": "text", "message_id": "om_1",
			"event_id": "evt_private", "thread_id": "omt_private", "root_id": "om_root", "parent_id": "om_parent",
		},
	})
	if apiErr != nil {
		t.Fatalf("dispatch: %v", apiErr)
	}
	if delivered != 1 {
		t.Fatalf("delivered = %d, want 1", delivered)
	}

	select {
	case cmd := <-sub.C:
		if cmd.Command != "status" || cmd.Text != "now" || cmd.ChatAlias != "ops" || cmd.ChatID != "" ||
			len(cmd.Metadata) != 2 || cmd.Metadata["chat_type"] != "group" || cmd.Metadata["message_type"] != "text" {
			t.Fatalf("command = %#v", cmd)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for command")
	}

	if apiErr := svc.RespondCommand(context.Background(), CommandResponse{
		DeliveryID: "evt_1",
		Title:      "Status",
		Markdown:   "all good",
	}); apiErr != nil {
		t.Fatalf("respond: %v", apiErr)
	}
	if sender.chatID != "oc_test" {
		t.Fatalf("reply chat id = %q", sender.chatID)
	}
	if sender.request.Source != "command" || sender.request.DedupeKey != "command:evt_1" || sender.request.Markdown != "all good" {
		t.Fatalf("reply request = %#v", sender.request)
	}

	if apiErr := svc.RespondCommand(context.Background(), CommandResponse{
		DeliveryID: "evt_1",
		Markdown:   "again",
	}); apiErr == nil || apiErr.Code != "already_responded" {
		t.Fatalf("expected already_responded, got %v", apiErr)
	}
	if sender.calls != 1 {
		t.Fatalf("sender calls = %d, want 1", sender.calls)
	}
}

func TestCommandRespondThreadsReplyToOriginalMessage(t *testing.T) {
	sender := &fakeSender{messageID: "om_reply"}
	svc := newTestService(sender)

	sub, apiErr := svc.SubscribeCommands(context.Background(), "example-service", []string{"status"})
	if apiErr != nil {
		t.Fatalf("subscribe: %v", apiErr)
	}
	defer sub.Close()

	_, apiErr = svc.DispatchCommand(context.Background(), CommandInput{
		DeliveryID: "evt_1",
		Command:    "status",
		ChatAlias:  "ops",
		Metadata:   map[string]string{"message_id": "om_original"},
	})
	if apiErr != nil {
		t.Fatalf("dispatch: %v", apiErr)
	}
	<-sub.C

	if apiErr := svc.RespondCommand(context.Background(), CommandResponse{
		DeliveryID: "evt_1",
		Markdown:   "all good",
	}); apiErr != nil {
		t.Fatalf("respond: %v", apiErr)
	}
	if sender.request.ReplyToMessageID != "om_original" {
		t.Fatalf("reply_to_message_id = %q, want om_original", sender.request.ReplyToMessageID)
	}
}

func TestCommandRespondWithoutMessageIDLeavesReplyEmpty(t *testing.T) {
	sender := &fakeSender{messageID: "om_reply"}
	svc := newTestService(sender)

	sub, apiErr := svc.SubscribeCommands(context.Background(), "example-service", []string{"status"})
	if apiErr != nil {
		t.Fatalf("subscribe: %v", apiErr)
	}
	defer sub.Close()

	_, apiErr = svc.DispatchCommand(context.Background(), CommandInput{
		DeliveryID: "evt_1",
		Command:    "status",
		ChatAlias:  "ops",
	})
	if apiErr != nil {
		t.Fatalf("dispatch: %v", apiErr)
	}
	<-sub.C

	if apiErr := svc.RespondCommand(context.Background(), CommandResponse{
		DeliveryID: "evt_1",
		Markdown:   "all good",
	}); apiErr != nil {
		t.Fatalf("respond: %v", apiErr)
	}
	if sender.request.ReplyToMessageID != "" {
		t.Fatalf("reply_to_message_id = %q, want empty", sender.request.ReplyToMessageID)
	}
}

func TestCommandDispatchRejectsOversizedMessageIDMetadata(t *testing.T) {
	svc := newTestService(&fakeSender{messageID: "om_reply"})
	_, apiErr := svc.DispatchCommand(context.Background(), CommandInput{
		DeliveryID: "evt_1",
		Command:    "status",
		ChatAlias:  "ops",
		Metadata:   map[string]string{"message_id": strings.Repeat("a", 161)},
	})
	if apiErr == nil || apiErr.Code != "field_too_large" {
		t.Fatalf("expected field_too_large, got %v", apiErr)
	}
}

func TestCommandDispatchWithoutSubscriberDoesNotCreateDelivery(t *testing.T) {
	svc := newTestService(&fakeSender{messageID: "om_reply"})
	delivered, apiErr := svc.DispatchCommand(context.Background(), CommandInput{
		DeliveryID: "evt_1",
		Command:    "status",
		ChatAlias:  "ops",
	})
	if apiErr != nil {
		t.Fatalf("dispatch: %v", apiErr)
	}
	if delivered != 0 {
		t.Fatalf("delivered = %d, want 0", delivered)
	}
	if apiErr := svc.RespondCommand(context.Background(), CommandResponse{DeliveryID: "evt_1", Markdown: "late"}); apiErr == nil || apiErr.Code != "unknown_delivery" {
		t.Fatalf("expected unknown_delivery, got %v", apiErr)
	}
}

func TestCommandRetryKeepsLegacyRouteSticky(t *testing.T) {
	svc := newTestService(&fakeSender{messageID: "om_reply"})
	legacy, apiErr := svc.SubscribeCommands(context.Background(), "legacy", []string{"status"})
	if apiErr != nil {
		t.Fatalf("subscribe legacy: %v", apiErr)
	}
	defer legacy.Close()
	agent := mustSubscribeAgent(t, svc, AgentSubscribeOptions{Provider: "agent", Commands: []string{"status"}})
	in := CommandInput{
		DeliveryID: "evt_sticky_legacy", Command: "status", Prompt: "status now", ChatAlias: "ops",
		Metadata: map[string]string{"message_id": "om_sticky_legacy", "chat_type": "group", "message_type": "text"},
	}

	if delivered, apiErr := svc.DispatchCommand(context.Background(), in); apiErr != nil || delivered != 1 {
		t.Fatalf("first legacy dispatch = %d, %v", delivered, apiErr)
	}
	select {
	case <-legacy.C:
	case <-time.After(time.Second):
		t.Fatal("legacy subscriber did not receive first event")
	}
	assertNoAgentEvent(t, agent)
	if delivered, apiErr := svc.DispatchCommand(context.Background(), in); apiErr != nil || delivered != 0 {
		t.Fatalf("legacy retry dispatch = %d, %v", delivered, apiErr)
	}
	assertNoCommandEvent(t, legacy)
	assertNoAgentEvent(t, agent)
}

func TestCommandRetryKeepsAgentRouteStickyAcrossSubscriberChurn(t *testing.T) {
	svc := newTestService(&fakeSender{messageID: "om_reply"})
	agent := mustSubscribeAgent(t, svc, AgentSubscribeOptions{Provider: "agent", Commands: []string{"status"}})
	first := CommandInput{
		DeliveryID: "evt_sticky_agent_first", Command: "status", Prompt: "status now", ChatAlias: "ops",
		Metadata: map[string]string{"message_id": "om_sticky_agent", "chat_type": "group", "message_type": "text"},
	}
	if delivered, apiErr := svc.DispatchCommand(context.Background(), first); apiErr != nil || delivered != 0 {
		t.Fatalf("first agent dispatch = %d, %v", delivered, apiErr)
	}
	if event := receiveAgentEvent(t, agent); event.DeliveryID != first.DeliveryID {
		t.Fatalf("agent event = %#v", event)
	}
	legacy, apiErr := svc.SubscribeCommands(context.Background(), "legacy", []string{"status"})
	if apiErr != nil {
		t.Fatalf("subscribe late legacy: %v", apiErr)
	}
	defer legacy.Close()
	retry := first
	retry.DeliveryID = "evt_sticky_agent_retry"
	if delivered, apiErr := svc.DispatchCommand(context.Background(), retry); apiErr != nil || delivered != 0 {
		t.Fatalf("agent retry dispatch = %d, %v", delivered, apiErr)
	}
	assertNoCommandEvent(t, legacy)
	assertNoAgentEvent(t, agent)
}

func TestCommandDispatchDeliversOncePerProvider(t *testing.T) {
	svc := newTestService(&fakeSender{messageID: "om_reply"})
	a1, apiErr := svc.SubscribeCommands(context.Background(), "provider-a", []string{"status"})
	if apiErr != nil {
		t.Fatalf("subscribe a1: %v", apiErr)
	}
	defer a1.Close()
	a2, apiErr := svc.SubscribeCommands(context.Background(), "provider-a", []string{"status"})
	if apiErr != nil {
		t.Fatalf("subscribe a2: %v", apiErr)
	}
	defer a2.Close()
	b, apiErr := svc.SubscribeCommands(context.Background(), "provider-b", []string{"status"})
	if apiErr != nil {
		t.Fatalf("subscribe b: %v", apiErr)
	}
	defer b.Close()

	delivered, apiErr := svc.DispatchCommand(context.Background(), CommandInput{
		DeliveryID: "evt_provider_once", Command: "status", Prompt: "status", ChatAlias: "ops",
	})
	if apiErr != nil || delivered != 2 {
		t.Fatalf("provider-deduped dispatch = %d, %v", delivered, apiErr)
	}
	aDeliveries := drainCommandEvents(a1) + drainCommandEvents(a2)
	bDeliveries := drainCommandEvents(b)
	if aDeliveries != 1 || bDeliveries != 1 {
		t.Fatalf("per-provider deliveries: provider-a=%d provider-b=%d", aDeliveries, bDeliveries)
	}
}

func assertNoCommandEvent(t *testing.T, sub *CommandSubscription) {
	t.Helper()
	select {
	case event := <-sub.C:
		t.Fatalf("unexpected command event: %#v", event)
	case <-time.After(40 * time.Millisecond):
	}
}

func drainCommandEvents(sub *CommandSubscription) int {
	count := 0
	for {
		select {
		case <-sub.C:
			count++
		default:
			return count
		}
	}
}

func TestSubscribeCommandsValidatesProviderAndCommands(t *testing.T) {
	svc := newTestService(&fakeSender{messageID: "om_reply"})
	if _, apiErr := svc.SubscribeCommands(context.Background(), "", []string{"status"}); apiErr == nil || apiErr.Code != "missing_provider" {
		t.Fatalf("expected missing_provider, got %v", apiErr)
	}
	if _, apiErr := svc.SubscribeCommands(context.Background(), "example-service", nil); apiErr == nil || apiErr.Code != "missing_command" {
		t.Fatalf("expected missing_command, got %v", apiErr)
	}
}
