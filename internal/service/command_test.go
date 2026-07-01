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

	sub, apiErr := svc.SubscribeCommands(context.Background(), "xipe", []string{"status"})
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
		Metadata:   map[string]string{"message_id": "om_1"},
	})
	if apiErr != nil {
		t.Fatalf("dispatch: %v", apiErr)
	}
	if delivered != 1 {
		t.Fatalf("delivered = %d, want 1", delivered)
	}

	select {
	case cmd := <-sub.C:
		if cmd.Command != "status" || cmd.Text != "now" || cmd.ChatAlias != "ops" || cmd.Metadata["message_id"] != "om_1" {
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

	sub, apiErr := svc.SubscribeCommands(context.Background(), "xipe", []string{"status"})
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

	sub, apiErr := svc.SubscribeCommands(context.Background(), "xipe", []string{"status"})
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

func TestSubscribeCommandsValidatesProviderAndCommands(t *testing.T) {
	svc := newTestService(&fakeSender{messageID: "om_reply"})
	if _, apiErr := svc.SubscribeCommands(context.Background(), "", []string{"status"}); apiErr == nil || apiErr.Code != "missing_provider" {
		t.Fatalf("expected missing_provider, got %v", apiErr)
	}
	if _, apiErr := svc.SubscribeCommands(context.Background(), "xipe", nil); apiErr == nil || apiErr.Code != "missing_command" {
		t.Fatalf("expected missing_command, got %v", apiErr)
	}
}
