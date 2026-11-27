package service

import (
	"context"
	"testing"

	"feishu-botd/internal/feishu"
)

func directReplyInput(deliveryID string, metadata map[string]string) CommandInput {
	return CommandInput{
		DeliveryID: deliveryID, Prompt: "这个风险具体指什么？",
		ConversationID: "conv_direct", ChatAlias: "direct", SenderID: "ou_sender",
		ChatID: "oc_direct", Metadata: metadata,
	}
}

func TestAttachedContextRequestCarriesDirectReplyParentOnlyForThreadlessDM(t *testing.T) {
	tests := []struct {
		name       string
		chatID     string
		metadata   map[string]string
		wantParent string
	}{
		{"dm reply to notice", "oc_direct", map[string]string{
			"chat_type": "p2p", "message_id": "om_reply", "parent_id": "om_notice",
		}, "om_notice"},
		{"dm without reply", "oc_direct", map[string]string{
			"chat_type": "p2p", "message_id": "om_reply",
		}, ""},
		{"dm thread keeps thread path", "oc_direct", map[string]string{
			"chat_type": "p2p", "message_id": "om_reply", "parent_id": "om_notice", "thread_id": "omt_dm",
		}, ""},
		{"dm parent equals trigger", "oc_direct", map[string]string{
			"chat_type": "p2p", "message_id": "om_reply", "parent_id": "om_reply",
		}, ""},
		{"dm without delivery chat", "", map[string]string{
			"chat_type": "p2p", "message_id": "om_reply", "parent_id": "om_notice",
		}, ""},
		{"group quote reply unchanged", "oc_group", map[string]string{
			"chat_type": "group", "message_id": "om_reply", "parent_id": "om_notice",
		}, ""},
		{"topic group unchanged", "oc_group", map[string]string{
			"chat_type": "topic_group", "message_id": "om_reply", "parent_id": "om_notice", "thread_id": "omt_topic",
		}, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			in := directReplyInput("d", tt.metadata)
			in.ChatID = tt.chatID
			got := attachedContextRequestFor(in)
			if got.ReplyParentMessageID != tt.wantParent {
				t.Fatalf("reply parent = %q, want %q", got.ReplyParentMessageID, tt.wantParent)
			}
			if got.ChatID != tt.chatID || got.TriggerMessageID != "om_reply" || got.ThreadID != tt.metadata["thread_id"] {
				t.Fatalf("request = %#v", got)
			}
		})
	}
}

func TestGetAgentAttachedContextPassesDirectReplyParentToLookup(t *testing.T) {
	backend := &attachedContextTestBackend{
		fakeSender: &fakeSender{messageID: "om_answer"},
		result:     feishu.AttachedContext{Status: feishu.AttachedContextFound},
	}
	svc := newAttachedContextTestService(backend)
	sub := mustSubscribeAgent(t, svc, AgentSubscribeOptions{Provider: "nous", IncludeUnmatchedMessages: true})
	t.Cleanup(sub.Close)
	mustDispatchAgentPrompt(t, svc, directReplyInput("delivery_dm_reply", map[string]string{
		"chat_type": "p2p", "message_id": "om_reply", "parent_id": "om_notice", "create_time": "2000",
	}))
	event := receiveAgentEvent(t, sub)
	if event.Message == nil || event.Message.ReplyToMessageRef != "" {
		t.Fatalf("unowned parent must not become a reply ref: %#v", event.Message)
	}
	if backend.calls != 0 {
		t.Fatalf("dispatch eagerly read context: calls=%d", backend.calls)
	}

	if _, apiErr := svc.GetAgentAttachedContext(context.Background(), AgentAttachedContextInput{
		Provider: "nous", DeliveryID: "delivery_dm_reply",
	}); apiErr != nil {
		t.Fatalf("get attached context: %v", apiErr)
	}
	want := feishu.AttachedContextRequest{
		TriggerMessageID: "om_reply", TriggerCreateTime: "2000",
		ChatID: "oc_direct", ReplyParentMessageID: "om_notice",
	}
	if len(backend.requests) != 1 || backend.requests[0] != want {
		t.Fatalf("lookup request = %#v, want %#v", backend.requests, want)
	}

	// A provider that did not receive this delivery never reaches the lookup.
	if _, apiErr := svc.GetAgentAttachedContext(context.Background(), AgentAttachedContextInput{
		Provider: "other", DeliveryID: "delivery_dm_reply",
	}); apiErr == nil || apiErr.Code != "unknown_delivery" {
		t.Fatalf("other provider error = %v", apiErr)
	}
	if backend.calls != 1 {
		t.Fatalf("unauthorized lookups reached Feishu: calls=%d", backend.calls)
	}
}
