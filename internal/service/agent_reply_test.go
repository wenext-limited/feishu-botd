package service

import (
	"context"
	"testing"
	"time"

	"feishu-botd/internal/config"
	"feishu-botd/internal/dedupe"
	"feishu-botd/internal/feishu"
	"feishu-botd/internal/ownership"
)

func replyRoutingService(t *testing.T) (*Service, *fakeOwnerStore) {
	t.Helper()
	cfg := config.Config{
		DedupeTTL:   time.Hour,
		SendTimeout: time.Second,
		ChannelRoutes: map[string]config.ChannelRoute{
			"ops": {AppAlias: "support", ChatID: "oc_support"},
		},
		Apps: map[string]config.AppConfig{
			"support": {
				AppID:     "app",
				AppSecret: "secret",
				Channels:  map[string]string{"ops": "oc_support"},
			},
		},
	}
	svc := NewMultiAppService(cfg, nil, dedupe.NewMemoryStore(time.Hour), nil)
	owners := &fakeOwnerStore{owners: make(map[string]ownership.Owner)}
	svc.SetAgentOwnershipStore(owners)
	return svc, owners
}

func groupReply(parentID, deliveryID, prompt string, unmentioned bool) CommandInput {
	return CommandInput{
		AppAlias:         "support",
		DeliveryID:       deliveryID,
		Command:          "what",
		Text:             "about this?",
		Prompt:           prompt,
		ConversationID:   "conv_support",
		ChatAlias:        "ops",
		SenderID:         "ou_sender",
		Metadata:         map[string]string{"chat_type": "group", "message_id": "om_reply", "parent_id": parentID},
		UnmentionedReply: unmentioned,
	}
}

func TestOwnedUnmentionedGroupReplyRoutesOnlyToAuthoringProvider(t *testing.T) {
	t.Parallel()
	svc, owners := replyRoutingService(t)
	parentRef := feishu.MessageRefForApp("support", "om_agent_answer")
	owners.owners[parentRef] = ownership.Owner{Provider: "nous", ExpiresAt: time.Now().Add(time.Hour).Unix()}
	nous := mustSubscribeAgent(t, svc, AgentSubscribeOptions{Provider: "nous", IncludeUnmatchedMessages: true})
	other := mustSubscribeAgent(t, svc, AgentSubscribeOptions{Provider: "other", IncludeUnmatchedMessages: true})
	legacy, apiErr := svc.SubscribeCommands(context.Background(), "legacy", []string{"what"})
	if apiErr != nil {
		t.Fatalf("subscribe legacy: %v", apiErr)
	}
	t.Cleanup(legacy.Close)

	in := groupReply("om_agent_answer", "delivery_reply_1", "what about retries?", true)
	if _, apiErr = svc.DispatchCommand(context.Background(), in); apiErr != nil {
		t.Fatalf("dispatch owned reply: %v", apiErr)
	}

	event := receiveAgentEvent(t, nous)
	if event.Message == nil || event.Message.Text != in.Prompt || event.Message.ReplyToMessageRef != parentRef {
		t.Fatalf("owned reply event = %#v", event)
	}
	assertNoAgentEvent(t, other)
	select {
	case command := <-legacy.C:
		t.Fatalf("owned agent reply reached legacy command subscriber: %#v", command)
	default:
	}
}

func TestUnknownUnmentionedGroupReplyIsAcknowledgedWithoutBroadcast(t *testing.T) {
	t.Parallel()
	svc, _ := replyRoutingService(t)
	nous := mustSubscribeAgent(t, svc, AgentSubscribeOptions{Provider: "nous", IncludeUnmatchedMessages: true})
	other := mustSubscribeAgent(t, svc, AgentSubscribeOptions{Provider: "other", IncludeUnmatchedMessages: true})

	if _, apiErr := svc.DispatchCommand(context.Background(), groupReply(
		"om_unowned_message",
		"delivery_reply_unknown",
		"this must stay private",
		true,
	)); apiErr != nil {
		t.Fatalf("unknown reply should be acknowledged: %v", apiErr)
	}

	assertNoAgentEvent(t, nous)
	assertNoAgentEvent(t, other)
}

func TestOwnedMentionedReplyKeepsProviderOwnershipAheadOfCommandMatching(t *testing.T) {
	t.Parallel()
	svc, owners := replyRoutingService(t)
	parentRef := feishu.MessageRefForApp("support", "om_agent_answer")
	owners.owners[parentRef] = ownership.Owner{Provider: "nous", ExpiresAt: time.Now().Add(time.Hour).Unix()}
	nous := mustSubscribeAgent(t, svc, AgentSubscribeOptions{Provider: "nous", IncludeUnmatchedMessages: true})
	other := mustSubscribeAgent(t, svc, AgentSubscribeOptions{Provider: "other", Commands: []string{"deploy"}})

	in := groupReply("om_agent_answer", "delivery_reply_mentioned", "deploy the same fix", false)
	in.Command = "deploy"
	in.Text = "the same fix"
	if _, apiErr := svc.DispatchCommand(context.Background(), in); apiErr != nil {
		t.Fatalf("dispatch owned mentioned reply: %v", apiErr)
	}

	event := receiveAgentEvent(t, nous)
	if event.Message == nil || event.Message.ReplyToMessageRef != parentRef {
		t.Fatalf("owned mentioned reply event = %#v", event)
	}
	assertNoAgentEvent(t, other)
}
