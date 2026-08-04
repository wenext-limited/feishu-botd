package service

import (
	"context"
	"testing"
	"time"

	"feishu-botd/internal/config"
	"feishu-botd/internal/dedupe"
	"feishu-botd/internal/ownership"
)

type fakeOwnerStore struct {
	owners map[string]ownership.Owner
}

func (s *fakeOwnerStore) Put(messageRef, provider string, now time.Time) error {
	s.owners[messageRef] = ownership.Owner{Provider: provider, ExpiresAt: now.Add(time.Hour).Unix()}
	return nil
}

func (s *fakeOwnerStore) Lookup(messageRef string, _ time.Time) (ownership.Owner, bool, error) {
	owner, ok := s.owners[messageRef]
	return owner, ok, nil
}

func reactionService(t *testing.T) (*Service, *fakeOwnerStore) {
	t.Helper()
	cfg := config.Config{
		DedupeTTL:   time.Hour,
		SendTimeout: time.Second,
		Apps: map[string]config.AppConfig{
			"support": {AppID: "app", AppSecret: "secret"},
		},
	}
	svc := NewMultiAppService(cfg, nil, dedupe.NewMemoryStore(time.Hour), nil)
	owners := &fakeOwnerStore{owners: make(map[string]ownership.Owner)}
	svc.SetAgentOwnershipStore(owners)
	return svc, owners
}

func TestReactionRoutesOnlyToTheOwningProvider(t *testing.T) {
	t.Parallel()
	svc, owners := reactionService(t)
	owners.owners["msgref_answer"] = ownership.Owner{Provider: "nous", ExpiresAt: time.Now().Add(time.Hour).Unix()}

	nous, apiErr := svc.SubscribeAgentEvents(context.Background(), AgentSubscribeOptions{
		Provider:                "nous",
		IncludeMessageReactions: true,
	})
	if apiErr != nil {
		t.Fatalf("subscribe nous: %v", apiErr)
	}
	defer nous.Close()
	other, apiErr := svc.SubscribeAgentEvents(context.Background(), AgentSubscribeOptions{
		Provider:                "other",
		IncludeMessageReactions: true,
	})
	if apiErr != nil {
		t.Fatalf("subscribe other: %v", apiErr)
	}
	defer other.Close()

	apiErr = svc.DispatchAgentMessageReaction(context.Background(), AgentMessageReactionInput{
		AppAlias:     "support",
		DeliveryID:   "delivery_reaction_1",
		MessageRef:   "msgref_answer",
		SenderID:     "ou_actor",
		ReactionType: "THUMBSUP",
		Operation:    MessageReactionAdded,
	})
	if apiErr != nil {
		t.Fatalf("dispatch: %v", apiErr)
	}

	select {
	case event := <-nous.C:
		if event.MessageReaction == nil || event.MessageReaction.MessageRef != "msgref_answer" || event.SenderID != "ou_actor" {
			t.Fatalf("event = %#v", event)
		}
	case <-time.After(time.Second):
		t.Fatal("owner did not receive reaction")
	}
	select {
	case event := <-other.C:
		t.Fatalf("unrelated provider received reaction: %#v", event)
	default:
	}
}

func TestReactionRetryIsIdempotentAndUnknownMessagesAreNotBroadcast(t *testing.T) {
	t.Parallel()
	svc, owners := reactionService(t)
	owners.owners["msgref_answer"] = ownership.Owner{Provider: "nous", ExpiresAt: time.Now().Add(time.Hour).Unix()}
	sub, apiErr := svc.SubscribeAgentEvents(context.Background(), AgentSubscribeOptions{
		Provider:                "nous",
		IncludeMessageReactions: true,
	})
	if apiErr != nil {
		t.Fatalf("subscribe: %v", apiErr)
	}
	defer sub.Close()

	in := AgentMessageReactionInput{
		AppAlias:     "support",
		DeliveryID:   "delivery_reaction_1",
		MessageRef:   "msgref_answer",
		SenderID:     "ou_actor",
		ReactionType: "ThumbsDown",
		Operation:    MessageReactionRemoved,
	}
	if apiErr := svc.DispatchAgentMessageReaction(context.Background(), in); apiErr != nil {
		t.Fatalf("first dispatch: %v", apiErr)
	}
	<-sub.C
	if apiErr := svc.DispatchAgentMessageReaction(context.Background(), in); apiErr != nil {
		t.Fatalf("duplicate dispatch: %v", apiErr)
	}
	select {
	case event := <-sub.C:
		t.Fatalf("duplicate reaction delivered: %#v", event)
	default:
	}

	in.DeliveryID = "delivery_reaction_2"
	in.MessageRef = "msgref_unrelated"
	if apiErr := svc.DispatchAgentMessageReaction(context.Background(), in); apiErr != nil {
		t.Fatalf("unknown reaction should be acknowledged: %#v", apiErr)
	}
	select {
	case event := <-sub.C:
		t.Fatalf("unknown reaction delivered: %#v", event)
	default:
	}
}

func TestReactionWithoutEligibleSubscriberIsAcknowledged(t *testing.T) {
	t.Parallel()
	svc, owners := reactionService(t)
	owners.owners["msgref_answer"] = ownership.Owner{Provider: "nous", ExpiresAt: time.Now().Add(time.Hour).Unix()}

	apiErr := svc.DispatchAgentMessageReaction(context.Background(), AgentMessageReactionInput{
		AppAlias:     "support",
		DeliveryID:   "delivery_reaction_1",
		MessageRef:   "msgref_answer",
		SenderID:     "ou_actor",
		ReactionType: "THUMBSUP",
		Operation:    MessageReactionAdded,
	})
	if apiErr != nil {
		t.Fatalf("unsubscribed reaction should be acknowledged: %#v", apiErr)
	}
}

func TestResponseReceiptCarriesMessageRef(t *testing.T) {
	t.Parallel()
	response := &agentResponse{
		responseID: "resp_1",
		messageRef: "msgref_answer",
		revision:   1,
		phase:      AgentResponsePhaseStreaming,
	}
	receipt := receiptFor(response, false)
	if receipt.MessageRef != "msgref_answer" {
		t.Fatalf("message ref = %q", receipt.MessageRef)
	}
}
