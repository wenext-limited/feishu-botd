package service

import (
	"context"
	"testing"
	"time"

	"feishu-botd/internal/feishu"
	"feishu-botd/internal/ownership"
)

// threadTestService is a single-app service whose card sends land in thread
// "omt_card" whenever they report one, with a recording ownership store.
func threadTestService(t *testing.T) (*Service, *fakeAgentBackend, *fakeOwnerStore) {
	t.Helper()
	backend := newFakeAgentBackend()
	backend.threadID = "omt_card"
	svc := newAgentTestService(backend)
	owners := &fakeOwnerStore{owners: make(map[string]ownership.Owner)}
	svc.SetAgentOwnershipStore(owners)
	return svc, backend, owners
}

func seedThreadDelivery(t *testing.T, svc *Service, deliveryID string, metadata map[string]string) {
	t.Helper()
	sub := mustSubscribeAgent(t, svc, AgentSubscribeOptions{Provider: "agent", Commands: []string{"ask"}})
	mustDispatchAgentPrompt(t, svc, CommandInput{
		DeliveryID:     deliveryID,
		Command:        "ask",
		Text:           "what changed?",
		Prompt:         "ask what changed?",
		ConversationID: "conv_test",
		ChatAlias:      "ops",
		SenderID:       "ou_sender",
		ChatID:         "oc_private_route",
		Metadata:       metadata,
	})
	receiveAgentEvent(t, sub)
}

func startInThread(t *testing.T, svc *Service, deliveryID string) AgentResponseReceipt {
	t.Helper()
	receipt, apiErr := svc.StartAgentResponse(context.Background(), StartAgentResponseInput{
		Provider: "agent", DeliveryID: deliveryID, OperationID: "start-1",
		Content: AgentResponseContent{Markdown: "Working…"}, ReplyInThread: true,
	})
	if apiErr != nil {
		t.Fatalf("start agent response: %v", apiErr)
	}
	return receipt
}

func TestAgentStartInThreadOpensAndOwnsTheThread(t *testing.T) {
	svc, backend, owners := threadTestService(t)
	seedThreadDelivery(t, svc, "evt_thread", map[string]string{"chat_type": "group", "message_id": "om_question"})

	receipt := startInThread(t, svc, "evt_thread")

	send := backend.sentCards[0]
	if send.ReplyToMessageID != "om_question" || !send.ReplyInThread {
		t.Fatalf("card send = %#v, want a thread reply to the question", send)
	}
	threadRef := feishu.ThreadRefForApp("", "omt_card")
	if receipt.ThreadRef != threadRef {
		t.Fatalf("thread ref = %q, want %q", receipt.ThreadRef, threadRef)
	}
	if owner, ok := owners.owners[threadRef]; !ok || owner.Provider != "agent" {
		t.Fatalf("thread owner = %#v, %v; want agent", owner, ok)
	}
	if _, ok := owners.owners[receipt.MessageRef]; !ok {
		t.Fatal("the card itself must stay owned")
	}
}

func TestAgentStartInSomeoneElsesThreadDoesNotClaimIt(t *testing.T) {
	svc, backend, owners := threadTestService(t)
	seedThreadDelivery(t, svc, "evt_their_thread", map[string]string{
		"chat_type": "group", "message_id": "om_question", "thread_id": "omt_card", "root_id": "om_root",
	})

	receipt := startInThread(t, svc, "evt_their_thread")

	if !backend.sentCards[0].ReplyInThread {
		t.Fatal("a reply inside a thread should stay in it")
	}
	if receipt.ThreadRef == "" {
		t.Fatal("the provider is still told which thread holds its card")
	}
	if _, ok := owners.owners[receipt.ThreadRef]; ok {
		t.Fatal("a thread the card did not open must not become the provider's")
	}
}

func TestAgentStartInThreadIsIgnoredForADirectMessage(t *testing.T) {
	svc, backend, owners := threadTestService(t)
	backend.threadID = ""
	seedThreadDelivery(t, svc, "evt_dm", map[string]string{"chat_type": "p2p", "message_id": "om_question"})

	receipt := startInThread(t, svc, "evt_dm")

	if backend.sentCards[0].ReplyInThread {
		t.Fatal("a direct message must not be answered in a thread")
	}
	if receipt.ThreadRef != "" || len(owners.owners) != 1 {
		t.Fatalf("thread ref = %q, owners = %#v; want the card alone", receipt.ThreadRef, owners.owners)
	}
}

func TestAgentStartWithoutThreadKeepsTheQuoteReply(t *testing.T) {
	svc, backend, _ := threadTestService(t)
	backend.threadID = ""
	seedThreadDelivery(t, svc, "evt_plain", map[string]string{"chat_type": "group", "message_id": "om_question"})

	startAgentResponse(t, svc, "agent", "evt_plain", AgentResponseContent{Markdown: "Working…"})

	if backend.sentCards[0].ReplyInThread {
		t.Fatal("a provider that did not ask for a thread got one")
	}
}

func TestUnmentionedMessageInAnOwnedThreadRoutesToItsProvider(t *testing.T) {
	t.Parallel()
	svc, owners := replyRoutingService(t)
	threadRef := feishu.ThreadRefForApp("support", "omt_card")
	owners.owners[threadRef] = ownership.Owner{Provider: "nous", ExpiresAt: time.Now().Add(time.Hour).Unix()}
	nous := mustSubscribeAgent(t, svc, AgentSubscribeOptions{Provider: "nous", IncludeUnmatchedMessages: true})
	other := mustSubscribeAgent(t, svc, AgentSubscribeOptions{Provider: "other", IncludeUnmatchedMessages: true})

	// Inside a thread Feishu points a plain message at the thread's root: the
	// person's question, which nobody owns.
	in := groupReply("om_question", "delivery_in_thread", "and the other branch?", true)
	in.Metadata["thread_id"] = "omt_card"
	in.Metadata["root_id"] = "om_question"
	if _, apiErr := svc.DispatchCommand(context.Background(), in); apiErr != nil {
		t.Fatalf("dispatch thread reply: %v", apiErr)
	}

	event := receiveAgentEvent(t, nous)
	if event.Message == nil || event.Message.Text != in.Prompt || event.Message.ThreadRef != threadRef {
		t.Fatalf("thread reply event = %#v", event)
	}
	if event.Message.Addressing != AgentAddressingReply {
		t.Fatalf("addressing = %v, want reply", event.Message.Addressing)
	}
	assertNoAgentEvent(t, other)
}

func TestUnmentionedMessageInAnUnownedThreadReachesNobody(t *testing.T) {
	t.Parallel()
	svc, _ := replyRoutingService(t)
	nous := mustSubscribeAgent(t, svc, AgentSubscribeOptions{Provider: "nous", IncludeUnmatchedMessages: true})

	in := groupReply("om_question", "delivery_other_thread", "just chatting", true)
	in.Metadata["thread_id"] = "omt_people"
	if _, apiErr := svc.DispatchCommand(context.Background(), in); apiErr != nil {
		t.Fatalf("dispatch thread reply: %v", apiErr)
	}

	assertNoAgentEvent(t, nous)
}
