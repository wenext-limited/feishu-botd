package service

import (
	"context"
	"errors"
	"testing"
)

func startAgentReactionFixture(t *testing.T, svc *Service, backend *fakeAgentBackend, deliveryID string) AgentResponseReceipt {
	t.Helper()
	mustSubscribeAgent(t, svc, AgentSubscribeOptions{Provider: "agent", Commands: []string{"ask"}})
	mustDispatchAgentPrompt(t, svc, CommandInput{
		DeliveryID: deliveryID, Command: "ask", Prompt: "ask", ChatAlias: "ops",
		Metadata: map[string]string{"message_id": "om_trigger_" + deliveryID},
	})
	return startAgentResponse(t, svc, "agent", deliveryID, AgentResponseContent{Markdown: "working"})
}

func TestAddAgentReactionPlacesTheChosenEmojiOnTheTriggeringMessage(t *testing.T) {
	backend := newFakeAgentBackend()
	svc := newAgentTestService(backend)
	receipt := startAgentReactionFixture(t, svc, backend, "evt_reaction_freeform")

	duplicate, apiErr := svc.AddAgentReaction(context.Background(), AddAgentReactionInput{
		Provider: "agent", ResponseID: receipt.ResponseID, OperationID: "react-1", EmojiType: "HEART",
	})
	if apiErr != nil {
		t.Fatalf("add agent reaction: %v", apiErr)
	}
	if duplicate {
		t.Fatal("first reaction reported as duplicate")
	}
	if len(backend.addedReactions) != 1 {
		t.Fatalf("added reactions = %d, want 1", len(backend.addedReactions))
	}
	added := backend.addedReactions[0]
	if added.MessageID != "om_trigger_evt_reaction_freeform" || added.EmojiType != "HEART" {
		t.Fatalf("added reaction = %#v", added)
	}
}

func TestAddAgentReactionIsIdempotentPerOperationID(t *testing.T) {
	backend := newFakeAgentBackend()
	svc := newAgentTestService(backend)
	receipt := startAgentReactionFixture(t, svc, backend, "evt_reaction_idempotent")

	in := AddAgentReactionInput{Provider: "agent", ResponseID: receipt.ResponseID, OperationID: "react-1", EmojiType: "HEART"}
	if duplicate, apiErr := svc.AddAgentReaction(context.Background(), in); apiErr != nil || duplicate {
		t.Fatalf("first call: duplicate=%v apiErr=%v", duplicate, apiErr)
	}
	duplicate, apiErr := svc.AddAgentReaction(context.Background(), in)
	if apiErr != nil {
		t.Fatalf("replay: %v", apiErr)
	}
	if !duplicate {
		t.Fatal("replayed operation id was not reported as a duplicate")
	}
	if len(backend.addedReactions) != 1 {
		t.Fatalf("added reactions = %d, want 1 (no re-add on replay)", len(backend.addedReactions))
	}
}

func TestAddAgentReactionRejectsOperationIDReuseWithADifferentEmoji(t *testing.T) {
	backend := newFakeAgentBackend()
	svc := newAgentTestService(backend)
	receipt := startAgentReactionFixture(t, svc, backend, "evt_reaction_conflict")

	first := AddAgentReactionInput{Provider: "agent", ResponseID: receipt.ResponseID, OperationID: "react-1", EmojiType: "HEART"}
	if _, apiErr := svc.AddAgentReaction(context.Background(), first); apiErr != nil {
		t.Fatalf("first call: %v", apiErr)
	}
	conflicting := first
	conflicting.EmojiType = "OnIt"
	_, apiErr := svc.AddAgentReaction(context.Background(), conflicting)
	if apiErr == nil || apiErr.Code != "operation_conflict" {
		t.Fatalf("conflicting reuse apiErr = %#v, want operation_conflict", apiErr)
	}
	if len(backend.addedReactions) != 1 {
		t.Fatalf("added reactions = %d, want 1", len(backend.addedReactions))
	}
}

func TestAddAgentReactionRequiresEmojiType(t *testing.T) {
	backend := newFakeAgentBackend()
	svc := newAgentTestService(backend)
	receipt := startAgentReactionFixture(t, svc, backend, "evt_reaction_missing_emoji")

	_, apiErr := svc.AddAgentReaction(context.Background(), AddAgentReactionInput{
		Provider: "agent", ResponseID: receipt.ResponseID, OperationID: "react-1",
	})
	if apiErr == nil || apiErr.Code != "missing_emoji_type" {
		t.Fatalf("apiErr = %#v, want missing_emoji_type", apiErr)
	}
	if len(backend.addedReactions) != 0 {
		t.Fatalf("added reactions = %d, want 0", len(backend.addedReactions))
	}
}

func TestAddAgentReactionRejectsAnUnknownResponse(t *testing.T) {
	backend := newFakeAgentBackend()
	svc := newAgentTestService(backend)

	_, apiErr := svc.AddAgentReaction(context.Background(), AddAgentReactionInput{
		Provider: "agent", ResponseID: "resp_does_not_exist", OperationID: "react-1", EmojiType: "HEART",
	})
	if apiErr == nil || apiErr.Code != "unknown_response" {
		t.Fatalf("apiErr = %#v, want unknown_response", apiErr)
	}
}

func TestAddAgentReactionPropagatesAFeishuRejection(t *testing.T) {
	backend := newFakeAgentBackend()
	backend.addReactionErr = errors.New("boom")
	svc := newAgentTestService(backend)
	receipt := startAgentReactionFixture(t, svc, backend, "evt_reaction_feishu_fail")

	_, apiErr := svc.AddAgentReaction(context.Background(), AddAgentReactionInput{
		Provider: "agent", ResponseID: receipt.ResponseID, OperationID: "react-1", EmojiType: "HEART",
	})
	if apiErr == nil {
		t.Fatal("want an error when Feishu rejects the reaction")
	}
}

// TestAddAgentReactionSurvivesFinish proves the key behavioral difference from
// the daemon-driven working reaction: a provider-chosen reaction is never
// auto-removed when the response closes, because it is a deliberate answer
// from the model, not a busy signal.
func TestAddAgentReactionSurvivesFinish(t *testing.T) {
	backend := newFakeAgentBackend()
	svc := newAgentTestService(backend)
	receipt := startAgentReactionFixture(t, svc, backend, "evt_reaction_survives_finish")

	if _, apiErr := svc.AddAgentReaction(context.Background(), AddAgentReactionInput{
		Provider: "agent", ResponseID: receipt.ResponseID, OperationID: "react-1", EmojiType: "HEART",
	}); apiErr != nil {
		t.Fatalf("add agent reaction: %v", apiErr)
	}

	if _, apiErr := svc.FinishAgentResponse(context.Background(), FinishAgentResponseInput{
		Provider: "agent", ResponseID: receipt.ResponseID, OperationID: "finish-1", ExpectedRevision: 1,
		Outcome: AgentResponseOutcomeCompleted, Markdown: "final",
	}); apiErr != nil {
		t.Fatalf("finish agent response: %v", apiErr)
	}

	if len(backend.removedReactions) != 0 {
		t.Fatalf("removed reactions after finish = %d, want 0: a provider-chosen reaction must outlive the response", len(backend.removedReactions))
	}
}
