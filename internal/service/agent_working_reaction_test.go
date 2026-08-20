package service

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"feishu-botd/internal/config"
	"feishu-botd/internal/dedupe"
)

func newAgentTestServiceWithWorkingReaction(backend *fakeAgentBackend, emoji string) *Service {
	cfg := config.Config{
		AppID:       "cli_test",
		AppSecret:   "secret",
		Channels:    map[string]string{"ops": "oc_test", "ci": "oc_ci"},
		DedupeTTL:   time.Hour,
		SendTimeout: time.Second,
		AgentProviders: map[string]config.AgentProviderConfig{
			"agent": {WorkingReactionEmoji: emoji},
		},
	}
	return NewService(cfg, backend, dedupe.NewMemoryStore(time.Hour), slog.Default())
}

func newAgentTestServiceWithWorkingReactionOverrides(backend *fakeAgentBackend, emoji string, overrides map[string]string) *Service {
	cfg := config.Config{
		AppID:       "cli_test",
		AppSecret:   "secret",
		Channels:    map[string]string{"ops": "oc_test", "ci": "oc_ci"},
		DedupeTTL:   time.Hour,
		SendTimeout: time.Second,
		AgentProviders: map[string]config.AgentProviderConfig{
			"agent": {WorkingReactionEmoji: emoji, WorkingReactionOverrides: overrides},
		},
	}
	return NewService(cfg, backend, dedupe.NewMemoryStore(time.Hour), slog.Default())
}

func TestAgentWorkingReactionOverrideAppliesWhenSenderNameMatches(t *testing.T) {
	backend := newFakeAgentBackend()
	backend.contactNames = map[string]string{"ou_sender_1": "王鑫禹"}
	svc := newAgentTestServiceWithWorkingReactionOverrides(backend, "OnIt", map[string]string{"王鑫禹": "HEART"})
	mustSubscribeAgent(t, svc, AgentSubscribeOptions{Provider: "agent", Commands: []string{"ask"}})
	mustDispatchAgentPrompt(t, svc, CommandInput{
		DeliveryID: "evt_reaction_override", Command: "ask", Prompt: "ask", ChatAlias: "ops",
		SenderID: "ou_sender_1",
		Metadata: map[string]string{"message_id": "om_trigger"},
	})

	startAgentResponse(t, svc, "agent", "evt_reaction_override", AgentResponseContent{Markdown: "working"})

	if len(backend.addedReactions) != 1 {
		t.Fatalf("added reactions = %d, want 1", len(backend.addedReactions))
	}
	if got := backend.addedReactions[0].EmojiType; got != "HEART" {
		t.Fatalf("emoji = %q, want HEART", got)
	}
}

func TestAgentWorkingReactionOverrideFallsBackWhenNameDoesNotMatch(t *testing.T) {
	backend := newFakeAgentBackend()
	backend.contactNames = map[string]string{"ou_sender_2": "someone else"}
	svc := newAgentTestServiceWithWorkingReactionOverrides(backend, "OnIt", map[string]string{"王鑫禹": "HEART"})
	mustSubscribeAgent(t, svc, AgentSubscribeOptions{Provider: "agent", Commands: []string{"ask"}})
	mustDispatchAgentPrompt(t, svc, CommandInput{
		DeliveryID: "evt_reaction_nomatch", Command: "ask", Prompt: "ask", ChatAlias: "ops",
		SenderID: "ou_sender_2",
		Metadata: map[string]string{"message_id": "om_trigger"},
	})

	startAgentResponse(t, svc, "agent", "evt_reaction_nomatch", AgentResponseContent{Markdown: "working"})

	if len(backend.addedReactions) != 1 {
		t.Fatalf("added reactions = %d, want 1", len(backend.addedReactions))
	}
	if got := backend.addedReactions[0].EmojiType; got != "OnIt" {
		t.Fatalf("emoji = %q, want the provider default OnIt", got)
	}
}

func TestAgentWorkingReactionOverrideSkipsContactLookupWhenNoOverridesConfigured(t *testing.T) {
	backend := newFakeAgentBackend()
	svc := newAgentTestServiceWithWorkingReaction(backend, "OnIt") // no overrides configured
	mustSubscribeAgent(t, svc, AgentSubscribeOptions{Provider: "agent", Commands: []string{"ask"}})
	mustDispatchAgentPrompt(t, svc, CommandInput{
		DeliveryID: "evt_reaction_nooverride", Command: "ask", Prompt: "ask", ChatAlias: "ops",
		SenderID: "ou_sender_1",
		Metadata: map[string]string{"message_id": "om_trigger"},
	})

	startAgentResponse(t, svc, "agent", "evt_reaction_nooverride", AgentResponseContent{Markdown: "working"})

	if len(backend.contactCalls) != 0 {
		t.Fatalf("contact lookups = %d, want 0: no overrides configured means no reason to call Contact", len(backend.contactCalls))
	}
	if got := backend.addedReactions[0].EmojiType; got != "OnIt" {
		t.Fatalf("emoji = %q, want the provider default OnIt", got)
	}
}

func TestAgentWorkingReactionOverrideFallsBackOnContactLookupFailure(t *testing.T) {
	backend := newFakeAgentBackend()
	backend.contactErr = errors.New("missing scope")
	svc := newAgentTestServiceWithWorkingReactionOverrides(backend, "OnIt", map[string]string{"王鑫禹": "HEART"})
	mustSubscribeAgent(t, svc, AgentSubscribeOptions{Provider: "agent", Commands: []string{"ask"}})
	mustDispatchAgentPrompt(t, svc, CommandInput{
		DeliveryID: "evt_reaction_contactfail", Command: "ask", Prompt: "ask", ChatAlias: "ops",
		SenderID: "ou_sender_1",
		Metadata: map[string]string{"message_id": "om_trigger"},
	})

	receipt, apiErr := svc.StartAgentResponse(context.Background(), StartAgentResponseInput{
		Provider: "agent", DeliveryID: "evt_reaction_contactfail", OperationID: "start-1",
		Content: AgentResponseContent{Markdown: "working"},
	})
	if apiErr != nil {
		t.Fatalf("start agent response failed the RPC on a contact lookup failure: %v", apiErr)
	}
	if receipt.ResponseID == "" {
		t.Fatal("start agent response returned no response id")
	}
	if len(backend.addedReactions) != 1 {
		t.Fatalf("added reactions = %d, want 1", len(backend.addedReactions))
	}
	if got := backend.addedReactions[0].EmojiType; got != "OnIt" {
		t.Fatalf("emoji = %q, want the provider default OnIt when the lookup fails", got)
	}
}

func TestAgentWorkingReactionAddedAtStartAndRemovedAtFinish(t *testing.T) {
	backend := newFakeAgentBackend()
	svc := newAgentTestServiceWithWorkingReaction(backend, "OnIt")
	mustSubscribeAgent(t, svc, AgentSubscribeOptions{Provider: "agent", Commands: []string{"ask"}})
	mustDispatchAgentPrompt(t, svc, CommandInput{
		DeliveryID: "evt_reaction", Command: "ask", Prompt: "ask", ChatAlias: "ops",
		Metadata: map[string]string{"message_id": "om_trigger"},
	})

	receipt := startAgentResponse(t, svc, "agent", "evt_reaction", AgentResponseContent{Markdown: "working"})

	if len(backend.addedReactions) != 1 {
		t.Fatalf("added reactions = %d, want 1", len(backend.addedReactions))
	}
	added := backend.addedReactions[0]
	if added.MessageID != "om_trigger" || added.EmojiType != "OnIt" {
		t.Fatalf("added reaction = %#v, want message_id=om_trigger emoji_type=OnIt", added)
	}
	if len(backend.removedReactions) != 0 {
		t.Fatalf("removed reactions before finish = %d, want 0", len(backend.removedReactions))
	}

	if _, apiErr := svc.FinishAgentResponse(context.Background(), FinishAgentResponseInput{
		Provider: "agent", ResponseID: receipt.ResponseID, OperationID: "finish-1", ExpectedRevision: 1,
		Outcome: AgentResponseOutcomeCompleted, Markdown: "final",
	}); apiErr != nil {
		t.Fatalf("finish agent response: %v", apiErr)
	}

	if len(backend.removedReactions) != 1 {
		t.Fatalf("removed reactions = %d, want 1", len(backend.removedReactions))
	}
	removed := backend.removedReactions[0]
	if removed.MessageID != "om_trigger" || removed.ReactionID != backend.reactionID {
		t.Fatalf("removed reaction = %#v, want message_id=om_trigger reaction_id=%s", removed, backend.reactionID)
	}
}

func TestAgentWorkingReactionNotConfiguredIsANoOp(t *testing.T) {
	backend := newFakeAgentBackend()
	svc := newAgentTestService(backend) // no WorkingReactionEmoji configured for this provider
	mustSubscribeAgent(t, svc, AgentSubscribeOptions{Provider: "agent", Commands: []string{"ask"}})
	mustDispatchAgentPrompt(t, svc, CommandInput{
		DeliveryID: "evt_reaction_off", Command: "ask", Prompt: "ask", ChatAlias: "ops",
		Metadata: map[string]string{"message_id": "om_trigger"},
	})

	receipt := startAgentResponse(t, svc, "agent", "evt_reaction_off", AgentResponseContent{Markdown: "working"})
	if len(backend.addedReactions) != 0 {
		t.Fatalf("added reactions = %d, want 0 when no emoji is configured", len(backend.addedReactions))
	}

	if _, apiErr := svc.FinishAgentResponse(context.Background(), FinishAgentResponseInput{
		Provider: "agent", ResponseID: receipt.ResponseID, OperationID: "finish-1", ExpectedRevision: 1,
		Outcome: AgentResponseOutcomeCompleted, Markdown: "final",
	}); apiErr != nil {
		t.Fatalf("finish agent response: %v", apiErr)
	}
	if len(backend.removedReactions) != 0 {
		t.Fatalf("removed reactions = %d, want 0: nothing was ever added", len(backend.removedReactions))
	}
}

func TestAgentWorkingReactionWithoutOriginMessageIsANoOp(t *testing.T) {
	backend := newFakeAgentBackend()
	svc := newAgentTestServiceWithWorkingReaction(backend, "OnIt")
	mustSubscribeAgent(t, svc, AgentSubscribeOptions{Provider: "agent", Commands: []string{"ask"}})
	// No "message_id" in Metadata: nothing to react to.
	mustDispatchAgentPrompt(t, svc, CommandInput{
		DeliveryID: "evt_reaction_nomsg", Command: "ask", Prompt: "ask", ChatAlias: "ops",
	})

	startAgentResponse(t, svc, "agent", "evt_reaction_nomsg", AgentResponseContent{Markdown: "working"})
	if len(backend.addedReactions) != 0 {
		t.Fatalf("added reactions = %d, want 0 with no origin message", len(backend.addedReactions))
	}
}

func TestAgentWorkingReactionAddFailureDoesNotBlockStart(t *testing.T) {
	backend := newFakeAgentBackend()
	backend.addReactionErr = errors.New("boom")
	svc := newAgentTestServiceWithWorkingReaction(backend, "OnIt")
	mustSubscribeAgent(t, svc, AgentSubscribeOptions{Provider: "agent", Commands: []string{"ask"}})
	mustDispatchAgentPrompt(t, svc, CommandInput{
		DeliveryID: "evt_reaction_addfail", Command: "ask", Prompt: "ask", ChatAlias: "ops",
		Metadata: map[string]string{"message_id": "om_trigger"},
	})

	receipt, apiErr := svc.StartAgentResponse(context.Background(), StartAgentResponseInput{
		Provider: "agent", DeliveryID: "evt_reaction_addfail", OperationID: "start-1",
		Content: AgentResponseContent{Markdown: "working"},
	})
	if apiErr != nil {
		t.Fatalf("start agent response failed the RPC on a reaction add failure: %v", apiErr)
	}
	if receipt.ResponseID == "" {
		t.Fatal("start agent response returned no response id")
	}

	// No reaction was recorded as active, so finish must not attempt to
	// remove one either.
	if _, apiErr := svc.FinishAgentResponse(context.Background(), FinishAgentResponseInput{
		Provider: "agent", ResponseID: receipt.ResponseID, OperationID: "finish-1", ExpectedRevision: 1,
		Outcome: AgentResponseOutcomeCompleted, Markdown: "final",
	}); apiErr != nil {
		t.Fatalf("finish agent response: %v", apiErr)
	}
	if len(backend.removedReactions) != 0 {
		t.Fatalf("removed reactions = %d, want 0: nothing to remove after a failed add", len(backend.removedReactions))
	}
}

func TestAgentWorkingReactionRemoveFailureDoesNotBlockFinish(t *testing.T) {
	backend := newFakeAgentBackend()
	svc := newAgentTestServiceWithWorkingReaction(backend, "OnIt")
	mustSubscribeAgent(t, svc, AgentSubscribeOptions{Provider: "agent", Commands: []string{"ask"}})
	mustDispatchAgentPrompt(t, svc, CommandInput{
		DeliveryID: "evt_reaction_removefail", Command: "ask", Prompt: "ask", ChatAlias: "ops",
		Metadata: map[string]string{"message_id": "om_trigger"},
	})
	receipt := startAgentResponse(t, svc, "agent", "evt_reaction_removefail", AgentResponseContent{Markdown: "working"})
	backend.removeReactionErr = errors.New("boom")

	finished, apiErr := svc.FinishAgentResponse(context.Background(), FinishAgentResponseInput{
		Provider: "agent", ResponseID: receipt.ResponseID, OperationID: "finish-1", ExpectedRevision: 1,
		Outcome: AgentResponseOutcomeCompleted, Markdown: "final",
	})
	if apiErr != nil {
		t.Fatalf("finish agent response failed the RPC on a reaction remove failure: %v", apiErr)
	}
	if finished.Phase != AgentResponsePhaseCompleted {
		t.Fatalf("phase = %v, want completed despite the reaction remove failure", finished.Phase)
	}
}
