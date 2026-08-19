package service

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"feishu-botd/internal/feishu"
)

// cotEventContents decodes every event's Content field for assertions that
// care about the AG-UI payload shape, not just the event type.
func cotEventContents(t *testing.T, events []feishu.CoTEvent) []map[string]any {
	t.Helper()
	out := make([]map[string]any, len(events))
	for i, event := range events {
		var content map[string]any
		if err := json.Unmarshal([]byte(event.Content), &content); err != nil {
			t.Fatalf("event %d content = %q is not JSON: %v", i, event.Content, err)
		}
		out[i] = content
	}
	return out
}

func cotEventTypes(events []feishu.CoTEvent) []feishu.CoTEventType {
	out := make([]feishu.CoTEventType, len(events))
	for i, event := range events {
		out[i] = event.Type
	}
	return out
}

func TestAgentCoTStartCreatesRunAndOpensStartedSteps(t *testing.T) {
	backend := newFakeAgentBackend()
	svc := newAgentTestService(backend)
	mustSubscribeAgent(t, svc, AgentSubscribeOptions{Provider: "agent", Commands: []string{"ask"}})
	mustDispatchAgentPrompt(t, svc, CommandInput{
		DeliveryID: "evt_cot_start", Command: "ask", Prompt: "ask", ChatAlias: "ops",
		Metadata: map[string]string{"message_id": "om_trigger"},
	})

	receipt, apiErr := svc.StartAgentResponse(context.Background(), StartAgentResponseInput{
		Provider: "agent", DeliveryID: "evt_cot_start", OperationID: "start-1",
		Content: AgentResponseContent{
			Markdown: "working", TimelineMarkdown: "step 1", TimelineTitle: "step 1",
			TimelineSteps: []AgentTimelineStep{{StepID: "s1", Label: "识别问题", State: AgentTimelineStepStateStarted}},
		},
	})
	if apiErr != nil {
		t.Fatalf("start agent response: %v", apiErr)
	}
	if receipt.Revision != 1 {
		t.Fatalf("revision = %d, want 1", receipt.Revision)
	}

	if len(backend.cotCreates) != 1 {
		t.Fatalf("cot creates = %d, want 1", len(backend.cotCreates))
	}
	create := backend.cotCreates[0]
	if create.ChatID != "oc_test" || create.OriginMessageID != "om_trigger" {
		t.Fatalf("cot create = %#v, want chat_id=oc_test origin_message_id=om_trigger", create)
	}

	if len(backend.cotAppends) != 1 {
		t.Fatalf("cot appends = %d, want 1", len(backend.cotAppends))
	}
	append0 := backend.cotAppends[0]
	if append0.CoTID != backend.cotID || append0.MessageID != backend.cotMessageID {
		t.Fatalf("cot append handle = %#v", append0)
	}
	gotTypes := cotEventTypes(append0.Events)
	wantTypes := []feishu.CoTEventType{feishu.CoTEventRunStarted, feishu.CoTEventStepStarted}
	if len(gotTypes) != len(wantTypes) || gotTypes[0] != wantTypes[0] || gotTypes[1] != wantTypes[1] {
		t.Fatalf("start event types = %v, want %v", gotTypes, wantTypes)
	}
	contents := cotEventContents(t, append0.Events)
	if contents[0]["runId"] != receipt.ResponseID || contents[0]["threadId"] != receipt.ResponseID {
		t.Fatalf("RUN_STARTED content = %#v, want run/thread id %q", contents[0], receipt.ResponseID)
	}
	if contents[1]["stepId"] != "s1" || contents[1]["stepName"] != "识别问题" {
		t.Fatalf("STEP_STARTED content = %#v", contents[1])
	}

	if len(backend.cotCompletes) != 0 {
		t.Fatalf("cot completes = %d, want 0 before finish", len(backend.cotCompletes))
	}
}

func TestAgentCoTStartWithoutStepsSkipsCreate(t *testing.T) {
	backend := newFakeAgentBackend()
	svc := newAgentTestService(backend)
	mustSubscribeAgent(t, svc, AgentSubscribeOptions{Provider: "agent", Commands: []string{"ask"}})
	mustDispatchAgentPrompt(t, svc, CommandInput{
		DeliveryID: "evt_cot_nosteps", Command: "ask", Prompt: "ask", ChatAlias: "ops",
		Metadata: map[string]string{"message_id": "om_trigger"},
	})

	startAgentResponse(t, svc, "agent", "evt_cot_nosteps", AgentResponseContent{Markdown: "answer"})

	if len(backend.cotCreates) != 0 {
		t.Fatalf("cot creates = %d, want 0 for a response with no timeline steps", len(backend.cotCreates))
	}
}

func TestAgentCoTStartWithoutOriginMessageSkipsCreate(t *testing.T) {
	backend := newFakeAgentBackend()
	svc := newAgentTestService(backend)
	mustSubscribeAgent(t, svc, AgentSubscribeOptions{Provider: "agent", Commands: []string{"ask"}})
	// No "message_id" in Metadata: nothing for message_cot to bind to.
	mustDispatchAgentPrompt(t, svc, CommandInput{
		DeliveryID: "evt_cot_nomsg", Command: "ask", Prompt: "ask", ChatAlias: "ops",
	})

	_, apiErr := svc.StartAgentResponse(context.Background(), StartAgentResponseInput{
		Provider: "agent", DeliveryID: "evt_cot_nomsg", OperationID: "start-1",
		Content: AgentResponseContent{
			Markdown:      "answer",
			TimelineSteps: []AgentTimelineStep{{StepID: "s1", Label: "step", State: AgentTimelineStepStateStarted}},
		},
	})
	if apiErr != nil {
		t.Fatalf("start agent response: %v", apiErr)
	}
	if len(backend.cotCreates) != 0 {
		t.Fatalf("cot creates = %d, want 0 with no origin message to bind to", len(backend.cotCreates))
	}
}

func TestAgentCoTUpdateEmitsOnlyNewTransitions(t *testing.T) {
	backend := newFakeAgentBackend()
	svc := newAgentTestService(backend)
	mustSubscribeAgent(t, svc, AgentSubscribeOptions{Provider: "agent", Commands: []string{"ask"}})
	mustDispatchAgentPrompt(t, svc, CommandInput{
		DeliveryID: "evt_cot_update", Command: "ask", Prompt: "ask", ChatAlias: "ops",
		Metadata: map[string]string{"message_id": "om_trigger"},
	})
	receipt := startAgentResponse(t, svc, "agent", "evt_cot_update", AgentResponseContent{
		Markdown:      "working",
		TimelineSteps: []AgentTimelineStep{{StepID: "s1", Label: "step one", State: AgentTimelineStepStateStarted}},
	})
	backend.cotAppends = nil // isolate Update's own append call

	updated, apiErr := svc.UpdateAgentResponse(context.Background(), UpdateAgentResponseInput{
		Provider: "agent", ResponseID: receipt.ResponseID, OperationID: "update-1", ExpectedRevision: 1,
		Markdown: "still working",
		TimelineSteps: []AgentTimelineStep{
			{StepID: "s1", Label: "step one", State: AgentTimelineStepStateStarted}, // repeated verbatim: must not re-emit
			{StepID: "s1", Label: "step one", State: AgentTimelineStepStateFinished},
			{StepID: "s2", Label: "step two", State: AgentTimelineStepStateStarted},
		},
	})
	if apiErr != nil {
		t.Fatalf("update agent response: %v", apiErr)
	}
	if updated.Revision != 2 {
		t.Fatalf("revision = %d, want 2", updated.Revision)
	}

	if len(backend.cotAppends) != 1 {
		t.Fatalf("cot appends = %d, want 1", len(backend.cotAppends))
	}
	gotTypes := cotEventTypes(backend.cotAppends[0].Events)
	wantTypes := []feishu.CoTEventType{feishu.CoTEventStepFinished, feishu.CoTEventStepStarted}
	if len(gotTypes) != 2 || gotTypes[0] != wantTypes[0] || gotTypes[1] != wantTypes[1] {
		t.Fatalf("update event types = %v, want [s1 finished, s2 started] = %v", gotTypes, wantTypes)
	}
	contents := cotEventContents(t, backend.cotAppends[0].Events)
	if contents[0]["stepId"] != "s1" || contents[1]["stepId"] != "s2" {
		t.Fatalf("update event step ids = %#v", contents)
	}
}

func TestAgentCoTStepFirstSeenFinishedEmitsBothTransitions(t *testing.T) {
	backend := newFakeAgentBackend()
	svc := newAgentTestService(backend)
	mustSubscribeAgent(t, svc, AgentSubscribeOptions{Provider: "agent", Commands: []string{"ask"}})
	mustDispatchAgentPrompt(t, svc, CommandInput{
		DeliveryID: "evt_cot_fast", Command: "ask", Prompt: "ask", ChatAlias: "ops",
		Metadata: map[string]string{"message_id": "om_trigger"},
	})

	// A step whose very first appearance already reports Finished (it ran to
	// completion between two provider snapshots) must still open before it
	// closes: the CoT stream has no notion of a step that finishes unstarted.
	_, apiErr := svc.StartAgentResponse(context.Background(), StartAgentResponseInput{
		Provider: "agent", DeliveryID: "evt_cot_fast", OperationID: "start-1",
		Content: AgentResponseContent{
			Markdown:      "working",
			TimelineSteps: []AgentTimelineStep{{StepID: "fast", Label: "quick step", State: AgentTimelineStepStateFinished}},
		},
	})
	if apiErr != nil {
		t.Fatalf("start agent response: %v", apiErr)
	}

	if len(backend.cotAppends) != 1 {
		t.Fatalf("cot appends = %d, want 1", len(backend.cotAppends))
	}
	gotTypes := cotEventTypes(backend.cotAppends[0].Events)
	wantTypes := []feishu.CoTEventType{feishu.CoTEventRunStarted, feishu.CoTEventStepStarted, feishu.CoTEventStepFinished}
	if len(gotTypes) != 3 {
		t.Fatalf("start event types = %v, want %v", gotTypes, wantTypes)
	}
	for i := range wantTypes {
		if gotTypes[i] != wantTypes[i] {
			t.Fatalf("start event types = %v, want %v", gotTypes, wantTypes)
		}
	}
}

func TestAgentCoTFinishCompletedEmitsRunFinishedDone(t *testing.T) {
	backend := newFakeAgentBackend()
	svc := newAgentTestService(backend)
	mustSubscribeAgent(t, svc, AgentSubscribeOptions{Provider: "agent", Commands: []string{"ask"}})
	mustDispatchAgentPrompt(t, svc, CommandInput{
		DeliveryID: "evt_cot_finish", Command: "ask", Prompt: "ask", ChatAlias: "ops",
		Metadata: map[string]string{"message_id": "om_trigger"},
	})
	receipt := startAgentResponse(t, svc, "agent", "evt_cot_finish", AgentResponseContent{
		Markdown:      "working",
		TimelineSteps: []AgentTimelineStep{{StepID: "s1", Label: "step", State: AgentTimelineStepStateStarted}},
	})
	backend.cotAppends = nil

	finished, apiErr := svc.FinishAgentResponse(context.Background(), FinishAgentResponseInput{
		Provider: "agent", ResponseID: receipt.ResponseID, OperationID: "finish-1", ExpectedRevision: 1,
		Outcome:  AgentResponseOutcomeCompleted,
		Markdown: "final answer",
		TimelineSteps: []AgentTimelineStep{
			{StepID: "s1", Label: "step", State: AgentTimelineStepStateFinished},
		},
	})
	if apiErr != nil {
		t.Fatalf("finish agent response: %v", apiErr)
	}
	if finished.Phase != AgentResponsePhaseCompleted {
		t.Fatalf("phase = %v, want completed", finished.Phase)
	}

	if len(backend.cotAppends) != 1 {
		t.Fatalf("cot appends = %d, want 1", len(backend.cotAppends))
	}
	gotTypes := cotEventTypes(backend.cotAppends[0].Events)
	wantTypes := []feishu.CoTEventType{feishu.CoTEventStepFinished, feishu.CoTEventRunFinished}
	if len(gotTypes) != 2 || gotTypes[0] != wantTypes[0] || gotTypes[1] != wantTypes[1] {
		t.Fatalf("finish event types = %v, want %v", gotTypes, wantTypes)
	}
	contents := cotEventContents(t, backend.cotAppends[0].Events)
	if contents[1]["status"] != "done" {
		t.Fatalf("RUN_FINISHED content = %#v, want status=done", contents[1])
	}

	if len(backend.cotCompletes) != 1 {
		t.Fatalf("cot completes = %d, want 1", len(backend.cotCompletes))
	}
	if backend.cotCompletes[0].Reason != feishu.CoTOutcomeDone {
		t.Fatalf("complete reason = %q, want done", backend.cotCompletes[0].Reason)
	}
}

func TestAgentCoTFinishFailedOutcomeCompletesWithError(t *testing.T) {
	backend := newFakeAgentBackend()
	svc := newAgentTestService(backend)
	mustSubscribeAgent(t, svc, AgentSubscribeOptions{Provider: "agent", Commands: []string{"ask"}})
	mustDispatchAgentPrompt(t, svc, CommandInput{
		DeliveryID: "evt_cot_failfinish", Command: "ask", Prompt: "ask", ChatAlias: "ops",
		Metadata: map[string]string{"message_id": "om_trigger"},
	})
	receipt := startAgentResponse(t, svc, "agent", "evt_cot_failfinish", AgentResponseContent{
		Markdown:      "working",
		TimelineSteps: []AgentTimelineStep{{StepID: "s1", Label: "step", State: AgentTimelineStepStateStarted}},
	})
	backend.cotAppends = nil

	if _, apiErr := svc.FinishAgentResponse(context.Background(), FinishAgentResponseInput{
		Provider: "agent", ResponseID: receipt.ResponseID, OperationID: "finish-1", ExpectedRevision: 1,
		Outcome:  AgentResponseOutcomeFailed,
		Markdown: "could not finish",
	}); apiErr != nil {
		t.Fatalf("finish agent response: %v", apiErr)
	}

	if len(backend.cotCompletes) != 1 || backend.cotCompletes[0].Reason != feishu.CoTOutcomeError {
		t.Fatalf("cot completes = %#v, want one with reason=error", backend.cotCompletes)
	}
	contents := cotEventContents(t, backend.cotAppends[0].Events)
	last := contents[len(contents)-1]
	if last["status"] != "error" {
		t.Fatalf("RUN_FINISHED content = %#v, want status=error", last)
	}
}

func TestAgentCoTCreateFailureDoesNotBlockStart(t *testing.T) {
	backend := newFakeAgentBackend()
	backend.cotCreateErr = errors.New("boom")
	svc := newAgentTestService(backend)
	mustSubscribeAgent(t, svc, AgentSubscribeOptions{Provider: "agent", Commands: []string{"ask"}})
	mustDispatchAgentPrompt(t, svc, CommandInput{
		DeliveryID: "evt_cot_createfail", Command: "ask", Prompt: "ask", ChatAlias: "ops",
		Metadata: map[string]string{"message_id": "om_trigger"},
	})

	receipt, apiErr := svc.StartAgentResponse(context.Background(), StartAgentResponseInput{
		Provider: "agent", DeliveryID: "evt_cot_createfail", OperationID: "start-1",
		Content: AgentResponseContent{
			Markdown:      "answer",
			TimelineSteps: []AgentTimelineStep{{StepID: "s1", Label: "step", State: AgentTimelineStepStateStarted}},
		},
	})
	if apiErr != nil {
		t.Fatalf("start agent response failed the RPC on a CoT create failure: %v", apiErr)
	}

	// No append ever attempted: the response has no CoT to append to.
	if len(backend.cotAppends) != 0 {
		t.Fatalf("cot appends = %d, want 0 after a failed create", len(backend.cotAppends))
	}

	// A later Update/Finish must not attempt CoT calls either.
	if _, apiErr := svc.UpdateAgentResponse(context.Background(), UpdateAgentResponseInput{
		Provider: "agent", ResponseID: receipt.ResponseID, OperationID: "update-1", ExpectedRevision: 1,
		Markdown:      "still working",
		TimelineSteps: []AgentTimelineStep{{StepID: "s1", Label: "step", State: AgentTimelineStepStateFinished}},
	}); apiErr != nil {
		t.Fatalf("update agent response: %v", apiErr)
	}
	if len(backend.cotAppends) != 0 {
		t.Fatalf("cot appends = %d, want 0: a response with no CoT must never append", len(backend.cotAppends))
	}
}

func TestAgentCoTAppendFailureDoesNotBlockUpdate(t *testing.T) {
	backend := newFakeAgentBackend()
	svc := newAgentTestService(backend)
	mustSubscribeAgent(t, svc, AgentSubscribeOptions{Provider: "agent", Commands: []string{"ask"}})
	mustDispatchAgentPrompt(t, svc, CommandInput{
		DeliveryID: "evt_cot_appendfail", Command: "ask", Prompt: "ask", ChatAlias: "ops",
		Metadata: map[string]string{"message_id": "om_trigger"},
	})
	receipt := startAgentResponse(t, svc, "agent", "evt_cot_appendfail", AgentResponseContent{
		Markdown:      "working",
		TimelineSteps: []AgentTimelineStep{{StepID: "s1", Label: "step", State: AgentTimelineStepStateStarted}},
	})
	backend.cotAppendErr = errors.New("boom")

	if _, apiErr := svc.UpdateAgentResponse(context.Background(), UpdateAgentResponseInput{
		Provider: "agent", ResponseID: receipt.ResponseID, OperationID: "update-1", ExpectedRevision: 1,
		Markdown:      "still working",
		TimelineSteps: []AgentTimelineStep{{StepID: "s1", Label: "step", State: AgentTimelineStepStateFinished}},
	}); apiErr != nil {
		t.Fatalf("update agent response failed the RPC on a CoT append failure: %v", apiErr)
	}
}

func TestAgentCoTCompleteFailureDoesNotBlockFinish(t *testing.T) {
	backend := newFakeAgentBackend()
	svc := newAgentTestService(backend)
	mustSubscribeAgent(t, svc, AgentSubscribeOptions{Provider: "agent", Commands: []string{"ask"}})
	mustDispatchAgentPrompt(t, svc, CommandInput{
		DeliveryID: "evt_cot_completefail", Command: "ask", Prompt: "ask", ChatAlias: "ops",
		Metadata: map[string]string{"message_id": "om_trigger"},
	})
	receipt := startAgentResponse(t, svc, "agent", "evt_cot_completefail", AgentResponseContent{
		Markdown:      "working",
		TimelineSteps: []AgentTimelineStep{{StepID: "s1", Label: "step", State: AgentTimelineStepStateStarted}},
	})
	backend.cotCompleteErr = errors.New("boom")

	finished, apiErr := svc.FinishAgentResponse(context.Background(), FinishAgentResponseInput{
		Provider: "agent", ResponseID: receipt.ResponseID, OperationID: "finish-1", ExpectedRevision: 1,
		Outcome: AgentResponseOutcomeCompleted, Markdown: "final",
	})
	if apiErr != nil {
		t.Fatalf("finish agent response failed the RPC on a CoT complete failure: %v", apiErr)
	}
	if finished.Phase != AgentResponsePhaseCompleted {
		t.Fatalf("phase = %v, want completed despite the CoT complete failure", finished.Phase)
	}
}
