package service

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"feishu-botd/internal/feishu"
)

// startAgentResponseWithCoT mirrors startAgentResponse but grants the CoT
// capability grpcapi would otherwise attach from the authenticated
// principal — every test in this file is specifically about CoT behavior, so
// it needs the grant on to exercise anything past the boundary check.
func startAgentResponseWithCoT(t *testing.T, svc *Service, provider, deliveryID string, content AgentResponseContent) AgentResponseReceipt {
	t.Helper()
	receipt, apiErr := svc.StartAgentResponse(context.Background(), StartAgentResponseInput{
		Provider: provider, DeliveryID: deliveryID, OperationID: "start-1", Content: content,
		AllowCoTProgress: true,
	})
	if apiErr != nil {
		t.Fatalf("start agent response: %v", apiErr)
	}
	return receipt
}

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
		AllowCoTProgress: true,
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

// TestAgentCoTStartWithoutStepsStillCreatesEagerly pins the fix for the bug
// found live: a provider cannot know at Start time whether it will run any
// tools, so gating creation on Start's own (necessarily empty) snapshot meant
// no CoT was ever created for such a provider. Creation is now unconditional
// whenever there is an origin message to bind to, opening with RUN_STARTED
// alone when there are no steps yet.
func TestAgentCoTStartWithoutStepsStillCreatesEagerly(t *testing.T) {
	backend := newFakeAgentBackend()
	svc := newAgentTestService(backend)
	mustSubscribeAgent(t, svc, AgentSubscribeOptions{Provider: "agent", Commands: []string{"ask"}})
	mustDispatchAgentPrompt(t, svc, CommandInput{
		DeliveryID: "evt_cot_nosteps", Command: "ask", Prompt: "ask", ChatAlias: "ops",
		Metadata: map[string]string{"message_id": "om_trigger"},
	})

	startAgentResponseWithCoT(t, svc, "agent", "evt_cot_nosteps", AgentResponseContent{Markdown: "answer"})

	if len(backend.cotCreates) != 1 {
		t.Fatalf("cot creates = %d, want 1 even with no timeline steps yet", len(backend.cotCreates))
	}
	if len(backend.cotAppends) != 1 {
		t.Fatalf("cot appends = %d, want 1", len(backend.cotAppends))
	}
	gotTypes := cotEventTypes(backend.cotAppends[0].Events)
	if len(gotTypes) != 1 || gotTypes[0] != feishu.CoTEventRunStarted {
		t.Fatalf("opening events = %v, want just [RUN_STARTED]", gotTypes)
	}
}

// TestAgentCoTCreationPrecedesTheCardSend is the ordering guarantee this
// whole redesign exists for: the CoT message must have an earlier timestamp
// than the card, so it always appears first in the chat. That requires
// Create to actually be called before SendCard, not merely before the
// response object exists.
func TestAgentCoTCreationPrecedesTheCardSend(t *testing.T) {
	backend := newFakeAgentBackend()
	svc := newAgentTestService(backend)
	mustSubscribeAgent(t, svc, AgentSubscribeOptions{Provider: "agent", Commands: []string{"ask"}})
	mustDispatchAgentPrompt(t, svc, CommandInput{
		DeliveryID: "evt_cot_order", Command: "ask", Prompt: "ask", ChatAlias: "ops",
		Metadata: map[string]string{"message_id": "om_trigger"},
	})

	startAgentResponseWithCoT(t, svc, "agent", "evt_cot_order", AgentResponseContent{Markdown: "answer"})

	cotIndex, cardIndex := -1, -1
	for i, call := range backend.callOrder {
		switch call {
		case "cot_create":
			if cotIndex == -1 {
				cotIndex = i
			}
		case "send_card":
			if cardIndex == -1 {
				cardIndex = i
			}
		}
	}
	if cotIndex == -1 || cardIndex == -1 {
		t.Fatalf("call order = %v, want both cot_create and send_card", backend.callOrder)
	}
	if cotIndex >= cardIndex {
		t.Fatalf("call order = %v, want cot_create before send_card", backend.callOrder)
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
		AllowCoTProgress: true,
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
	receipt := startAgentResponseWithCoT(t, svc, "agent", "evt_cot_update", AgentResponseContent{
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
		AllowCoTProgress: true,
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
	receipt := startAgentResponseWithCoT(t, svc, "agent", "evt_cot_finish", AgentResponseContent{
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

	// The step diff and RUN_FINISHED travel as two separate append calls —
	// advanceAgentCoT settles the outstanding steps first, then
	// finishAgentCoT closes the run — rather than one batched call.
	if len(backend.cotAppends) != 2 {
		t.Fatalf("cot appends = %d, want 2: %#v", len(backend.cotAppends), backend.cotAppends)
	}
	stepAppend := cotEventTypes(backend.cotAppends[0].Events)
	if len(stepAppend) != 1 || stepAppend[0] != feishu.CoTEventStepFinished {
		t.Fatalf("first append = %v, want [STEP_FINISHED]", stepAppend)
	}
	runAppend := cotEventTypes(backend.cotAppends[1].Events)
	if len(runAppend) != 1 || runAppend[0] != feishu.CoTEventRunFinished {
		t.Fatalf("second append = %v, want [RUN_FINISHED]", runAppend)
	}
	runContent := cotEventContents(t, backend.cotAppends[1].Events)
	if runContent[0]["status"] != "done" {
		t.Fatalf("RUN_FINISHED content = %#v, want status=done", runContent[0])
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
	receipt := startAgentResponseWithCoT(t, svc, "agent", "evt_cot_failfinish", AgentResponseContent{
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
		AllowCoTProgress: true,
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
	receipt := startAgentResponseWithCoT(t, svc, "agent", "evt_cot_appendfail", AgentResponseContent{
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
	receipt := startAgentResponseWithCoT(t, svc, "agent", "evt_cot_completefail", AgentResponseContent{
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

// TestAgentCoTCreatesLazilyOnFirstStepsWhereverTheyArrive reproduces the real
// production shape: a provider (ibot) cannot know at Start time whether it
// will run any tools, so Start always carries an empty step snapshot and the
// first steps only show up on a later Update. A prior version of this wiring
// only ever attempted Create from Start, so this response's CoT was silently
// never created — no error, no log line, nothing — across an entire live
// deployment before it was caught. This pins the fix: Create must be
// attempted the first time ANY call carries a non-empty snapshot.
// TestAgentCoTOpensAtStartAndStepsStreamInViaUpdate proves the create/diff
// split across the two calls: Start opens the CoT eagerly (RUN_STARTED
// alone, since nothing has run yet), and the first Update reporting a real
// step appends to that SAME CoT rather than creating a second one.
func TestAgentCoTOpensAtStartAndStepsStreamInViaUpdate(t *testing.T) {
	backend := newFakeAgentBackend()
	svc := newAgentTestService(backend)
	mustSubscribeAgent(t, svc, AgentSubscribeOptions{Provider: "agent", Commands: []string{"ask"}})
	mustDispatchAgentPrompt(t, svc, CommandInput{
		DeliveryID: "evt_cot_lazy", Command: "ask", Prompt: "ask", ChatAlias: "ops",
		Metadata: map[string]string{"message_id": "om_trigger"},
	})

	// Start carries no steps at all — nothing has run yet — but still opens
	// the CoT immediately.
	receipt := startAgentResponseWithCoT(t, svc, "agent", "evt_cot_lazy", AgentResponseContent{Markdown: "working"})
	if len(backend.cotCreates) != 1 {
		t.Fatalf("cot creates after a step-less start = %d, want 1 (eager)", len(backend.cotCreates))
	}
	if create := backend.cotCreates[0]; create.ChatID != "oc_test" || create.OriginMessageID != "om_trigger" {
		t.Fatalf("cot create = %#v, want the ids resolved at start", create)
	}

	if _, apiErr := svc.UpdateAgentResponse(context.Background(), UpdateAgentResponseInput{
		Provider: "agent", ResponseID: receipt.ResponseID, OperationID: "update-1", ExpectedRevision: 1,
		Markdown:      "still working",
		TimelineSteps: []AgentTimelineStep{{StepID: "s1", Label: "step one", State: AgentTimelineStepStateStarted}},
	}); apiErr != nil {
		t.Fatalf("update agent response: %v", apiErr)
	}

	if len(backend.cotCreates) != 1 {
		t.Fatalf("cot creates after update = %d, want still 1: a step must not open a second CoT", len(backend.cotCreates))
	}
	if len(backend.cotAppends) != 2 {
		t.Fatalf("cot appends = %d, want 2: [RUN_STARTED] from start, [STEP_STARTED] from update", len(backend.cotAppends))
	}
	startTypes := cotEventTypes(backend.cotAppends[0].Events)
	if len(startTypes) != 1 || startTypes[0] != feishu.CoTEventRunStarted {
		t.Fatalf("start's append = %v, want [RUN_STARTED]", startTypes)
	}
	updateTypes := cotEventTypes(backend.cotAppends[1].Events)
	if len(updateTypes) != 1 || updateTypes[0] != feishu.CoTEventStepStarted {
		t.Fatalf("update's append = %v, want [STEP_STARTED]", updateTypes)
	}

	// Finish must complete the CoT Start opened.
	if _, apiErr := svc.FinishAgentResponse(context.Background(), FinishAgentResponseInput{
		Provider: "agent", ResponseID: receipt.ResponseID, OperationID: "finish-1", ExpectedRevision: 2,
		Outcome:  AgentResponseOutcomeCompleted,
		Markdown: "final answer",
		TimelineSteps: []AgentTimelineStep{
			{StepID: "s1", Label: "step one", State: AgentTimelineStepStateFinished},
		},
	}); apiErr != nil {
		t.Fatalf("finish agent response: %v", apiErr)
	}
	if len(backend.cotCompletes) != 1 {
		t.Fatalf("cot completes = %d, want 1", len(backend.cotCompletes))
	}
}

// TestAgentCoTFinishCreatesLazilyForARunTooShortToCoalesceAnUpdate covers the
// even narrower case: a run whose only steps ever reported arrive in the
// Finish call itself, because it finished before the update coalescer's
// timer fired even once.
// TestAgentCoTFinishClosesARunTooShortToCoalesceAnUpdate covers a run whose
// only steps ever reported arrive in the Finish call itself, because it
// finished before the update coalescer's timer fired even once. Start still
// opened the CoT eagerly (with zero steps), so Finish's job here is to fold
// the run's only step in and close it — not to create anything.
func TestAgentCoTFinishClosesARunTooShortToCoalesceAnUpdate(t *testing.T) {
	backend := newFakeAgentBackend()
	svc := newAgentTestService(backend)
	mustSubscribeAgent(t, svc, AgentSubscribeOptions{Provider: "agent", Commands: []string{"ask"}})
	mustDispatchAgentPrompt(t, svc, CommandInput{
		DeliveryID: "evt_cot_instant", Command: "ask", Prompt: "ask", ChatAlias: "ops",
		Metadata: map[string]string{"message_id": "om_trigger"},
	})
	receipt := startAgentResponseWithCoT(t, svc, "agent", "evt_cot_instant", AgentResponseContent{Markdown: "working"})
	if len(backend.cotCreates) != 1 {
		t.Fatalf("cot creates after start = %d, want 1 (eager)", len(backend.cotCreates))
	}

	if _, apiErr := svc.FinishAgentResponse(context.Background(), FinishAgentResponseInput{
		Provider: "agent", ResponseID: receipt.ResponseID, OperationID: "finish-1", ExpectedRevision: 1,
		Outcome:  AgentResponseOutcomeCompleted,
		Markdown: "answered",
		TimelineSteps: []AgentTimelineStep{
			{StepID: "s1", Label: "the only step", State: AgentTimelineStepStateFinished},
		},
	}); apiErr != nil {
		t.Fatalf("finish agent response: %v", apiErr)
	}

	if len(backend.cotCreates) != 1 {
		t.Fatalf("cot creates after finish = %d, want still 1: finish must not create a second CoT", len(backend.cotCreates))
	}
	if len(backend.cotCompletes) != 1 {
		t.Fatalf("cot completes = %d, want 1", len(backend.cotCompletes))
	}
}

// TestAgentCoTNeverRetriesAFailedLazyCreate proves cotAttempted latches
// across calls, not just within one: a create failure on Update must not be
// retried on the following Finish.
// TestAgentCoTNeverRetriesAFailedCreate proves cotAttempted latches across
// every remaining call in a response's lifetime, not just within one: the
// eager attempt at Start fails, and neither Update nor Finish tries again.
func TestAgentCoTNeverRetriesAFailedCreate(t *testing.T) {
	backend := newFakeAgentBackend()
	backend.cotCreateErr = errors.New("boom")
	svc := newAgentTestService(backend)
	mustSubscribeAgent(t, svc, AgentSubscribeOptions{Provider: "agent", Commands: []string{"ask"}})
	mustDispatchAgentPrompt(t, svc, CommandInput{
		DeliveryID: "evt_cot_retry", Command: "ask", Prompt: "ask", ChatAlias: "ops",
		Metadata: map[string]string{"message_id": "om_trigger"},
	})
	receipt := startAgentResponseWithCoT(t, svc, "agent", "evt_cot_retry", AgentResponseContent{Markdown: "working"})
	if len(backend.cotCreates) != 1 {
		t.Fatalf("cot creates after start's failing eager attempt = %d, want 1", len(backend.cotCreates))
	}

	if _, apiErr := svc.UpdateAgentResponse(context.Background(), UpdateAgentResponseInput{
		Provider: "agent", ResponseID: receipt.ResponseID, OperationID: "update-1", ExpectedRevision: 1,
		Markdown:      "still working",
		TimelineSteps: []AgentTimelineStep{{StepID: "s1", Label: "step", State: AgentTimelineStepStateStarted}},
	}); apiErr != nil {
		t.Fatalf("update agent response: %v", apiErr)
	}
	if len(backend.cotCreates) != 1 {
		t.Fatalf("cot creates after update = %d, want still 1: update must not retry start's failed attempt", len(backend.cotCreates))
	}

	if _, apiErr := svc.FinishAgentResponse(context.Background(), FinishAgentResponseInput{
		Provider: "agent", ResponseID: receipt.ResponseID, OperationID: "finish-1", ExpectedRevision: 2,
		Outcome:  AgentResponseOutcomeCompleted,
		Markdown: "final",
		TimelineSteps: []AgentTimelineStep{
			{StepID: "s1", Label: "step", State: AgentTimelineStepStateFinished},
			{StepID: "s2", Label: "another step", State: AgentTimelineStepStateStarted},
		},
	}); apiErr != nil {
		t.Fatalf("finish agent response: %v", apiErr)
	}
	if len(backend.cotCreates) != 1 {
		t.Fatalf("cot creates after finish = %d, want still 1: a failed create must never retry", len(backend.cotCreates))
	}
	if len(backend.cotCompletes) != 0 {
		t.Fatalf("cot completes = %d, want 0: there is no CoT to complete", len(backend.cotCompletes))
	}
}
