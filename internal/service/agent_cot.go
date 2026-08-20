package service

import (
	"context"
	"errors"
	"strings"
	"time"

	"feishu-botd/internal/feishu"
)

// cotRunID is stable for a response's whole lifetime and doubles as both the
// AG-UI thread and run id. It is already an opaque, provider-facing handle,
// so reusing it here keeps raw Feishu identifiers out of the CoT event stream
// exactly like everywhere else in this response's contract.
func cotRunID(response *agentResponse) string {
	return response.responseID
}

// advanceAgentCoT gets a response's CoT to reflect a new cumulative step
// snapshot: creates it on first contact with a non-empty snapshot, or diffs
// it against the last one once it is already active. Exactly one of the two
// happens per call.
//
// Creation is deliberately NOT tied to Start. A provider cannot know at Start
// time whether its run will call any tools — that is discovered mid-run — so
// gating creation on Start's own steps (the first cut of this) meant no
// provider whose tools start after its first card write could ever get a
// CoT: Start always carried an empty snapshot, and nothing downstream ever
// looked again. Every call that carries a non-empty snapshot is now a
// creation opportunity, whichever one turns out to be first — Start, an
// early Update, or even Finish for a run too short to coalesce an Update at
// all. response.cotChatID/cotOriginMessageID are captured once, at Start,
// specifically so a later call still has something to bind the create to.
func (s *Service) advanceAgentCoT(
	ctx context.Context,
	cotMessages feishu.CoTMessages,
	response *agentResponse,
	steps []AgentTimelineStep,
) {
	if cotMessages == nil || len(steps) == 0 {
		return
	}
	if response.cotStepFinished != nil {
		events := s.agentCoTStepEvents(response, steps, time.Now())
		s.appendAgentCoTEvents(ctx, cotMessages, response, events)
		return
	}
	// Already tried once for this response and it did not stick (create
	// failed, or there was no origin message to bind to) — never retry.
	if response.cotAttempted {
		return
	}
	s.startAgentCoT(ctx, cotMessages, response, steps)
}

// startAgentCoT creates the native Feishu CoT progress message, bound to the
// message that triggered the run, and opens it with RUN_STARTED plus
// whatever steps the triggering snapshot already carries. Called at most
// once per response — every path into it already checked cotAttempted is
// false — and always marks the attempt made, win or lose, so
// advanceAgentCoT never calls it twice.
func (s *Service) startAgentCoT(
	ctx context.Context,
	cotMessages feishu.CoTMessages,
	response *agentResponse,
	steps []AgentTimelineStep,
) {
	response.cotAttempted = true
	// cotChatID/cotOriginMessageID are empty for a delivery with no
	// triggering message to reply to (e.g. a top-level send to a channel).
	// message_cot has no unbound form, so there is nothing useful to
	// attempt — and no warning worth logging for an entirely predictable
	// case that will never change for this response.
	if response.cotChatID == "" || response.cotOriginMessageID == "" {
		return
	}
	cotID, cotMessageID, err := cotMessages.Create(ctx, feishu.CoTCreateRequest{
		ChatID:          response.cotChatID,
		OriginMessageID: response.cotOriginMessageID,
	})
	if err != nil {
		s.logAgentCoTFailure("cot create", response.responseID, err)
		return
	}
	response.cotID = cotID
	response.cotMessageID = cotMessageID
	response.cotStepFinished = make(map[string]bool, len(steps))

	now := time.Now()
	events := make([]feishu.CoTEvent, 0, len(steps)+1)
	if run, runErr := feishu.NewCoTRunStartedEvent(cotRunID(response), cotRunID(response), now); runErr == nil {
		events = append(events, run)
	} else {
		s.logAgentCoTFailure("cot run started", response.responseID, runErr)
	}
	events = append(events, s.agentCoTStepEvents(response, steps, now)...)
	s.appendAgentCoTEvents(ctx, cotMessages, response, events)
}

// agentCoTStepEvents builds the events for steps whose reported state moved
// since the last snapshot, and commits those transitions to
// response.cotStepFinished regardless of whether the later append succeeds:
// bookkeeping only ever moves forward, matching the CoT surface's own
// best-effort contract, where a dropped event is a rendering gap on Feishu's
// side and never a reason to resend or block the caller.
//
// A step seen for the first time always opens with STEP_STARTED, even when
// the snapshot already reports it Finished — the CoT stream has no notion of
// a step that finishes without starting, so a step that ran to completion
// between two snapshots gets both events, timestamped a millisecond apart to
// keep them strictly ordered in one batch.
func (s *Service) agentCoTStepEvents(response *agentResponse, steps []AgentTimelineStep, at time.Time) []feishu.CoTEvent {
	events := make([]feishu.CoTEvent, 0, len(steps))
	for _, step := range steps {
		stepID := strings.TrimSpace(step.StepID)
		label := strings.TrimSpace(step.Label)
		if stepID == "" || label == "" || step.State == AgentTimelineStepStateUnspecified {
			continue
		}
		finished, seen := response.cotStepFinished[stepID]
		if !seen {
			response.cotStepFinished[stepID] = false
			if event, err := feishu.NewCoTStepStartedEvent(stepID, label, at); err == nil {
				events = append(events, event)
			} else {
				s.logAgentCoTFailure("cot step started", response.responseID, err)
			}
			finished = false
		}
		if step.State == AgentTimelineStepStateFinished && !finished {
			response.cotStepFinished[stepID] = true
			if event, err := feishu.NewCoTStepFinishedEvent(stepID, label, at.Add(time.Millisecond)); err == nil {
				events = append(events, event)
			} else {
				s.logAgentCoTFailure("cot step finished", response.responseID, err)
			}
		}
	}
	return events
}

// appendAgentCoTEvents pushes a built event batch, logging rather than
// failing the caller's operation on any error — the CoT surface is strictly
// supplementary to the card the response actually delivers.
func (s *Service) appendAgentCoTEvents(ctx context.Context, cotMessages feishu.CoTMessages, response *agentResponse, events []feishu.CoTEvent) {
	if len(events) == 0 {
		return
	}
	if err := cotMessages.AppendEvents(ctx, feishu.CoTAppendRequest{
		CoTID: response.cotID, MessageID: response.cotMessageID, Events: events,
	}); err != nil {
		s.logAgentCoTFailure("cot append", response.responseID, err)
	}
}

// finishAgentCoT closes an active CoT: RUN_FINISHED, then Complete with the
// outcome mapped to the CoT's own done/error vocabulary. Any step transitions
// in Finish's own snapshot must already be applied — the caller runs
// advanceAgentCoT first, exactly as an Update does, so a run too short to
// coalesce even one Update still gets its steps recorded before the run
// closes. Best effort like every other CoT call; a response with no active
// CoT is a no-op.
func (s *Service) finishAgentCoT(
	ctx context.Context,
	cotMessages feishu.CoTMessages,
	response *agentResponse,
	phase AgentResponsePhase,
) {
	if cotMessages == nil || response.cotStepFinished == nil {
		return
	}
	now := time.Now()
	outcome := feishu.CoTOutcomeDone
	if phase == AgentResponsePhaseFailed || phase == AgentResponsePhaseCancelled {
		outcome = feishu.CoTOutcomeError
	}
	var events []feishu.CoTEvent
	if run, err := feishu.NewCoTRunFinishedEvent(cotRunID(response), cotRunID(response), outcome, now); err == nil {
		events = append(events, run)
	} else {
		s.logAgentCoTFailure("cot run finished", response.responseID, err)
	}
	s.appendAgentCoTEvents(ctx, cotMessages, response, events)
	if err := cotMessages.Complete(ctx, feishu.CoTCompleteRequest{
		CoTID: response.cotID, MessageID: response.cotMessageID, Reason: outcome,
	}); err != nil {
		s.logAgentCoTFailure("cot complete", response.responseID, err)
	}
}

// logAgentCoTFailure mirrors logFeishuFailure's structured-vs-generic split
// for *feishu.CoTAPIError specifically, so a CoT rejection keeps its class
// (including "already_terminal", which is an expected race, not a fault) and
// Feishu's request id instead of collapsing into an undifferentiated
// "transport" line.
func (s *Service) logAgentCoTFailure(operation, responseID string, err error) {
	correlationID := opaqueLogCorrelationID("agent", responseID)
	var apiErr *feishu.CoTAPIError
	if errors.As(err, &apiErr) {
		s.logger.Warn("agent cot operation failed",
			"operation", operation,
			"correlation", correlationID,
			"class", apiErr.Class,
			"http_status", apiErr.HTTPStatus,
			"code", apiErr.Code,
			"request_id", apiErr.RequestID,
		)
		return
	}
	errorClass := "transport"
	switch {
	case errors.Is(err, context.Canceled):
		errorClass = "context_canceled"
	case errors.Is(err, context.DeadlineExceeded):
		errorClass = "deadline_exceeded"
	}
	s.logger.Warn("agent cot operation failed",
		"operation", operation,
		"correlation", correlationID,
		"error_class", errorClass,
	)
}
