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

// startAgentCoT creates the native Feishu CoT progress message for a response
// whose Start carried timeline steps, bound to the message that triggered the
// run. Best effort throughout: any failure here just means this response has
// no CoT, and every later diff/finish call becomes a no-op because
// response.cotStepFinished stays nil.
func (s *Service) startAgentCoT(
	ctx context.Context,
	cotMessages feishu.CoTMessages,
	response *agentResponse,
	chatID, originMessageID string,
	steps []AgentTimelineStep,
) {
	// chatID/originMessageID come from delivery.input.Metadata, which is empty
	// for a delivery with no triggering message to reply to (e.g. a top-level
	// send to a channel). message_cot has no unbound form, so there is nothing
	// useful to attempt — and no warning worth logging for an entirely
	// predictable case.
	if cotMessages == nil || len(steps) == 0 || chatID == "" || originMessageID == "" {
		return
	}
	cotID, cotMessageID, err := cotMessages.Create(ctx, feishu.CoTCreateRequest{
		ChatID:          chatID,
		OriginMessageID: originMessageID,
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

// diffAgentCoTSteps advances an already-created CoT to match a new cumulative
// step snapshot, emitting only the transitions since the last snapshot. A
// response with no active CoT, or an update carrying no steps, is a no-op.
func (s *Service) diffAgentCoTSteps(
	ctx context.Context,
	cotMessages feishu.CoTMessages,
	response *agentResponse,
	steps []AgentTimelineStep,
) {
	if cotMessages == nil || response.cotStepFinished == nil || len(steps) == 0 {
		return
	}
	events := s.agentCoTStepEvents(response, steps, time.Now())
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

// finishAgentCoT closes an active CoT: a final step diff, RUN_FINISHED, then
// Complete with the outcome mapped to the CoT's own done/error vocabulary.
// Best effort like every other CoT call; a response with no active CoT is a
// no-op.
func (s *Service) finishAgentCoT(
	ctx context.Context,
	cotMessages feishu.CoTMessages,
	response *agentResponse,
	steps []AgentTimelineStep,
	phase AgentResponsePhase,
) {
	if cotMessages == nil || response.cotStepFinished == nil {
		return
	}
	now := time.Now()
	events := s.agentCoTStepEvents(response, steps, now)
	outcome := feishu.CoTOutcomeDone
	if phase == AgentResponsePhaseFailed || phase == AgentResponsePhaseCancelled {
		outcome = feishu.CoTOutcomeError
	}
	if run, err := feishu.NewCoTRunFinishedEvent(cotRunID(response), cotRunID(response), outcome, now.Add(time.Millisecond)); err == nil {
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
