package service

import (
	"context"
	"errors"
	"strings"
	"time"

	"feishu-botd/internal/feishu"
)

// agentCoTState is the CoT bookkeeping for one response's lifetime, held
// separately from agentResponse so the eager Start-time attempt — which runs
// before any agentResponse exists, see StartAgentResponse — and the lazy
// Update/Finish fallback can share the same create/diff logic. attempted is
// set the first time any call tries to create the CoT, success or failure,
// so a failed (or unbindable) create is never retried for this response.
type agentCoTState struct {
	attempted    bool
	id           string
	messageID    string
	stepFinished map[string]bool
}

// cotRunID doubles as both the AG-UI thread and run id — an already-opaque,
// provider-facing handle, reused here so raw Feishu identifiers never enter
// the CoT event stream.
func cotRunID(responseID string) string {
	return responseID
}

// createAgentCoT creates the CoT and opens it with RUN_STARTED plus whatever
// steps are already known, unconditionally — not gated on steps being
// non-empty. That is what lets a response's CoT message always precede its
// card: StartAgentResponse calls this before the card is sent, and a
// provider (like most agents) that cannot know at Start time whether it will
// run any tools still gets a CoT opened immediately, with steps streaming in
// later via advanceAgentCoT. Every caller shares this one function so
// "attempted" and the opening batch are computed exactly once no matter
// which call site triggers them.
func (s *Service) createAgentCoT(
	ctx context.Context,
	cotMessages feishu.CoTMessages,
	cot *agentCoTState,
	responseID, chatID, originMessageID string,
	steps []AgentTimelineStep,
) {
	cot.attempted = true
	// message_cot has no unbound form, and no origin message means there
	// never will be one for this response — nothing worth logging for an
	// entirely predictable case.
	if chatID == "" || originMessageID == "" {
		return
	}
	id, messageID, err := cotMessages.Create(ctx, feishu.CoTCreateRequest{
		ChatID: chatID, OriginMessageID: originMessageID,
	})
	if err != nil {
		s.logAgentCoTFailure("cot create", responseID, err)
		return
	}
	cot.id = id
	cot.messageID = messageID
	cot.stepFinished = make(map[string]bool, len(steps))

	now := time.Now()
	events := make([]feishu.CoTEvent, 0, len(steps)+1)
	if run, runErr := feishu.NewCoTRunStartedEvent(cotRunID(responseID), cotRunID(responseID), now); runErr == nil {
		events = append(events, run)
	} else {
		s.logAgentCoTFailure("cot run started", responseID, runErr)
	}
	events = append(events, s.agentCoTStepEvents(cot, responseID, steps, now)...)
	s.appendAgentCoTEvents(ctx, cotMessages, cot, responseID, events)
}

// advanceAgentCoT gets a response's CoT to reflect a new cumulative step
// snapshot: creates it on first contact with a non-empty snapshot if
// createAgentCoT has not already run — the eager Start-time attempt found no
// origin message to bind to, which never changes for this response — or
// diffs it against the last snapshot once it is already active.
func (s *Service) advanceAgentCoT(
	ctx context.Context,
	cotMessages feishu.CoTMessages,
	cot *agentCoTState,
	responseID, chatID, originMessageID string,
	steps []AgentTimelineStep,
) {
	if cotMessages == nil || len(steps) == 0 {
		return
	}
	if cot.stepFinished != nil {
		events := s.agentCoTStepEvents(cot, responseID, steps, time.Now())
		s.appendAgentCoTEvents(ctx, cotMessages, cot, responseID, events)
		return
	}
	if cot.attempted {
		return // already tried once (create failed, or unbindable) — never retry
	}
	s.createAgentCoT(ctx, cotMessages, cot, responseID, chatID, originMessageID, steps)
}

// agentCoTStepEvents builds the events for steps whose reported state moved
// since the last snapshot, and commits those transitions to
// cot.stepFinished regardless of whether the later append succeeds:
// bookkeeping only ever moves forward, matching the CoT surface's own
// best-effort contract, where a dropped event is a rendering gap on Feishu's
// side and never a reason to resend or block the caller.
//
// A step seen for the first time always opens with STEP_STARTED, even when
// the snapshot already reports it Finished — the CoT stream has no notion of
// a step that finishes without starting, so a step that ran to completion
// between two snapshots gets both events, timestamped a millisecond apart to
// keep them strictly ordered in one batch.
func (s *Service) agentCoTStepEvents(cot *agentCoTState, responseID string, steps []AgentTimelineStep, at time.Time) []feishu.CoTEvent {
	events := make([]feishu.CoTEvent, 0, len(steps))
	for _, step := range steps {
		stepID := strings.TrimSpace(step.StepID)
		label := strings.TrimSpace(step.Label)
		if stepID == "" || label == "" || step.State == AgentTimelineStepStateUnspecified {
			continue
		}
		finished, seen := cot.stepFinished[stepID]
		if !seen {
			cot.stepFinished[stepID] = false
			if event, err := feishu.NewCoTStepStartedEvent(stepID, label, at); err == nil {
				events = append(events, event)
			} else {
				s.logAgentCoTFailure("cot step started", responseID, err)
			}
			finished = false
		}
		if step.State == AgentTimelineStepStateFinished && !finished {
			cot.stepFinished[stepID] = true
			if event, err := feishu.NewCoTStepFinishedEvent(stepID, label, at.Add(time.Millisecond)); err == nil {
				events = append(events, event)
			} else {
				s.logAgentCoTFailure("cot step finished", responseID, err)
			}
		}
	}
	return events
}

// appendAgentCoTEvents pushes a built event batch, logging rather than
// failing the caller's operation on any error — the CoT surface is strictly
// supplementary to the card the response actually delivers.
func (s *Service) appendAgentCoTEvents(ctx context.Context, cotMessages feishu.CoTMessages, cot *agentCoTState, responseID string, events []feishu.CoTEvent) {
	if len(events) == 0 {
		return
	}
	if err := cotMessages.AppendEvents(ctx, feishu.CoTAppendRequest{
		CoTID: cot.id, MessageID: cot.messageID, Events: events,
	}); err != nil {
		s.logAgentCoTFailure("cot append", responseID, err)
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
	cot *agentCoTState,
	responseID string,
	phase AgentResponsePhase,
) {
	if cotMessages == nil || cot.stepFinished == nil {
		return
	}
	now := time.Now()
	outcome := feishu.CoTOutcomeDone
	if phase == AgentResponsePhaseFailed || phase == AgentResponsePhaseCancelled {
		outcome = feishu.CoTOutcomeError
	}
	var events []feishu.CoTEvent
	if run, err := feishu.NewCoTRunFinishedEvent(cotRunID(responseID), cotRunID(responseID), outcome, now); err == nil {
		events = append(events, run)
	} else {
		s.logAgentCoTFailure("cot run finished", responseID, err)
	}
	s.appendAgentCoTEvents(ctx, cotMessages, cot, responseID, events)
	if err := cotMessages.Complete(ctx, feishu.CoTCompleteRequest{
		CoTID: cot.id, MessageID: cot.messageID, Reason: outcome,
	}); err != nil {
		s.logAgentCoTFailure("cot complete", responseID, err)
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
