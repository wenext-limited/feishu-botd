package service

import (
	"context"
	"strings"

	"feishu-botd/internal/feishu"
	"feishu-botd/internal/notify"
)

// ReplaceAgentResponseInput addresses one already-terminal response. The
// operation keeps the terminal phase intact and advances the semantic revision
// only after every CardKit mutation has committed.
type ReplaceAgentResponseInput struct {
	Provider         string
	ResponseID       string
	OperationID      string
	ExpectedRevision uint64
	Markdown         string
	Summary          string
	TimelineMarkdown string
	TimelineTitle    string
}

// ReplaceAgentResponse settles detached work into its original response card
// without reopening streaming mode or sending a second message.
func (s *Service) ReplaceAgentResponse(ctx context.Context, in ReplaceAgentResponseInput) (AgentResponseReceipt, *notify.APIError) {
	provider, responseID, operationID, apiErr := validateAgentIdentity(in.Provider, in.ResponseID, in.OperationID)
	if apiErr != nil {
		return AgentResponseReceipt{}, apiErr
	}
	if in.ExpectedRevision == 0 {
		return AgentResponseReceipt{}, notify.BadRequest("invalid_revision", "expected_revision must be positive")
	}
	markdown := strings.TrimSpace(in.Markdown)
	if markdown == "" {
		return AgentResponseReceipt{}, notify.BadRequest("missing_markdown", "markdown is required")
	}
	summary := strings.TrimSpace(in.Summary)
	if len(markdown) > maxAgentCardBytes || len(summary) > maxAgentTitleBytes {
		return AgentResponseReceipt{}, notify.BadRequest("field_too_large", "one or more fields are too large")
	}
	timeline, apiErr := normalizedTimelineParts(in.TimelineMarkdown, in.TimelineTitle)
	if apiErr != nil {
		return AgentResponseReceipt{}, apiErr
	}
	response := s.agentBroker.lookupResponse(provider, responseID)
	if response == nil || !s.appAllowed(provider, response.appAlias) {
		return AgentResponseReceipt{}, notify.NewAPIError(404, "unknown_response", "unknown response", false)
	}
	backend, ok := s.backendForApp(response.appAlias)
	if !ok || backend.dynamicCards == nil {
		return AgentResponseReceipt{}, notify.NotImplemented("agent_streaming_unavailable", "agent streaming is unavailable for this sender")
	}
	fingerprint := hashJSON(struct {
		Expected uint64
		Markdown string
		Summary  string
		Timeline agentTimelineParts
	}{in.ExpectedRevision, markdown, summary, timeline})
	return s.applyAgentReplacement(ctx, backend.dynamicCards, response, operationID, fingerprint, in.ExpectedRevision, markdown, summary, timeline)
}

func (s *Service) applyAgentReplacement(
	ctx context.Context,
	dynamicCards feishu.DynamicCards,
	response *agentResponse,
	operationID, fingerprint string,
	expected uint64,
	markdown, summary string,
	timeline agentTimelineParts,
) (AgentResponseReceipt, *notify.APIError) {
	response.mu.Lock()
	defer response.mu.Unlock()
	if apiErr := response.checkCardBudget(markdown, timeline); apiErr != nil {
		return AgentResponseReceipt{}, apiErr
	}
	op, apiErr := response.beginTerminalOperation(operationID, fingerprint, expected)
	if apiErr != nil {
		return AgentResponseReceipt{}, apiErr
	}
	if op.complete {
		return operationReceipt(response, op, true), nil
	}
	if op.panelJSON == "" {
		op.panelJSON = agentTerminalReplacementPatch(markdown, response.timeline.present, timeline)
		op.panelSeq = response.nextSequence + 1
		op.panelUUID = operationUUID("replace", response.responseID, operationID, 64)
		op.settingsJSON = agentFinishSettings(summary)
		op.settingsSeq = op.panelSeq + 1
		op.settingsUUID = operationUUID("replace-settings", response.responseID, operationID, 64)
		op.phase = response.phase
	}
	callCtx, cancel := context.WithTimeout(ctx, s.cfg.SendTimeout)
	defer cancel()
	if !op.panelDone {
		if err := response.waitForMutation(callCtx); err != nil {
			return AgentResponseReceipt{}, notify.NewAPIError(502, "feishu_unavailable", "Feishu card replacement was cancelled", true)
		}
		if err := dynamicCards.BatchUpdate(callCtx, feishu.CardBatchUpdate{
			CardID: response.cardID, ActionsJSON: op.panelJSON,
			UUID: op.panelUUID, Sequence: op.panelSeq,
		}); err != nil {
			s.logAgentCardFailure("terminal replacement", response.responseID, err)
			if isCardAPIRejection(err) && !isRetryableCardFailure(err) {
				if op.panelAmbiguous {
					op.panelClosed = true
					return AgentResponseReceipt{}, operationStateUnknownError()
				}
				response.abortOperation(operationID)
			} else if isAmbiguousCardFailure(err) {
				op.panelAmbiguous = true
			}
			return AgentResponseReceipt{}, agentCardCallError(err, "Feishu card replacement failed")
		}
		op.panelDone = true
		response.nextSequence = op.panelSeq
	}
	if !op.settingsDone {
		if err := response.waitForMutation(callCtx); err != nil {
			return AgentResponseReceipt{}, notify.NewAPIError(502, "feishu_unavailable", "Feishu replacement summary was cancelled", true)
		}
		if err := dynamicCards.UpdateSettings(callCtx, feishu.CardSettingsUpdate{
			CardID: response.cardID, SettingsJSON: op.settingsJSON,
			UUID: op.settingsUUID, Sequence: op.settingsSeq,
		}); err != nil {
			s.logAgentCardFailure("terminal replacement settings", response.responseID, err)
			if isCardAPIRejection(err) && !isRetryableCardFailure(err) {
				if op.settingsAmbiguous {
					op.settingsClosed = true
					return AgentResponseReceipt{}, operationStateUnknownError()
				}
				response.abortOperation(operationID)
			} else if isAmbiguousCardFailure(err) {
				op.settingsAmbiguous = true
			}
			return AgentResponseReceipt{}, agentCardCallError(err, "Feishu replacement summary failed")
		}
		op.settingsDone = true
		response.nextSequence = op.settingsSeq
	}
	op.complete = true
	response.revision++
	op.revision = response.revision
	op.phase = response.phase
	response.pendingOp = ""
	if response.timeline.present {
		if timeline.Markdown != "" {
			response.timeline.markdown = timeline.Markdown
		}
		if timeline.Title != "" {
			response.timeline.title = timeline.Title
		}
	}
	op.compact()
	return operationReceipt(response, op, false), nil
}

func (r *agentResponse) beginTerminalOperation(operationID, fingerprint string, expected uint64) (*agentOperation, *notify.APIError) {
	if existing, ok := r.operations[operationID]; ok {
		if existing.kind != "replace" || existing.fingerprint != fingerprint {
			return nil, notify.NewAPIError(409, "operation_conflict", "operation id reused with different content", false)
		}
		if existing.outcomeUnknown() {
			return nil, operationStateUnknownError()
		}
		return existing, nil
	}
	if r.phase == AgentResponsePhaseStreaming || r.phase == AgentResponsePhaseUnspecified {
		return nil, notify.NewAPIError(412, "response_not_closed", "response is not terminal", false)
	}
	if r.pendingOp != "" {
		if pending := r.operations[r.pendingOp]; pending != nil && pending.outcomeUnknown() {
			return nil, operationStateUnknownError()
		}
		return nil, notify.NewAPIError(409, "operation_in_flight", "another response operation is in flight", true)
	}
	if expected != r.revision {
		return nil, notify.NewAPIError(409, "revision_conflict", "expected_revision does not match the current revision", true)
	}
	op := &agentOperation{kind: "replace", fingerprint: fingerprint}
	r.operations[operationID] = op
	r.pendingOp = operationID
	return op, nil
}
