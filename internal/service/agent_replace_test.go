package service

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

func finishedResponseForReplacement(t *testing.T, backend *fakeAgentBackend, withTimeline bool) (*Service, AgentResponseReceipt) {
	t.Helper()
	svc := newAgentTestService(backend)
	_, _ = seedAgentDelivery(t, svc, "agent", "evt_replace", false)
	content := AgentResponseContent{Markdown: "正在调查…"}
	if withTimeline {
		content.TimelineMarkdown = "**任务进行中**"
		content.TimelineTitle = "正在调查"
	}
	started := startAgentResponse(t, svc, "agent", "evt_replace", content)
	finished, apiErr := svc.FinishAgentResponse(context.Background(), FinishAgentResponseInput{
		Provider: "agent", ResponseID: started.ResponseID, OperationID: "finish-replace",
		ExpectedRevision: started.Revision, Outcome: AgentResponseOutcomeCompleted,
		Markdown: "调查已开始", Summary: "调查已开始",
		TimelineMarkdown: "**调查已开始**", TimelineTitle: "调查已开始",
	})
	if apiErr != nil {
		t.Fatalf("finish before replacement: %v", apiErr)
	}
	return svc, finished
}

func TestAgentReplacementPreservesTerminalPhaseAndIsIdempotent(t *testing.T) {
	backend := newFakeAgentBackend()
	svc, finished := finishedResponseForReplacement(t, backend, true)
	beforeBatch := len(backend.batchUpdates)
	beforeSettings := len(backend.settingsUpdates)
	in := ReplaceAgentResponseInput{
		Provider: "agent", ResponseID: finished.ResponseID, OperationID: "replace-1",
		ExpectedRevision: finished.Revision, Markdown: "这是最终调查结果。", Summary: "调查完成",
		TimelineMarkdown: "**任务已完成**", TimelineTitle: "任务已完成",
	}

	replaced, apiErr := svc.ReplaceAgentResponse(context.Background(), in)
	if apiErr != nil {
		t.Fatalf("replace terminal response: %v", apiErr)
	}
	if replaced.Revision != finished.Revision+1 || replaced.Phase != AgentResponsePhaseCompleted || replaced.Duplicate {
		t.Fatalf("replacement receipt = %#v", replaced)
	}
	if len(backend.batchUpdates) != beforeBatch+1 || len(backend.settingsUpdates) != beforeSettings+1 {
		t.Fatalf("replacement calls: batch=%d settings=%d", len(backend.batchUpdates)-beforeBatch, len(backend.settingsUpdates)-beforeSettings)
	}
	patch := backend.batchUpdates[len(backend.batchUpdates)-1]
	for _, want := range []string{agentContentElementID, agentTimelineElementID, agentTimelinePanelElementID, "这是最终调查结果。", "任务已完成"} {
		if !strings.Contains(patch.ActionsJSON, want) {
			t.Fatalf("replacement patch missing %q: %s", want, patch.ActionsJSON)
		}
	}
	var settings struct {
		Config struct {
			StreamingMode bool `json:"streaming_mode"`
			Summary       struct {
				Content string `json:"content"`
			} `json:"summary"`
		} `json:"config"`
	}
	if err := json.Unmarshal([]byte(backend.settingsUpdates[len(backend.settingsUpdates)-1].SettingsJSON), &settings); err != nil {
		t.Fatalf("decode replacement settings: %v", err)
	}
	if settings.Config.StreamingMode || settings.Config.Summary.Content != "调查完成" {
		t.Fatalf("replacement settings = %#v", settings)
	}

	replay, apiErr := svc.ReplaceAgentResponse(context.Background(), in)
	if apiErr != nil {
		t.Fatalf("replay replacement: %v", apiErr)
	}
	if !replay.Duplicate || replay.Revision != replaced.Revision || replay.Phase != AgentResponsePhaseCompleted {
		t.Fatalf("replacement replay = %#v", replay)
	}
	if len(backend.batchUpdates) != beforeBatch+1 || len(backend.settingsUpdates) != beforeSettings+1 {
		t.Fatal("idempotent replay repeated CardKit mutations")
	}
}

func TestAgentReplacementRequiresTheExactTerminalRevision(t *testing.T) {
	backend := newFakeAgentBackend()
	svc, finished := finishedResponseForReplacement(t, backend, false)
	before := len(backend.batchUpdates)
	_, apiErr := svc.ReplaceAgentResponse(context.Background(), ReplaceAgentResponseInput{
		Provider: "agent", ResponseID: finished.ResponseID, OperationID: "replace-stale",
		ExpectedRevision: finished.Revision - 1, Markdown: "late answer",
	})
	if apiErr == nil || apiErr.Code != "revision_conflict" {
		t.Fatalf("stale replacement error = %v", apiErr)
	}
	if len(backend.batchUpdates) != before {
		t.Fatal("a stale replacement reached CardKit")
	}
}

func TestAgentReplacementRetriesTheSameAmbiguousBatchMutation(t *testing.T) {
	backend := newFakeAgentBackend()
	svc, finished := finishedResponseForReplacement(t, backend, false)
	backend.batchErr = errors.New("replacement response lost")
	in := ReplaceAgentResponseInput{
		Provider: "agent", ResponseID: finished.ResponseID, OperationID: "replace-retry",
		ExpectedRevision: finished.Revision, Markdown: "final answer", Summary: "done",
	}
	if _, apiErr := svc.ReplaceAgentResponse(context.Background(), in); apiErr == nil || apiErr.Code != "feishu_unavailable" || !apiErr.Retryable {
		t.Fatalf("ambiguous replacement error = %v", apiErr)
	}
	first := backend.batchUpdates[len(backend.batchUpdates)-1]
	response := svc.agentBroker.lookupResponse("agent", finished.ResponseID)
	response.mu.Lock()
	response.lastMutationAt = time.Time{}
	response.mu.Unlock()
	backend.batchErr = nil
	replaced, apiErr := svc.ReplaceAgentResponse(context.Background(), in)
	if apiErr != nil {
		t.Fatalf("replacement retry: %v", apiErr)
	}
	second := backend.batchUpdates[len(backend.batchUpdates)-1]
	if first.UUID != second.UUID || first.Sequence != second.Sequence || first.ActionsJSON != second.ActionsJSON {
		t.Fatalf("replacement retry changed identity: first=%#v second=%#v", first, second)
	}
	if replaced.Phase != AgentResponsePhaseCompleted {
		t.Fatalf("replacement reopened response: %#v", replaced)
	}
}
