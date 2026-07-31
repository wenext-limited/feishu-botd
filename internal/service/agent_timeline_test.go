package service

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

// The exact bytes produced before the timeline panel existed. A provider that
// sends no timeline field must keep getting this card, so these strings are
// pinned rather than derived from the builder they are checking.
const (
	pinnedPlainAgentCard = `{"body":{"elements":[{"tag":"markdown","element_id":"agent_answer",` +
		`"content":"Working…"}]},"config":{"streaming_mode":true,"update_multi":true},"schema":"2.0"}`
	pinnedActionAgentCard = `{"body":{"elements":[{"tag":"markdown","element_id":"agent_answer",` +
		`"content":"Working…"},{"tag":"button","element_id":"agent_action_1","text":{"tag":"plain_text",` +
		`"content":"Stop"},"type":"danger","behaviors":[{"type":"callback","value":{"action_id":"stop",` +
		`"payload_json":"{\"reason\":\"user\"}"}}]}]},"config":{"streaming_mode":true,` +
		`"summary":{"content":"Agent answer"},"update_multi":true},"header":{"title":{"content":"Agent answer",` +
		`"tag":"plain_text"}},"schema":"2.0"}`
)

// timelineCard is the decoded shape of a card that carries a timeline panel.
type timelineCard struct {
	Body struct {
		Elements []struct {
			Tag       string `json:"tag"`
			ElementID string `json:"element_id"`
			Content   string `json:"content"`
			Expanded  bool   `json:"expanded"`
			Header    struct {
				Title struct {
					Tag     string `json:"tag"`
					Content string `json:"content"`
				} `json:"title"`
				IconPosition      string `json:"icon_position"`
				IconExpandedAngle int    `json:"icon_expanded_angle"`
			} `json:"header"`
			Elements []struct {
				Tag       string `json:"tag"`
				ElementID string `json:"element_id"`
				Content   string `json:"content"`
			} `json:"elements"`
		} `json:"elements"`
	} `json:"body"`
}

// timelineHeaderPatch is the decoded shape of the batch_update action list that
// moves the collapsed panel header.
type timelineHeaderPatch []struct {
	Action string `json:"action"`
	Params struct {
		ElementID      string `json:"element_id"`
		PartialElement struct {
			Header struct {
				Title struct {
					Tag     string `json:"tag"`
					Content string `json:"content"`
				} `json:"title"`
			} `json:"header"`
		} `json:"partial_element"`
	} `json:"params"`
}

func startTimelineResponse(t *testing.T, svc *Service, deliveryID string) AgentResponseReceipt {
	t.Helper()
	_, _ = seedAgentDelivery(t, svc, "agent", deliveryID, false)
	return startAgentResponse(t, svc, "agent", deliveryID, AgentResponseContent{
		Title: "Agent answer", Markdown: "Working…",
		TimelineMarkdown: "1. Reading the repository", TimelineTitle: "Reading the repository",
	})
}

func decodeTimelineCard(t *testing.T, cardJSON string) timelineCard {
	t.Helper()
	var card timelineCard
	if err := json.Unmarshal([]byte(cardJSON), &card); err != nil {
		t.Fatalf("decode timeline card: %v", err)
	}
	return card
}

func TestAgentCardWithoutTimelineKeepsPinnedBytes(t *testing.T) {
	for _, testCase := range []struct {
		name    string
		content AgentResponseContent
		want    string
	}{
		{
			name:    "answer only",
			content: AgentResponseContent{Markdown: "Working…"},
			want:    pinnedPlainAgentCard,
		},
		{
			name: "title and action",
			content: AgentResponseContent{
				Title: "Agent answer", Markdown: "Working…",
				Actions: []AgentResponseAction{{
					ActionID: "stop", Label: "Stop", PayloadJSON: `{"reason":"user"}`,
					Style: AgentResponseActionStyleDanger,
				}},
			},
			want: pinnedActionAgentCard,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			card, apiErr := buildAgentCard(testCase.content)
			if apiErr != nil {
				t.Fatalf("build agent card: %v", apiErr)
			}
			if card.json != testCase.want {
				t.Fatalf("card JSON changed for a provider that sends no timeline:\n got %s\nwant %s", card.json, testCase.want)
			}
			if card.timeline.present {
				t.Fatalf("card reported a timeline panel: %#v", card.timeline)
			}
		})
	}
}

func TestAgentStartCreatesCollapsibleTimelinePanel(t *testing.T) {
	backend := newFakeAgentBackend()
	svc := newAgentTestService(backend)
	_ = startTimelineResponse(t, svc, "evt_timeline_start")

	card := decodeTimelineCard(t, backend.createdCards[0])
	if len(card.Body.Elements) != 2 {
		t.Fatalf("card elements = %#v", card.Body.Elements)
	}
	// The panel leads the card so its collapsed header reads as a progress
	// line above the answer it is producing.
	panel := card.Body.Elements[0]
	if panel.Tag != "collapsible_panel" || panel.ElementID != agentTimelinePanelElementID {
		t.Fatalf("timeline panel element = %#v", panel)
	}
	if panel.Expanded {
		t.Fatalf("timeline panel is expanded by default: %#v", panel)
	}
	if panel.Header.Title.Tag != "plain_text" || panel.Header.Title.Content != "Reading the repository" {
		t.Fatalf("panel header title = %#v", panel.Header.Title)
	}
	if panel.Header.IconPosition != "right" || panel.Header.IconExpandedAngle != -180 {
		t.Fatalf("panel header disclosure icon = %#v", panel.Header)
	}
	if len(panel.Elements) != 1 {
		t.Fatalf("panel children = %#v", panel.Elements)
	}
	body := panel.Elements[0]
	if body.Tag != "markdown" || body.ElementID != agentTimelineElementID || body.Content != "1. Reading the repository" {
		t.Fatalf("panel body element = %#v", body)
	}
	if answer := card.Body.Elements[1]; answer.ElementID != agentContentElementID || answer.Content != "Working…" {
		t.Fatalf("answer element = %#v", answer)
	}
}

func TestAgentStartCreatesPanelFromTitleAlone(t *testing.T) {
	backend := newFakeAgentBackend()
	svc := newAgentTestService(backend)
	_, _ = seedAgentDelivery(t, svc, "agent", "evt_timeline_title_only", false)
	_ = startAgentResponse(t, svc, "agent", "evt_timeline_title_only", AgentResponseContent{
		Markdown: "Working…", TimelineTitle: "Planning",
	})

	card := decodeTimelineCard(t, backend.createdCards[0])
	panel := card.Body.Elements[0]
	if panel.Tag != "collapsible_panel" || panel.Header.Title.Content != "Planning" {
		t.Fatalf("panel from title alone = %#v", panel)
	}
	if panel.Elements[0].Content != agentPlaceholderMarkdown {
		t.Fatalf("empty panel body = %q, want the answer placeholder", panel.Elements[0].Content)
	}
}

func TestAgentTimelineUpdateRoutesEachPartIndependently(t *testing.T) {
	for _, testCase := range []struct {
		name             string
		timelineMarkdown string
		timelineTitle    string
		wantElementIDs   []string
		wantHeaderTitle  string
	}{
		{
			name:             "both parts",
			timelineMarkdown: "1. Reading\n2. Editing",
			timelineTitle:    "Editing",
			wantElementIDs:   []string{agentContentElementID, agentTimelineElementID},
			wantHeaderTitle:  "Editing",
		},
		{
			name:             "body only",
			timelineMarkdown: "1. Reading\n2. Editing",
			wantElementIDs:   []string{agentContentElementID, agentTimelineElementID},
		},
		{
			name:            "header only",
			timelineTitle:   "Editing",
			wantElementIDs:  []string{agentContentElementID},
			wantHeaderTitle: "Editing",
		},
		{
			name:           "neither part changes",
			wantElementIDs: []string{agentContentElementID},
		},
		{
			name:             "resending the current parts changes nothing",
			timelineMarkdown: "1. Reading the repository",
			timelineTitle:    "Reading the repository",
			wantElementIDs:   []string{agentContentElementID},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			backend := newFakeAgentBackend()
			svc := newAgentTestService(backend)
			start := startTimelineResponse(t, svc, "evt_timeline_route")

			updated, apiErr := svc.UpdateAgentResponse(context.Background(), UpdateAgentResponseInput{
				Provider: "agent", ResponseID: start.ResponseID, OperationID: "update-1",
				ExpectedRevision: 1, Markdown: "Partial answer",
				TimelineMarkdown: testCase.timelineMarkdown, TimelineTitle: testCase.timelineTitle,
			})
			if apiErr != nil {
				t.Fatalf("timeline update: %v", apiErr)
			}
			// However many Feishu calls the parts needed, one RPC is one revision.
			if updated.Revision != 2 {
				t.Fatalf("timeline update receipt = %#v", updated)
			}

			gotElementIDs := make([]string, 0, len(backend.contentUpdates))
			for _, update := range backend.contentUpdates {
				gotElementIDs = append(gotElementIDs, update.ElementID)
			}
			if strings.Join(gotElementIDs, ",") != strings.Join(testCase.wantElementIDs, ",") {
				t.Fatalf("content updates = %v, want %v", gotElementIDs, testCase.wantElementIDs)
			}
			assertStrictlyIncreasingSequences(t, backend)

			if testCase.wantHeaderTitle == "" {
				if len(backend.batchUpdates) != 0 {
					t.Fatalf("unchanged header still patched: %#v", backend.batchUpdates)
				}
				return
			}
			if len(backend.batchUpdates) != 1 {
				t.Fatalf("header patch count = %d, want 1", len(backend.batchUpdates))
			}
			var patch timelineHeaderPatch
			if err := json.Unmarshal([]byte(backend.batchUpdates[0].ActionsJSON), &patch); err != nil {
				t.Fatalf("decode header patch: %v", err)
			}
			if len(patch) != 1 || patch[0].Action != "partial_update_element" {
				t.Fatalf("header patch actions = %#v", patch)
			}
			if patch[0].Params.ElementID != agentTimelinePanelElementID {
				t.Fatalf("header patch target = %q", patch[0].Params.ElementID)
			}
			title := patch[0].Params.PartialElement.Header.Title
			if title.Tag != "plain_text" || title.Content != testCase.wantHeaderTitle {
				t.Fatalf("header patch title = %#v", title)
			}
		})
	}
}

// assertStrictlyIncreasingSequences checks the card-wide sequence domain the
// Feishu CardKit APIs require, across every operation kind.
func assertStrictlyIncreasingSequences(t *testing.T, backend *fakeAgentBackend) {
	t.Helper()
	type call struct {
		kind     string
		sequence int32
	}
	calls := make([]call, 0)
	for _, update := range backend.contentUpdates {
		calls = append(calls, call{"content " + update.ElementID, update.Sequence})
	}
	for _, update := range backend.batchUpdates {
		calls = append(calls, call{"batch", update.Sequence})
	}
	for _, update := range backend.settingsUpdates {
		calls = append(calls, call{"settings", update.Sequence})
	}
	seen := make(map[int32]string, len(calls))
	for _, made := range calls {
		if made.sequence <= 0 {
			t.Fatalf("call %q used a non-positive sequence %d", made.kind, made.sequence)
		}
		if other, duplicate := seen[made.sequence]; duplicate {
			t.Fatalf("calls %q and %q share sequence %d", other, made.kind, made.sequence)
		}
		seen[made.sequence] = made.kind
	}
}

func TestAgentTimelineFieldsAreIgnoredWithoutAPanel(t *testing.T) {
	backend := newFakeAgentBackend()
	svc := newAgentTestService(backend)
	_, _ = seedAgentDelivery(t, svc, "agent", "evt_no_panel", false)
	start := startAgentResponse(t, svc, "agent", "evt_no_panel", AgentResponseContent{Markdown: "Working…"})

	updated, apiErr := svc.UpdateAgentResponse(context.Background(), UpdateAgentResponseInput{
		Provider: "agent", ResponseID: start.ResponseID, OperationID: "update-ignored",
		ExpectedRevision: 1, Markdown: "Partial answer",
		TimelineMarkdown: "1. Reading", TimelineTitle: "Reading",
	})
	if apiErr != nil {
		t.Fatalf("update on a panel-less response: %v", apiErr)
	}
	if updated.Revision != 2 {
		t.Fatalf("panel-less update receipt = %#v", updated)
	}
	if len(backend.contentUpdates) != 1 || backend.contentUpdates[0].ElementID != agentContentElementID {
		t.Fatalf("panel-less update calls = %#v", backend.contentUpdates)
	}
	if len(backend.batchUpdates) != 0 {
		t.Fatalf("panel-less update patched a panel: %#v", backend.batchUpdates)
	}

	finished, apiErr := svc.FinishAgentResponse(context.Background(), FinishAgentResponseInput{
		Provider: "agent", ResponseID: start.ResponseID, OperationID: "finish-ignored",
		ExpectedRevision: 2, Outcome: AgentResponseOutcomeCompleted, Markdown: "Final answer",
		TimelineMarkdown: "1. Reading\n2. Done", TimelineTitle: "Completed",
	})
	if apiErr != nil {
		t.Fatalf("finish on a panel-less response: %v", apiErr)
	}
	if finished.Revision != 3 || len(backend.batchUpdates) != 0 {
		t.Fatalf("panel-less finish = %#v, patches=%d", finished, len(backend.batchUpdates))
	}
}

func TestAgentTimelineTitleIsGatedLikeTheCardTitle(t *testing.T) {
	oversized := strings.Repeat("x", maxAgentTitleBytes+1)

	t.Run("start", func(t *testing.T) {
		backend := newFakeAgentBackend()
		svc := newAgentTestService(backend)
		_, _ = seedAgentDelivery(t, svc, "agent", "evt_title_gate", false)
		if _, apiErr := svc.StartAgentResponse(context.Background(), StartAgentResponseInput{
			Provider: "agent", DeliveryID: "evt_title_gate", OperationID: "start-oversized-title",
			Content: AgentResponseContent{Markdown: "Working…", TimelineTitle: oversized},
		}); apiErr == nil || apiErr.Code != "field_too_large" {
			t.Fatalf("oversized start timeline title error = %v", apiErr)
		}
		if len(backend.createdCards) != 0 {
			t.Fatalf("oversized timeline title reached CardKit: %d cards", len(backend.createdCards))
		}
	})

	t.Run("update and finish", func(t *testing.T) {
		backend := newFakeAgentBackend()
		svc := newAgentTestService(backend)
		start := startTimelineResponse(t, svc, "evt_title_gate_update")

		if _, apiErr := svc.UpdateAgentResponse(context.Background(), UpdateAgentResponseInput{
			Provider: "agent", ResponseID: start.ResponseID, OperationID: "update-oversized-title",
			ExpectedRevision: 1, Markdown: "Partial", TimelineTitle: oversized,
		}); apiErr == nil || apiErr.Code != "field_too_large" {
			t.Fatalf("oversized update timeline title error = %v", apiErr)
		}
		if _, apiErr := svc.FinishAgentResponse(context.Background(), FinishAgentResponseInput{
			Provider: "agent", ResponseID: start.ResponseID, OperationID: "finish-oversized-title",
			ExpectedRevision: 1, Outcome: AgentResponseOutcomeCompleted, Markdown: "Final",
			TimelineTitle: oversized,
		}); apiErr == nil || apiErr.Code != "field_too_large" {
			t.Fatalf("oversized finish timeline title error = %v", apiErr)
		}
		if len(backend.contentUpdates) != 0 || len(backend.batchUpdates) != 0 {
			t.Fatalf("rejected timeline title still mutated the card: content=%#v batch=%#v",
				backend.contentUpdates, backend.batchUpdates)
		}
	})
}

func TestAgentTimelineSharesTheCardSizeBudget(t *testing.T) {
	backend := newFakeAgentBackend()
	svc := newAgentTestService(backend)
	start := startTimelineResponse(t, svc, "evt_size_budget")

	answer := strings.Repeat("a", 20*1024)
	for _, testCase := range []struct {
		name             string
		timelineMarkdown string
		wantCode         string
	}{
		{name: "answer and timeline fit together", timelineMarkdown: strings.Repeat("t", 9*1024)},
		{name: "combined overflow", timelineMarkdown: strings.Repeat("t", 11*1024), wantCode: "field_too_large"},
		{name: "timeline alone overflows", timelineMarkdown: strings.Repeat("t", maxAgentCardBytes+1), wantCode: "field_too_large"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			_, apiErr := svc.UpdateAgentResponse(context.Background(), UpdateAgentResponseInput{
				Provider: "agent", ResponseID: start.ResponseID, OperationID: "size-" + testCase.name,
				ExpectedRevision: 1, Markdown: answer, TimelineMarkdown: testCase.timelineMarkdown,
			})
			if testCase.wantCode == "" {
				if apiErr != nil {
					t.Fatalf("update within budget: %v", apiErr)
				}
				return
			}
			if apiErr == nil || apiErr.Code != testCase.wantCode {
				t.Fatalf("over-budget update error = %v, want %s", apiErr, testCase.wantCode)
			}
		})
	}
}

func TestAgentTimelineUpdateReplayRepeatsOnlyTheFailedCall(t *testing.T) {
	backend := newFakeAgentBackend()
	backend.batchErr = errors.New("header patch timeout")
	svc := newAgentTestService(backend)
	start := startTimelineResponse(t, svc, "evt_timeline_replay")
	in := UpdateAgentResponseInput{
		Provider: "agent", ResponseID: start.ResponseID, OperationID: "update-replay",
		ExpectedRevision: 1, Markdown: "Partial answer",
		TimelineMarkdown: "1. Reading\n2. Editing", TimelineTitle: "Editing",
	}

	if _, apiErr := svc.UpdateAgentResponse(context.Background(), in); apiErr == nil ||
		apiErr.Code != "feishu_unavailable" || !apiErr.Retryable {
		t.Fatalf("first timeline update error = %v", apiErr)
	}
	if len(backend.contentUpdates) != 2 || len(backend.batchUpdates) != 1 {
		t.Fatalf("first attempt calls: content=%d batch=%d", len(backend.contentUpdates), len(backend.batchUpdates))
	}
	firstPatch := backend.batchUpdates[0]

	backend.batchErr = nil
	updated, apiErr := svc.UpdateAgentResponse(context.Background(), in)
	if apiErr != nil {
		t.Fatalf("timeline update retry: %v", apiErr)
	}
	// The answer and the panel body were already accepted by Feishu, so the
	// retry must reach only the header the first attempt left in doubt.
	if len(backend.contentUpdates) != 2 {
		t.Fatalf("retry repeated an accepted content call: %#v", backend.contentUpdates)
	}
	if len(backend.batchUpdates) != 2 {
		t.Fatalf("retry header patch count = %d, want 2", len(backend.batchUpdates))
	}
	secondPatch := backend.batchUpdates[1]
	if secondPatch.UUID != firstPatch.UUID || secondPatch.Sequence != firstPatch.Sequence ||
		secondPatch.ActionsJSON != firstPatch.ActionsJSON {
		t.Fatalf("retry changed header idempotency identity: first=%#v second=%#v", firstPatch, secondPatch)
	}
	if updated.Revision != 2 || updated.Duplicate {
		t.Fatalf("timeline update retry receipt = %#v", updated)
	}

	replay, apiErr := svc.UpdateAgentResponse(context.Background(), in)
	if apiErr != nil {
		t.Fatalf("completed timeline update replay: %v", apiErr)
	}
	if !replay.Duplicate || replay.Revision != 2 {
		t.Fatalf("completed replay receipt = %#v", replay)
	}
	if len(backend.contentUpdates) != 2 || len(backend.batchUpdates) != 2 {
		t.Fatalf("completed replay performed I/O: content=%d batch=%d",
			len(backend.contentUpdates), len(backend.batchUpdates))
	}

	response := svc.agentBroker.lookupResponse("agent", start.ResponseID)
	response.mu.Lock()
	defer response.mu.Unlock()
	assertCompactedTimelineOperations(t, response.operations)
}

func assertCompactedTimelineOperations(t *testing.T, operations map[string]*agentOperation) {
	t.Helper()
	for operationID, op := range operations {
		if op.timeline != "" || op.timelineUUID != "" || op.timelineSeq != 0 || op.timelineDone ||
			op.timelineAmbiguous || op.timelineClosed ||
			op.panelTitle != "" || op.panelJSON != "" || op.panelUUID != "" || op.panelSeq != 0 ||
			op.panelDone || op.panelAmbiguous || op.panelClosed {
			t.Fatalf("operation %q retained timeline payload: %#v", operationID, op)
		}
	}
}

func TestAgentFinishSettlesTimelineBeforeDisablingStreaming(t *testing.T) {
	backend := newFakeAgentBackend()
	svc := newAgentTestService(backend)
	start := startTimelineResponse(t, svc, "evt_timeline_finish")

	finished, apiErr := svc.FinishAgentResponse(context.Background(), FinishAgentResponseInput{
		Provider: "agent", ResponseID: start.ResponseID, OperationID: "finish-timeline",
		ExpectedRevision: 1, Outcome: AgentResponseOutcomeCompleted,
		Markdown: "Final answer", Summary: "Completed",
		TimelineMarkdown: "1. Reading\n2. Editing\n3. Done", TimelineTitle: "Completed in 3 steps",
	})
	if apiErr != nil {
		t.Fatalf("finish with timeline: %v", apiErr)
	}
	if finished.Revision != 2 || finished.Phase != AgentResponsePhaseCompleted {
		t.Fatalf("finish receipt = %#v", finished)
	}
	if len(backend.contentUpdates) != 2 || len(backend.batchUpdates) != 1 || len(backend.settingsUpdates) != 1 {
		t.Fatalf("finish calls: content=%d batch=%d settings=%d",
			len(backend.contentUpdates), len(backend.batchUpdates), len(backend.settingsUpdates))
	}
	answer, timeline := backend.contentUpdates[0], backend.contentUpdates[1]
	if answer.ElementID != agentContentElementID || timeline.ElementID != agentTimelineElementID {
		t.Fatalf("finish content order = %q then %q", answer.ElementID, timeline.ElementID)
	}
	// Streaming mode closes the card, so it has to be the last thing written.
	settings := backend.settingsUpdates[0]
	if settings.Sequence <= backend.batchUpdates[0].Sequence || backend.batchUpdates[0].Sequence <= timeline.Sequence ||
		timeline.Sequence <= answer.Sequence {
		t.Fatalf("finish sequence order: answer=%d timeline=%d header=%d settings=%d",
			answer.Sequence, timeline.Sequence, backend.batchUpdates[0].Sequence, settings.Sequence)
	}
	assertStrictlyIncreasingSequences(t, backend)
}
