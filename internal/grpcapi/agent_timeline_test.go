package grpcapi

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	pb "feishu-botd/gen/feishubotd/v1"
	"feishu-botd/internal/feishu"
	"feishu-botd/internal/service"
)

func (f *fakeAgentSender) batchSnapshot() []feishu.CardBatchUpdate {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]feishu.CardBatchUpdate(nil), f.batchUpdates...)
}

// TestGRPCAgentTimelineFieldsReachTheCard walks the wire fields end to end:
// Start opens the panel, Update advances one part at a time, and Finish
// settles the header. It exists so a missing field in the proto-to-service
// conversion cannot pass unnoticed.
func TestGRPCAgentTimelineFieldsReachTheCard(t *testing.T) {
	sender := &fakeAgentSender{fakeSender: fakeSender{messageID: "unused_fixture"}}
	conn, svc := startAgentUnixServer(t, sender)
	client := pb.NewCommandServiceClient(conn)

	streamCtx, cancelStream := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelStream()
	stream, err := client.SubscribeAgentEvents(streamCtx, &pb.SubscribeAgentEventsRequest{
		Provider: "fixture-agent", Commands: []string{"ask"},
	})
	if err != nil {
		t.Fatalf("subscribe agent events: %v", err)
	}
	_ = dispatchAndReceiveAgentEvent(t, svc, stream, service.CommandInput{
		DeliveryID: "delivery_timeline", ConversationID: "conversation_timeline",
		Command: "ask", Prompt: "ask the fixture", ChatAlias: "ops", SenderID: "sender_fixture",
		Metadata: map[string]string{"message_id": "inbound_timeline_fixture"},
	})

	started, err := client.StartAgentResponse(context.Background(), &pb.StartAgentResponseRequest{
		Provider: "fixture-agent", DeliveryId: "delivery_timeline", OperationId: "start_timeline",
		Content: &pb.AgentResponseContent{
			Title: "Fixture answer", Markdown: "Working on it.",
			TimelineMarkdown: "1. Reading", TimelineTitle: "Reading",
		},
	})
	if err != nil {
		t.Fatalf("start agent response: %v", err)
	}
	responseID := started.GetResponse().GetResponseId()
	createdCard, _, _, _ := sender.snapshot()
	if !strings.Contains(createdCard, `"tag":"collapsible_panel"`) ||
		!strings.Contains(createdCard, `"content":"1. Reading"`) {
		t.Fatalf("started card has no timeline panel: %s", createdCard)
	}

	updated, err := client.UpdateAgentResponse(context.Background(), &pb.UpdateAgentResponseRequest{
		Provider: "fixture-agent", ResponseId: responseID, OperationId: "update_timeline",
		ExpectedRevision: 1, Markdown: "Partial answer.",
		TimelineMarkdown: "1. Reading\n2. Editing",
	})
	if err != nil {
		t.Fatalf("update agent response: %v", err)
	}
	requireAgentReceipt(t, updated.GetResponse(), responseID, 2, pb.AgentResponsePhase_AGENT_RESPONSE_PHASE_STREAMING, false)
	if patches := sender.batchSnapshot(); len(patches) != 0 {
		t.Fatalf("update without a timeline_title patched the header: %#v", patches)
	}

	finished, err := client.FinishAgentResponse(context.Background(), &pb.FinishAgentResponseRequest{
		Provider: "fixture-agent", ResponseId: responseID, OperationId: "finish_timeline",
		ExpectedRevision: 2, Outcome: pb.AgentResponseOutcome_AGENT_RESPONSE_OUTCOME_COMPLETED,
		Markdown: "Final answer.", Summary: "Completed",
		TimelineTitle: "Completed in 2 steps",
	})
	if err != nil {
		t.Fatalf("finish agent response: %v", err)
	}
	requireAgentReceipt(t, finished.GetResponse(), responseID, 3, pb.AgentResponsePhase_AGENT_RESPONSE_PHASE_COMPLETED, false)

	patches := sender.batchSnapshot()
	if len(patches) != 1 {
		t.Fatalf("finish header patch count = %d, want 1", len(patches))
	}
	var actions []struct {
		Action string          `json:"action"`
		Params json.RawMessage `json:"params"`
	}
	if err := json.Unmarshal([]byte(patches[0].ActionsJSON), &actions); err != nil {
		t.Fatalf("decode header patch: %v", err)
	}
	if len(actions) != 1 || actions[0].Action != "partial_update_element" {
		t.Fatalf("header patch actions = %#v", actions)
	}
	if !strings.Contains(string(actions[0].Params), "Completed in 2 steps") {
		t.Fatalf("header patch params = %s", actions[0].Params)
	}

	_, _, contentUpdates, settingUpdates := sender.snapshot()
	if len(contentUpdates) != 3 || len(settingUpdates) != 1 {
		t.Fatalf("timeline lifecycle calls: content=%d settings=%d", len(contentUpdates), len(settingUpdates))
	}
	if settingUpdates[0].Sequence <= patches[0].Sequence {
		t.Fatalf("streaming was disabled before the header settled: settings=%d header=%d",
			settingUpdates[0].Sequence, patches[0].Sequence)
	}
}
