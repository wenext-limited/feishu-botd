package grpcapi

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "feishu-botd/gen/feishubotd/v1"
	"feishu-botd/internal/config"
	"feishu-botd/internal/dedupe"
	"feishu-botd/internal/feishu"
	"feishu-botd/internal/service"
)

func TestGRPCAgentAttachedContextStreamsHeaderThenBoundedImageChunks(t *testing.T) {
	imageData := bytes.Repeat([]byte("image-fixture"), 20_000)
	sender := &fakeAgentSender{
		fakeSender: fakeSender{messageID: "unused_fixture"},
		attachedContext: feishu.AttachedContext{
			Status: feishu.AttachedContextFound,
			Messages: []feishu.AttachedContextMessage{{
				AuthorLabel: "participant-1", AuthorType: "user", Text: "crashes on launch",
				Images: []feishu.AttachedContextImage{{MediaType: "image/png", Data: imageData}},
			}},
			Issues: []feishu.AttachedContextIssue{{
				Code: feishu.AttachedContextIssueVideoOmitted, Count: 1,
			}},
		},
	}
	conn, svc := startAgentUnixServer(t, sender)
	client := pb.NewCommandServiceClient(conn)

	subCtx, cancelSub := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelSub()
	sub, err := client.SubscribeAgentEvents(subCtx, &pb.SubscribeAgentEventsRequest{
		Provider: "fixture-agent", IncludeUnmatchedMessages: true,
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	_ = dispatchAndReceiveAgentEvent(t, svc, sub, service.CommandInput{
		DeliveryID: "delivery_context_fixture", Command: "看看", Prompt: "看看这个问题",
		ConversationID: "conversation_fixture", ChatAlias: "ops", SenderID: "sender_fixture",
		Metadata: map[string]string{
			"chat_type": "topic_group", "message_type": "text", "message_id": "om_guide",
			"thread_id": "omt_private", "create_time": "1754380800123",
		},
	})

	stream, err := client.GetAgentAttachedContext(context.Background(), &pb.GetAgentAttachedContextRequest{
		Provider: "fixture-agent", DeliveryId: "delivery_context_fixture",
	})
	if err != nil {
		t.Fatalf("get attached context: %v", err)
	}
	first, err := stream.Recv()
	if err != nil {
		t.Fatalf("receive header: %v", err)
	}
	header := first.GetHeader()
	if header == nil || header.GetStatus() != pb.AgentAttachedContextStatus_AGENT_ATTACHED_CONTEXT_STATUS_FOUND ||
		len(header.GetMessages()) != 1 || len(header.GetMessages()[0].GetImages()) != 1 {
		t.Fatalf("header = %#v", header)
	}
	message := header.GetMessages()[0]
	if message.GetAuthorLabel() != "participant-1" || message.GetText() != "crashes on launch" {
		t.Fatalf("message = %#v", message)
	}
	descriptor := message.GetImages()[0]
	if descriptor.GetImageIndex() != 0 || descriptor.GetMediaType() != "image/png" || descriptor.GetByteSize() != uint64(len(imageData)) {
		t.Fatalf("image descriptor = %#v", descriptor)
	}

	var reconstructed []byte
	chunkCount := 0
	for {
		frame, recvErr := stream.Recv()
		if recvErr == io.EOF {
			break
		}
		if recvErr != nil {
			t.Fatalf("receive image chunk: %v", recvErr)
		}
		chunk := frame.GetImageChunk()
		if chunk == nil || chunk.GetImageIndex() != 0 || chunk.GetOffset() != uint64(len(reconstructed)) {
			t.Fatalf("chunk = %#v reconstructed=%d", chunk, len(reconstructed))
		}
		if len(chunk.GetData()) == 0 || len(chunk.GetData()) > attachedContextImageChunkBytes {
			t.Fatalf("chunk size = %d", len(chunk.GetData()))
		}
		reconstructed = append(reconstructed, chunk.GetData()...)
		chunkCount++
		if chunk.GetFinal() != (len(reconstructed) == len(imageData)) {
			t.Fatalf("final=%t reconstructed=%d total=%d", chunk.GetFinal(), len(reconstructed), len(imageData))
		}
	}
	if chunkCount < 2 || !bytes.Equal(reconstructed, imageData) {
		t.Fatalf("chunks=%d reconstructed=%d want=%d", chunkCount, len(reconstructed), len(imageData))
	}
	sender.mu.Lock()
	calls := sender.attachedCalls
	sender.mu.Unlock()
	if calls != 1 {
		t.Fatalf("attached context lookup calls = %d", calls)
	}
}

type fakeAgentSender struct {
	fakeSender

	mu              sync.Mutex
	createdCardJSON string
	cardSends       []feishu.CardSendRequest
	contentUpdates  []feishu.CardContentUpdate
	settingUpdates  []feishu.CardSettingsUpdate
	batchUpdates    []feishu.CardBatchUpdate
	attachedContext feishu.AttachedContext
	attachedCalls   int
}

func (f *fakeAgentSender) LookupAttachedContext(_ context.Context, _ feishu.AttachedContextRequest) (feishu.AttachedContext, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.attachedCalls++
	return f.attachedContext, nil
}

func (f *fakeAgentSender) CreateCard(_ context.Context, cardJSON string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.createdCardJSON = cardJSON
	return "card_fixture", nil
}

func (f *fakeAgentSender) SendCard(_ context.Context, req feishu.CardSendRequest) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cardSends = append(f.cardSends, req)
	return "message_fixture", nil
}

func (f *fakeAgentSender) UpdateContent(_ context.Context, req feishu.CardContentUpdate) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.contentUpdates = append(f.contentUpdates, req)
	return nil
}

func (f *fakeAgentSender) UpdateSettings(_ context.Context, req feishu.CardSettingsUpdate) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.settingUpdates = append(f.settingUpdates, req)
	return nil
}

func (f *fakeAgentSender) BatchUpdate(_ context.Context, req feishu.CardBatchUpdate) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.batchUpdates = append(f.batchUpdates, req)
	return nil
}

func (f *fakeAgentSender) snapshot() (string, []feishu.CardSendRequest, []feishu.CardContentUpdate, []feishu.CardSettingsUpdate) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.createdCardJSON,
		append([]feishu.CardSendRequest(nil), f.cardSends...),
		append([]feishu.CardContentUpdate(nil), f.contentUpdates...),
		append([]feishu.CardSettingsUpdate(nil), f.settingUpdates...)
}

func startAgentUnixServer(t *testing.T, sender *fakeAgentSender) (*grpc.ClientConn, *service.Service) {
	t.Helper()
	cfg := testConfig()
	cfg.AgentProviders = map[string]config.AgentProviderConfig{
		"fixture-agent": {
			AuthToken: fixtureAgentToken, AllowedCommands: []string{"ask"},
			AllowUnmatchedMessages: true, AllowCardActions: true, AllowAttachedContext: true,
		},
	}
	svc := service.NewService(cfg, sender, dedupe.NewMemoryStore(time.Hour), slog.Default())
	srv := NewServer(cfg, svc, slog.Default())
	ctx, cancel := context.WithCancel(context.Background())
	socketPath := tempSocket(t)
	errCh := make(chan error, 1)
	go func() { errCh <- srv.ListenAndServeUnix(ctx, socketPath) }()
	t.Cleanup(func() {
		cancel()
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), time.Second)
		defer shutdownCancel()
		_ = srv.Shutdown(shutdownCtx)
	})

	conn := dial(t, func(ctx context.Context) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", socketPath)
	}, grpc.WithPerRPCCredentials(testBearerCredentials{token: fixtureAgentToken}))
	waitHealthy(t, conn, errCh)
	return conn, svc
}

type agentReceiveResult struct {
	response *pb.SubscribeAgentEventsResponse
	err      error
}

func dispatchAndReceiveAgentEvent(
	t *testing.T,
	svc *service.Service,
	stream pb.CommandService_SubscribeAgentEventsClient,
	input service.CommandInput,
) *pb.InboundAgentEvent {
	t.Helper()
	received := make(chan agentReceiveResult, 1)
	go func() {
		response, err := stream.Recv()
		received <- agentReceiveResult{response: response, err: err}
	}()

	for attempt := 0; attempt < 100; attempt++ {
		if _, apiErr := svc.DispatchCommand(context.Background(), input); apiErr != nil {
			t.Fatalf("dispatch agent prompt: %v", apiErr)
		}
		select {
		case result := <-received:
			if result.err != nil {
				t.Fatalf("receive agent prompt: %v", result.err)
			}
			if result.response.GetEvent() == nil {
				t.Fatalf("agent response has no event: %#v", result.response)
			}
			return result.response.GetEvent()
		case <-time.After(10 * time.Millisecond):
		}
	}
	t.Fatal("agent subscription did not receive the prompt")
	return nil
}

func requireAgentReceipt(
	t *testing.T,
	receipt *pb.AgentResponseReceipt,
	responseID string,
	revision uint64,
	phase pb.AgentResponsePhase,
	duplicate bool,
) {
	t.Helper()
	if receipt == nil {
		t.Fatal("agent response receipt is nil")
	}
	if receipt.GetResponseId() == "" {
		t.Fatal("agent response id is empty")
	}
	if responseID != "" && receipt.GetResponseId() != responseID {
		t.Fatalf("response id = %q, want %q", receipt.GetResponseId(), responseID)
	}
	if receipt.GetRevision() != revision || receipt.GetPhase() != phase || receipt.GetDuplicate() != duplicate {
		t.Fatalf("receipt = %#v, want revision=%d phase=%v duplicate=%t", receipt, revision, phase, duplicate)
	}
}

func TestAgentEventToProtoCarriesNativeReaction(t *testing.T) {
	t.Parallel()

	out := agentEventToProto(service.AgentEvent{
		DeliveryID: "delivery_reaction_fixture",
		SenderID:   "sender_fixture",
		MessageReaction: &service.AgentMessageReaction{
			MessageRef:   "msgref_fixture",
			ReactionType: "ThumbsDown",
			Operation:    service.MessageReactionRemoved,
		},
	}).GetEvent()
	if out == nil {
		t.Fatal("converted event is nil")
	}
	reaction := out.GetMessageReaction()
	if reaction == nil || reaction.GetMessageRef() != "msgref_fixture" || reaction.GetReactionType() != "ThumbsDown" || reaction.GetOperation() != pb.MessageReactionOperation_MESSAGE_REACTION_OPERATION_REMOVED {
		t.Fatalf("converted reaction = %#v", reaction)
	}
}

func TestGRPCAgentResponseLifecycle(t *testing.T) {
	sender := &fakeAgentSender{fakeSender: fakeSender{messageID: "unused_fixture"}}
	conn, svc := startAgentUnixServer(t, sender)
	client := pb.NewCommandServiceClient(conn)

	streamCtx, cancelStream := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelStream()
	stream, err := client.SubscribeAgentEvents(streamCtx, &pb.SubscribeAgentEventsRequest{
		Provider:                 "fixture-agent",
		Commands:                 []string{"ask"},
		IncludeUnmatchedMessages: true,
		IncludeCardActions:       true,
	})
	if err != nil {
		t.Fatalf("subscribe agent events: %v", err)
	}

	event := dispatchAndReceiveAgentEvent(t, svc, stream, service.CommandInput{
		DeliveryID:        "delivery_fixture",
		ConversationID:    "conversation_fixture",
		ConversationTitle: "Yoki QA",
		Command:           "ask",
		Text:              "the fixture",
		Prompt:            "Explain the fixture.\nKeep it concise.",
		ChatAlias:         "ops",
		SenderID:          "sender_fixture",
		Metadata: map[string]string{
			"chat_type":    "group",
			"message_type": "text",
			"message_id":   "inbound_message_fixture",
			"parent_id":    "parent_message_fixture",
			"thread_id":    "internal_thread_fixture",
		},
	})
	message := event.GetMessage()
	if event.GetDeliveryId() != "delivery_fixture" || event.GetConversationId() != "conversation_fixture" || event.GetChatAlias() != "ops" || event.GetSenderId() != "sender_fixture" {
		t.Fatalf("agent event identity = %#v", event)
	}
	if message == nil || message.GetText() != "Explain the fixture.\nKeep it concise." || message.GetCommand() != "ask" || message.GetCommandText() != "the fixture" {
		t.Fatalf("agent message = %#v", message)
	}
	if message.GetReplyToMessageRef() != feishu.MessageRefForApp(config.DefaultAppAlias, "parent_message_fixture") || message.GetConversationTitle() != "Yoki QA" {
		t.Fatalf("agent message context = %#v", message)
	}
	if len(event.GetMetadata()) != 2 || event.GetMetadata()["chat_type"] != "group" || event.GetMetadata()["message_type"] != "text" {
		t.Fatalf("public metadata = %#v", event.GetMetadata())
	}
	if _, exposed := event.GetMetadata()["message_id"]; exposed {
		t.Fatalf("private message route exposed in metadata: %#v", event.GetMetadata())
	}

	startRequest := &pb.StartAgentResponseRequest{
		Provider:    "fixture-agent",
		DeliveryId:  "delivery_fixture",
		OperationId: "start_fixture",
		Content: &pb.AgentResponseContent{
			Title:    "Fixture answer",
			Markdown: "Working on it.",
			Actions: []*pb.AgentResponseAction{{
				ActionId:    "retry_fixture",
				Label:       "Retry",
				PayloadJson: `{"source":"fixture"}`,
				Style:       pb.AgentResponseActionStyle_AGENT_RESPONSE_ACTION_STYLE_DANGER,
			}},
		},
	}
	started, err := client.StartAgentResponse(context.Background(), startRequest)
	if err != nil {
		t.Fatalf("start agent response: %v", err)
	}
	requireAgentReceipt(t, started.GetResponse(), "", 1, pb.AgentResponsePhase_AGENT_RESPONSE_PHASE_STREAMING, false)
	if got, want := started.GetResponse().GetMessageRef(), feishu.MessageRefForApp(config.DefaultAppAlias, "message_fixture"); got != want {
		t.Fatalf("response message ref = %q, want %q", got, want)
	}
	responseID := started.GetResponse().GetResponseId()
	if apiErr := svc.DispatchAgentCardAction(context.Background(), service.AgentCardActionInput{
		DeliveryID: "action_fixture", MessageID: "message_fixture", SenderID: "actor_fixture",
		ActionID: "retry_fixture", PayloadJSON: `{"value":{"source":"fixture"}}`,
		ActionPayloadJSON: `{"source":"fixture"}`,
	}); apiErr != nil {
		t.Fatalf("dispatch card action: %v", apiErr)
	}
	actionResponse, err := stream.Recv()
	if err != nil {
		t.Fatalf("receive card action: %v", err)
	}
	actionEvent := actionResponse.GetEvent()
	cardAction := actionEvent.GetCardAction()
	if cardAction == nil || cardAction.GetResponseId() != responseID || cardAction.GetActionId() != "retry_fixture" || cardAction.GetPayloadJson() != `{"value":{"source":"fixture"}}` {
		t.Fatalf("card action event = %#v", actionEvent)
	}
	encodedAction, err := json.Marshal(actionEvent)
	if err != nil {
		t.Fatalf("encode card action event: %v", err)
	}
	if strings.Contains(string(encodedAction), "message_fixture") || strings.Contains(string(encodedAction), "inbound_message_fixture") {
		t.Fatalf("card action exposed private message routing: %s", encodedAction)
	}

	duplicateStart, err := client.StartAgentResponse(context.Background(), startRequest)
	if err != nil {
		t.Fatalf("retry start agent response: %v", err)
	}
	requireAgentReceipt(t, duplicateStart.GetResponse(), responseID, 1, pb.AgentResponsePhase_AGENT_RESPONSE_PHASE_STREAMING, true)

	createdCard, cardSends, contentUpdates, settingUpdates := sender.snapshot()
	if len(cardSends) != 1 || cardSends[0].ReplyToMessageID != "inbound_message_fixture" {
		t.Fatalf("card sends = %#v", cardSends)
	}
	if len(contentUpdates) != 0 || len(settingUpdates) != 0 {
		t.Fatalf("unexpected start mutations: content=%#v settings=%#v", contentUpdates, settingUpdates)
	}
	var card map[string]any
	if err := json.Unmarshal([]byte(createdCard), &card); err != nil {
		t.Fatalf("decode created card: %v", err)
	}
	body, _ := card["body"].(map[string]any)
	elements, _ := body["elements"].([]any)
	if len(elements) < 2 {
		t.Fatalf("created card elements = %#v, want markdown and button elements", elements)
	}
	button, _ := elements[1].(map[string]any)
	if button["tag"] != "button" || button["type"] != "danger" {
		t.Fatalf("action button = %#v, want danger JSON 2.0 button", button)
	}
	behaviors, _ := button["behaviors"].([]any)
	if len(behaviors) != 1 {
		t.Fatalf("button behaviors = %#v, want one callback", behaviors)
	}
	callback, _ := behaviors[0].(map[string]any)
	value, _ := callback["value"].(map[string]any)
	if callback["type"] != "callback" || value["action_id"] != "retry_fixture" || value["payload_json"] != `{"source":"fixture"}` {
		t.Fatalf("button callback = %#v, want retry_fixture", callback)
	}

	updateRequest := &pb.UpdateAgentResponseRequest{
		Provider:         "fixture-agent",
		ResponseId:       responseID,
		OperationId:      "update_fixture",
		ExpectedRevision: 1,
		Markdown:         "Working on it.\nFirst result.",
	}
	updated, err := client.UpdateAgentResponse(context.Background(), updateRequest)
	if err != nil {
		t.Fatalf("update agent response: %v", err)
	}
	requireAgentReceipt(t, updated.GetResponse(), responseID, 2, pb.AgentResponsePhase_AGENT_RESPONSE_PHASE_STREAMING, false)

	duplicateUpdate, err := client.UpdateAgentResponse(context.Background(), updateRequest)
	if err != nil {
		t.Fatalf("retry update agent response: %v", err)
	}
	requireAgentReceipt(t, duplicateUpdate.GetResponse(), responseID, 2, pb.AgentResponsePhase_AGENT_RESPONSE_PHASE_STREAMING, true)

	_, err = client.UpdateAgentResponse(context.Background(), &pb.UpdateAgentResponseRequest{
		Provider:         "fixture-agent",
		ResponseId:       responseID,
		OperationId:      "stale_fixture",
		ExpectedRevision: 1,
		Markdown:         "A stale snapshot.",
	})
	if status.Code(err) != codes.Aborted {
		t.Fatalf("revision conflict code = %v, want Aborted", status.Code(err))
	}
	if detail := botdDetail(t, err); detail == nil || detail.GetCode() != "revision_conflict" || !detail.GetRetryable() {
		t.Fatalf("revision conflict detail = %#v", detail)
	}

	_, err = client.UpdateAgentResponse(context.Background(), &pb.UpdateAgentResponseRequest{
		Provider:         "different-fixture-agent",
		ResponseId:       responseID,
		OperationId:      "private_fixture",
		ExpectedRevision: 2,
		Markdown:         "Must not reveal response ownership.",
	})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("foreign provider code = %v, want PermissionDenied", status.Code(err))
	}
	if detail := botdDetail(t, err); detail == nil || detail.GetCode() != "provider_identity_mismatch" {
		t.Fatalf("foreign provider detail = %#v", detail)
	}

	finished, err := client.FinishAgentResponse(context.Background(), &pb.FinishAgentResponseRequest{
		Provider:         "fixture-agent",
		ResponseId:       responseID,
		OperationId:      "finish_fixture",
		ExpectedRevision: 2,
		Outcome:          pb.AgentResponseOutcome_AGENT_RESPONSE_OUTCOME_FAILED,
		Markdown:         "The fixture could not be completed.",
		Summary:          "Fixture failed",
	})
	if err != nil {
		t.Fatalf("finish agent response: %v", err)
	}
	requireAgentReceipt(t, finished.GetResponse(), responseID, 3, pb.AgentResponsePhase_AGENT_RESPONSE_PHASE_FAILED, false)

	_, err = client.UpdateAgentResponse(context.Background(), &pb.UpdateAgentResponseRequest{
		Provider:         "fixture-agent",
		ResponseId:       responseID,
		OperationId:      "after_finish_fixture",
		ExpectedRevision: 3,
		Markdown:         "This update is too late.",
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("terminal update code = %v, want FailedPrecondition", status.Code(err))
	}
	if detail := botdDetail(t, err); detail == nil || detail.GetCode() != "response_closed" || detail.GetRetryable() {
		t.Fatalf("terminal update detail = %#v", detail)
	}

	_, cardSends, contentUpdates, settingUpdates = sender.snapshot()
	if len(cardSends) != 1 || len(contentUpdates) != 2 || len(settingUpdates) != 1 {
		t.Fatalf("card operation counts: sends=%d content=%d settings=%d", len(cardSends), len(contentUpdates), len(settingUpdates))
	}
}

func TestAgentEnumMappings(t *testing.T) {
	actionStyles := []struct {
		proto pb.AgentResponseActionStyle
		want  service.AgentResponseActionStyle
	}{
		{pb.AgentResponseActionStyle_AGENT_RESPONSE_ACTION_STYLE_UNSPECIFIED, service.AgentResponseActionStyleUnspecified},
		{pb.AgentResponseActionStyle_AGENT_RESPONSE_ACTION_STYLE_DEFAULT, service.AgentResponseActionStyleDefault},
		{pb.AgentResponseActionStyle_AGENT_RESPONSE_ACTION_STYLE_PRIMARY, service.AgentResponseActionStylePrimary},
		{pb.AgentResponseActionStyle_AGENT_RESPONSE_ACTION_STYLE_DANGER, service.AgentResponseActionStyleDanger},
		{pb.AgentResponseActionStyle(99), service.AgentResponseActionStyle(99)},
	}
	for _, test := range actionStyles {
		if got := agentActionStyleFromProto(test.proto); got != test.want {
			t.Errorf("agentActionStyleFromProto(%v) = %v, want %v", test.proto, got, test.want)
		}
	}

	outcomes := []struct {
		proto pb.AgentResponseOutcome
		want  service.AgentResponseOutcome
	}{
		{pb.AgentResponseOutcome_AGENT_RESPONSE_OUTCOME_UNSPECIFIED, service.AgentResponseOutcomeUnspecified},
		{pb.AgentResponseOutcome_AGENT_RESPONSE_OUTCOME_COMPLETED, service.AgentResponseOutcomeCompleted},
		{pb.AgentResponseOutcome_AGENT_RESPONSE_OUTCOME_FAILED, service.AgentResponseOutcomeFailed},
		{pb.AgentResponseOutcome_AGENT_RESPONSE_OUTCOME_CANCELLED, service.AgentResponseOutcomeCancelled},
	}
	for _, test := range outcomes {
		if got := agentOutcomeFromProto(test.proto); got != test.want {
			t.Errorf("agentOutcomeFromProto(%v) = %v, want %v", test.proto, got, test.want)
		}
	}

	phases := []struct {
		service service.AgentResponsePhase
		want    pb.AgentResponsePhase
	}{
		{service.AgentResponsePhaseUnspecified, pb.AgentResponsePhase_AGENT_RESPONSE_PHASE_UNSPECIFIED},
		{service.AgentResponsePhaseStreaming, pb.AgentResponsePhase_AGENT_RESPONSE_PHASE_STREAMING},
		{service.AgentResponsePhaseCompleted, pb.AgentResponsePhase_AGENT_RESPONSE_PHASE_COMPLETED},
		{service.AgentResponsePhaseFailed, pb.AgentResponsePhase_AGENT_RESPONSE_PHASE_FAILED},
		{service.AgentResponsePhaseCancelled, pb.AgentResponsePhase_AGENT_RESPONSE_PHASE_CANCELLED},
	}
	for _, test := range phases {
		if got := agentPhaseToProto(test.service); got != test.want {
			t.Errorf("agentPhaseToProto(%v) = %v, want %v", test.service, got, test.want)
		}
	}
}
