package grpcapi

import (
	"context"
	"log/slog"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "feishu-botd/gen/feishubotd/v1"
	"feishu-botd/internal/config"
	"feishu-botd/internal/dedupe"
	"feishu-botd/internal/service"
)

// startFollowUpUnixServer mirrors startAgentUnixServer with the follow-up
// capability under test, so the denied case exercises the real config default.
func startFollowUpUnixServer(t *testing.T, sender *fakeAgentSender, allowFollowUp bool) (*grpc.ClientConn, *service.Service) {
	t.Helper()
	cfg := testConfig()
	cfg.AgentProviders = map[string]config.AgentProviderConfig{
		"fixture-agent": {
			AuthToken: fixtureAgentToken, AllowedCommands: []string{"ask"},
			AllowUnmatchedMessages: true, AllowCardActions: true,
			AllowFollowUpMessages: allowFollowUp,
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

// seedFollowUpConversation subscribes the fixture provider and delivers one
// prompt, which is what earns the provider its follow-up scope.
func seedFollowUpConversation(t *testing.T, svc *service.Service, client pb.CommandServiceClient) string {
	t.Helper()
	streamCtx, cancelStream := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancelStream)
	stream, err := client.SubscribeAgentEvents(streamCtx, &pb.SubscribeAgentEventsRequest{
		Provider:                 "fixture-agent",
		IncludeUnmatchedMessages: true,
	})
	if err != nil {
		t.Fatalf("subscribe agent events: %v", err)
	}
	event := dispatchAndReceiveAgentEvent(t, svc, stream, service.CommandInput{
		DeliveryID:     "delivery_follow_up",
		ConversationID: "conversation_follow_up",
		Command:        "ask",
		Prompt:         "Research this and report back.",
		ChatAlias:      "ops",
		SenderID:       "sender_fixture",
		Metadata: map[string]string{
			"chat_type": "group", "message_type": "text", "message_id": "inbound_message_fixture",
		},
	})
	return event.GetConversationId()
}

func followUpRequest(conversationID, operationID string) *pb.SendAgentFollowUpRequest {
	return &pb.SendAgentFollowUpRequest{
		Provider:       "fixture-agent",
		ConversationId: conversationID,
		OperationId:    operationID,
		Markdown:       "The deep run finished. Three regressions found.",
		Summary:        "Deep run finished",
	}
}

func TestGRPCAgentFollowUpDeliversThenReplays(t *testing.T) {
	sender := &fakeAgentSender{fakeSender: fakeSender{messageID: "om_follow_up"}}
	conn, svc := startFollowUpUnixServer(t, sender, true)
	client := pb.NewCommandServiceClient(conn)
	conversationID := seedFollowUpConversation(t, svc, client)

	response, err := client.SendAgentFollowUp(context.Background(), followUpRequest(conversationID, "follow-up-1"))
	if err != nil {
		t.Fatalf("send agent follow-up: %v", err)
	}
	receipt := response.GetFollowUp()
	if receipt.GetFollowUpId() == "" || receipt.GetDuplicate() {
		t.Fatalf("follow-up receipt = %#v", receipt)
	}
	if sender.chatID != "oc_test" || sender.request.Markdown != "The deep run finished. Three regressions found." {
		t.Fatalf("follow-up send = chat %q request %#v", sender.chatID, sender.request)
	}

	replay, err := client.SendAgentFollowUp(context.Background(), followUpRequest(conversationID, "follow-up-1"))
	if err != nil {
		t.Fatalf("replay agent follow-up: %v", err)
	}
	if !replay.GetFollowUp().GetDuplicate() || replay.GetFollowUp().GetFollowUpId() != receipt.GetFollowUpId() {
		t.Fatalf("replay receipt = %#v, want duplicate of %q", replay.GetFollowUp(), receipt.GetFollowUpId())
	}
	if sender.calls != 1 {
		t.Fatalf("send calls = %d, want 1", sender.calls)
	}
}

func TestGRPCAgentFollowUpRequiresTheProviderCapability(t *testing.T) {
	sender := &fakeAgentSender{fakeSender: fakeSender{messageID: "om_follow_up"}}
	conn, svc := startFollowUpUnixServer(t, sender, false)
	client := pb.NewCommandServiceClient(conn)
	conversationID := seedFollowUpConversation(t, svc, client)

	_, err := client.SendAgentFollowUp(context.Background(), followUpRequest(conversationID, "follow-up-1"))
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("follow-up without the capability = %v, want PermissionDenied", err)
	}
	if detail := botdDetail(t, err); detail == nil || detail.GetCode() != "provider_scope_denied" {
		t.Fatalf("scope denial detail = %#v", detail)
	}
	if sender.calls != 0 {
		t.Fatalf("denied follow-up reached Feishu: %d calls", sender.calls)
	}
}

func TestGRPCAgentFollowUpRejectsUnknownConversation(t *testing.T) {
	sender := &fakeAgentSender{fakeSender: fakeSender{messageID: "om_follow_up"}}
	conn, svc := startFollowUpUnixServer(t, sender, true)
	client := pb.NewCommandServiceClient(conn)
	_ = seedFollowUpConversation(t, svc, client)

	_, err := client.SendAgentFollowUp(context.Background(), followUpRequest("conversation_elsewhere", "follow-up-1"))
	if status.Code(err) != codes.NotFound {
		t.Fatalf("unknown conversation = %v, want NotFound", err)
	}
	if detail := botdDetail(t, err); detail == nil || detail.GetCode() != "unknown_conversation" || detail.GetRetryable() {
		t.Fatalf("unknown conversation detail = %#v", detail)
	}
	if sender.calls != 0 {
		t.Fatalf("unknown conversation reached Feishu: %d calls", sender.calls)
	}
}

func TestGRPCAgentFollowUpRejectsProviderIdentityMismatch(t *testing.T) {
	sender := &fakeAgentSender{fakeSender: fakeSender{messageID: "om_follow_up"}}
	conn, svc := startFollowUpUnixServer(t, sender, true)
	client := pb.NewCommandServiceClient(conn)
	conversationID := seedFollowUpConversation(t, svc, client)

	request := followUpRequest(conversationID, "follow-up-1")
	request.Provider = "other-agent"
	_, err := client.SendAgentFollowUp(context.Background(), request)
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("provider mismatch = %v, want PermissionDenied", err)
	}
	if detail := botdDetail(t, err); detail == nil || detail.GetCode() != "provider_identity_mismatch" {
		t.Fatalf("provider mismatch detail = %#v", detail)
	}
	if sender.calls != 0 {
		t.Fatalf("mismatched provider reached Feishu: %d calls", sender.calls)
	}
}

func TestGRPCAgentFollowUpConflictingReuseIsAlreadyExists(t *testing.T) {
	sender := &fakeAgentSender{fakeSender: fakeSender{messageID: "om_follow_up"}}
	conn, svc := startFollowUpUnixServer(t, sender, true)
	client := pb.NewCommandServiceClient(conn)
	conversationID := seedFollowUpConversation(t, svc, client)

	if _, err := client.SendAgentFollowUp(context.Background(), followUpRequest(conversationID, "follow-up-1")); err != nil {
		t.Fatalf("send agent follow-up: %v", err)
	}
	conflicting := followUpRequest(conversationID, "follow-up-1")
	conflicting.Markdown = "A different answer."
	_, err := client.SendAgentFollowUp(context.Background(), conflicting)
	if status.Code(err) != codes.AlreadyExists {
		t.Fatalf("conflicting reuse = %v, want AlreadyExists", err)
	}
	if detail := botdDetail(t, err); detail == nil || detail.GetCode() != "operation_conflict" {
		t.Fatalf("conflicting reuse detail = %#v", detail)
	}
	if sender.calls != 1 {
		t.Fatalf("send calls = %d, want 1", sender.calls)
	}
}
