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

// startAgentReactionUnixServer mirrors startCoTUnixServer with the agent
// reaction capability under test, so the denied case exercises the real
// config default rather than a hand-built principal.
func startAgentReactionUnixServer(t *testing.T, sender *fakeAgentSender, allowAgentReactions bool) (*grpc.ClientConn, *service.Service) {
	t.Helper()
	cfg := testConfig()
	cfg.AgentProviders = map[string]config.AgentProviderConfig{
		"fixture-agent": {
			AuthToken: fixtureAgentToken, AllowedCommands: []string{"ask"},
			AllowUnmatchedMessages: true, AllowCardActions: true,
			AllowAgentReactions: allowAgentReactions,
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

// startFixtureAgentResponse subscribes the fixture provider, delivers one
// prompt, and starts a response over the real gRPC path, returning its
// response id — what AddAgentReaction needs to target.
func startFixtureAgentResponse(t *testing.T, svc *service.Service, client pb.CommandServiceClient, deliveryID string) string {
	t.Helper()
	streamCtx, cancelStream := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancelStream)
	stream, err := client.SubscribeAgentEvents(streamCtx, &pb.SubscribeAgentEventsRequest{
		Provider: "fixture-agent", Commands: []string{"ask"},
	})
	if err != nil {
		t.Fatalf("subscribe agent events: %v", err)
	}
	_ = dispatchAndReceiveAgentEvent(t, svc, stream, service.CommandInput{
		DeliveryID: deliveryID, ConversationID: "conversation_" + deliveryID,
		Command: "ask", Prompt: "ask the fixture", ChatAlias: "ops", SenderID: "sender_fixture",
		Metadata: map[string]string{"message_id": "inbound_" + deliveryID},
	})
	started, err := client.StartAgentResponse(context.Background(), &pb.StartAgentResponseRequest{
		Provider: "fixture-agent", DeliveryId: deliveryID, OperationId: "start_" + deliveryID,
		Content: &pb.AgentResponseContent{Markdown: "working"},
	})
	if err != nil {
		t.Fatalf("start agent response: %v", err)
	}
	responseID := started.GetResponse().GetResponseId()
	if responseID == "" {
		t.Fatal("start agent response returned no response id")
	}
	return responseID
}

func TestGRPCAddAgentReactionRequiresTheProviderCapability(t *testing.T) {
	sender := &fakeAgentSender{fakeSender: fakeSender{messageID: "om_reaction_fixture"}}
	conn, svc := startAgentReactionUnixServer(t, sender, false)
	client := pb.NewCommandServiceClient(conn)
	responseID := startFixtureAgentResponse(t, svc, client, "delivery_reaction_denied")

	_, err := client.AddAgentReaction(context.Background(), &pb.AddAgentReactionRequest{
		Provider: "fixture-agent", ResponseId: responseID, OperationId: "react-1", EmojiType: "HEART",
	})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("reaction without the capability = %v, want PermissionDenied", err)
	}
	if detail := botdDetail(t, err); detail == nil || detail.GetCode() != "provider_scope_denied" {
		t.Fatalf("scope denial detail = %#v", detail)
	}
	if len(sender.addedReactions) != 0 {
		t.Fatalf("denied reaction reached Feishu: %d calls", len(sender.addedReactions))
	}
}

func TestGRPCAddAgentReactionPlacesTheChosenEmojiThenReplays(t *testing.T) {
	sender := &fakeAgentSender{fakeSender: fakeSender{messageID: "om_reaction_fixture"}}
	conn, svc := startAgentReactionUnixServer(t, sender, true)
	client := pb.NewCommandServiceClient(conn)
	responseID := startFixtureAgentResponse(t, svc, client, "delivery_reaction_granted")

	request := &pb.AddAgentReactionRequest{
		Provider: "fixture-agent", ResponseId: responseID, OperationId: "react-1", EmojiType: "HEART",
	}
	resp, err := client.AddAgentReaction(context.Background(), request)
	if err != nil {
		t.Fatalf("add agent reaction: %v", err)
	}
	if resp.GetDuplicate() {
		t.Fatal("first reaction reported as duplicate")
	}
	if len(sender.addedReactions) != 1 || sender.addedReactions[0].EmojiType != "HEART" {
		t.Fatalf("added reactions = %#v", sender.addedReactions)
	}

	replay, err := client.AddAgentReaction(context.Background(), request)
	if err != nil {
		t.Fatalf("replay add agent reaction: %v", err)
	}
	if !replay.GetDuplicate() {
		t.Fatal("replayed operation id was not reported as a duplicate")
	}
	if len(sender.addedReactions) != 1 {
		t.Fatalf("added reactions after replay = %d, want 1", len(sender.addedReactions))
	}
}

func TestGRPCAddAgentReactionRejectsUnknownResponse(t *testing.T) {
	sender := &fakeAgentSender{fakeSender: fakeSender{messageID: "om_reaction_fixture"}}
	conn, _ := startAgentReactionUnixServer(t, sender, true)
	client := pb.NewCommandServiceClient(conn)

	_, err := client.AddAgentReaction(context.Background(), &pb.AddAgentReactionRequest{
		Provider: "fixture-agent", ResponseId: "resp_does_not_exist", OperationId: "react-1", EmojiType: "HEART",
	})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("unknown response = %v, want NotFound", err)
	}
	if len(sender.addedReactions) != 0 {
		t.Fatalf("unknown response reached Feishu: %d calls", len(sender.addedReactions))
	}
}
