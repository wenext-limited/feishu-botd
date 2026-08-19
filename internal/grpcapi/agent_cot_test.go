package grpcapi

import (
	"context"
	"log/slog"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"

	pb "feishu-botd/gen/feishubotd/v1"
	"feishu-botd/internal/config"
	"feishu-botd/internal/dedupe"
	"feishu-botd/internal/service"
)

// startCoTUnixServer mirrors startAgentUnixServer with the CoT progress
// capability under test, so the denied case exercises the real config
// default rather than a hand-built principal.
func startCoTUnixServer(t *testing.T, sender *fakeAgentSender, allowCoTProgress bool) (*grpc.ClientConn, *service.Service) {
	t.Helper()
	cfg := testConfig()
	cfg.AgentProviders = map[string]config.AgentProviderConfig{
		"fixture-agent": {
			AuthToken: fixtureAgentToken, AllowedCommands: []string{"ask"},
			AllowUnmatchedMessages: true, AllowCardActions: true,
			AllowCoTProgress: allowCoTProgress,
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

func cotStartRequest(deliveryID, operationID string) *pb.StartAgentResponseRequest {
	return &pb.StartAgentResponseRequest{
		Provider: "fixture-agent", DeliveryId: deliveryID, OperationId: operationID,
		Content: &pb.AgentResponseContent{
			Markdown: "working",
		},
		TimelineSteps: []*pb.AgentTimelineStep{
			{StepId: "s1", Label: "识别问题", State: pb.AgentTimelineStepState_AGENT_TIMELINE_STEP_STATE_STARTED},
		},
	}
}

// TestGRPCAgentCoTStepsRequireGrant proves the capability gate lives at the
// gRPC boundary, not inside the service: a provider without AllowCoTProgress
// has its steps silently dropped, and the base card Start still succeeds —
// exactly like an unauthorized follow-up degrades the card update rather than
// failing it. A granted provider's steps reach the CoT transport.
func TestGRPCAgentCoTStepsRequireGrant(t *testing.T) {
	for _, tc := range []struct {
		name     string
		granted  bool
		wantCoTs int
	}{
		{name: "not granted: steps silently dropped", granted: false, wantCoTs: 0},
		{name: "granted: steps reach the CoT transport", granted: true, wantCoTs: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sender := &fakeAgentSender{fakeSender: fakeSender{messageID: "unused_fixture"}}
			conn, svc := startCoTUnixServer(t, sender, tc.granted)
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
				DeliveryID: "delivery_cot_gate", ConversationID: "conversation_cot_gate",
				Command: "ask", Prompt: "ask the fixture", ChatAlias: "ops", SenderID: "sender_fixture",
				Metadata: map[string]string{"message_id": "inbound_cot_gate_fixture"},
			})

			started, err := client.StartAgentResponse(context.Background(), cotStartRequest("delivery_cot_gate", "start_cot_gate"))
			if err != nil {
				t.Fatalf("start agent response: %v", err)
			}
			if started.GetResponse().GetResponseId() == "" {
				t.Fatal("start agent response returned no response id")
			}

			creates, _, _ := sender.cotSnapshot()
			if len(creates) != tc.wantCoTs {
				t.Fatalf("cot creates = %d, want %d", len(creates), tc.wantCoTs)
			}
		})
	}
}
