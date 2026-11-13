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

func TestReloadSwapsCredentialsAndResetsOnlyChangedProviders(t *testing.T) {
	cfg := testConfig()
	cfg.AuthToken = fixtureGeneralTCPToken
	cfg.DedupeTTL = 2 * time.Hour
	cfg.AgentProviders = map[string]config.AgentProviderConfig{
		"kept":    {AuthToken: fixtureAgentToken, AllowedCommands: []string{"ask"}},
		"changed": {AuthToken: secondAgentToken, AllowedCommands: []string{"ping"}},
	}
	svc := service.NewService(cfg, &fakeSender{messageID: "om_1"}, dedupe.NewMemoryStore(time.Hour), slog.Default())
	srv := NewServer(cfg, svc, slog.Default())
	ctx, cancel := context.WithCancel(context.Background())
	socketPath := tempSocket(t)
	errCh := make(chan error, 1)
	go func() { errCh <- srv.ListenAndServeUnix(ctx, socketPath) }()
	t.Cleanup(func() {
		cancel()
		sc, c := context.WithTimeout(context.Background(), time.Second)
		defer c()
		_ = srv.Shutdown(sc)
	})
	dialer := func(ctx context.Context) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", socketPath)
	}
	health := dial(t, dialer)
	waitHealthy(t, health, errCh)
	providerClient := func(token string) pb.CommandServiceClient {
		return pb.NewCommandServiceClient(dial(t, dialer, grpc.WithPerRPCCredentials(testBearerCredentials{token: token})))
	}

	streamCtx, cancelStreams := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelStreams()
	keptStream, err := providerClient(fixtureAgentToken).SubscribeAgentEvents(streamCtx, &pb.SubscribeAgentEventsRequest{
		Provider: "kept", Commands: []string{"ask"},
	})
	if err != nil {
		t.Fatal(err)
	}
	changedStream, err := providerClient(secondAgentToken).SubscribeAgentEvents(streamCtx, &pb.SubscribeAgentEventsRequest{
		Provider: "changed", Commands: []string{"ping"},
	})
	if err != nil {
		t.Fatal(err)
	}
	// Each stream is registered once a dispatch reaches it.
	_ = dispatchAndReceiveAgentEvent(t, svc, changedStream, service.CommandInput{
		DeliveryID: "delivery_before", Command: "ping", Prompt: "ping", ConversationID: "conv_before",
		ChatAlias: "ops", SenderID: "ou_sender",
		Metadata: map[string]string{"chat_type": "group", "message_type": "text", "message_id": "om_before"},
	})
	_ = dispatchAndReceiveAgentEvent(t, svc, keptStream, service.CommandInput{
		DeliveryID: "delivery_before2", Command: "ask", Prompt: "ask", ConversationID: "conv_before2",
		ChatAlias: "ops", SenderID: "ou_sender",
		Metadata: map[string]string{"chat_type": "group", "message_type": "text", "message_id": "om_before2"},
	})

	const rotatedToken = "rotated-agent-token-0123456789abcdef0123456789"
	next := cfg
	next.AgentProviders = map[string]config.AgentProviderConfig{
		"kept":    {AuthToken: fixtureAgentToken, AllowedCommands: []string{"ask"}},
		"changed": {AuthToken: rotatedToken, AllowedCommands: []string{"ping"}},
	}
	plan := config.PlanReload(cfg, next)
	svc.ApplyConfig(next, plan, nil, nil, time.Now())
	srv.ApplyConfig(next)

	if _, err := changedStream.Recv(); status.Code(err) != codes.Unavailable ||
		botdDetail(t, err).GetCode() != "provider_reconfigured" {
		t.Fatalf("changed provider stream ended with %v", err)
	}
	_ = dispatchAndReceiveAgentEvent(t, svc, keptStream, service.CommandInput{
		DeliveryID: "delivery_after", Command: "ask", Prompt: "ask", ConversationID: "conv_after",
		ChatAlias: "ops", SenderID: "ou_sender",
		Metadata: map[string]string{"chat_type": "group", "message_type": "text", "message_id": "om_after"},
	})

	oldCtx, cancelOld := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelOld()
	old, err := providerClient(secondAgentToken).SubscribeAgentEvents(oldCtx, &pb.SubscribeAgentEventsRequest{
		Provider: "changed", Commands: []string{"ping"},
	})
	if err == nil {
		_, err = old.Recv()
	}
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("old token after rotation: %v", err)
	}
	rotated, err := providerClient(rotatedToken).SubscribeAgentEvents(streamCtx, &pb.SubscribeAgentEventsRequest{
		Provider: "changed", Commands: []string{"ping"},
	})
	if err != nil {
		t.Fatal(err)
	}
	_ = dispatchAndReceiveAgentEvent(t, svc, rotated, service.CommandInput{
		DeliveryID: "delivery_rotated", Command: "ping", Prompt: "ping", ConversationID: "conv_rotated",
		ChatAlias: "ops", SenderID: "ou_sender",
		Metadata: map[string]string{"chat_type": "group", "message_type": "text", "message_id": "om_rotated"},
	})

	ready, err := pb.NewBotdHealthServiceClient(health).Ready(context.Background(), &pb.ReadyRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if ready.GetConfig().GetGeneration() != 2 || ready.GetConfig().GetLastReload() != "ok" {
		t.Fatalf("ready config = %v", ready.GetConfig())
	}
}
