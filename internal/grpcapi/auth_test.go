package grpcapi

import (
	"context"
	"log/slog"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	pb "feishu-botd/gen/feishubotd/v1"
	"feishu-botd/internal/config"
	"feishu-botd/internal/dedupe"
	"feishu-botd/internal/service"
)

const (
	fixtureAgentToken      = "fixture-agent-token-0123456789abcdef0123456789"
	secondAgentToken       = "second-agent-token-0123456789abcdef0123456789"
	fixtureGeneralTCPToken = "general-notifier-token-0123456789abcdef012345"
)

type testBearerCredentials struct{ token string }

func (c testBearerCredentials) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	return map[string]string{"authorization": "Bearer " + c.token}, nil
}

func (testBearerCredentials) RequireTransportSecurity() bool { return false }

func bearerContext(token string) context.Context {
	return metadata.AppendToOutgoingContext(context.Background(), "authorization", "Bearer "+token)
}

func TestIsHealthMethod(t *testing.T) {
	if !isHealthMethod("/feishubotd.v1.BotdHealthService/Health") {
		t.Fatal("botd health must be exempt")
	}
	if !isHealthMethod("/grpc.health.v1.Health/Check") {
		t.Fatal("standard grpc health must be exempt")
	}
	if isHealthMethod("/feishubotd.v1.NotificationService/SendNotification") {
		t.Fatal("notification must not be exempt")
	}
}

func TestProviderMethodClassification(t *testing.T) {
	for _, method := range []string{
		pb.CommandService_SubscribeAgentEvents_FullMethodName,
		pb.CommandService_StartAgentResponse_FullMethodName,
		pb.CommandService_UpdateAgentResponse_FullMethodName,
		pb.CommandService_FinishAgentResponse_FullMethodName,
	} {
		if !isAgentMethod(method) || !requiresProviderAuth(method, false) {
			t.Fatalf("agent method was not provider scoped: %s", method)
		}
	}
	for _, method := range []string{pb.CommandService_Subscribe_FullMethodName, pb.CommandService_Respond_FullMethodName} {
		if !isLegacyProviderMethod(method) || requiresProviderAuth(method, false) || !requiresProviderAuth(method, true) {
			t.Fatalf("legacy provider method policy is wrong: %s", method)
		}
	}
	if requiresProviderAuth(pb.NotificationService_SendNotification_FullMethodName, true) {
		t.Fatal("notification RPC was classified as a provider method")
	}
}

// startTCPServer serves on a live, already-bound loopback listener (handed
// straight to serveTCP) to avoid the close-then-rebind port race.
func startTCPServer(t *testing.T, sender *fakeSender, token string) *grpc.ClientConn {
	t.Helper()
	cfg := testConfig()
	cfg.AuthToken = token
	conn, _ := startTCPServerWithConfig(t, cfg, sender)
	return conn
}

func startTCPServerWithConfig(t *testing.T, cfg config.Config, sender *fakeSender) (*grpc.ClientConn, *service.Service) {
	t.Helper()
	svc := service.NewService(cfg, sender, dedupe.NewMemoryStore(time.Hour), slog.Default())
	srv := NewServer(cfg, svc, slog.Default())

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- srv.serveTCP(ctx, ln) }()
	t.Cleanup(func() {
		cancel()
		sc, c := context.WithTimeout(context.Background(), time.Second)
		defer c()
		_ = srv.Shutdown(sc)
	})

	conn := dial(t, func(ctx context.Context) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "tcp", addr)
	})
	waitHealthy(t, conn, errCh)
	return conn, svc
}

func TestGRPCTCPRequiresBearerToken(t *testing.T) {
	conn := startTCPServer(t, &fakeSender{messageID: "om_1"}, "s3cret")
	nc := pb.NewNotificationServiceClient(conn)
	req := &pb.SendNotificationRequest{
		Source:        "x",
		SourceEventId: "e",
		DedupeKey:     "k",
		Severity:      pb.Severity_SEVERITY_INFO,
		Title:         "T",
		Markdown:      "b",
		Target:        channelTarget("ops"),
	}

	missingErr := func() error { _, err := nc.SendNotification(context.Background(), req); return err }()
	if status.Code(missingErr) != codes.Unauthenticated {
		t.Fatalf("missing token code = %v, want Unauthenticated", status.Code(missingErr))
	}
	// The auth rejection must carry the in-contract BotdError detail, like HTTP 401.
	if d := botdDetail(t, missingErr); d == nil || d.GetCode() != "unauthorized" {
		t.Fatalf("unauthenticated detail = %#v", d)
	}

	ctxBad := metadata.AppendToOutgoingContext(context.Background(), "authorization", "Bearer nope")
	if _, err := nc.SendNotification(ctxBad, req); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("wrong token code = %v, want Unauthenticated", status.Code(err))
	}

	ctxOK := metadata.AppendToOutgoingContext(context.Background(), "authorization", "Bearer s3cret")
	resp, err := nc.SendNotification(ctxOK, req)
	if err != nil {
		t.Fatalf("valid token send: %v", err)
	}
	if resp.GetMessageId() != "om_1" {
		t.Fatalf("resp = %#v", resp)
	}
}

func TestGRPCTCPHealthExemptFromAuth(t *testing.T) {
	conn := startTCPServer(t, &fakeSender{messageID: "om_1"}, "s3cret")
	// No token supplied; health must still answer on the authenticated listener.
	if _, err := pb.NewBotdHealthServiceClient(conn).Health(context.Background(), &pb.HealthRequest{}); err != nil {
		t.Fatalf("health without token: %v", err)
	}
}

func TestLegacyCommandTCPKeepsGeneralTokenModeWithoutScopedProviders(t *testing.T) {
	cfg := testConfig()
	cfg.AuthToken = fixtureGeneralTCPToken
	conn, svc := startTCPServerWithConfig(t, cfg, &fakeSender{messageID: "unused"})
	client := pb.NewCommandServiceClient(conn)
	streamCtx, cancel := context.WithTimeout(bearerContext(fixtureGeneralTCPToken), 2*time.Second)
	defer cancel()
	stream, err := client.Subscribe(streamCtx, &pb.SubscribeRequest{Provider: "legacy-fixture", Commands: []string{"status"}})
	if err != nil {
		t.Fatalf("legacy subscribe with general token: %v", err)
	}
	received := make(chan error, 1)
	go func() {
		_, recvErr := stream.Recv()
		received <- recvErr
	}()
	for attempt := 0; attempt < 100; attempt++ {
		delivered, apiErr := svc.DispatchCommand(context.Background(), service.CommandInput{
			DeliveryID: "legacy-general-delivery", Command: "status", ChatAlias: "ops",
		})
		if apiErr != nil {
			t.Fatalf("dispatch legacy command: %v", apiErr)
		}
		if delivered > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	select {
	case recvErr := <-received:
		if recvErr != nil {
			t.Fatalf("receive legacy command: %v", recvErr)
		}
	case <-time.After(time.Second):
		t.Fatal("legacy general-token provider did not receive command")
	}
}

func TestAgentRPCsRequireAuthenticatedProviderOnUnix(t *testing.T) {
	cfg := testConfig()
	cfg.AgentProviders = map[string]config.AgentProviderConfig{
		"agent-a": {AuthToken: fixtureAgentToken, AllowUnmatchedMessages: true},
		"agent-b": {AuthToken: secondAgentToken},
	}
	conn, svc := startUnixServerWithService(t, cfg, &fakeSender{messageID: "unused"})
	client := pb.NewCommandServiceClient(conn)
	request := &pb.StartAgentResponseRequest{
		Provider: "agent-a", DeliveryId: "unknown-delivery", OperationId: "start-1",
		Content: &pb.AgentResponseContent{Markdown: "fixture"},
	}

	_, err := client.StartAgentResponse(context.Background(), request)
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("missing provider token code = %v, want Unauthenticated", status.Code(err))
	}
	if detail := botdDetail(t, err); detail.GetCode() != "unauthorized" || detail.GetRequestId() == "" {
		t.Fatalf("missing provider token detail = %#v", detail)
	}

	_, err = client.StartAgentResponse(bearerContext(fixtureGeneralTCPToken), request)
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("general token on agent RPC code = %v, want Unauthenticated", status.Code(err))
	}

	mismatch := proto.Clone(request).(*pb.StartAgentResponseRequest)
	mismatch.Provider = "agent-b"
	_, err = client.StartAgentResponse(bearerContext(fixtureAgentToken), mismatch)
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("provider mismatch code = %v, want PermissionDenied", status.Code(err))
	}
	if detail := botdDetail(t, err); detail.GetCode() != "provider_identity_mismatch" {
		t.Fatalf("provider mismatch detail = %#v", detail)
	}

	duplicateAuth := metadata.NewOutgoingContext(context.Background(), metadata.Pairs(
		"authorization", "Bearer "+fixtureAgentToken,
		"authorization", "Bearer "+fixtureAgentToken,
	))
	_, err = client.StartAgentResponse(duplicateAuth, request)
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("duplicate authorization metadata code = %v, want Unauthenticated", status.Code(err))
	}

	_, err = client.StartAgentResponse(bearerContext(fixtureAgentToken), request)
	if status.Code(err) != codes.Unimplemented {
		t.Fatalf("valid provider token code = %v, want service Unimplemented", status.Code(err))
	}

	scopeCtx, cancelScope := context.WithTimeout(bearerContext(secondAgentToken), time.Second)
	defer cancelScope()
	outOfScope, err := client.SubscribeAgentEvents(scopeCtx, &pb.SubscribeAgentEventsRequest{
		Provider: "agent-b", IncludeUnmatchedMessages: true,
	})
	if err == nil {
		_, err = outOfScope.Recv()
	}
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("out-of-scope unmatched subscription code = %v, want PermissionDenied", status.Code(err))
	}
	if detail := botdDetail(t, err); detail.GetCode() != "provider_scope_denied" {
		t.Fatalf("out-of-scope subscription detail = %#v", detail)
	}

	actionScopeCtx, cancelActionScope := context.WithTimeout(bearerContext(fixtureAgentToken), time.Second)
	defer cancelActionScope()
	outOfScopeActions, err := client.SubscribeAgentEvents(actionScopeCtx, &pb.SubscribeAgentEventsRequest{
		Provider: "agent-a", IncludeUnmatchedMessages: true, IncludeCardActions: true,
	})
	if err == nil {
		_, err = outOfScopeActions.Recv()
	}
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("out-of-scope action subscription code = %v, want PermissionDenied", status.Code(err))
	}

	streamCtx, cancel := context.WithTimeout(bearerContext(fixtureAgentToken), 2*time.Second)
	defer cancel()
	stream, err := client.SubscribeAgentEvents(streamCtx, &pb.SubscribeAgentEventsRequest{
		Provider: "agent-a", IncludeUnmatchedMessages: true,
	})
	if err != nil {
		t.Fatalf("subscribe with provider token: %v", err)
	}
	event := dispatchAndReceiveAgentEvent(t, svc, stream, service.CommandInput{
		DeliveryID: "delivery-auth-fixture", Command: "hello", Prompt: "safe fixture prompt",
		ChatAlias: "ops", Metadata: map[string]string{"chat_type": "group"},
	})
	if event.GetDeliveryId() != "delivery-auth-fixture" {
		t.Fatalf("received event = %#v", event)
	}
}

func TestScopedProviderUnixSocketSeparatesGeneralOutboundAuthority(t *testing.T) {
	providerConfig := map[string]config.AgentProviderConfig{
		"agent-a": {AuthToken: fixtureAgentToken, AllowUnmatchedMessages: true},
	}
	request := &pb.SendNotificationRequest{
		Source: "local-fixture", SourceEventId: "event-1", DedupeKey: "local-fixture:event-1",
		Severity: pb.Severity_SEVERITY_INFO, Title: "Fixture", Markdown: "Safe fixture",
		Target: channelTarget("ops"),
	}

	withoutGeneral := testConfig()
	withoutGeneral.AgentProviders = providerConfig
	closedConn, _ := startUnixServerWithService(t, withoutGeneral, &fakeSender{messageID: "unused"})
	if _, err := pb.NewNotificationServiceClient(closedConn).SendNotification(context.Background(), request); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("scoped Unix without general token code = %v, want Unauthenticated", status.Code(err))
	}

	withGeneral := testConfig()
	withGeneral.AuthToken = fixtureGeneralTCPToken
	withGeneral.AgentProviders = providerConfig
	conn, _ := startUnixServerWithService(t, withGeneral, &fakeSender{messageID: "message-fixture"})
	client := pb.NewNotificationServiceClient(conn)
	if _, err := client.SendNotification(bearerContext(fixtureAgentToken), request); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("provider token on Unix notification code = %v, want Unauthenticated", status.Code(err))
	}
	if _, err := client.SendNotification(bearerContext(fixtureGeneralTCPToken), request); err != nil {
		t.Fatalf("general token on scoped Unix notification: %v", err)
	}
}

func TestScopedProvidersSeparateNotificationAndInboundCommandAuthorityOnTCP(t *testing.T) {
	cfg := testConfig()
	cfg.AuthToken = fixtureGeneralTCPToken
	cfg.AgentProviders = map[string]config.AgentProviderConfig{
		"agent-a": {AuthToken: fixtureAgentToken, AllowedCommands: []string{"status"}, AllowLegacyCommands: true},
		"agent-b": {AuthToken: secondAgentToken, AllowedCommands: []string{"status"}, AllowLegacyCommands: true},
	}
	sender := &fakeSender{messageID: "message-fixture"}
	conn, svc := startTCPServerWithConfig(t, cfg, sender)
	notifications := pb.NewNotificationServiceClient(conn)
	commands := pb.NewCommandServiceClient(conn)
	notifyRequest := &pb.SendNotificationRequest{
		Source: "fixture", SourceEventId: "event-1", DedupeKey: "fixture:event-1",
		Severity: pb.Severity_SEVERITY_INFO, Title: "Fixture", Markdown: "Safe fixture",
		Target: channelTarget("ops"),
	}

	if _, err := notifications.SendNotification(bearerContext(fixtureGeneralTCPToken), notifyRequest); err != nil {
		t.Fatalf("general token notification: %v", err)
	}
	if _, err := notifications.SendNotification(bearerContext(fixtureAgentToken), notifyRequest); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("provider token notification code = %v, want Unauthenticated", status.Code(err))
	}

	legacyGeneralCtx, cancelGeneral := context.WithTimeout(bearerContext(fixtureGeneralTCPToken), time.Second)
	defer cancelGeneral()
	legacyGeneral, err := commands.Subscribe(legacyGeneralCtx, &pb.SubscribeRequest{Provider: "agent-a", Commands: []string{"status"}})
	if err == nil {
		_, err = legacyGeneral.Recv()
	}
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("general token legacy subscribe code = %v, want Unauthenticated", status.Code(err))
	}

	legacyMismatchCtx, cancelMismatch := context.WithTimeout(bearerContext(secondAgentToken), time.Second)
	defer cancelMismatch()
	legacyMismatch, err := commands.Subscribe(legacyMismatchCtx, &pb.SubscribeRequest{Provider: "agent-a", Commands: []string{"status"}})
	if err == nil {
		_, err = legacyMismatch.Recv()
	}
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("mismatched legacy subscribe code = %v, want PermissionDenied", status.Code(err))
	}

	legacyScopeCtx, cancelLegacyScope := context.WithTimeout(bearerContext(fixtureAgentToken), time.Second)
	defer cancelLegacyScope()
	legacyOutOfScope, err := commands.Subscribe(legacyScopeCtx, &pb.SubscribeRequest{Provider: "agent-a", Commands: []string{"deploy"}})
	if err == nil {
		_, err = legacyOutOfScope.Recv()
	}
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("out-of-scope legacy command code = %v, want PermissionDenied", status.Code(err))
	}

	legacyCtx, cancelLegacy := context.WithTimeout(bearerContext(fixtureAgentToken), 2*time.Second)
	defer cancelLegacy()
	legacy, err := commands.Subscribe(legacyCtx, &pb.SubscribeRequest{Provider: "agent-a", Commands: []string{"status"}})
	if err != nil {
		t.Fatalf("scoped legacy subscribe: %v", err)
	}
	received := make(chan error, 1)
	go func() {
		_, recvErr := legacy.Recv()
		received <- recvErr
	}()
	for attempt := 0; attempt < 100; attempt++ {
		delivered, apiErr := svc.DispatchCommand(context.Background(), service.CommandInput{
			DeliveryID: "legacy-auth-delivery", Command: "status", ChatAlias: "ops",
			Metadata: map[string]string{"message_id": "message-inbound-fixture"},
		})
		if apiErr != nil {
			t.Fatalf("dispatch legacy command: %v", apiErr)
		}
		if delivered > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	select {
	case recvErr := <-received:
		if recvErr != nil {
			t.Fatalf("receive scoped legacy command: %v", recvErr)
		}
	case <-time.After(time.Second):
		t.Fatal("scoped legacy provider did not receive command")
	}

	respondRequest := &pb.RespondRequest{
		DeliveryId: "legacy-auth-delivery",
		Reply:      &pb.RespondRequest_Markdown{Markdown: &pb.MarkdownContent{Markdown: "fixture response"}},
	}
	if _, err := commands.Respond(bearerContext(secondAgentToken), respondRequest); status.Code(err) != codes.NotFound {
		t.Fatalf("non-recipient provider respond code = %v, want NotFound", status.Code(err))
	}
	if _, err := commands.Respond(bearerContext(fixtureGeneralTCPToken), respondRequest); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("general token legacy respond code = %v, want Unauthenticated", status.Code(err))
	}
	if _, err := commands.Respond(bearerContext(fixtureAgentToken), respondRequest); err != nil {
		t.Fatalf("recipient provider respond: %v", err)
	}
}
