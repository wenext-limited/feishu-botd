package grpcapi

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	pb "feishu-botd/gen/feishubotd/v1"
)

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

// startTCPServer serves on a live, already-bound loopback listener (handed
// straight to serveTCP) to avoid the close-then-rebind port race.
func startTCPServer(t *testing.T, sender *fakeSender, token string) *grpc.ClientConn {
	t.Helper()
	cfg := testConfig()
	cfg.AuthToken = token
	srv := newTestServer(cfg, sender)

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
	return conn
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
