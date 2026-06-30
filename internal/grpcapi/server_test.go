package grpcapi

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	pb "feishu-botd/gen/feishubotd/v1"
	"feishu-botd/internal/config"
	"feishu-botd/internal/dedupe"
	"feishu-botd/internal/notify"
	"feishu-botd/internal/service"
)

type fakeSender struct {
	messageID string
	err       error
	readyErr  error
	calls     int
	chatID    string
	request   notify.Request
	started   chan struct{} // closed when Send begins (optional)
	release   chan struct{} // blocks Send until closed (optional)
}

func (f *fakeSender) Ready(_ context.Context) error { return f.readyErr }

func (f *fakeSender) Send(ctx context.Context, chatID string, req notify.Request) (string, error) {
	f.calls++
	f.chatID = chatID
	f.request = req
	if f.started != nil {
		close(f.started)
	}
	if f.release != nil {
		// Respect context cancellation so a forced gRPC Stop() (which cancels the
		// RPC context) can unblock an in-flight send.
		select {
		case <-f.release:
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}
	if f.err != nil {
		return "", f.err
	}
	return f.messageID, nil
}

func validNotifyRequest(channel, dedupeKey string) *pb.SendNotificationRequest {
	return &pb.SendNotificationRequest{
		Source:        "xipe",
		SourceEventId: "evt_1",
		DedupeKey:     dedupeKey,
		Severity:      pb.Severity_SEVERITY_CRITICAL,
		Title:         "Title",
		Markdown:      "**body**",
		Target:        channelTarget(channel),
	}
}

func testConfig() config.Config {
	return config.Config{
		AppID:       "cli_test",
		AppSecret:   "secret",
		Channels:    map[string]string{"ops": "oc_test", "ci": "oc_ci"},
		Services:    map[string]config.ServiceConfig{"jenkins": {DefaultChannel: "ci"}},
		DedupeTTL:   time.Hour,
		SendTimeout: time.Second,
	}
}

func newTestServer(cfg config.Config, sender *fakeSender) *Server {
	svc := service.NewService(cfg, sender, dedupe.NewMemoryStore(time.Hour), slog.Default())
	return NewServer(cfg, svc, slog.Default())
}

var socketCounter atomic.Int64

func tempSocket(t *testing.T) string {
	t.Helper()
	// Keep the path short: macOS sun_path is capped near 104 bytes.
	path := filepath.Join("/tmp", fmt.Sprintf("fbd-grpc-%d-%d.sock", os.Getpid(), socketCounter.Add(1)))
	t.Cleanup(func() { _ = os.Remove(path) })
	return path
}

// startUnixServer starts a gRPC server on a temp Unix socket and returns a
// connected client.
func startUnixServer(t *testing.T, sender *fakeSender) *grpc.ClientConn {
	t.Helper()
	srv := newTestServer(testConfig(), sender)
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

	conn := dial(t, func(ctx context.Context) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", socketPath)
	})
	waitHealthy(t, conn, errCh)
	return conn
}

func dial(t *testing.T, dialer func(context.Context) (net.Conn, error)) *grpc.ClientConn {
	t.Helper()
	conn, err := grpc.NewClient(
		"passthrough:///feishu-botd",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return dialer(ctx)
		}),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func waitHealthy(t *testing.T, conn *grpc.ClientConn, errCh chan error) {
	t.Helper()
	client := pb.NewBotdHealthServiceClient(conn)
	var lastErr error
	for i := 0; i < 100; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		_, err := client.Health(ctx, &pb.HealthRequest{})
		cancel()
		if err == nil {
			return
		}
		lastErr = err
		select {
		case e := <-errCh:
			t.Fatalf("grpc server exited early: %v", e)
		case <-time.After(20 * time.Millisecond):
		}
	}
	t.Fatalf("grpc server did not become ready: %v", lastErr)
}

func channelTarget(alias string) *pb.MessageTarget {
	return &pb.MessageTarget{To: &pb.MessageTarget_Channel{Channel: alias}}
}

func botdDetail(t *testing.T, err error) *pb.BotdError {
	t.Helper()
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("not a status error: %v", err)
	}
	for _, d := range st.Details() {
		if be, ok := d.(*pb.BotdError); ok {
			return be
		}
	}
	return nil
}

func TestGRPCUnixHealthAndNotify(t *testing.T) {
	sender := &fakeSender{messageID: "om_1"}
	conn := startUnixServer(t, sender)

	health, err := pb.NewBotdHealthServiceClient(conn).Health(context.Background(), &pb.HealthRequest{})
	if err != nil {
		t.Fatalf("health: %v", err)
	}
	if health.GetStatus() != "ok" || health.GetService() != "feishu-botd" || health.GetVersion() != service.Version {
		t.Fatalf("health = %#v", health)
	}

	nc := pb.NewNotificationServiceClient(conn)
	req := &pb.SendNotificationRequest{
		Source:        "xipe",
		SourceEventId: "evt_1",
		DedupeKey:     "k1",
		Severity:      pb.Severity_SEVERITY_CRITICAL,
		Title:         "Title",
		Markdown:      "**body**",
		Target:        channelTarget("ops"),
	}
	resp, err := nc.SendNotification(context.Background(), req)
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	if resp.GetMessageId() != "om_1" || resp.GetDuplicate() || resp.GetProvider() != "feishu" {
		t.Fatalf("resp = %#v", resp)
	}
	if sender.chatID != "oc_test" {
		t.Fatalf("sender chat id = %q", sender.chatID)
	}

	dup, err := nc.SendNotification(context.Background(), req)
	if err != nil {
		t.Fatalf("send dup: %v", err)
	}
	if !dup.GetDuplicate() {
		t.Fatal("second call not marked duplicate")
	}
	if sender.calls != 1 {
		t.Fatalf("sender calls = %d, want 1", sender.calls)
	}
}

func TestGRPCUnknownChannelReturnsNotFoundDetail(t *testing.T) {
	conn := startUnixServer(t, &fakeSender{messageID: "om_1"})
	_, err := pb.NewNotificationServiceClient(conn).SendNotification(context.Background(), &pb.SendNotificationRequest{
		Source:        "x",
		SourceEventId: "e",
		DedupeKey:     "k",
		Severity:      pb.Severity_SEVERITY_INFO,
		Title:         "T",
		Markdown:      "b",
		Target:        channelTarget("nope"),
	})
	st, _ := status.FromError(err)
	if st.Code() != codes.NotFound {
		t.Fatalf("code = %v, want NotFound", st.Code())
	}
	detail := botdDetail(t, err)
	if detail == nil || detail.GetCode() != "unknown_channel" {
		t.Fatalf("detail = %#v", detail)
	}
}

func TestGRPCProviderFailureReturnsUnavailable(t *testing.T) {
	conn := startUnixServer(t, &fakeSender{err: errors.New("boom")})
	_, err := pb.NewNotificationServiceClient(conn).SendNotification(context.Background(), &pb.SendNotificationRequest{
		Source:        "x",
		SourceEventId: "e",
		DedupeKey:     "k",
		Severity:      pb.Severity_SEVERITY_INFO,
		Title:         "T",
		Markdown:      "b",
		Target:        channelTarget("ops"),
	})
	st, _ := status.FromError(err)
	if st.Code() != codes.Unavailable {
		t.Fatalf("code = %v, want Unavailable", st.Code())
	}
	if detail := botdDetail(t, err); detail == nil || !detail.GetRetryable() {
		t.Fatalf("expected retryable detail, got %#v", detail)
	}
}

func TestGRPCSendMessageMarkdownCardAndUnimplemented(t *testing.T) {
	sender := &fakeSender{messageID: "om_msg"}
	conn := startUnixServer(t, sender)
	nc := pb.NewNotificationServiceClient(conn)

	resp, err := nc.SendMessage(context.Background(), &pb.SendMessageRequest{
		Target:  channelTarget("ops"),
		Content: &pb.SendMessageRequest_Markdown{Markdown: &pb.MarkdownContent{Markdown: "**hi**"}},
	})
	if err != nil {
		t.Fatalf("send markdown: %v", err)
	}
	if resp.GetMessageId() != "om_msg" {
		t.Fatalf("resp = %#v", resp)
	}
	if sender.request.Markdown != "**hi**" || sender.request.CardJSON != "" {
		t.Fatalf("sender markdown request = %#v", sender.request)
	}

	sender.messageID = "om_card"
	cardJSON := `{"type":"template","data":{"template_id":"tpl"}}`
	resp, err = nc.SendMessage(context.Background(), &pb.SendMessageRequest{
		Target:  channelTarget("ops"),
		Content: &pb.SendMessageRequest_Card{Card: &pb.CardContent{CardJson: cardJSON}},
	})
	if err != nil {
		t.Fatalf("send card: %v", err)
	}
	if resp.GetMessageId() != "om_card" {
		t.Fatalf("card resp = %#v", resp)
	}
	if sender.request.CardJSON != cardJSON || sender.request.Markdown != "" {
		t.Fatalf("sender card request = %#v", sender.request)
	}

	sender.messageID = "om_default"
	resp, err = nc.SendMessage(context.Background(), &pb.SendMessageRequest{
		Source:  "jenkins",
		Content: &pb.SendMessageRequest_Markdown{Markdown: &pb.MarkdownContent{Markdown: "**default**"}},
	})
	if err != nil {
		t.Fatalf("send with service default: %v", err)
	}
	if resp.GetMessageId() != "om_default" || sender.chatID != "oc_ci" || sender.request.Target.Channel != "ci" {
		t.Fatalf("service default resp=%#v chat=%q req=%#v", resp, sender.chatID, sender.request)
	}

	_, err = nc.SendMessage(context.Background(), &pb.SendMessageRequest{
		Target:  channelTarget("ops"),
		Content: &pb.SendMessageRequest_Text{Text: &pb.TextContent{Text: "plain"}},
	})
	if st, _ := status.FromError(err); st.Code() != codes.Unimplemented {
		t.Fatalf("text content code = %v, want Unimplemented", st.Code())
	}
	// The Unimplemented error must still carry the in-contract BotdError detail.
	if d := botdDetail(t, err); d == nil || d.GetCode() != "unimplemented" {
		t.Fatalf("unimplemented detail = %#v", d)
	}
}

// TestGRPCInFlightReturnsAborted drives a real in-flight reservation end-to-end:
// while the first send is blocked, a second send with the same key returns
// codes.Aborted with a retryable dedupe_in_flight detail.
func TestGRPCInFlightReturnsAborted(t *testing.T) {
	sender := &fakeSender{messageID: "om_1", started: make(chan struct{}), release: make(chan struct{})}
	conn := startUnixServer(t, sender)
	nc := pb.NewNotificationServiceClient(conn)

	firstDone := make(chan struct{})
	go func() {
		defer close(firstDone)
		_, _ = nc.SendNotification(context.Background(), validNotifyRequest("ops", "k1"))
	}()
	<-sender.started // first send is in flight; reservation held inFlight

	_, err := nc.SendNotification(context.Background(), validNotifyRequest("ops", "k1"))
	if status.Code(err) != codes.Aborted {
		t.Fatalf("code = %v, want Aborted", status.Code(err))
	}
	if d := botdDetail(t, err); d == nil || d.GetCode() != "dedupe_in_flight" || !d.GetRetryable() {
		t.Fatalf("detail = %#v", d)
	}

	close(sender.release)
	<-firstDone
}

// TestGRPCRequestIDEchoedAndInDetail asserts the request-id interceptor honors a
// client-supplied id, mints one when absent, echoes it in response headers, and
// surfaces it in the BotdError detail.
func TestGRPCRequestIDEchoedAndInDetail(t *testing.T) {
	conn := startUnixServer(t, &fakeSender{messageID: "om_1"})
	nc := pb.NewNotificationServiceClient(conn)

	ctx := metadata.AppendToOutgoingContext(context.Background(), "x-request-id", "req_client_1")
	var hdr metadata.MD
	_, err := nc.SendNotification(ctx, validNotifyRequest("nope", "k1"), grpc.Header(&hdr))
	if got := hdr.Get("x-request-id"); len(got) == 0 || got[0] != "req_client_1" {
		t.Fatalf("echoed x-request-id = %v, want [req_client_1]", got)
	}
	if d := botdDetail(t, err); d == nil || d.GetRequestId() != "req_client_1" {
		t.Fatalf("detail request id = %#v", d)
	}

	var hdr2 metadata.MD
	_, err = nc.SendNotification(context.Background(), validNotifyRequest("nope", "k2"), grpc.Header(&hdr2))
	minted := hdr2.Get("x-request-id")
	if len(minted) == 0 || minted[0] == "" {
		t.Fatalf("expected a minted x-request-id, got %v", minted)
	}
	if d := botdDetail(t, err); d == nil || d.GetRequestId() != minted[0] {
		t.Fatalf("minted id mismatch: header=%v detail=%#v", minted, d)
	}
}

// TestGRPCShutdownForcesStopWhenContextExpires exercises the forced-Stop
// fallback in Server.Shutdown: with an in-flight RPC and an already-expired
// context, Shutdown must escalate to gs.Stop() and return promptly.
func TestGRPCShutdownForcesStopWhenContextExpires(t *testing.T) {
	sender := &fakeSender{messageID: "om_1", started: make(chan struct{}), release: make(chan struct{})}
	srv := newTestServer(testConfig(), sender)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	socketPath := tempSocket(t)
	errCh := make(chan error, 1)
	go func() { errCh <- srv.ListenAndServeUnix(ctx, socketPath) }()

	conn := dial(t, func(ctx context.Context) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", socketPath)
	})
	waitHealthy(t, conn, errCh)

	rpcDone := make(chan struct{})
	go func() {
		defer close(rpcDone)
		_, _ = pb.NewNotificationServiceClient(conn).SendNotification(context.Background(), validNotifyRequest("ops", "k1"))
	}()
	<-sender.started // RPC is in flight; GracefulStop would block on it

	expired, cancelExpired := context.WithCancel(context.Background())
	cancelExpired() // already-expired shutdown budget
	shutDone := make(chan struct{})
	go func() { _ = srv.Shutdown(expired); close(shutDone) }()
	select {
	case <-shutDone:
	case <-time.After(3 * time.Second):
		t.Fatal("Shutdown did not force-stop within 3s")
	}

	close(sender.release) // unblock the server-side handler goroutine
	<-rpcDone
}
