package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/oops-rs/feishu-botd/internal/config"
	"github.com/oops-rs/feishu-botd/internal/dedupe"
	"github.com/oops-rs/feishu-botd/internal/notify"
)

type fakeSender struct {
	messageID string
	err       error
	readyErr  error
	calls     int
	chatID    string
	request   notify.Request
}

func (f *fakeSender) Ready(_ context.Context) error { return f.readyErr }

func (f *fakeSender) Send(_ context.Context, chatID string, req notify.Request) (string, error) {
	f.calls++
	f.chatID = chatID
	f.request = req
	if f.err != nil {
		return "", f.err
	}
	return f.messageID, nil
}

func testServer(sender *fakeSender) *Server {
	cfg := config.Config{AppID: "cli_test", AppSecret: "secret", BindAddr: "127.0.0.1:0", AuthToken: "token", Channels: map[string]string{"ops": "oc_test"}, DedupeTTL: time.Hour, SendTimeout: time.Second}
	return NewServer(cfg, sender, dedupe.NewMemoryStore(time.Hour), slog.Default())
}

func validBody() []byte {
	body := notify.Request{Source: "xipe", SourceEventID: "evt_1", DedupeKey: "xipe:evt_1:ops", Severity: "critical", Title: "Title", Markdown: "**Body**", Target: notify.Target{Channel: "ops"}, Metadata: map[string]string{"trigger": "reauth_required"}}
	data, _ := json.Marshal(body)
	return data
}

func TestHealthAndReady(t *testing.T) {
	server := testServer(&fakeSender{messageID: "om_1"})
	rec := httptest.NewRecorder()
	server.handler(false).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("health status = %d", rec.Code)
	}
	rec = httptest.NewRecorder()
	server.handler(false).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("ready status = %d", rec.Code)
	}
}

func TestNotifySuccessAndDuplicate(t *testing.T) {
	sender := &fakeSender{messageID: "om_1"}
	server := testServer(sender)
	h := server.handler(true)
	for i := 0; i < 2; i++ {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/v1/notify", bytes.NewReader(validBody()))
		req.Header.Set("Authorization", "Bearer token")
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("notify %d status = %d body=%s", i, rec.Code, rec.Body.String())
		}
		var resp notify.Response
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			t.Fatal(err)
		}
		if i == 1 && !resp.Duplicate {
			t.Fatal("second response was not duplicate")
		}
	}
	if sender.calls != 1 {
		t.Fatalf("sender calls = %d", sender.calls)
	}
	if sender.chatID != "oc_test" {
		t.Fatalf("sender chat id = %q", sender.chatID)
	}
	if sender.request.Title != "Title" || sender.request.Markdown != "**Body**" {
		t.Fatalf("sender request = %#v", sender.request)
	}
}

func TestNotifyDedupeConflict(t *testing.T) {
	sender := &fakeSender{messageID: "om_1"}
	server := testServer(sender)
	h := server.handler(true)
	req := httptest.NewRequest(http.MethodPost, "/v1/notify", bytes.NewReader(validBody()))
	req.Header.Set("Authorization", "Bearer token")
	h.ServeHTTP(httptest.NewRecorder(), req)

	var body notify.Request
	_ = json.Unmarshal(validBody(), &body)
	body.Title = "Different"
	data, _ := json.Marshal(body)
	rec := httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/v1/notify", bytes.NewReader(data))
	req.Header.Set("Authorization", "Bearer token")
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusConflict {
		t.Fatalf("conflict status = %d", rec.Code)
	}
}

func TestNotifyAuthRequired(t *testing.T) {
	server := testServer(&fakeSender{messageID: "om_1"})
	rec := httptest.NewRecorder()
	server.handler(true).ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/notify", bytes.NewReader(validBody())))
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d", rec.Code)
	}
}

func TestNotifyProviderFailure(t *testing.T) {
	server := testServer(&fakeSender{err: errors.New("boom secret-token")})
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/notify", bytes.NewReader(validBody()))
	req.Header.Set("Authorization", "Bearer token")
	server.handler(true).ServeHTTP(rec, req)
	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status = %d", rec.Code)
	}
}

func TestRedactedErrorRemovesConfiguredSecrets(t *testing.T) {
	server := testServer(&fakeSender{messageID: "om_1"})
	server.cfg.AppSecret = "secret-value"
	server.cfg.AuthToken = "token-value"
	server.cfg.Channels["ops"] = "oc_secret"
	msg := server.redactedError(errors.New("secret-value token-value oc_secret visible"))
	for _, leaked := range []string{"secret-value", "token-value", "oc_secret"} {
		if bytes.Contains([]byte(msg), []byte(leaked)) {
			t.Fatalf("redacted message leaked %q: %s", leaked, msg)
		}
	}
}

func TestReadyReportsAuthFailure(t *testing.T) {
	server := testServer(&fakeSender{readyErr: errors.New("token failed")})
	rec := httptest.NewRecorder()
	server.handler(false).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("ready status = %d", rec.Code)
	}
}

func TestUnixSocketServerServesHealth(t *testing.T) {
	server := testServer(&fakeSender{messageID: "om_1"})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	socketPath := filepath.Join("/tmp", fmt.Sprintf("feishu-botd-test-%d.sock", time.Now().UnixNano()))
	defer os.Remove(socketPath)
	errCh := make(chan error, 1)
	go func() {
		errCh <- server.ListenAndServeUnix(ctx, socketPath)
	}()
	defer func() {
		cancel()
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), time.Second)
		defer shutdownCancel()
		_ = server.Shutdown(shutdownCtx)
	}()

	client := http.Client{
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, "unix", socketPath)
			},
		},
		Timeout: time.Second,
	}

	var lastErr error
	for i := 0; i < 50; i++ {
		resp, err := client.Get("http://unix/healthz")
		if err == nil {
			defer resp.Body.Close()
			body, _ := io.ReadAll(resp.Body)
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("health status = %d body=%s", resp.StatusCode, string(body))
			}
			return
		}
		lastErr = err
		select {
		case err := <-errCh:
			t.Fatalf("unix server exited early: %v", err)
		case <-time.After(20 * time.Millisecond):
		}
	}
	t.Fatalf("unix server did not become ready: %v", lastErr)
}
