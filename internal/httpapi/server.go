package httpapi

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/oops-rs/feishu-botd/internal/config"
	"github.com/oops-rs/feishu-botd/internal/dedupe"
	"github.com/oops-rs/feishu-botd/internal/feishu"
	"github.com/oops-rs/feishu-botd/internal/notify"
)

const Version = "0.1.0"

type Server struct {
	cfg     config.Config
	sender  feishu.Sender
	store   *dedupe.MemoryStore
	logger  *slog.Logger
	servers []*http.Server
	mu      sync.Mutex
}

func NewServer(cfg config.Config, sender feishu.Sender, store *dedupe.MemoryStore, logger *slog.Logger) *Server {
	return &Server{cfg: cfg, sender: sender, store: store, logger: logger}
}

func (s *Server) ListenAndServeUnix(ctx context.Context, socketPath string) error {
	_ = os.Remove(socketPath)
	ln, err := net.Listen("unix", socketPath)
	if err != nil {
		return err
	}
	if err := os.Chmod(socketPath, 0o660); err != nil {
		_ = ln.Close()
		return err
	}
	server := &http.Server{Handler: s.handler(false)}
	s.track(server)
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()
	s.logger.Info("listening on unix socket", "socket", socketPath)
	return server.Serve(ln)
}

func (s *Server) ListenAndServeTCP(ctx context.Context, bindAddr string) error {
	ln, err := net.Listen("tcp", bindAddr)
	if err != nil {
		return err
	}
	server := &http.Server{Handler: s.handler(true)}
	s.track(server)
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()
	s.logger.Info("listening on tcp", "addr", bindAddr)
	return server.Serve(ln)
}

func (s *Server) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	servers := append([]*http.Server(nil), s.servers...)
	s.mu.Unlock()
	var errs []error
	for _, server := range servers {
		if err := server.Shutdown(ctx); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (s *Server) track(server *http.Server) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.servers = append(s.servers, server)
}

func (s *Server) handler(requireAuth bool) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.handleHealth)
	mux.HandleFunc("GET /readyz", s.handleReady)
	mux.HandleFunc("POST /v1/notify", func(w http.ResponseWriter, r *http.Request) {
		if requireAuth && !s.authorized(r) {
			s.writeError(w, r, notify.NewAPIError(401, "unauthorized", "missing or invalid bearer token", false))
			return
		}
		s.handleNotify(w, r)
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		s.writeError(w, r, notify.NewAPIError(404, "not_found", "not found", false))
	})
	return requestIDMiddleware(mux)
}

func (s *Server) authorized(r *http.Request) bool {
	expected := s.cfg.AuthToken
	if expected == "" {
		return false
	}
	got := strings.TrimSpace(r.Header.Get("Authorization"))
	if !strings.HasPrefix(got, "Bearer ") {
		return false
	}
	got = strings.TrimSpace(strings.TrimPrefix(got, "Bearer "))
	return subtle.ConstantTimeCompare([]byte(got), []byte(expected)) == 1
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok", "service": "feishu-botd", "version": Version})
}

func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	checks := map[string]string{"config": "ok", "feishu_auth": "ok", "channels": "ok", "dedupe_store": "ok"}
	status := http.StatusOK
	if s.cfg.AppID == "" || s.cfg.AppSecret == "" {
		checks["feishu_auth"] = "missing_credentials"
		status = http.StatusServiceUnavailable
	}
	if len(s.cfg.Channels) == 0 {
		checks["channels"] = "missing_channels"
		status = http.StatusServiceUnavailable
	}
	if !s.store.Ready() {
		checks["dedupe_store"] = "unavailable"
		status = http.StatusServiceUnavailable
	}
	if status == http.StatusOK {
		ctx, cancel := context.WithTimeout(r.Context(), s.cfg.SendTimeout)
		defer cancel()
		if err := s.sender.Ready(ctx); err != nil {
			checks["feishu_auth"] = "unavailable"
			status = http.StatusServiceUnavailable
			s.logger.Warn("readiness auth check failed", "error", s.redactedError(err))
		}
	}
	state := "ready"
	if status != http.StatusOK {
		state = "unready"
	}
	writeJSON(w, status, map[string]any{"status": state, "checks": checks})
}

func (s *Server) handleNotify(w http.ResponseWriter, r *http.Request) {
	var req notify.Request
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64*1024))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		s.writeError(w, r, notify.BadRequest("invalid_json", "invalid JSON request"))
		return
	}
	if err := req.Validate(s.cfg.Channels); err != nil {
		s.writeError(w, r, err)
		return
	}
	fingerprint := req.Fingerprint()
	reserved := s.store.Reserve(req.Source, req.DedupeKey, fingerprint)
	if reserved.Conflict {
		s.writeError(w, r, notify.NewAPIError(409, "dedupe_conflict", "dedupe key reused with different content", false))
		return
	}
	if reserved.InFlight {
		s.writeError(w, r, notify.NewAPIError(409, "dedupe_in_flight", "dedupe key is already being sent", true))
		return
	}
	if reserved.Duplicate {
		writeJSON(w, http.StatusOK, notify.Response{Status: "sent", Provider: reserved.Result.Provider, DedupeKey: req.DedupeKey, MessageID: reserved.Result.MessageID, Duplicate: true})
		return
	}

	chatID := s.cfg.Channels[req.Target.Channel]
	ctx, cancel := context.WithTimeout(r.Context(), s.cfg.SendTimeout)
	defer cancel()
	messageID, err := s.sender.Send(ctx, chatID, req)
	if err != nil {
		s.store.Abort(req.Source, req.DedupeKey)
		s.logger.Warn("notify send failed", "source", req.Source, "event", req.SourceEventID, "channel", req.Target.Channel, "error", s.redactedError(err))
		s.writeError(w, r, notify.NewAPIError(502, "feishu_unavailable", "Feishu send failed", true))
		return
	}

	result := dedupe.Result{Provider: "feishu", MessageID: messageID}
	s.store.Commit(req.Source, req.DedupeKey, result)
	s.logger.Info("notify sent", "source", req.Source, "event", req.SourceEventID, "channel", req.Target.Channel, "severity", req.Severity)
	writeJSON(w, http.StatusOK, notify.Response{Status: "sent", Provider: result.Provider, DedupeKey: req.DedupeKey, MessageID: result.MessageID, Duplicate: false})
}

func (s *Server) writeError(w http.ResponseWriter, r *http.Request, err *notify.APIError) {
	writeJSON(w, err.Status, notify.ErrorResponse{Error: notify.ErrorBody{Code: err.Code, Message: err.Message, Retryable: err.Retryable, RequestID: requestID(r)}})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func requestIDMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("X-Request-Id")
		if id == "" {
			id = fmt.Sprintf("req_%d", time.Now().UnixNano())
		}
		ctx := context.WithValue(r.Context(), requestIDKey{}, id)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

type requestIDKey struct{}

func requestID(r *http.Request) string {
	if value, ok := r.Context().Value(requestIDKey{}).(string); ok {
		return value
	}
	return ""
}

func (s *Server) redactedError(err error) string {
	msg := err.Error()
	for _, secret := range s.redactionValues() {
		msg = strings.ReplaceAll(msg, secret, "<redacted>")
	}
	if len(msg) > 180 {
		return msg[:180] + "..."
	}
	return msg
}

func (s *Server) redactionValues() []string {
	values := []string{s.cfg.AppSecret, s.cfg.AuthToken}
	for _, chatID := range s.cfg.Channels {
		values = append(values, chatID)
	}
	kept := values[:0]
	for _, value := range values {
		value = strings.TrimSpace(value)
		if len(value) >= 4 {
			kept = append(kept, value)
		}
	}
	return kept
}
