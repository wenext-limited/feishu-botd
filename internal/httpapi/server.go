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

	"feishu-botd/internal/config"
	"feishu-botd/internal/notify"
	"feishu-botd/internal/service"
)

// Server is the HTTP compatibility shim. It owns no business logic; every
// request delegates to the shared *service.Service so the HTTP and gRPC
// transports stay in lockstep.
type Server struct {
	cfg     config.Config
	svc     *service.Service
	logger  *slog.Logger
	servers []*http.Server
	mu      sync.Mutex
}

func NewServer(cfg config.Config, svc *service.Service, logger *slog.Logger) *Server {
	if logger == nil {
		logger = slog.Default()
	}
	return &Server{cfg: cfg, svc: svc, logger: logger}
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
	mux.HandleFunc("POST /v1/message", func(w http.ResponseWriter, r *http.Request) {
		if requireAuth && !s.authorized(r) {
			s.writeError(w, r, notify.NewAPIError(401, "unauthorized", "missing or invalid bearer token", false))
			return
		}
		s.handleMessage(w, r)
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
	info := s.svc.Health()
	writeJSON(w, http.StatusOK, map[string]string{"status": info.Status, "service": info.Service, "version": info.Version})
}

func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	ready, checks := s.svc.Ready(r.Context())
	status := http.StatusOK
	state := "ready"
	if !ready {
		status = http.StatusServiceUnavailable
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
	resp, apiErr := s.svc.SendNotification(r.Context(), req)
	if apiErr != nil {
		s.writeError(w, r, apiErr)
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

type messageRequest struct {
	Source    string           `json:"source"`
	DedupeKey string           `json:"dedupe_key"`
	Target    notify.Target    `json:"target"`
	Markdown  *messageMarkdown `json:"markdown,omitempty"`
	Card      json.RawMessage  `json:"card,omitempty"`
	MsgType   string           `json:"msg_type,omitempty"`
}

type messageMarkdown struct {
	Title    string `json:"title"`
	Markdown string `json:"markdown"`
}

func (s *Server) handleMessage(w http.ResponseWriter, r *http.Request) {
	var req messageRequest
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 128*1024))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		s.writeError(w, r, notify.BadRequest("invalid_json", "invalid JSON request"))
		return
	}
	if req.MsgType != "" && req.MsgType != "interactive" {
		s.writeError(w, r, notify.BadRequest("unsupported_msg_type", "only interactive card msg_type is supported"))
		return
	}

	input := service.MessageInput{
		Channel:   req.Target.Channel,
		Source:    req.Source,
		DedupeKey: req.DedupeKey,
		CardJSON:  cardJSONFromHTTP(req.Card),
	}
	if req.Markdown != nil {
		input.Title = req.Markdown.Title
		input.Markdown = req.Markdown.Markdown
	}

	result, apiErr := s.svc.SendMessage(r.Context(), input)
	if apiErr != nil {
		s.writeError(w, r, apiErr)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"provider":   result.Provider,
		"message_id": result.MessageID,
		"duplicate":  result.Duplicate,
	})
}

func cardJSONFromHTTP(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var wrapper map[string]json.RawMessage
	if err := json.Unmarshal(raw, &wrapper); err == nil && len(wrapper) == 1 {
		if value, ok := wrapper["card_json"]; ok {
			var cardJSON string
			if err := json.Unmarshal(value, &cardJSON); err == nil {
				return strings.TrimSpace(cardJSON)
			}
		}
	}
	return strings.TrimSpace(string(raw))
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
		// Echo it back, mirroring the gRPC interceptor's x-request-id header.
		w.Header().Set("X-Request-Id", id)
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
