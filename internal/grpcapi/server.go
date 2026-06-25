// Package grpcapi is the gRPC transport for feishu-botd. It is a thin adapter:
// every RPC converts proto <-> domain types and delegates to the shared
// *service.Service, so the gRPC and HTTP transports cannot drift.
package grpcapi

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"os"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	pb "feishu-botd/gen/feishubotd/v1"
	"feishu-botd/internal/config"
	"feishu-botd/internal/service"
)

// Server hosts the gRPC services over one or more listeners. The Unix listener
// is unauthenticated (local trust); the TCP listener requires a bearer token.
type Server struct {
	cfg    config.Config
	svc    *service.Service
	logger *slog.Logger

	mu      sync.Mutex
	servers []*grpc.Server
}

func NewServer(cfg config.Config, svc *service.Service, logger *slog.Logger) *Server {
	if logger == nil {
		logger = slog.Default()
	}
	return &Server{cfg: cfg, svc: svc, logger: logger}
}

// newGRPCServer builds a grpc.Server with the standard interceptor chain. The
// bearer-auth interceptor is added only when requireAuth is set (TCP listener).
func (s *Server) newGRPCServer(requireAuth bool) *grpc.Server {
	unary := []grpc.UnaryServerInterceptor{
		recoveryUnaryInterceptor(s.logger, s.svc.Redact),
		requestIDUnaryInterceptor(),
	}
	stream := []grpc.StreamServerInterceptor{
		recoveryStreamInterceptor(s.logger, s.svc.Redact),
		requestIDStreamInterceptor(),
	}
	if requireAuth {
		unary = append(unary, authUnaryInterceptor(s.cfg.AuthToken))
		stream = append(stream, authStreamInterceptor(s.cfg.AuthToken))
	}

	gs := grpc.NewServer(
		grpc.ChainUnaryInterceptor(unary...),
		grpc.ChainStreamInterceptor(stream...),
	)

	pb.RegisterNotificationServiceServer(gs, &notificationServer{svc: s.svc})
	pb.RegisterBotdHealthServiceServer(gs, &healthServer{svc: s.svc})

	// Standard gRPC health service so grpc_health_probe works for future
	// gRPC-only deployments. CommandService and ProviderService remain skeletons
	// and are deliberately NOT registered in this slice.
	healthSrv := health.NewServer()
	healthSrv.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(gs, healthSrv)

	return gs
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
	gs := s.newGRPCServer(false)
	s.track(gs)
	go func() {
		<-ctx.Done()
		gs.GracefulStop()
	}()
	s.logger.Info("grpc listening on unix socket", "socket", socketPath)
	return ignoreStopped(gs.Serve(ln))
}

func (s *Server) ListenAndServeTCP(ctx context.Context, bindAddr string) error {
	ln, err := net.Listen("tcp", bindAddr)
	if err != nil {
		return err
	}
	return s.serveTCP(ctx, ln)
}

// serveTCP serves the authenticated gRPC server on an already-bound listener.
// Taking a live listener lets tests avoid the close-then-rebind port race.
func (s *Server) serveTCP(ctx context.Context, ln net.Listener) error {
	gs := s.newGRPCServer(true)
	s.track(gs)
	go func() {
		<-ctx.Done()
		gs.GracefulStop()
	}()
	s.logger.Info("grpc listening on tcp", "addr", ln.Addr().String())
	return ignoreStopped(gs.Serve(ln))
}

// Shutdown gracefully stops every listener, forcing a hard stop if ctx expires
// first. Each GracefulStop is joined before return so the call is race-clean.
func (s *Server) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	servers := append([]*grpc.Server(nil), s.servers...)
	s.mu.Unlock()

	for _, gs := range servers {
		done := make(chan struct{})
		go func(g *grpc.Server) {
			g.GracefulStop()
			close(done)
		}(gs)
		select {
		case <-done:
		case <-ctx.Done():
			gs.Stop()
			<-done
		}
	}
	return nil
}

func (s *Server) track(gs *grpc.Server) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.servers = append(s.servers, gs)
}

// ignoreStopped treats a graceful/forced stop as a clean exit, mirroring how the
// HTTP server treats http.ErrServerClosed.
func ignoreStopped(err error) error {
	if errors.Is(err, grpc.ErrServerStopped) {
		return nil
	}
	return err
}
