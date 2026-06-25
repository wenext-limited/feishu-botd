package grpcapi

import (
	"context"
	"crypto/subtle"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"feishu-botd/internal/notify"
)

// isHealthMethod reports whether a fully-qualified gRPC method belongs to a
// health service. Health stays unauthenticated even on the loopback TCP path so
// process managers and grpc_health_probe can poll without a token.
func isHealthMethod(fullMethod string) bool {
	return strings.HasPrefix(fullMethod, "/feishubotd.v1.BotdHealthService/") ||
		strings.HasPrefix(fullMethod, "/grpc.health.v1.Health/")
}

// authorized performs a constant-time bearer-token check against incoming
// metadata. It mirrors the HTTP shim's Authorization handling.
func authorized(ctx context.Context, expected string) bool {
	if expected == "" {
		return false
	}
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return false
	}
	vals := md.Get("authorization")
	if len(vals) == 0 {
		return false
	}
	got := strings.TrimSpace(vals[0])
	if !strings.HasPrefix(got, "Bearer ") {
		return false
	}
	got = strings.TrimSpace(strings.TrimPrefix(got, "Bearer "))
	return subtle.ConstantTimeCompare([]byte(got), []byte(expected)) == 1
}

// authUnaryInterceptor enforces the bearer token on the loopback TCP listener.
// It is never installed on the Unix listener, whose peers are local and trusted
// like the HTTP Unix socket.
func unauthenticated(ctx context.Context) error {
	// Mirror the HTTP 401 body: a stable code + request id inside a BotdError
	// detail, mapped to codes.Unauthenticated.
	return grpcError(notify.NewAPIError(401, "unauthorized", "missing or invalid bearer token", false), requestIDFromContext(ctx))
}

func authUnaryInterceptor(token string) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if isHealthMethod(info.FullMethod) {
			return handler(ctx, req)
		}
		if !authorized(ctx, token) {
			return nil, unauthenticated(ctx)
		}
		return handler(ctx, req)
	}
}

func authStreamInterceptor(token string) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if isHealthMethod(info.FullMethod) {
			return handler(srv, ss)
		}
		if !authorized(ss.Context(), token) {
			return unauthenticated(ss.Context())
		}
		return handler(srv, ss)
	}
}
