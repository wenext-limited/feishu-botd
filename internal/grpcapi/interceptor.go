package grpcapi

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type requestIDKey struct{}

// requestIDFromContext returns the request id attached by the request-id
// interceptor, or "" if none.
func requestIDFromContext(ctx context.Context) string {
	if value, ok := ctx.Value(requestIDKey{}).(string); ok {
		return value
	}
	return ""
}

// withRequestID propagates an incoming x-request-id metadata value (or mints a
// new one) into the context and echoes it back as a response header, mirroring
// the HTTP X-Request-Id behavior.
func withRequestID(ctx context.Context) context.Context {
	id := ""
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if vals := md.Get("x-request-id"); len(vals) > 0 {
			id = strings.TrimSpace(vals[0])
		}
	}
	if id == "" {
		id = fmt.Sprintf("req_%d", time.Now().UnixNano())
	}
	_ = grpc.SetHeader(ctx, metadata.Pairs("x-request-id", id))
	return context.WithValue(ctx, requestIDKey{}, id)
}

func requestIDUnaryInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		return handler(withRequestID(ctx), req)
	}
}

func requestIDStreamInterceptor() grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		return handler(srv, &contextStream{ServerStream: ss, ctx: withRequestID(ss.Context())})
	}
}

// contextStream overrides ServerStream.Context so downstream handlers observe
// the request-id-enriched context.
type contextStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *contextStream) Context() context.Context { return s.ctx }

func recoveryUnaryInterceptor(logger *slog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
		defer func() {
			if r := recover(); r != nil {
				logger.Error("grpc handler panic",
					"method", info.FullMethod,
					"correlation", grpcRequestLogCorrelation(ctx),
					"panic_class", "handler_panic",
				)
				err = status.Error(codes.Internal, "internal error")
			}
		}()
		return handler(ctx, req)
	}
}

func recoveryStreamInterceptor(logger *slog.Logger) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) (err error) {
		defer func() {
			if r := recover(); r != nil {
				logger.Error("grpc stream handler panic",
					"method", info.FullMethod,
					"correlation", grpcRequestLogCorrelation(ss.Context()),
					"panic_class", "handler_panic",
				)
				err = status.Error(codes.Internal, "internal error")
			}
		}()
		return handler(srv, ss)
	}
}

func grpcRequestLogCorrelation(ctx context.Context) string {
	requestID := requestIDFromContext(ctx)
	if requestID == "" {
		if md, ok := metadata.FromIncomingContext(ctx); ok {
			if values := md.Get("x-request-id"); len(values) == 1 {
				requestID = strings.TrimSpace(values[0])
			}
		}
	}
	sum := sha256.Sum256([]byte("feishu-botd/grpc-request-log/v1\x00" + requestID))
	return "grpc_" + hex.EncodeToString(sum[:8])
}
