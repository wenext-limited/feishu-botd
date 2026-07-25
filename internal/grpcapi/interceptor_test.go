package grpcapi

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestRecoveryInterceptorsNeverLogPanicOrRequestIDContent(t *testing.T) {
	const (
		privatePanic     = "PRIVATE_PROMPT_AND_CARD_ID_IN_PANIC"
		privateRequestID = "PRIVATE_CALLER_REQUEST_ID"
	)
	for _, test := range []struct {
		name string
		run  func(*slog.Logger) error
	}{
		{
			name: "unary",
			run: func(logger *slog.Logger) error {
				ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-request-id", privateRequestID))
				_, err := recoveryUnaryInterceptor(logger)(ctx, nil, &grpc.UnaryServerInfo{FullMethod: "/fixture.Service/Unary"}, func(context.Context, any) (any, error) {
					panic(privatePanic)
				})
				return err
			},
		},
		{
			name: "stream",
			run: func(logger *slog.Logger) error {
				ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-request-id", privateRequestID))
				stream := &panicTestServerStream{ctx: ctx}
				return recoveryStreamInterceptor(logger)(nil, stream, &grpc.StreamServerInfo{FullMethod: "/fixture.Service/Stream"}, func(any, grpc.ServerStream) error {
					panic(privatePanic)
				})
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			var output bytes.Buffer
			logger := slog.New(slog.NewJSONHandler(&output, nil))
			if err := test.run(logger); status.Code(err) != codes.Internal {
				t.Fatalf("recovery error code = %v, want Internal", status.Code(err))
			}
			logged := output.String()
			if strings.Contains(logged, privatePanic) || strings.Contains(logged, privateRequestID) {
				t.Fatal("recovery log exposed panic or request-id content")
			}
			for _, required := range []string{`"panic_class":"handler_panic"`, `"correlation":"grpc_`, "/fixture.Service/"} {
				if !strings.Contains(logged, required) {
					t.Fatalf("recovery log missing safe field %q: %s", required, logged)
				}
			}
		})
	}
}

type panicTestServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *panicTestServerStream) Context() context.Context { return s.ctx }
