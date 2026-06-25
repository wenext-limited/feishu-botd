package grpcapi

import (
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "feishu-botd/gen/feishubotd/v1"
	"feishu-botd/internal/notify"
)

func TestGRPCCodeMapping(t *testing.T) {
	cases := []struct {
		apiErr *notify.APIError
		want   codes.Code
	}{
		{notify.BadRequest("missing_title", "x"), codes.InvalidArgument},
		{notify.NewAPIError(401, "unauthorized", "x", false), codes.Unauthenticated},
		{notify.NewAPIError(404, "unknown_channel", "x", false), codes.NotFound},
		{notify.NewAPIError(409, "dedupe_conflict", "x", false), codes.AlreadyExists},
		{notify.NewAPIError(409, "dedupe_in_flight", "x", true), codes.Aborted},
		{notify.NewAPIError(501, "unimplemented", "x", false), codes.Unimplemented},
		{notify.NewAPIError(502, "feishu_unavailable", "x", true), codes.Unavailable},
		{notify.NewAPIError(500, "internal", "x", false), codes.Internal},
	}
	for _, tc := range cases {
		if got := grpcCode(tc.apiErr); got != tc.want {
			t.Fatalf("code for %s = %v, want %v", tc.apiErr.Code, got, tc.want)
		}
	}
}

func TestGRPCErrorCarriesBotdDetail(t *testing.T) {
	apiErr := notify.NewAPIError(404, "unknown_channel", "unknown channel", false)
	err := grpcError(apiErr, "req_123")

	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("not a status error: %v", err)
	}
	if st.Code() != codes.NotFound {
		t.Fatalf("code = %v", st.Code())
	}
	var detail *pb.BotdError
	for _, d := range st.Details() {
		if be, ok := d.(*pb.BotdError); ok {
			detail = be
		}
	}
	if detail == nil {
		t.Fatal("missing BotdError detail")
	}
	if detail.GetCode() != "unknown_channel" || detail.GetRequestId() != "req_123" || detail.GetRetryable() {
		t.Fatalf("unexpected detail: %#v", detail)
	}
}
