package grpcapi

import (
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "feishu-botd/gen/feishubotd/v1"
	"feishu-botd/internal/notify"
)

// grpcCode maps the internal HTTP-status-based APIError onto a canonical gRPC
// code. The 409 case branches on the stable machine code because conflict and
// in-flight carry different retry semantics.
func grpcCode(apiErr *notify.APIError) codes.Code {
	switch apiErr.Status {
	case 400:
		return codes.InvalidArgument
	case 401:
		return codes.Unauthenticated
	case 404:
		return codes.NotFound
	case 409:
		if apiErr.Code == "dedupe_in_flight" || apiErr.Code == "response_in_flight" {
			return codes.Aborted
		}
		return codes.AlreadyExists
	case 501:
		return codes.Unimplemented
	case 502:
		return codes.Unavailable
	default:
		return codes.Internal
	}
}

// grpcError converts an internal *notify.APIError into a gRPC status error. The
// stable machine code, retryability, and request id are attached as a neutral,
// in-contract BotdError detail so clients can branch on the string code without
// vendoring google.rpc. A dumb client still gets a sensible canonical code.
func grpcError(apiErr *notify.APIError, requestID string) error {
	st := status.New(grpcCode(apiErr), apiErr.Message)
	detail := &pb.BotdError{
		Code:      apiErr.Code,
		Message:   apiErr.Message,
		Retryable: apiErr.Retryable,
		RequestId: requestID,
	}
	if withDetails, err := st.WithDetails(detail); err == nil {
		return withDetails.Err()
	}
	return st.Err()
}
