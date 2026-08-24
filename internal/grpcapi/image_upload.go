package grpcapi

import (
	"errors"
	"io"

	pb "feishu-botd/gen/feishubotd/v1"
	"feishu-botd/internal/notify"
	"feishu-botd/internal/service"
)

const (
	// uploadImageMaxChunkBytes is the per-frame ceiling the proto documents. It
	// is enforced rather than assumed: on this side botd is the receiver, so
	// nothing else makes the number true.
	uploadImageMaxChunkBytes = 64 * 1024
	// uploadImageMaxBytes mirrors the service-layer ceiling. Checking it while
	// reading means an oversized upload is refused after one frame too many
	// instead of after the whole image has crossed the socket.
	uploadImageMaxBytes = 5 * 1024 * 1024
)

// UploadAgentImage reassembles one image from its frames and returns the
// Feishu image_key a response markdown can embed.
func (c *commandServer) UploadAgentImage(stream pb.CommandService_UploadAgentImageServer) error {
	ctx := stream.Context()
	requestID := requestIDFromContext(ctx)

	header, data, apiErr := receiveAgentImage(stream)
	if apiErr != nil {
		return grpcError(apiErr, requestID)
	}
	if err := authorizeAgentImageUpload(ctx, header.GetProvider()); err != nil {
		return err
	}

	result, apiErr := c.svc.UploadAgentImage(ctx, service.AgentImageUploadInput{
		Provider:       header.GetProvider(),
		DeliveryID:     header.GetDeliveryId(),
		ConversationID: header.GetConversationId(),
		OperationID:    header.GetOperationId(),
		Data:           data,
	})
	if apiErr != nil {
		return grpcError(apiErr, requestID)
	}
	return stream.SendAndClose(&pb.UploadAgentImageResponse{
		ImageKey:  result.ImageKey,
		Duplicate: result.Duplicate,
		MediaType: result.MediaType,
	})
}

// receiveAgentImage drains the request stream into one header and one byte
// slice. A stream that never sends a header, sends two, or overruns the size
// ceiling is refused here rather than reaching the service.
func receiveAgentImage(
	stream pb.CommandService_UploadAgentImageServer,
) (*pb.UploadAgentImageHeader, []byte, *notify.APIError) {
	var header *pb.UploadAgentImageHeader
	var data []byte
	for {
		frame, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, nil, notify.BadRequest("stream_failed", "image upload stream ended early")
		}
		switch payload := frame.GetFrame().(type) {
		case *pb.UploadAgentImageRequest_Header:
			if header != nil {
				return nil, nil, notify.BadRequest("invalid_frame", "the header may only be sent once")
			}
			if payload.Header == nil {
				return nil, nil, notify.BadRequest("invalid_frame", "header frame is empty")
			}
			header = payload.Header
		case *pb.UploadAgentImageRequest_Chunk:
			if header == nil {
				return nil, nil, notify.BadRequest("invalid_frame", "the first frame must be the header")
			}
			chunk := payload.Chunk.GetData()
			if len(chunk) > uploadImageMaxChunkBytes {
				return nil, nil, notify.BadRequest("invalid_frame", "an image chunk may not exceed 64 KiB")
			}
			if len(data)+len(chunk) > uploadImageMaxBytes {
				return nil, nil, notify.BadRequest("invalid_image", "image exceeds the daemon size limit")
			}
			data = append(data, chunk...)
		default:
			return nil, nil, notify.BadRequest("invalid_frame", "unknown image upload frame")
		}
	}
	if header == nil {
		return nil, nil, notify.BadRequest("invalid_frame", "the first frame must be the header")
	}
	return header, data, nil
}
