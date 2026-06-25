package grpcapi

import (
	"context"

	pb "feishu-botd/gen/feishubotd/v1"
	"feishu-botd/internal/notify"
	"feishu-botd/internal/service"
)

// notificationServer adapts NotificationService onto the shared core service.
type notificationServer struct {
	pb.UnimplementedNotificationServiceServer
	svc *service.Service
}

func (n *notificationServer) SendNotification(ctx context.Context, in *pb.SendNotificationRequest) (*pb.SendNotificationResponse, error) {
	resp, apiErr := n.svc.SendNotification(ctx, notifyRequestFromProto(in))
	if apiErr != nil {
		return nil, grpcError(apiErr, requestIDFromContext(ctx))
	}
	return notifyResponseToProto(resp), nil
}

func (n *notificationServer) SendMessage(ctx context.Context, in *pb.SendMessageRequest) (*pb.SendMessageResponse, error) {
	markdown := in.GetMarkdown()
	if markdown == nil {
		// text and card are reserved skeletons; an empty oneof is a bad request.
		// Both route through grpcError so the BotdError detail (stable code +
		// request id) is present, like every other gRPC error.
		if in.GetText() != nil || in.GetCard() != nil {
			return nil, grpcError(notify.NotImplemented("unimplemented", "only markdown content is implemented in v1"), requestIDFromContext(ctx))
		}
		return nil, grpcError(notify.BadRequest("missing_content", "message content is required"), requestIDFromContext(ctx))
	}

	result, apiErr := n.svc.SendMessage(ctx, service.MessageInput{
		Channel:   targetChannel(in.GetTarget()),
		Source:    in.GetSource(),
		DedupeKey: in.GetDedupeKey(),
		Title:     markdown.GetTitle(),
		Markdown:  markdown.GetMarkdown(),
	})
	if apiErr != nil {
		return nil, grpcError(apiErr, requestIDFromContext(ctx))
	}
	return &pb.SendMessageResponse{
		Provider:  result.Provider,
		MessageId: result.MessageID,
		Duplicate: result.Duplicate,
	}, nil
}
