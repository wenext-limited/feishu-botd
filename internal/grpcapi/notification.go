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
	input := service.MessageInput{
		Channel:   targetChannel(in.GetTarget()),
		Source:    in.GetSource(),
		DedupeKey: in.GetDedupeKey(),
	}

	switch content := in.GetContent().(type) {
	case *pb.SendMessageRequest_Markdown:
		input.Title = content.Markdown.GetTitle()
		input.Markdown = content.Markdown.GetMarkdown()
	case *pb.SendMessageRequest_Card:
		input.CardJSON = content.Card.GetCardJson()
	case *pb.SendMessageRequest_Text:
		return nil, grpcError(notify.NotImplemented("unimplemented", "text content is not implemented in v1"), requestIDFromContext(ctx))
	default:
		return nil, grpcError(notify.BadRequest("missing_content", "message content is required"), requestIDFromContext(ctx))
	}

	result, apiErr := n.svc.SendMessage(ctx, input)
	if apiErr != nil {
		return nil, grpcError(apiErr, requestIDFromContext(ctx))
	}
	return &pb.SendMessageResponse{
		Provider:  result.Provider,
		MessageId: result.MessageID,
		Duplicate: result.Duplicate,
	}, nil
}
