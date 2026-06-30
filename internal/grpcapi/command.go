package grpcapi

import (
	"context"

	pb "feishu-botd/gen/feishubotd/v1"
	"feishu-botd/internal/notify"
	"feishu-botd/internal/service"
)

// commandServer adapts CommandService onto the shared command broker.
type commandServer struct {
	pb.UnimplementedCommandServiceServer
	svc *service.Service
}

func (c *commandServer) Subscribe(in *pb.SubscribeRequest, stream pb.CommandService_SubscribeServer) error {
	sub, apiErr := c.svc.SubscribeCommands(stream.Context(), in.GetProvider(), in.GetCommands())
	if apiErr != nil {
		return grpcError(apiErr, requestIDFromContext(stream.Context()))
	}
	defer sub.Close()

	for {
		select {
		case <-stream.Context().Done():
			return nil
		case cmd, ok := <-sub.C:
			if !ok {
				return nil
			}
			if err := stream.Send(commandToProto(cmd)); err != nil {
				return err
			}
		}
	}
}

func (c *commandServer) Respond(ctx context.Context, in *pb.RespondRequest) (*pb.RespondResponse, error) {
	resp := service.CommandResponse{DeliveryID: in.GetDeliveryId()}
	switch reply := in.GetReply().(type) {
	case *pb.RespondRequest_Markdown:
		resp.Title = reply.Markdown.GetTitle()
		resp.Markdown = reply.Markdown.GetMarkdown()
	case *pb.RespondRequest_Card:
		resp.CardJSON = reply.Card.GetCardJson()
	default:
		return nil, grpcError(notify.BadRequest("missing_content", "message content is required"), requestIDFromContext(ctx))
	}

	if apiErr := c.svc.RespondCommand(ctx, resp); apiErr != nil {
		return nil, grpcError(apiErr, requestIDFromContext(ctx))
	}
	return &pb.RespondResponse{Accepted: true}, nil
}

func commandToProto(cmd service.CommandInput) *pb.SubscribeResponse {
	return &pb.SubscribeResponse{
		Command: &pb.InboundCommand{
			DeliveryId: cmd.DeliveryID,
			Command:    cmd.Command,
			Text:       cmd.Text,
			ChatAlias:  cmd.ChatAlias,
			SenderId:   cmd.SenderID,
			Metadata:   cmd.Metadata,
		},
	}
}
