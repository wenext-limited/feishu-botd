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
	if err := authorizeLegacySubscription(stream.Context(), in.GetProvider(), in.GetCommands()); err != nil {
		return err
	}
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
	provider, err := authorizeLegacyResponse(ctx)
	if err != nil {
		return nil, err
	}
	resp := service.CommandResponse{Provider: provider, DeliveryID: in.GetDeliveryId()}
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

func (c *commandServer) SubscribeAgentEvents(in *pb.SubscribeAgentEventsRequest, stream pb.CommandService_SubscribeAgentEventsServer) error {
	if err := authorizeAgentSubscription(
		stream.Context(), in.GetProvider(), in.GetCommands(), in.GetIncludeUnmatchedMessages(), in.GetIncludeCardActions(),
	); err != nil {
		return err
	}
	sub, apiErr := c.svc.SubscribeAgentEvents(stream.Context(), service.AgentSubscribeOptions{
		Provider:                 in.GetProvider(),
		Commands:                 in.GetCommands(),
		IncludeUnmatchedMessages: in.GetIncludeUnmatchedMessages(),
		IncludeCardActions:       in.GetIncludeCardActions(),
	})
	if apiErr != nil {
		return grpcError(apiErr, requestIDFromContext(stream.Context()))
	}
	defer sub.Close()

	for {
		select {
		case <-stream.Context().Done():
			return nil
		case event, ok := <-sub.C:
			if !ok {
				return nil
			}
			if err := stream.Send(agentEventToProto(event)); err != nil {
				return err
			}
		}
	}
}

func (c *commandServer) StartAgentResponse(ctx context.Context, in *pb.StartAgentResponseRequest) (*pb.StartAgentResponseResponse, error) {
	if err := requireProviderIdentity(ctx, in.GetProvider()); err != nil {
		return nil, err
	}
	if in.GetContent() == nil {
		return nil, grpcError(notify.BadRequest("missing_content", "agent response content is required"), requestIDFromContext(ctx))
	}
	receipt, apiErr := c.svc.StartAgentResponse(ctx, service.StartAgentResponseInput{
		Provider:    in.GetProvider(),
		DeliveryID:  in.GetDeliveryId(),
		OperationID: in.GetOperationId(),
		Content:     agentContentFromProto(in.GetContent()),
	})
	if apiErr != nil {
		return nil, grpcError(apiErr, requestIDFromContext(ctx))
	}
	return &pb.StartAgentResponseResponse{Response: agentReceiptToProto(receipt)}, nil
}

func (c *commandServer) UpdateAgentResponse(ctx context.Context, in *pb.UpdateAgentResponseRequest) (*pb.UpdateAgentResponseResponse, error) {
	if err := requireProviderIdentity(ctx, in.GetProvider()); err != nil {
		return nil, err
	}
	receipt, apiErr := c.svc.UpdateAgentResponse(ctx, service.UpdateAgentResponseInput{
		Provider:         in.GetProvider(),
		ResponseID:       in.GetResponseId(),
		OperationID:      in.GetOperationId(),
		ExpectedRevision: in.GetExpectedRevision(),
		Markdown:         in.GetMarkdown(),
	})
	if apiErr != nil {
		return nil, grpcError(apiErr, requestIDFromContext(ctx))
	}
	return &pb.UpdateAgentResponseResponse{Response: agentReceiptToProto(receipt)}, nil
}

func (c *commandServer) FinishAgentResponse(ctx context.Context, in *pb.FinishAgentResponseRequest) (*pb.FinishAgentResponseResponse, error) {
	if err := requireProviderIdentity(ctx, in.GetProvider()); err != nil {
		return nil, err
	}
	receipt, apiErr := c.svc.FinishAgentResponse(ctx, service.FinishAgentResponseInput{
		Provider:         in.GetProvider(),
		ResponseID:       in.GetResponseId(),
		OperationID:      in.GetOperationId(),
		ExpectedRevision: in.GetExpectedRevision(),
		Outcome:          agentOutcomeFromProto(in.GetOutcome()),
		Markdown:         in.GetMarkdown(),
		Summary:          in.GetSummary(),
	})
	if apiErr != nil {
		return nil, grpcError(apiErr, requestIDFromContext(ctx))
	}
	return &pb.FinishAgentResponseResponse{Response: agentReceiptToProto(receipt)}, nil
}

func (c *commandServer) SendAgentFollowUp(ctx context.Context, in *pb.SendAgentFollowUpRequest) (*pb.SendAgentFollowUpResponse, error) {
	if err := authorizeAgentFollowUp(ctx, in.GetProvider()); err != nil {
		return nil, err
	}
	receipt, apiErr := c.svc.SendAgentFollowUp(ctx, service.SendAgentFollowUpInput{
		Provider:       in.GetProvider(),
		ConversationID: in.GetConversationId(),
		OperationID:    in.GetOperationId(),
		Markdown:       in.GetMarkdown(),
		Summary:        in.GetSummary(),
	})
	if apiErr != nil {
		return nil, grpcError(apiErr, requestIDFromContext(ctx))
	}
	return &pb.SendAgentFollowUpResponse{FollowUp: agentFollowUpReceiptToProto(receipt)}, nil
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

func agentEventToProto(event service.AgentEvent) *pb.SubscribeAgentEventsResponse {
	out := &pb.InboundAgentEvent{
		DeliveryId:     event.DeliveryID,
		ConversationId: event.ConversationID,
		ChatAlias:      event.ChatAlias,
		SenderId:       event.SenderID,
		Metadata:       event.Metadata,
	}
	if event.Message != nil {
		out.Payload = &pb.InboundAgentEvent_Message{Message: &pb.InboundAgentMessage{
			Text: event.Message.Text, Command: event.Message.Command, CommandText: event.Message.CommandText,
		}}
	} else if event.CardAction != nil {
		out.Payload = &pb.InboundAgentEvent_CardAction{CardAction: &pb.InboundCardAction{
			ResponseId:  event.CardAction.ResponseID,
			ActionId:    event.CardAction.ActionID,
			PayloadJson: event.CardAction.PayloadJSON,
		}}
	}
	return &pb.SubscribeAgentEventsResponse{Event: out}
}

func agentContentFromProto(in *pb.AgentResponseContent) service.AgentResponseContent {
	out := service.AgentResponseContent{Title: in.GetTitle(), Markdown: in.GetMarkdown()}
	for _, action := range in.GetActions() {
		out.Actions = append(out.Actions, service.AgentResponseAction{
			ActionID: action.GetActionId(), PayloadJSON: action.GetPayloadJson(), Label: action.GetLabel(),
			Style: agentActionStyleFromProto(action.GetStyle()),
		})
	}
	return out
}

func agentActionStyleFromProto(in pb.AgentResponseActionStyle) service.AgentResponseActionStyle {
	switch in {
	case pb.AgentResponseActionStyle_AGENT_RESPONSE_ACTION_STYLE_DEFAULT:
		return service.AgentResponseActionStyleDefault
	case pb.AgentResponseActionStyle_AGENT_RESPONSE_ACTION_STYLE_PRIMARY:
		return service.AgentResponseActionStylePrimary
	case pb.AgentResponseActionStyle_AGENT_RESPONSE_ACTION_STYLE_DANGER:
		return service.AgentResponseActionStyleDanger
	default:
		// Preserve unknown numeric enum values so the service validation rejects
		// them instead of silently changing an unsupported style to "default".
		return service.AgentResponseActionStyle(in)
	}
}

func agentOutcomeFromProto(in pb.AgentResponseOutcome) service.AgentResponseOutcome {
	switch in {
	case pb.AgentResponseOutcome_AGENT_RESPONSE_OUTCOME_COMPLETED:
		return service.AgentResponseOutcomeCompleted
	case pb.AgentResponseOutcome_AGENT_RESPONSE_OUTCOME_FAILED:
		return service.AgentResponseOutcomeFailed
	case pb.AgentResponseOutcome_AGENT_RESPONSE_OUTCOME_CANCELLED:
		return service.AgentResponseOutcomeCancelled
	default:
		return service.AgentResponseOutcomeUnspecified
	}
}

func agentReceiptToProto(in service.AgentResponseReceipt) *pb.AgentResponseReceipt {
	return &pb.AgentResponseReceipt{
		ResponseId: in.ResponseID,
		Revision:   in.Revision,
		Phase:      agentPhaseToProto(in.Phase),
		Duplicate:  in.Duplicate,
	}
}

func agentFollowUpReceiptToProto(in service.AgentFollowUpReceipt) *pb.AgentFollowUpReceipt {
	return &pb.AgentFollowUpReceipt{FollowUpId: in.FollowUpID, Duplicate: in.Duplicate}
}

func agentPhaseToProto(in service.AgentResponsePhase) pb.AgentResponsePhase {
	switch in {
	case service.AgentResponsePhaseStreaming:
		return pb.AgentResponsePhase_AGENT_RESPONSE_PHASE_STREAMING
	case service.AgentResponsePhaseCompleted:
		return pb.AgentResponsePhase_AGENT_RESPONSE_PHASE_COMPLETED
	case service.AgentResponsePhaseFailed:
		return pb.AgentResponsePhase_AGENT_RESPONSE_PHASE_FAILED
	case service.AgentResponsePhaseCancelled:
		return pb.AgentResponsePhase_AGENT_RESPONSE_PHASE_CANCELLED
	default:
		return pb.AgentResponsePhase_AGENT_RESPONSE_PHASE_UNSPECIFIED
	}
}
