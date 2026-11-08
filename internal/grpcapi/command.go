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
	allowedApps, allowedAppsConfigured := authenticatedProviderAppScope(stream.Context())
	sub, apiErr := c.svc.SubscribeCommandsForApps(stream.Context(), service.CommandSubscribeOptions{
		Provider:              in.GetProvider(),
		Commands:              in.GetCommands(),
		AllowedApps:           allowedApps,
		AllowedAppsConfigured: allowedAppsConfigured,
	})
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
		stream.Context(), in.GetProvider(), in.GetCommands(), in.GetIncludeUnmatchedMessages(), in.GetIncludeCardActions(), in.GetIncludeMessageReactions(),
		in.GetIncludeAmbientMessages(),
	); err != nil {
		return err
	}
	allowedApps, allowedAppsConfigured := authenticatedProviderAppScope(stream.Context())
	sub, apiErr := c.svc.SubscribeAgentEvents(stream.Context(), service.AgentSubscribeOptions{
		Provider:                 in.GetProvider(),
		Commands:                 in.GetCommands(),
		IncludeUnmatchedMessages: in.GetIncludeUnmatchedMessages(),
		IncludeCardActions:       in.GetIncludeCardActions(),
		IncludeMessageReactions:  in.GetIncludeMessageReactions(),
		IncludeAmbientMessages:   in.GetIncludeAmbientMessages(),
		AllowedApps:              allowedApps,
		AllowedAppsConfigured:    allowedAppsConfigured,
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
	principal, _ := authenticatedProvider(ctx)
	content := agentContentFromProto(in.GetContent())
	content.TimelineSteps = agentTimelineStepsFromProto(principal.allowCoTProgress, in.GetTimelineSteps())
	receipt, apiErr := c.svc.StartAgentResponse(ctx, service.StartAgentResponseInput{
		Provider:         in.GetProvider(),
		DeliveryID:       in.GetDeliveryId(),
		OperationID:      in.GetOperationId(),
		Content:          content,
		AllowCoTProgress: principal.allowCoTProgress,
	})
	if apiErr != nil {
		return nil, grpcError(apiErr, requestIDFromContext(ctx))
	}
	// Start alone: the label is settled when the response opens, and
	// agentReceiptToProto deliberately does not carry it, so Update, Finish
	// and Replace cannot restate it as though it were per-revision.
	return &pb.StartAgentResponseResponse{
		Response:           agentReceiptToProto(receipt),
		MatchedSenderLabel: receipt.MatchedSenderLabel,
	}, nil
}

func (c *commandServer) UpdateAgentResponse(ctx context.Context, in *pb.UpdateAgentResponseRequest) (*pb.UpdateAgentResponseResponse, error) {
	if err := requireProviderIdentity(ctx, in.GetProvider()); err != nil {
		return nil, err
	}
	principal, _ := authenticatedProvider(ctx)
	receipt, apiErr := c.svc.UpdateAgentResponse(ctx, service.UpdateAgentResponseInput{
		Provider:         in.GetProvider(),
		ResponseID:       in.GetResponseId(),
		OperationID:      in.GetOperationId(),
		ExpectedRevision: in.GetExpectedRevision(),
		Markdown:         in.GetMarkdown(),
		TimelineMarkdown: in.GetTimelineMarkdown(),
		TimelineTitle:    in.GetTimelineTitle(),
		TimelineSteps:    agentTimelineStepsFromProto(principal.allowCoTProgress, in.GetTimelineSteps()),
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
	principal, _ := authenticatedProvider(ctx)
	receipt, apiErr := c.svc.FinishAgentResponse(ctx, service.FinishAgentResponseInput{
		Provider:         in.GetProvider(),
		ResponseID:       in.GetResponseId(),
		OperationID:      in.GetOperationId(),
		ExpectedRevision: in.GetExpectedRevision(),
		Outcome:          agentOutcomeFromProto(in.GetOutcome()),
		Markdown:         in.GetMarkdown(),
		Summary:          in.GetSummary(),
		TimelineMarkdown: in.GetTimelineMarkdown(),
		TimelineTitle:    in.GetTimelineTitle(),
		TimelineSteps:    agentTimelineStepsFromProto(principal.allowCoTProgress, in.GetTimelineSteps()),
	})
	if apiErr != nil {
		return nil, grpcError(apiErr, requestIDFromContext(ctx))
	}
	return &pb.FinishAgentResponseResponse{Response: agentReceiptToProto(receipt)}, nil
}

func (c *commandServer) ReplaceAgentResponse(ctx context.Context, in *pb.ReplaceAgentResponseRequest) (*pb.ReplaceAgentResponseResponse, error) {
	if err := requireProviderIdentity(ctx, in.GetProvider()); err != nil {
		return nil, err
	}
	receipt, apiErr := c.svc.ReplaceAgentResponse(ctx, service.ReplaceAgentResponseInput{
		Provider:         in.GetProvider(),
		ResponseID:       in.GetResponseId(),
		OperationID:      in.GetOperationId(),
		ExpectedRevision: in.GetExpectedRevision(),
		Markdown:         in.GetMarkdown(),
		Summary:          in.GetSummary(),
		TimelineMarkdown: in.GetTimelineMarkdown(),
		TimelineTitle:    in.GetTimelineTitle(),
	})
	if apiErr != nil {
		return nil, grpcError(apiErr, requestIDFromContext(ctx))
	}
	return &pb.ReplaceAgentResponseResponse{Response: agentReceiptToProto(receipt)}, nil
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
		MentionUserID:  in.GetMentionUserId(),
	})
	if apiErr != nil {
		return nil, grpcError(apiErr, requestIDFromContext(ctx))
	}
	return &pb.SendAgentFollowUpResponse{FollowUp: agentFollowUpReceiptToProto(receipt)}, nil
}

func (c *commandServer) PostAgentMessage(ctx context.Context, in *pb.PostAgentMessageRequest) (*pb.PostAgentMessageResponse, error) {
	if err := authorizeAgentProactive(ctx, in.GetProvider()); err != nil {
		return nil, err
	}
	input := service.PostAgentMessageInput{
		Provider:      in.GetProvider(),
		OperationID:   in.GetOperationId(),
		ReplyInThread: in.GetReplyInThread(),
		Markdown:      in.GetMarkdown(),
		Summary:       in.GetSummary(),
		MentionUserID: in.GetMentionUserId(),
	}
	switch target := in.GetTarget().(type) {
	case *pb.PostAgentMessageRequest_ChatAlias:
		input.ChatAlias = target.ChatAlias
	case *pb.PostAgentMessageRequest_ReplyToMessageRef:
		input.ReplyToMessageRef = target.ReplyToMessageRef
	}
	receipt, apiErr := c.svc.PostAgentMessage(ctx, input)
	if apiErr != nil {
		return nil, grpcError(apiErr, requestIDFromContext(ctx))
	}
	return &pb.PostAgentMessageResponse{
		MessageRef: receipt.MessageRef,
		ThreadRef:  receipt.ThreadRef,
		Duplicate:  receipt.Duplicate,
	}, nil
}

func (c *commandServer) PostAgentCard(ctx context.Context, in *pb.PostAgentCardRequest) (*pb.PostAgentCardResponse, error) {
	if err := authorizeAgentProactive(ctx, in.GetProvider()); err != nil {
		return nil, err
	}
	input := service.PostAgentCardInput{
		Provider:      in.GetProvider(),
		OperationID:   in.GetOperationId(),
		ReplyInThread: in.GetReplyInThread(),
		Content:       agentCardContentFromProto(in.GetContent()),
	}
	switch target := in.GetTarget().(type) {
	case *pb.PostAgentCardRequest_ChatAlias:
		input.ChatAlias = target.ChatAlias
	case *pb.PostAgentCardRequest_ReplyToMessageRef:
		input.ReplyToMessageRef = target.ReplyToMessageRef
	}
	receipt, apiErr := c.svc.PostAgentCard(ctx, input)
	if apiErr != nil {
		return nil, grpcError(apiErr, requestIDFromContext(ctx))
	}
	return &pb.PostAgentCardResponse{
		MessageRef: receipt.MessageRef,
		ThreadRef:  receipt.ThreadRef,
		Duplicate:  receipt.Duplicate,
	}, nil
}

func (c *commandServer) UpdateAgentCard(ctx context.Context, in *pb.UpdateAgentCardRequest) (*pb.UpdateAgentCardResponse, error) {
	if err := authorizeAgentProactive(ctx, in.GetProvider()); err != nil {
		return nil, err
	}
	receipt, apiErr := c.svc.UpdateAgentCard(ctx, service.UpdateAgentCardInput{
		Provider:    in.GetProvider(),
		MessageRef:  in.GetMessageRef(),
		OperationID: in.GetOperationId(),
		Content:     agentCardContentFromProto(in.GetContent()),
	})
	if apiErr != nil {
		return nil, grpcError(apiErr, requestIDFromContext(ctx))
	}
	return &pb.UpdateAgentCardResponse{Duplicate: receipt.Duplicate, Superseded: receipt.Superseded}, nil
}

func (c *commandServer) PinAgentMessage(ctx context.Context, in *pb.PinAgentMessageRequest) (*pb.PinAgentMessageResponse, error) {
	if err := authorizeAgentPin(ctx, in.GetProvider()); err != nil {
		return nil, err
	}
	if apiErr := c.svc.PinAgentMessage(ctx, service.PinAgentMessageInput{
		Provider:   in.GetProvider(),
		MessageRef: in.GetMessageRef(),
	}); apiErr != nil {
		return nil, grpcError(apiErr, requestIDFromContext(ctx))
	}
	return &pb.PinAgentMessageResponse{}, nil
}

// agentCardContentFromProto maps the wire enum onto the service's. An unknown
// value stays out of range so the service rejects it rather than dropping it.
func agentCardContentFromProto(in *pb.AgentCardContent) service.AgentCardContent {
	out := service.AgentCardContent{
		Title:    in.GetTitle(),
		Markdown: in.GetMarkdown(),
		Summary:  in.GetSummary(),
	}
	switch in.GetState() {
	case pb.AgentCardState_AGENT_CARD_STATE_UNSPECIFIED:
		out.State = service.AgentCardStateUnspecified
	case pb.AgentCardState_AGENT_CARD_STATE_PROCESSING:
		out.State = service.AgentCardStateProcessing
	case pb.AgentCardState_AGENT_CARD_STATE_ACTIVE:
		out.State = service.AgentCardStateActive
	case pb.AgentCardState_AGENT_CARD_STATE_SUSPENDED:
		out.State = service.AgentCardStateSuspended
	case pb.AgentCardState_AGENT_CARD_STATE_ARCHIVED:
		out.State = service.AgentCardStateArchived
	default:
		out.State = service.AgentCardState(-1)
	}
	for _, step := range in.GetSteps() {
		out.Steps = append(out.Steps, service.AgentCardStep{Label: step.GetLabel(), Done: step.GetDone()})
	}
	for _, link := range in.GetLinks() {
		out.Links = append(out.Links, service.AgentCardLink{Label: link.GetLabel(), URL: link.GetUrl()})
	}
	return out
}

func (c *commandServer) AddAgentReaction(ctx context.Context, in *pb.AddAgentReactionRequest) (*pb.AddAgentReactionResponse, error) {
	if err := authorizeAgentReaction(ctx, in.GetProvider()); err != nil {
		return nil, err
	}
	duplicate, apiErr := c.svc.AddAgentReaction(ctx, service.AddAgentReactionInput{
		Provider:    in.GetProvider(),
		ResponseID:  in.GetResponseId(),
		OperationID: in.GetOperationId(),
		EmojiType:   in.GetEmojiType(),
	})
	if apiErr != nil {
		return nil, grpcError(apiErr, requestIDFromContext(ctx))
	}
	return &pb.AddAgentReactionResponse{Duplicate: duplicate}, nil
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
		message := &pb.InboundAgentMessage{
			Text: event.Message.Text, Command: event.Message.Command, CommandText: event.Message.CommandText,
			ReplyToMessageRef: event.Message.ReplyToMessageRef, ConversationTitle: event.Message.ConversationTitle,
			MessageRef: event.Message.MessageRef, ThreadRef: event.Message.ThreadRef,
			ThreadRootMessageRef: event.Message.ThreadRootMessageRef,
			Addressing:           agentAddressingToProto(event.Message.Addressing),
		}
		for _, mention := range event.Message.Mentions {
			message.Mentions = append(message.Mentions, &pb.InboundMention{UserId: mention.UserID, Name: mention.Name})
		}
		out.Payload = &pb.InboundAgentEvent_Message{Message: message}
	} else if event.CardAction != nil {
		out.Payload = &pb.InboundAgentEvent_CardAction{CardAction: &pb.InboundCardAction{
			ResponseId:  event.CardAction.ResponseID,
			ActionId:    event.CardAction.ActionID,
			PayloadJson: event.CardAction.PayloadJSON,
		}}
	} else if event.MessageReaction != nil {
		out.Payload = &pb.InboundAgentEvent_MessageReaction{MessageReaction: &pb.InboundMessageReaction{
			MessageRef:   event.MessageReaction.MessageRef,
			ReactionType: event.MessageReaction.ReactionType,
			Operation:    messageReactionOperationToProto(event.MessageReaction.Operation),
		}}
	}
	return &pb.SubscribeAgentEventsResponse{Event: out}
}

// agentContentFromProto translates the card body only. TimelineSteps lives on
// StartAgentResponseRequest itself, not on AgentResponseContent (see the
// proto's own comment), so a caller that wants steps sets
// service.AgentResponseContent.TimelineSteps separately after this returns.
func agentContentFromProto(in *pb.AgentResponseContent) service.AgentResponseContent {
	out := service.AgentResponseContent{
		Title:            in.GetTitle(),
		Markdown:         in.GetMarkdown(),
		TimelineMarkdown: in.GetTimelineMarkdown(),
		TimelineTitle:    in.GetTimelineTitle(),
	}
	for _, action := range in.GetActions() {
		out.Actions = append(out.Actions, service.AgentResponseAction{
			ActionID: action.GetActionId(), PayloadJSON: action.GetPayloadJson(), Label: action.GetLabel(),
			Style: agentActionStyleFromProto(action.GetStyle()),
		})
	}
	return out
}

// agentTimelineStepsFromProto translates the wire steps into the service's
// own domain type. A provider without the CoT capability grant has its steps
// dropped here rather than rejected: the base card update this call also
// carries must never fail for a missing CoT grant, so an unauthorized
// provider that sends steps anyway simply gets the response it would have
// gotten without them.
func agentTimelineStepsFromProto(allowCoTProgress bool, steps []*pb.AgentTimelineStep) []service.AgentTimelineStep {
	if !allowCoTProgress || len(steps) == 0 {
		return nil
	}
	out := make([]service.AgentTimelineStep, 0, len(steps))
	for _, step := range steps {
		out = append(out, service.AgentTimelineStep{
			StepID: step.GetStepId(),
			Label:  step.GetLabel(),
			State:  agentTimelineStepStateFromProto(step.GetState()),
		})
	}
	return out
}

func agentTimelineStepStateFromProto(state pb.AgentTimelineStepState) service.AgentTimelineStepState {
	switch state {
	case pb.AgentTimelineStepState_AGENT_TIMELINE_STEP_STATE_STARTED:
		return service.AgentTimelineStepStateStarted
	case pb.AgentTimelineStepState_AGENT_TIMELINE_STEP_STATE_FINISHED:
		return service.AgentTimelineStepStateFinished
	default:
		return service.AgentTimelineStepStateUnspecified
	}
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
		MessageRef: in.MessageRef,
	}
}

func agentAddressingToProto(in service.AgentAddressing) pb.InboundAddressing {
	switch in {
	case service.AgentAddressingDirect:
		return pb.InboundAddressing_INBOUND_ADDRESSING_DIRECT
	case service.AgentAddressingMention:
		return pb.InboundAddressing_INBOUND_ADDRESSING_MENTION
	case service.AgentAddressingReply:
		return pb.InboundAddressing_INBOUND_ADDRESSING_REPLY
	case service.AgentAddressingAmbient:
		return pb.InboundAddressing_INBOUND_ADDRESSING_AMBIENT
	default:
		return pb.InboundAddressing_INBOUND_ADDRESSING_UNSPECIFIED
	}
}

func messageReactionOperationToProto(in service.MessageReactionOperation) pb.MessageReactionOperation {
	switch in {
	case service.MessageReactionAdded:
		return pb.MessageReactionOperation_MESSAGE_REACTION_OPERATION_ADDED
	case service.MessageReactionRemoved:
		return pb.MessageReactionOperation_MESSAGE_REACTION_OPERATION_REMOVED
	default:
		return pb.MessageReactionOperation_MESSAGE_REACTION_OPERATION_UNSPECIFIED
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
