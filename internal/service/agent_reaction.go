package service

import (
	"context"
	"errors"

	"feishu-botd/internal/feishu"
)

// resolveAgentWorkingReactionEmoji picks the working-reaction emoji for one
// response: the provider's configured default, unless the provider also
// configures per-sender overrides and the triggering message's sender
// resolves — via a live Contact API call, since Feishu's message events carry
// only an id, never a name — to a display name one of those overrides
// matches. No overrides configured is the common case and skips the Contact
// API call entirely, so a provider that never needed per-sender behavior
// pays no extra latency or API quota for it.
func (s *Service) resolveAgentWorkingReactionEmoji(
	ctx context.Context,
	contactUsers feishu.ContactUsers,
	response *agentResponse,
	provider, senderID string,
) string {
	defaultEmoji := s.cfg.AgentWorkingReaction(provider)
	overrides := s.cfg.AgentWorkingReactionOverrides(provider)
	if len(overrides) == 0 || contactUsers == nil || senderID == "" {
		return defaultEmoji
	}
	name, err := contactUsers.DisplayName(ctx, senderID)
	if err != nil {
		s.logAgentReactionFailure("contact lookup", response.responseID, err)
		return defaultEmoji
	}
	if emoji, ok := overrides[name]; ok && emoji != "" {
		return emoji
	}
	return defaultEmoji
}

// addAgentWorkingReaction places the configured "working" reaction on the
// message that triggered this response, if the provider has one configured
// and there is a message to place it on. Best effort: any failure here just
// means no reaction shows, and removeAgentWorkingReaction becomes a no-op
// since response.reactionID stays empty.
func (s *Service) addAgentWorkingReaction(
	ctx context.Context,
	reactions feishu.ReactionMessages,
	response *agentResponse,
	messageID, emojiType string,
) {
	if reactions == nil || messageID == "" || emojiType == "" {
		return
	}
	reactionID, err := reactions.AddReaction(ctx, feishu.AddReactionRequest{
		MessageID: messageID, EmojiType: emojiType,
	})
	if err != nil {
		s.logAgentReactionFailure("reaction add", response.responseID, err)
		return
	}
	response.reactionMessageID = messageID
	response.reactionID = reactionID
}

// removeAgentWorkingReaction clears a reaction addAgentWorkingReaction placed,
// if any. Best effort like every other reaction call; a response with no
// active reaction is a no-op.
func (s *Service) removeAgentWorkingReaction(
	ctx context.Context,
	reactions feishu.ReactionMessages,
	response *agentResponse,
) {
	if reactions == nil || response.reactionID == "" {
		return
	}
	if err := reactions.RemoveReaction(ctx, feishu.RemoveReactionRequest{
		MessageID: response.reactionMessageID, ReactionID: response.reactionID,
	}); err != nil {
		s.logAgentReactionFailure("reaction remove", response.responseID, err)
		return
	}
	response.reactionMessageID = ""
	response.reactionID = ""
}

// logAgentReactionFailure mirrors logAgentCoTFailure's structured-vs-generic
// split, for the same reason: a reaction rejection should keep its HTTP
// status, code, and Feishu request id in the log instead of collapsing into
// an undifferentiated "transport" line.
func (s *Service) logAgentReactionFailure(operation, responseID string, err error) {
	correlationID := opaqueLogCorrelationID("agent", responseID)
	var apiErr *feishu.DynamicCardAPIError
	if errors.As(err, &apiErr) {
		s.logger.Warn("agent reaction operation failed",
			"operation", operation,
			"correlation", correlationID,
			"http_status", apiErr.HTTPStatus,
			"code", apiErr.Code,
			"request_id", apiErr.RequestID,
		)
		return
	}
	errorClass := "transport"
	switch {
	case errors.Is(err, context.Canceled):
		errorClass = "context_canceled"
	case errors.Is(err, context.DeadlineExceeded):
		errorClass = "deadline_exceeded"
	}
	s.logger.Warn("agent reaction operation failed",
		"operation", operation,
		"correlation", correlationID,
		"error_class", errorClass,
	)
}
