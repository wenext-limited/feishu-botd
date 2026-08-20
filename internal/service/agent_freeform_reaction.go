package service

import (
	"context"
	"strings"

	"feishu-botd/internal/feishu"
	"feishu-botd/internal/notify"
)

// maxAgentReactionEmojiBytes bounds AddAgentReactionInput.EmojiType. Feishu's
// own emoji keys are short fixed words (e.g. "OnIt", "HEART"); this is a
// generous ceiling against a malformed or hostile provider, not a real limit
// on the vocabulary.
const maxAgentReactionEmojiBytes = 64

// agentFreeformReaction is one provider-chosen reaction recorded against a
// response, keyed by the operation id that placed it — see
// agentResponse.freeformReactions.
type agentFreeformReaction struct {
	emojiType  string
	reactionID string
}

// AddAgentReactionInput is the provider-initiated reaction request: the
// model chose this emoji, not the daemon — contrast agent_reaction.go's
// daemon-driven working reaction.
type AddAgentReactionInput struct {
	Provider    string
	ResponseID  string
	OperationID string
	EmojiType   string
}

// AddAgentReaction places one native Feishu reaction on the message that
// triggered response, chosen by the provider. Unlike the working reaction,
// this is never auto-removed at Finish: a deliberate answer from the model
// is not a busy signal, so it outlives the response. Idempotent per
// operation id, like every other agent RPC — replaying the same id with the
// same emoji returns duplicate=true; replaying it with a different emoji is
// a conflict, since that is not the same request retried.
func (s *Service) AddAgentReaction(ctx context.Context, in AddAgentReactionInput) (bool, *notify.APIError) {
	provider, responseID, operationID, apiErr := validateAgentIdentity(in.Provider, in.ResponseID, in.OperationID)
	if apiErr != nil {
		return false, apiErr
	}
	emojiType := strings.TrimSpace(in.EmojiType)
	if emojiType == "" {
		return false, notify.BadRequest("missing_emoji_type", "emoji_type is required")
	}
	if len(emojiType) > maxAgentReactionEmojiBytes {
		return false, notify.BadRequest("field_too_large", "one or more fields are too large")
	}

	response := s.agentBroker.lookupResponse(provider, responseID)
	if response == nil || !s.appAllowed(provider, response.appAlias) {
		return false, notify.NewAPIError(404, "unknown_response", "unknown response", false)
	}
	backend, ok := s.backendForApp(response.appAlias)
	if !ok || backend.reactions == nil {
		return false, notify.NotImplemented("agent_reactions_unavailable", "agent reactions are unavailable for this sender")
	}

	response.mu.Lock()
	defer response.mu.Unlock()

	if existing, seen := response.freeformReactions[operationID]; seen {
		if existing.emojiType != emojiType {
			return false, notify.NewAPIError(409, "operation_conflict", "operation id reused with different content", false)
		}
		return true, nil
	}
	// cotOriginMessageID is misnamed for this caller — it is captured once at
	// Start regardless of CoT, as the general "message this response is
	// replying to" reference; see its doc comment on agentResponse.
	originMessageID := response.cotOriginMessageID
	if originMessageID == "" {
		return false, notify.NewAPIError(404, "unknown_message", "no triggering message to react to", false)
	}

	callCtx, cancel := context.WithTimeout(ctx, s.cfg.SendTimeout)
	defer cancel()
	reactionID, err := backend.reactions.AddReaction(callCtx, feishu.AddReactionRequest{
		MessageID: originMessageID, EmojiType: emojiType,
	})
	if err != nil {
		s.logAgentReactionFailure("agent reaction add", response.responseID, err)
		return false, agentCardCallError(err, "Feishu reaction add failed")
	}
	if response.freeformReactions == nil {
		response.freeformReactions = make(map[string]agentFreeformReaction)
	}
	response.freeformReactions[operationID] = agentFreeformReaction{emojiType: emojiType, reactionID: reactionID}
	return false, nil
}
