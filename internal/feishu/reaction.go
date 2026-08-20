package feishu

import (
	"context"
	"fmt"
	"strings"

	larkcore "github.com/larksuite/oapi-sdk-go/v3/core"
	larkim "github.com/larksuite/oapi-sdk-go/v3/service/im/v1"
)

const maxReactionIdentifierLength = 128

// ReactionMessages adds and removes a native Feishu reaction on a message the
// daemon did not author — the "working on it" acknowledgment placed on the
// user's triggering message, unrelated to any card or CoT surface.
type ReactionMessages interface {
	AddReaction(ctx context.Context, in AddReactionRequest) (string, error)
	RemoveReaction(ctx context.Context, in RemoveReactionRequest) error
}

type AddReactionRequest struct {
	MessageID string
	// EmojiType is one of Feishu's fixed emoji keys (e.g. "OnIt"), not free
	// text: https://open.feishu.cn/document/uAjLw4CM/ukTMukTMukTM/reference/im-v1/message-reaction/emojis-introduce
	EmojiType string
}

type RemoveReactionRequest struct {
	MessageID string
	// ReactionID is the id AddReaction returned. The API only allows deleting
	// a reaction whose original adder was this same credential.
	ReactionID string
}

type reactionAPI interface {
	Create(context.Context, *larkim.CreateMessageReactionReq, ...larkcore.RequestOptionFunc) (*larkim.CreateMessageReactionResp, error)
	Delete(context.Context, *larkim.DeleteMessageReactionReq, ...larkcore.RequestOptionFunc) (*larkim.DeleteMessageReactionResp, error)
}

func (s *ChannelSender) AddReaction(ctx context.Context, in AddReactionRequest) (string, error) {
	if s == nil || s.reactionAPI == nil {
		return "", fmt.Errorf("feishu reaction add is not configured")
	}
	messageID, err := validateRequired("message_id", in.MessageID, maxReactionIdentifierLength)
	if err != nil {
		return "", err
	}
	emojiType, err := validateRequired("emoji_type", in.EmojiType, maxReactionIdentifierLength)
	if err != nil {
		return "", err
	}

	body := larkim.NewCreateMessageReactionReqBodyBuilder().
		ReactionType(larkim.NewEmojiBuilder().EmojiType(emojiType).Build()).
		Build()
	req := larkim.NewCreateMessageReactionReqBuilder().
		MessageId(messageID).
		Body(body).
		Build()
	// The builder retains the body only in a private ApiReq. Keep the
	// exported mirror populated too so callers (and tests) can inspect the
	// exact payload without reflection — see cardkit.go's CreateCard.
	req.Body = body
	resp, callErr := s.reactionAPI.Create(ctx, req)
	if callErr != nil {
		return "", fmt.Errorf("feishu reaction add failed: %w", callErr)
	}
	if resp == nil {
		return "", fmt.Errorf("feishu reaction add returned an empty response")
	}
	if !resp.Success() {
		return "", dynamicCardResponseError("reaction add", resp.ApiResp, resp.Code, resp.Msg)
	}
	if resp.Data == nil || resp.Data.ReactionId == nil {
		return "", fmt.Errorf("feishu reaction add returned no reaction id")
	}
	reactionID := strings.TrimSpace(*resp.Data.ReactionId)
	if reactionID == "" {
		return "", fmt.Errorf("feishu reaction add returned no reaction id")
	}
	return reactionID, nil
}

func (s *ChannelSender) RemoveReaction(ctx context.Context, in RemoveReactionRequest) error {
	if s == nil || s.reactionAPI == nil {
		return fmt.Errorf("feishu reaction remove is not configured")
	}
	messageID, err := validateRequired("message_id", in.MessageID, maxReactionIdentifierLength)
	if err != nil {
		return err
	}
	reactionID, err := validateRequired("reaction_id", in.ReactionID, maxReactionIdentifierLength)
	if err != nil {
		return err
	}

	req := larkim.NewDeleteMessageReactionReqBuilder().
		MessageId(messageID).
		ReactionId(reactionID).
		Build()
	resp, callErr := s.reactionAPI.Delete(ctx, req)
	if callErr != nil {
		return fmt.Errorf("feishu reaction remove failed: %w", callErr)
	}
	if resp == nil {
		return fmt.Errorf("feishu reaction remove returned an empty response")
	}
	if !resp.Success() {
		return dynamicCardResponseError("reaction remove", resp.ApiResp, resp.Code, resp.Msg)
	}
	return nil
}
