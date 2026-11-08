package feishu

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	larkcore "github.com/larksuite/oapi-sdk-go/v3/core"
	larkcardkit "github.com/larksuite/oapi-sdk-go/v3/service/cardkit/v1"
	larkim "github.com/larksuite/oapi-sdk-go/v3/service/im/v1"
)

// AgentCards is the proactive card path: a CardKit entity posted with no
// inbound delivery to answer, and later replaced wholesale. It shares
// CreateCard with DynamicCards but sends with the thread controls and thread
// id a proactive post needs, which a response card never did.
//
// Implementations must keep CreateCard and PostCard separate for the same
// reason as DynamicCards: a CardKit entity can be sent once, so a retry after
// an ambiguous send must reuse both the card id and the message UUID.
type AgentCards interface {
	CreateCard(ctx context.Context, cardJSON string) (string, error)
	PostCard(ctx context.Context, req CardPostRequest) (PostedMessage, error)
	ReplaceCard(ctx context.Context, req CardReplaceRequest) error
}

// CardPostRequest carries daemon-private routing ids. Exactly one of ChatID
// (a new root) and ReplyToMessageID (a reply) is required.
type CardPostRequest struct {
	ChatID           string
	ReplyToMessageID string
	// ReplyInThread asks Feishu to answer inside a thread, opening one on the
	// target when it has none. Only meaningful with ReplyToMessageID.
	ReplyInThread bool
	CardID        string
	UUID          string
}

// CardReplaceRequest is one full CardKit card update. Sequence must be
// strictly greater than every earlier operation on the card.
type CardReplaceRequest struct {
	CardID   string
	CardJSON string
	UUID     string
	Sequence int32
}

// MessagePinner pins one message to its chat's pin list.
type MessagePinner interface {
	PinMessage(ctx context.Context, messageID string) error
}

type cardKitCardReplaceAPI interface {
	Update(context.Context, *larkcardkit.UpdateCardReq, ...larkcore.RequestOptionFunc) (*larkcardkit.UpdateCardResp, error)
}

type pinAPI interface {
	Create(context.Context, *larkim.CreatePinReq, ...larkcore.RequestOptionFunc) (*larkim.CreatePinResp, error)
}

// PostCard sends an existing CardKit entity as a new message or a reply and
// reports the thread holding it. API rejections are DynamicCardAPIError, as
// for SendCard, so the card retry rules apply unchanged.
func (s *ChannelSender) PostCard(ctx context.Context, in CardPostRequest) (PostedMessage, error) {
	if s.messageAPI == nil {
		return PostedMessage{}, fmt.Errorf("feishu card post is not configured")
	}
	cardID, err := validateRequired("card_id", in.CardID, 0)
	if err != nil {
		return PostedMessage{}, err
	}
	uuid, err := validateRequired("message uuid", in.UUID, maxMessageUUIDLength)
	if err != nil {
		return PostedMessage{}, err
	}
	content, err := json.Marshal(cardReference{Type: "card", Data: cardReferenceData{CardID: cardID}})
	if err != nil {
		return PostedMessage{}, fmt.Errorf("encode Feishu card reference: %w", err)
	}
	replyTo := strings.TrimSpace(in.ReplyToMessageID)
	chatID := strings.TrimSpace(in.ChatID)
	if (replyTo == "") == (chatID == "") {
		return PostedMessage{}, fmt.Errorf("exactly one of chat_id or reply_to_message_id is required")
	}

	if replyTo != "" {
		builder := larkim.NewReplyMessageReqBodyBuilder().
			MsgType(larkim.MsgTypeInteractive).
			Content(string(content)).
			Uuid(uuid)
		if in.ReplyInThread {
			builder = builder.ReplyInThread(true)
		}
		body := builder.Build()
		req := larkim.NewReplyMessageReqBuilder().
			MessageId(replyTo).
			Body(body).
			Build()
		req.Body = body
		resp, callErr := s.messageAPI.Reply(ctx, req)
		if callErr != nil {
			return PostedMessage{}, fmt.Errorf("feishu card post reply failed: %w", callErr)
		}
		if resp == nil {
			return PostedMessage{}, fmt.Errorf("feishu card post reply returned an empty response")
		}
		if !resp.Success() {
			return PostedMessage{}, dynamicCardResponseError("card post reply", resp.ApiResp, resp.Code, resp.Msg)
		}
		if resp.Data == nil || resp.Data.MessageId == nil || strings.TrimSpace(*resp.Data.MessageId) == "" {
			return PostedMessage{}, fmt.Errorf("feishu card post reply returned no message id")
		}
		return PostedMessage{MessageIDs: []string{strings.TrimSpace(*resp.Data.MessageId)}, ThreadID: deref(resp.Data.ThreadId)}, nil
	}

	body := larkim.NewCreateMessageReqBodyBuilder().
		ReceiveId(chatID).
		MsgType(larkim.MsgTypeInteractive).
		Content(string(content)).
		Uuid(uuid).
		Build()
	req := larkim.NewCreateMessageReqBuilder().
		ReceiveIdType(larkim.CreateMessageV1ReceiveIDTypeChatId).
		Body(body).
		Build()
	req.Body = body
	resp, callErr := s.messageAPI.Create(ctx, req)
	if callErr != nil {
		return PostedMessage{}, fmt.Errorf("feishu card post failed: %w", callErr)
	}
	if resp == nil {
		return PostedMessage{}, fmt.Errorf("feishu card post returned an empty response")
	}
	if !resp.Success() {
		return PostedMessage{}, dynamicCardResponseError("card post", resp.ApiResp, resp.Code, resp.Msg)
	}
	if resp.Data == nil || resp.Data.MessageId == nil || strings.TrimSpace(*resp.Data.MessageId) == "" {
		return PostedMessage{}, fmt.Errorf("feishu card post returned no message id")
	}
	return PostedMessage{MessageIDs: []string{strings.TrimSpace(*resp.Data.MessageId)}, ThreadID: deref(resp.Data.ThreadId)}, nil
}

// ReplaceCard replaces a CardKit entity's whole Card JSON 2.0 document.
func (s *ChannelSender) ReplaceCard(ctx context.Context, in CardReplaceRequest) error {
	if s.cardReplaceAPI == nil {
		return fmt.Errorf("feishu card replace is not configured")
	}
	cardID, err := validateRequired("card_id", in.CardID, 0)
	if err != nil {
		return err
	}
	uuid, err := validateCardOperation(in.UUID, in.Sequence)
	if err != nil {
		return err
	}
	if err := validateCardDocument(in.CardJSON); err != nil {
		return err
	}
	card := larkcardkit.NewCardBuilder().Type("card_json").Data(in.CardJSON).Build()
	body := larkcardkit.NewUpdateCardReqBodyBuilder().
		Card(card).
		Uuid(uuid).
		Sequence(int(in.Sequence)).
		Build()
	req := larkcardkit.NewUpdateCardReqBuilder().
		CardId(cardID).
		Body(body).
		Build()
	req.Body = body
	resp, callErr := s.cardReplaceAPI.Update(ctx, req)
	if callErr != nil {
		return fmt.Errorf("feishu card replace failed: %w", callErr)
	}
	if resp == nil {
		return fmt.Errorf("feishu card replace returned an empty response")
	}
	if !resp.Success() {
		return dynamicCardResponseError("card replace", resp.ApiResp, resp.Code, resp.Msg)
	}
	return nil
}

// PinMessage pins messageID. Feishu answers a message that is already pinned
// with the existing pin, so a repeat is a success. It needs one of the
// im:message.pins:write_only, im:message, or im:message:send_as_bot scopes.
func (s *ChannelSender) PinMessage(ctx context.Context, messageID string) error {
	if s.pinAPI == nil {
		return &MessageSendError{Operation: "message_pin", Class: "not_configured"}
	}
	messageID = strings.TrimSpace(messageID)
	if messageID == "" {
		return &MessageSendError{Operation: "message_pin", Class: "invalid_destination"}
	}
	body := larkim.NewCreatePinReqBodyBuilder().MessageId(messageID).Build()
	req := larkim.NewCreatePinReqBuilder().Body(body).Build()
	req.Body = body
	resp, err := s.pinAPI.Create(ctx, req)
	if err != nil {
		return messageTransportError("message_pin", err)
	}
	if resp == nil {
		return &MessageSendError{Operation: "message_pin", Class: "invalid_response"}
	}
	if !resp.Success() {
		return messageResponseError("message_pin", resp.ApiResp, resp.Code)
	}
	return nil
}

var (
	_ AgentCards    = (*ChannelSender)(nil)
	_ MessagePinner = (*ChannelSender)(nil)
)
