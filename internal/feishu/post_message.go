package feishu

import (
	"context"
	"encoding/json"
	"strings"

	larkim "github.com/larksuite/oapi-sdk-go/v3/service/im/v1"

	"feishu-botd/internal/notify"
)

// MessagePoster posts one ordinary message that no inbound delivery
// triggered, and reports the identity of every Feishu message it created.
// Sender.Send keeps only the first message id because a notification or a
// follow-up never needs more; a proactive post does, because every part it
// splits into becomes an agent-owned reply target.
type MessagePoster interface {
	PostMessage(ctx context.Context, req PostMessageRequest) (PostedMessage, error)
}

// PostMessageRequest carries daemon-private routing ids. Exactly one of
// ChatID (a new root message) or ReplyToMessageID (a reply) is used; a reply
// wins when both are present.
type PostMessageRequest struct {
	ChatID           string
	ReplyToMessageID string
	// ReplyInThread asks Feishu to answer inside a thread, opening one on the
	// target when it has none. In a topic group Feishu already does so.
	ReplyInThread bool
	// Content is rendered exactly as an ordinary send: Title, Markdown, and an
	// optional native MentionUserID. Source plus DedupeKey seed the Feishu
	// message UUID, so a retry of the same post cannot be delivered twice.
	Content notify.Request
}

// PostedMessage holds daemon-private Feishu ids. MessageIDs is never empty on
// success and lists every part in order; ThreadID is the thread holding the
// first part, empty when Feishu reported none. When a later part fails,
// PostMessage returns the parts Feishu already accepted beside the error, so
// the caller can still own them.
type PostedMessage struct {
	MessageIDs []string
	ThreadID   string
}

func (s *ChannelSender) PostMessage(ctx context.Context, req PostMessageRequest) (PostedMessage, error) {
	if s.messageAPI == nil {
		return PostedMessage{}, &MessageSendError{Operation: "message_post", Class: "not_configured"}
	}
	chatID := strings.TrimSpace(req.ChatID)
	replyTo := strings.TrimSpace(req.ReplyToMessageID)
	if chatID == "" && replyTo == "" {
		return PostedMessage{}, &MessageSendError{Operation: "message_post", Class: "invalid_destination"}
	}
	parts, err := ordinaryMessageParts(req.Content)
	if err != nil {
		return PostedMessage{}, err
	}
	uuidSeed, err := ordinaryMessageUUIDSeed(req.Content)
	if err != nil {
		return PostedMessage{}, err
	}

	posted := PostedMessage{MessageIDs: make([]string, 0, len(parts))}
	for index, part := range parts {
		messageID, threadID, sendErr := s.postOrdinaryPart(ctx, chatID, replyTo, req.ReplyInThread, ordinaryMessageUUID(uuidSeed, index), part)
		if sendErr != nil {
			return posted, sendErr
		}
		posted.MessageIDs = append(posted.MessageIDs, messageID)
		if index == 0 {
			posted.ThreadID = threadID
		}
	}
	return posted, nil
}

// postOrdinaryPart mirrors sendOrdinaryPart, including the plain-text fallback
// for a post Feishu refuses to render, while also reporting the thread id.
func (s *ChannelSender) postOrdinaryPart(ctx context.Context, chatID, replyTo string, inThread bool, uuid string, part ordinaryMessagePart) (string, string, *MessageSendError) {
	messageID, threadID, sendErr := s.postOrdinaryCall(ctx, chatID, replyTo, inThread, uuid, part.messageType, part.content)
	if sendErr != nil && part.fallbackText != "" && isMessageFormatError(sendErr.Code) {
		content, err := json.Marshal(map[string]string{"text": part.fallbackText})
		if err != nil {
			return "", "", &MessageSendError{Operation: "message_encode", Class: "encode_failed"}
		}
		return s.postOrdinaryCall(ctx, chatID, replyTo, inThread, uuid, larkim.MsgTypeText, string(content))
	}
	return messageID, threadID, sendErr
}

func (s *ChannelSender) postOrdinaryCall(ctx context.Context, chatID, replyTo string, inThread bool, uuid, messageType, content string) (string, string, *MessageSendError) {
	threadID := ""
	if replyTo != "" {
		messageID, err := s.retryOrdinaryMessage(ctx, "message_reply", func() (string, *MessageSendError) {
			builder := larkim.NewReplyMessageReqBodyBuilder().
				MsgType(messageType).
				Content(content).
				Uuid(uuid)
			if inThread {
				builder = builder.ReplyInThread(true)
			}
			body := builder.Build()
			req := larkim.NewReplyMessageReqBuilder().
				MessageId(replyTo).
				Body(body).
				Build()
			req.Body = body
			resp, err := s.messageAPI.Reply(ctx, req)
			if err != nil {
				return "", messageTransportError("message_reply", err)
			}
			if resp == nil {
				return "", &MessageSendError{Operation: "message_reply", Class: "invalid_response"}
			}
			if !resp.Success() {
				return "", messageResponseError("message_reply", resp.ApiResp, resp.Code)
			}
			if resp.Data == nil || resp.Data.MessageId == nil || strings.TrimSpace(*resp.Data.MessageId) == "" {
				return "", &MessageSendError{Operation: "message_reply", Class: "invalid_response"}
			}
			threadID = deref(resp.Data.ThreadId)
			return strings.TrimSpace(*resp.Data.MessageId), nil
		})
		return messageID, threadID, err
	}

	messageID, err := s.retryOrdinaryMessage(ctx, "message_create", func() (string, *MessageSendError) {
		body := larkim.NewCreateMessageReqBodyBuilder().
			ReceiveId(chatID).
			MsgType(messageType).
			Content(content).
			Uuid(uuid).
			Build()
		req := larkim.NewCreateMessageReqBuilder().
			ReceiveIdType(larkim.CreateMessageV1ReceiveIDTypeChatId).
			Body(body).
			Build()
		req.Body = body
		resp, err := s.messageAPI.Create(ctx, req)
		if err != nil {
			return "", messageTransportError("message_create", err)
		}
		if resp == nil {
			return "", &MessageSendError{Operation: "message_create", Class: "invalid_response"}
		}
		if !resp.Success() {
			return "", messageResponseError("message_create", resp.ApiResp, resp.Code)
		}
		if resp.Data == nil || resp.Data.MessageId == nil || strings.TrimSpace(*resp.Data.MessageId) == "" {
			return "", &MessageSendError{Operation: "message_create", Class: "invalid_response"}
		}
		threadID = deref(resp.Data.ThreadId)
		return strings.TrimSpace(*resp.Data.MessageId), nil
	})
	return messageID, threadID, err
}
