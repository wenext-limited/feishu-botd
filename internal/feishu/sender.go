package feishu

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	lark "github.com/larksuite/oapi-sdk-go/v3"
	"github.com/larksuite/oapi-sdk-go/v3/channel/outbound"
	larkcore "github.com/larksuite/oapi-sdk-go/v3/core"
	larkim "github.com/larksuite/oapi-sdk-go/v3/service/im/v1"

	"feishu-botd/internal/notify"
)

type Sender interface {
	Ready(ctx context.Context) error
	Send(ctx context.Context, chatID string, req notify.Request) (string, error)
}

// MessageSendError is the privacy-safe error returned by ordinary outbound
// messages. It deliberately excludes Feishu's response message and the raw
// transport error because either can echo request URLs, routing identifiers,
// card JSON, or user content.
type MessageSendError struct {
	Operation  string
	Class      string
	HTTPStatus int
	Code       int
	RequestID  string
	Retryable  bool

	cause error
}

func (e *MessageSendError) Error() string {
	message := fmt.Sprintf("feishu %s failed: class=%s", e.Operation, e.Class)
	if e.HTTPStatus != 0 {
		message += fmt.Sprintf(" http_status=%d", e.HTTPStatus)
	}
	if e.Code != 0 {
		message += fmt.Sprintf(" code=%d", e.Code)
	}
	if e.RequestID != "" {
		message += " request_id=" + e.RequestID
	}
	return message
}

func (e *MessageSendError) Unwrap() error { return e.cause }

const (
	ordinaryMarkdownChunkLimit = 3500
	ordinarySendMaxAttempts    = 3
	ordinarySendRetryBase      = 500 * time.Millisecond
)

type ordinaryMessagePart struct {
	messageType  string
	content      string
	fallbackText string
}

type ordinaryPost struct {
	ZhCn ordinaryPostLanguage `json:"zh_cn"`
}

type ordinaryPostLanguage struct {
	Title   string                  `json:"title,omitempty"`
	Content [][]ordinaryPostElement `json:"content"`
}

type ordinaryPostElement struct {
	Tag  string `json:"tag"`
	Text string `json:"text"`
}

type ChannelSender struct {
	appID      string
	appSecret  string
	client     *lark.Client
	cardAPI    cardKitCardAPI
	elementAPI cardKitElementAPI
	messageAPI cardKitMessageAPI

	retryMaxAttempts int
	retryBase        time.Duration

	mu             sync.Mutex
	readyUntil     time.Time
	lastReadyError error
}

func NewChannelSender(appID, appSecret string, logger *slog.Logger) *ChannelSender {
	if logger == nil {
		logger = slog.Default()
	}
	client := lark.NewClient(
		appID,
		appSecret,
		lark.WithReqTimeout(15*time.Second),
		lark.WithLogger(safeSDKLogger{logger: logger}),
	)
	return &ChannelSender{
		appID:      appID,
		appSecret:  appSecret,
		client:     client,
		cardAPI:    client.Cardkit.V1.Card,
		elementAPI: client.Cardkit.V1.CardElement,
		messageAPI: client.Im.V1.Message,
	}
}

func (s *ChannelSender) Ready(ctx context.Context) error {
	now := time.Now()
	s.mu.Lock()
	if now.Before(s.readyUntil) {
		err := s.lastReadyError
		s.mu.Unlock()
		return err
	}
	s.mu.Unlock()

	resp, err := s.client.GetTenantAccessTokenBySelfBuiltApp(ctx, &larkcore.SelfBuiltTenantAccessTokenReq{AppID: s.appID, AppSecret: s.appSecret})
	if err == nil && (resp == nil || !resp.Success() || resp.TenantAccessToken == "") {
		if resp == nil {
			err = fmt.Errorf("tenant token response was empty")
		} else {
			err = fmt.Errorf("tenant token rejected: code=%d", resp.Code)
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastReadyError = err
	if err == nil {
		s.readyUntil = now.Add(5 * time.Minute)
	} else {
		s.readyUntil = now.Add(30 * time.Second)
	}
	return err
}

func (s *ChannelSender) Send(ctx context.Context, chatID string, req notify.Request) (string, error) {
	if s.messageAPI == nil {
		return "", &MessageSendError{Operation: "message_send", Class: "not_configured"}
	}
	chatID = strings.TrimSpace(chatID)
	if chatID == "" {
		return "", &MessageSendError{Operation: "message_send", Class: "invalid_destination"}
	}
	parts, err := ordinaryMessageParts(req)
	if err != nil {
		return "", err
	}
	uuidSeed, err := ordinaryMessageUUIDSeed(req)
	if err != nil {
		return "", err
	}

	replyTo := strings.TrimSpace(req.ReplyToMessageID)
	firstMessageID := ""
	for index, part := range parts {
		messageID, sendErr := s.sendOrdinaryPart(ctx, chatID, replyTo, ordinaryMessageUUID(uuidSeed, index), part)
		if sendErr != nil {
			return "", sendErr
		}
		if firstMessageID == "" {
			firstMessageID = messageID
		}
	}
	if firstMessageID == "" {
		return "", &MessageSendError{Operation: "message_send", Class: "invalid_response"}
	}
	return firstMessageID, nil
}

func ordinaryMessageParts(req notify.Request) ([]ordinaryMessagePart, *MessageSendError) {
	if req.CardJSON != "" {
		return []ordinaryMessagePart{{messageType: larkim.MsgTypeInteractive, content: req.CardJSON}}, nil
	}
	if req.Markdown == "" {
		return nil, &MessageSendError{Operation: "message_encode", Class: "missing_content"}
	}

	chunks := outbound.SplitWithCodeFences(req.Markdown, ordinaryMarkdownChunkLimit)
	parts := make([]ordinaryMessagePart, 0, len(chunks))
	for _, chunk := range chunks {
		content, err := json.Marshal(ordinaryPost{ZhCn: ordinaryPostLanguage{
			Title: req.Title,
			Content: [][]ordinaryPostElement{{{
				Tag:  "md",
				Text: chunk,
			}}},
		}})
		if err != nil {
			return nil, &MessageSendError{Operation: "message_encode", Class: "encode_failed"}
		}
		parts = append(parts, ordinaryMessagePart{
			messageType:  larkim.MsgTypePost,
			content:      string(content),
			fallbackText: chunk,
		})
	}
	if len(parts) == 0 {
		return nil, &MessageSendError{Operation: "message_encode", Class: "missing_content"}
	}
	return parts, nil
}

func (s *ChannelSender) sendOrdinaryPart(ctx context.Context, chatID, replyTo, uuid string, part ordinaryMessagePart) (string, *MessageSendError) {
	messageID, sendErr := s.sendOrdinaryCall(ctx, chatID, replyTo, uuid, part.messageType, part.content)
	if sendErr != nil && part.fallbackText != "" && isMessageFormatError(sendErr.Code) {
		content, err := json.Marshal(map[string]string{"text": part.fallbackText})
		if err != nil {
			return "", &MessageSendError{Operation: "message_encode", Class: "encode_failed"}
		}
		return s.sendOrdinaryCall(ctx, chatID, replyTo, uuid, larkim.MsgTypeText, string(content))
	}
	return messageID, sendErr
}

func (s *ChannelSender) sendOrdinaryCall(ctx context.Context, chatID, replyTo, uuid, messageType, content string) (string, *MessageSendError) {
	if replyTo != "" {
		return s.retryOrdinaryMessage(ctx, "message_reply", func() (string, *MessageSendError) {
			body := larkim.NewReplyMessageReqBodyBuilder().
				MsgType(messageType).
				Content(content).
				Uuid(uuid).
				Build()
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
			return strings.TrimSpace(*resp.Data.MessageId), nil
		})
	}

	return s.retryOrdinaryMessage(ctx, "message_create", func() (string, *MessageSendError) {
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
		return strings.TrimSpace(*resp.Data.MessageId), nil
	})
}

func (s *ChannelSender) retryOrdinaryMessage(ctx context.Context, operation string, call func() (string, *MessageSendError)) (string, *MessageSendError) {
	maxAttempts := s.retryMaxAttempts
	if maxAttempts <= 0 || maxAttempts > ordinarySendMaxAttempts {
		maxAttempts = ordinarySendMaxAttempts
	}
	baseDelay := s.retryBase
	if baseDelay <= 0 || baseDelay > time.Minute {
		baseDelay = ordinarySendRetryBase
	}
	// Three short attempts finish far inside Feishu's one-hour IM UUID
	// deduplication horizon. Every attempt reuses the request body and UUID.
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		messageID, err := call()
		if err == nil || !err.Retryable || attempt == maxAttempts {
			return messageID, err
		}
		delay := baseDelay
		for i := 1; i < attempt; i++ {
			delay *= 3
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return "", messageTransportError(operation, ctx.Err())
		case <-timer.C:
		}
	}
	return "", &MessageSendError{Operation: operation, Class: "transport", Retryable: true}
}

func messageTransportError(operation string, err error) *MessageSendError {
	switch {
	case errors.Is(err, context.Canceled):
		return &MessageSendError{Operation: operation, Class: "context_canceled", cause: context.Canceled}
	case errors.Is(err, context.DeadlineExceeded):
		return &MessageSendError{Operation: operation, Class: "deadline_exceeded", cause: context.DeadlineExceeded}
	default:
		return &MessageSendError{Operation: operation, Class: "transport", Retryable: true}
	}
}

func messageResponseError(operation string, apiResp *larkcore.ApiResp, code int) *MessageSendError {
	err := &MessageSendError{Operation: operation, Class: "api_rejected", Code: code}
	if apiResp != nil {
		err.HTTPStatus = apiResp.StatusCode
		err.RequestID = apiResp.RequestId()
	}
	err.Retryable = code == 230020 || err.HTTPStatus == 429 || err.HTTPStatus >= 500
	return err
}

func ordinaryMessageUUIDSeed(req notify.Request) ([]byte, *MessageSendError) {
	if source := strings.TrimSpace(req.Source); source != "" {
		if dedupeKey := strings.TrimSpace(req.DedupeKey); dedupeKey != "" {
			return []byte("dedupe\x00" + source + "\x00" + dedupeKey), nil
		}
	}
	seed := make([]byte, 32)
	if _, err := rand.Read(seed); err != nil {
		return nil, &MessageSendError{Operation: "message_uuid", Class: "generation_failed"}
	}
	return seed, nil
}

func ordinaryMessageUUID(seed []byte, part int) string {
	hash := sha256.New()
	_, _ = hash.Write([]byte("feishu-botd/ordinary-message/v1\x00"))
	_, _ = hash.Write(seed)
	_, _ = fmt.Fprintf(hash, "\x00%d", part)
	// 7-byte prefix + 20 digest bytes encoded as hex = 47 characters, below
	// Feishu's 50-character IM UUID limit.
	return "notify_" + fmt.Sprintf("%x", hash.Sum(nil)[:20])
}

func isMessageFormatError(code int) bool { return code == 230001 }
