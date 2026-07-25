package feishu

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"unicode/utf8"

	larkcore "github.com/larksuite/oapi-sdk-go/v3/core"
	larkcardkit "github.com/larksuite/oapi-sdk-go/v3/service/cardkit/v1"
	larkim "github.com/larksuite/oapi-sdk-go/v3/service/im/v1"
)

const (
	// These values mirror the current Feishu IM/CardKit OpenAPI request
	// validation rules. The generated v3.9.7 SDK does not enforce them.
	maxElementIDLength   = 20
	maxMessageUUIDLength = 50
	maxCardOpUUIDLength  = 64
)

// DynamicCards is the native CardKit path used by progressive agent replies.
// It is deliberately separate from Sender: ordinary notifications remain
// one-shot messages, while a dynamic card has a multi-operation lifecycle.
//
// Implementations must keep CreateCard and SendCard separate. A CardKit entity
// can be sent only once, so a retry after an ambiguous send must reuse both the
// card id and message UUID returned/used by the original attempt.
type DynamicCards interface {
	CreateCard(ctx context.Context, cardJSON string) (string, error)
	SendCard(ctx context.Context, req CardSendRequest) (string, error)
	UpdateContent(ctx context.Context, req CardContentUpdate) error
	UpdateSettings(ctx context.Context, req CardSettingsUpdate) error
	BatchUpdate(ctx context.Context, req CardBatchUpdate) error
}

type CardSendRequest struct {
	ChatID string
	// Exactly one of ChatID and ReplyToMessageID is required. The adapter never
	// falls back from a failed reply to a new top-level message.
	ReplyToMessageID string
	CardID           string
	UUID             string
}

type CardContentUpdate struct {
	CardID    string
	ElementID string
	Content   string
	UUID      string
	Sequence  int32
}

type CardSettingsUpdate struct {
	CardID       string
	SettingsJSON string
	UUID         string
	Sequence     int32
}

type CardBatchUpdate struct {
	CardID      string
	ActionsJSON string
	UUID        string
	Sequence    int32
}

// DynamicCardAPIError is a non-success response returned by Feishu. Transport
// failures remain wrapped as their original Go error so errors.Is/As continue
// to work. RequestID is safe to log and is the primary Feishu support handle.
type DynamicCardAPIError struct {
	Operation  string
	HTTPStatus int
	Code       int
	Message    string
	RequestID  string
}

func (e *DynamicCardAPIError) Error() string {
	if e.RequestID == "" {
		return fmt.Sprintf("feishu %s rejected: code=%d msg=%s", e.Operation, e.Code, e.Message)
	}
	return fmt.Sprintf("feishu %s rejected: code=%d msg=%s request_id=%s", e.Operation, e.Code, e.Message, e.RequestID)
}

type cardKitCardAPI interface {
	Create(context.Context, *larkcardkit.CreateCardReq, ...larkcore.RequestOptionFunc) (*larkcardkit.CreateCardResp, error)
	Settings(context.Context, *larkcardkit.SettingsCardReq, ...larkcore.RequestOptionFunc) (*larkcardkit.SettingsCardResp, error)
	BatchUpdate(context.Context, *larkcardkit.BatchUpdateCardReq, ...larkcore.RequestOptionFunc) (*larkcardkit.BatchUpdateCardResp, error)
}

type cardKitElementAPI interface {
	Content(context.Context, *larkcardkit.ContentCardElementReq, ...larkcore.RequestOptionFunc) (*larkcardkit.ContentCardElementResp, error)
}

type cardKitMessageAPI interface {
	Create(context.Context, *larkim.CreateMessageReq, ...larkcore.RequestOptionFunc) (*larkim.CreateMessageResp, error)
	Reply(context.Context, *larkim.ReplyMessageReq, ...larkcore.RequestOptionFunc) (*larkim.ReplyMessageResp, error)
}

func (s *ChannelSender) CreateCard(ctx context.Context, cardJSON string) (string, error) {
	if s.cardAPI == nil {
		return "", fmt.Errorf("feishu card create is not configured")
	}
	if err := validateCardDocument(cardJSON); err != nil {
		return "", err
	}

	body := larkcardkit.NewCreateCardReqBodyBuilder().
		Type("card_json").
		Data(cardJSON).
		Build()
	req := larkcardkit.NewCreateCardReqBuilder().
		Body(body).
		Build()
	// Generated request builders retain the body only in a private ApiReq.
	// Keep the exported mirror populated too so request decorators and tests can
	// inspect the exact payload without reflection.
	req.Body = body
	resp, err := s.cardAPI.Create(ctx, req)
	if err != nil {
		return "", fmt.Errorf("feishu card create failed: %w", err)
	}
	if resp == nil {
		return "", fmt.Errorf("feishu card create returned an empty response")
	}
	if !resp.Success() {
		return "", dynamicCardResponseError("card create", resp.ApiResp, resp.Code, resp.Msg)
	}
	if resp.Data == nil || resp.Data.CardId == nil || strings.TrimSpace(*resp.Data.CardId) == "" {
		return "", fmt.Errorf("feishu card create returned no card id")
	}
	return strings.TrimSpace(*resp.Data.CardId), nil
}

func (s *ChannelSender) SendCard(ctx context.Context, in CardSendRequest) (string, error) {
	if s.messageAPI == nil {
		return "", fmt.Errorf("feishu card send is not configured")
	}
	cardID, err := validateRequired("card_id", in.CardID, 0)
	if err != nil {
		return "", err
	}
	uuid, err := validateRequired("message uuid", in.UUID, maxMessageUUIDLength)
	if err != nil {
		return "", err
	}

	content, err := json.Marshal(cardReference{
		Type: "card",
		Data: cardReferenceData{CardID: cardID},
	})
	if err != nil {
		return "", fmt.Errorf("encode Feishu card reference: %w", err)
	}

	replyTo := strings.TrimSpace(in.ReplyToMessageID)
	chatID := strings.TrimSpace(in.ChatID)
	if (replyTo == "") == (chatID == "") {
		return "", fmt.Errorf("exactly one of chat_id or reply_to_message_id is required")
	}
	if replyTo != "" {
		body := larkim.NewReplyMessageReqBodyBuilder().
			MsgType(larkim.MsgTypeInteractive).
			Content(string(content)).
			Uuid(uuid).
			Build()
		req := larkim.NewReplyMessageReqBuilder().
			MessageId(replyTo).
			Body(body).
			Build()
		req.Body = body
		resp, callErr := s.messageAPI.Reply(ctx, req)
		if callErr != nil {
			return "", fmt.Errorf("feishu card reply failed: %w", callErr)
		}
		if resp == nil {
			return "", fmt.Errorf("feishu card reply returned an empty response")
		}
		if !resp.Success() {
			return "", dynamicCardResponseError("card reply", resp.ApiResp, resp.Code, resp.Msg)
		}
		if resp.Data == nil || resp.Data.MessageId == nil || strings.TrimSpace(*resp.Data.MessageId) == "" {
			return "", fmt.Errorf("feishu card reply returned no message id")
		}
		return strings.TrimSpace(*resp.Data.MessageId), nil
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
		return "", fmt.Errorf("feishu card send failed: %w", callErr)
	}
	if resp == nil {
		return "", fmt.Errorf("feishu card send returned an empty response")
	}
	if !resp.Success() {
		return "", dynamicCardResponseError("card send", resp.ApiResp, resp.Code, resp.Msg)
	}
	if resp.Data == nil || resp.Data.MessageId == nil || strings.TrimSpace(*resp.Data.MessageId) == "" {
		return "", fmt.Errorf("feishu card send returned no message id")
	}
	return strings.TrimSpace(*resp.Data.MessageId), nil
}

func (s *ChannelSender) UpdateContent(ctx context.Context, in CardContentUpdate) error {
	if s.elementAPI == nil {
		return fmt.Errorf("feishu card content update is not configured")
	}
	cardID, err := validateRequired("card_id", in.CardID, 0)
	if err != nil {
		return err
	}
	elementID, err := validateRequired("element_id", in.ElementID, maxElementIDLength)
	if err != nil {
		return err
	}
	uuid, err := validateCardOperation(in.UUID, in.Sequence)
	if err != nil {
		return err
	}
	if in.Content == "" {
		return fmt.Errorf("content is required")
	}

	body := larkcardkit.NewContentCardElementReqBodyBuilder().
		Uuid(uuid).
		Content(in.Content).
		Sequence(int(in.Sequence)).
		Build()
	req := larkcardkit.NewContentCardElementReqBuilder().
		CardId(cardID).
		ElementId(elementID).
		Body(body).
		Build()
	req.Body = body
	resp, callErr := s.elementAPI.Content(ctx, req)
	if callErr != nil {
		return fmt.Errorf("feishu card content update failed: %w", callErr)
	}
	if resp == nil {
		return fmt.Errorf("feishu card content update returned an empty response")
	}
	if !resp.Success() {
		return dynamicCardResponseError("card content update", resp.ApiResp, resp.Code, resp.Msg)
	}
	return nil
}

func (s *ChannelSender) UpdateSettings(ctx context.Context, in CardSettingsUpdate) error {
	if s.cardAPI == nil {
		return fmt.Errorf("feishu card settings update is not configured")
	}
	cardID, err := validateRequired("card_id", in.CardID, 0)
	if err != nil {
		return err
	}
	uuid, err := validateCardOperation(in.UUID, in.Sequence)
	if err != nil {
		return err
	}
	if err := validateJSONObject("settings_json", in.SettingsJSON, 0); err != nil {
		return err
	}

	body := larkcardkit.NewSettingsCardReqBodyBuilder().
		Settings(in.SettingsJSON).
		Uuid(uuid).
		Sequence(int(in.Sequence)).
		Build()
	req := larkcardkit.NewSettingsCardReqBuilder().
		CardId(cardID).
		Body(body).
		Build()
	req.Body = body
	resp, callErr := s.cardAPI.Settings(ctx, req)
	if callErr != nil {
		return fmt.Errorf("feishu card settings update failed: %w", callErr)
	}
	if resp == nil {
		return fmt.Errorf("feishu card settings update returned an empty response")
	}
	if !resp.Success() {
		return dynamicCardResponseError("card settings update", resp.ApiResp, resp.Code, resp.Msg)
	}
	return nil
}

func (s *ChannelSender) BatchUpdate(ctx context.Context, in CardBatchUpdate) error {
	if s.cardAPI == nil {
		return fmt.Errorf("feishu card batch update is not configured")
	}
	cardID, err := validateRequired("card_id", in.CardID, 0)
	if err != nil {
		return err
	}
	uuid, err := validateCardOperation(in.UUID, in.Sequence)
	if err != nil {
		return err
	}
	if err := validateJSONArray("actions_json", in.ActionsJSON, 0); err != nil {
		return err
	}

	body := larkcardkit.NewBatchUpdateCardReqBodyBuilder().
		Uuid(uuid).
		Sequence(int(in.Sequence)).
		Actions(in.ActionsJSON).
		Build()
	req := larkcardkit.NewBatchUpdateCardReqBuilder().
		CardId(cardID).
		Body(body).
		Build()
	req.Body = body
	resp, callErr := s.cardAPI.BatchUpdate(ctx, req)
	if callErr != nil {
		return fmt.Errorf("feishu card batch update failed: %w", callErr)
	}
	if resp == nil {
		return fmt.Errorf("feishu card batch update returned an empty response")
	}
	if !resp.Success() {
		return dynamicCardResponseError("card batch update", resp.ApiResp, resp.Code, resp.Msg)
	}
	return nil
}

type cardReference struct {
	Type string            `json:"type"`
	Data cardReferenceData `json:"data"`
}

type cardReferenceData struct {
	CardID string `json:"card_id"`
}

func validateCardDocument(cardJSON string) error {
	if err := validateJSONObject("card_json", cardJSON, 0); err != nil {
		return err
	}
	var envelope struct {
		Schema string `json:"schema"`
	}
	if err := json.Unmarshal([]byte(cardJSON), &envelope); err != nil {
		return fmt.Errorf("card_json must be valid JSON: %w", err)
	}
	if envelope.Schema != "2.0" {
		return fmt.Errorf("card_json schema must be 2.0")
	}
	return nil
}

func validateCardOperation(uuid string, sequence int32) (string, error) {
	uuid, err := validateRequired("operation uuid", uuid, maxCardOpUUIDLength)
	if err != nil {
		return "", err
	}
	if sequence <= 0 {
		return "", fmt.Errorf("sequence must be a positive int32")
	}
	return uuid, nil
}

func validateRequired(name, value string, maxLength int) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", fmt.Errorf("%s is required", name)
	}
	if maxLength > 0 && utf8.RuneCountInString(value) > maxLength {
		return "", fmt.Errorf("%s exceeds %d characters", name, maxLength)
	}
	return value, nil
}

func validateJSONObject(name, value string, maxLength int) error {
	if value == "" {
		return fmt.Errorf("%s is required", name)
	}
	if maxLength > 0 && utf8.RuneCountInString(value) > maxLength {
		return fmt.Errorf("%s exceeds %d characters", name, maxLength)
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal([]byte(value), &object); err != nil || object == nil {
		return fmt.Errorf("%s must be a JSON object", name)
	}
	return nil
}

func validateJSONArray(name, value string, maxLength int) error {
	if value == "" {
		return fmt.Errorf("%s is required", name)
	}
	if maxLength > 0 && utf8.RuneCountInString(value) > maxLength {
		return fmt.Errorf("%s exceeds %d characters", name, maxLength)
	}
	var array []struct {
		Action string          `json:"action"`
		Params json.RawMessage `json:"params"`
	}
	if err := json.Unmarshal([]byte(value), &array); err != nil || len(array) == 0 {
		return fmt.Errorf("%s must be a non-empty JSON array", name)
	}
	for _, action := range array {
		if strings.TrimSpace(action.Action) == "" || len(action.Params) == 0 || action.Params[0] != '{' {
			return fmt.Errorf("%s entries must contain action and object params", name)
		}
	}
	return nil
}

func dynamicCardResponseError(operation string, apiResp *larkcore.ApiResp, code int, message string) error {
	err := &DynamicCardAPIError{Operation: operation, Code: code, Message: message}
	if apiResp != nil {
		err.HTTPStatus = apiResp.StatusCode
		err.RequestID = apiResp.RequestId()
	}
	return err
}

var _ DynamicCards = (*ChannelSender)(nil)
