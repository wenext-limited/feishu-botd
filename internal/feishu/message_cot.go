package feishu

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	larkcore "github.com/larksuite/oapi-sdk-go/v3/core"
)

// CoTMessages is the native Feishu progress surface (im/v1/message_cot) used to
// show an agent's reasoning steps beside the message that triggered the run. It
// is deliberately separate from DynamicCards: a CoT message is a first-party
// widget with its own three-call lifecycle (create, append, complete) rather
// than a card the daemon renders itself.
//
// The API is undocumented and may be allowlisted per tenant, so every caller
// must treat CoT progress as best effort and must never fail a request because
// a CoT call failed.
//
// The pinned SDK ships only the MessageCot *model*: there are no resource
// methods for these three endpoints. They are therefore hand-built larkcore
// requests. Every raw-HTTP detail lives in this file so that a future SDK
// resource is a single-file replacement.
type CoTMessages interface {
	Create(ctx context.Context, req CoTCreateRequest) (string, string, error)
	AppendEvents(ctx context.Context, req CoTAppendRequest) error
	Complete(ctx context.Context, req CoTCompleteRequest) error
}

const (
	cotBasePath          = "/open-apis/im/v1/message_cot"
	cotCompletePath      = cotBasePath + "/complete/:cot_id"
	cotIDPathParam       = "cot_id"
	cotReceiveIDTypeKey  = "receive_id_type"
	cotReceiveIDTypeChat = "chat_id"
	cotMessageIDKey      = "message_id"
	cotReasonKey         = "reason"

	cotOperationCreate   = "cot_create"
	cotOperationAppend   = "cot_append"
	cotOperationComplete = "cot_complete"

	cotClassNotConfigured   = "not_configured"
	cotClassInvalidResponse = "invalid_response"
	cotClassAPIRejected     = "api_rejected"
	cotClassAlreadyTerminal = "already_terminal"
	cotClassTransport       = "transport"
	cotClassCanceled        = "context_canceled"
	cotClassDeadline        = "deadline_exceeded"

	// The endpoint rejects a completion whose message already reached a terminal
	// state. That is the state the caller wanted, so it is reported as success.
	cotTerminalStatusMarker = "already in terminal status"

	// The API is undocumented, so these are our own conservative payload caps
	// rather than published limits. They exist to bound a single request, not to
	// mirror a server rule.
	maxCoTIdentifierLength   = 128
	maxCoTLabelLength        = 200
	maxCoTEventTypeLength    = 64
	maxCoTEventContentLength = 4096
	maxCoTEventsPerRequest   = 100
)

// ErrCoTAlreadyTerminal marks a response rejected because the CoT message has
// already reached a terminal state. Complete resolves it to success; other
// operations surface it so a caller can tell a lost race from a real failure.
var ErrCoTAlreadyTerminal = errors.New("feishu cot message is already in terminal status")

// CoTEventType is an AG-UI event name. Only the four values below are proven
// against the live API; the SDK model names TOOL_CALL_START as another valid
// type but its content shape is unverified, so it is not offered here.
type CoTEventType string

const (
	CoTEventRunStarted   CoTEventType = "RUN_STARTED"
	CoTEventStepStarted  CoTEventType = "STEP_STARTED"
	CoTEventStepFinished CoTEventType = "STEP_FINISHED"
	CoTEventRunFinished  CoTEventType = "RUN_FINISHED"
)

// CoTOutcome is the terminal state vocabulary shared by the RUN_FINISHED event
// content and the completion call's reason parameter.
type CoTOutcome string

const (
	CoTOutcomeDone  CoTOutcome = "done"
	CoTOutcomeError CoTOutcome = "error"
)

type CoTCreateRequest struct {
	ChatID string
	// OriginMessageID binds the CoT widget to the user's triggering message.
	// The API has no unbound form.
	OriginMessageID string
}

type CoTAppendRequest struct {
	CoTID     string
	MessageID string
	Events    []CoTEvent
}

type CoTCompleteRequest struct {
	CoTID     string
	MessageID string
	Reason    CoTOutcome
}

// CoTEvent is one AG-UI event. Content is the already-marshalled JSON object
// the API expects as a string. Build events with the NewCoT*Event constructors
// so the proven content shapes stay in this file.
type CoTEvent struct {
	Type    CoTEventType
	Content string
	At      time.Time
}

// CoTAPIError is a failed message_cot call. Following the ordinary message
// adapter, it deliberately omits Feishu's response message and the raw
// transport error: either can echo request URLs, routing identifiers, or user
// content. Code and RequestID are safe to log and are the Feishu support
// handle. Unwrap exposes only classified sentinels, never a raw transport
// error.
type CoTAPIError struct {
	Operation  string
	Class      string
	HTTPStatus int
	Code       int
	RequestID  string

	cause error
}

func (e *CoTAPIError) Error() string {
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

func (e *CoTAPIError) Unwrap() error { return e.cause }

// cotTransport is the raw OpenAPI seam these three calls ride on. *lark.Client
// satisfies it, and its Do is a thin wrapper over larkcore.Request bound to the
// sender's config, token cache, base URL and timeout.
type cotTransport interface {
	Do(ctx context.Context, req *larkcore.ApiReq, options ...larkcore.RequestOptionFunc) (*larkcore.ApiResp, error)
}

type cotCreateBody struct {
	ReceiveID       string `json:"receive_id"`
	OriginMessageID string `json:"origin_message_id"`
}

type cotAppendBody struct {
	CoTID     string         `json:"cot_id"`
	MessageID string         `json:"message_id"`
	Events    []cotWireEvent `json:"events"`
}

type cotWireEvent struct {
	EventType string `json:"event_type"`
	Content   string `json:"content"`
	Timestamp any    `json:"timestamp"`
}

type cotCreateData struct {
	CoTID     string `json:"cot_id"`
	MessageID string `json:"message_id"`
}

type cotEnvelope struct {
	Code int             `json:"code"`
	Msg  string          `json:"msg"`
	Data json.RawMessage `json:"data"`
}

type cotRunContent struct {
	ThreadID string `json:"threadId"`
	RunID    string `json:"runId"`
	Status   string `json:"status,omitempty"`
}

type cotStepContent struct {
	StepID   string `json:"stepId"`
	StepName string `json:"stepName"`
}

func (s *ChannelSender) Create(ctx context.Context, in CoTCreateRequest) (string, string, error) {
	api := s.cotAPI()
	if api == nil {
		return "", "", &CoTAPIError{Operation: cotOperationCreate, Class: cotClassNotConfigured}
	}
	chatID, err := validateRequired("chat_id", in.ChatID, maxCoTIdentifierLength)
	if err != nil {
		return "", "", err
	}
	originMessageID, err := validateRequired("origin_message_id", in.OriginMessageID, maxCoTIdentifierLength)
	if err != nil {
		return "", "", err
	}

	data, err := s.callCoT(ctx, api, cotOperationCreate, &larkcore.ApiReq{
		HttpMethod:                http.MethodPost,
		ApiPath:                   cotBasePath,
		Body:                      cotCreateBody{ReceiveID: chatID, OriginMessageID: originMessageID},
		QueryParams:               larkcore.QueryParams{cotReceiveIDTypeKey: []string{cotReceiveIDTypeChat}},
		SupportedAccessTokenTypes: []larkcore.AccessTokenType{larkcore.AccessTokenTypeTenant},
	})
	if err != nil {
		return "", "", err
	}
	var created cotCreateData
	if unmarshalErr := json.Unmarshal(data, &created); unmarshalErr != nil {
		return "", "", &CoTAPIError{Operation: cotOperationCreate, Class: cotClassInvalidResponse}
	}
	cotID := strings.TrimSpace(created.CoTID)
	messageID := strings.TrimSpace(created.MessageID)
	if cotID == "" || messageID == "" {
		return "", "", &CoTAPIError{Operation: cotOperationCreate, Class: cotClassInvalidResponse}
	}
	return cotID, messageID, nil
}

func (s *ChannelSender) AppendEvents(ctx context.Context, in CoTAppendRequest) error {
	api := s.cotAPI()
	if api == nil {
		return &CoTAPIError{Operation: cotOperationAppend, Class: cotClassNotConfigured}
	}
	cotID, err := validateRequired(cotIDPathParam, in.CoTID, maxCoTIdentifierLength)
	if err != nil {
		return err
	}
	messageID, err := validateRequired(cotMessageIDKey, in.MessageID, maxCoTIdentifierLength)
	if err != nil {
		return err
	}
	events, err := encodeCoTEvents(in.Events)
	if err != nil {
		return err
	}

	_, err = s.callCoT(ctx, api, cotOperationAppend, &larkcore.ApiReq{
		HttpMethod:                http.MethodPut,
		ApiPath:                   cotBasePath,
		Body:                      cotAppendBody{CoTID: cotID, MessageID: messageID, Events: events},
		SupportedAccessTokenTypes: []larkcore.AccessTokenType{larkcore.AccessTokenTypeTenant},
	})
	return err
}

func (s *ChannelSender) Complete(ctx context.Context, in CoTCompleteRequest) error {
	api := s.cotAPI()
	if api == nil {
		return &CoTAPIError{Operation: cotOperationComplete, Class: cotClassNotConfigured}
	}
	cotID, err := validateRequired(cotIDPathParam, in.CoTID, maxCoTIdentifierLength)
	if err != nil {
		return err
	}
	messageID, err := validateRequired(cotMessageIDKey, in.MessageID, maxCoTIdentifierLength)
	if err != nil {
		return err
	}
	reason, err := validateCoTOutcome(cotReasonKey, in.Reason)
	if err != nil {
		return err
	}

	// Path and query values are escaped by the SDK request translator:
	// PathParams go through url.PathEscape and QueryParams through url.Values.
	_, err = s.callCoT(ctx, api, cotOperationComplete, &larkcore.ApiReq{
		HttpMethod: http.MethodPost,
		ApiPath:    cotCompletePath,
		PathParams: larkcore.PathParams{cotIDPathParam: cotID},
		QueryParams: larkcore.QueryParams{
			cotMessageIDKey: []string{messageID},
			cotReasonKey:    []string{string(reason)},
		},
		SupportedAccessTokenTypes: []larkcore.AccessTokenType{larkcore.AccessTokenTypeTenant},
	})
	if errors.Is(err, ErrCoTAlreadyTerminal) {
		// The message already holds the state this call wanted, so completing it
		// again is a no-op rather than a failure.
		return nil
	}
	return err
}

// NewCoTRunStartedEvent opens a run. Content shape: {threadId, runId}.
func NewCoTRunStartedEvent(threadID, runID string, at time.Time) (CoTEvent, error) {
	content, err := coTRunContent(threadID, runID, "")
	if err != nil {
		return CoTEvent{}, err
	}
	return newCoTEvent(CoTEventRunStarted, content, at)
}

// NewCoTRunFinishedEvent closes a run. Content shape: {threadId, runId, status}.
func NewCoTRunFinishedEvent(threadID, runID string, outcome CoTOutcome, at time.Time) (CoTEvent, error) {
	status, err := validateCoTOutcome("outcome", outcome)
	if err != nil {
		return CoTEvent{}, err
	}
	content, err := coTRunContent(threadID, runID, string(status))
	if err != nil {
		return CoTEvent{}, err
	}
	return newCoTEvent(CoTEventRunFinished, content, at)
}

// NewCoTStepStartedEvent opens a step. Content shape: {stepId, stepName}.
func NewCoTStepStartedEvent(stepID, stepName string, at time.Time) (CoTEvent, error) {
	content, err := coTStepContent(stepID, stepName)
	if err != nil {
		return CoTEvent{}, err
	}
	return newCoTEvent(CoTEventStepStarted, content, at)
}

// NewCoTStepFinishedEvent closes a step. Content shape: {stepId, stepName}.
func NewCoTStepFinishedEvent(stepID, stepName string, at time.Time) (CoTEvent, error) {
	content, err := coTStepContent(stepID, stepName)
	if err != nil {
		return CoTEvent{}, err
	}
	return newCoTEvent(CoTEventStepFinished, content, at)
}

func coTRunContent(threadID, runID, status string) (cotRunContent, error) {
	thread, err := validateRequired("thread_id", threadID, maxCoTLabelLength)
	if err != nil {
		return cotRunContent{}, err
	}
	run, err := validateRequired("run_id", runID, maxCoTLabelLength)
	if err != nil {
		return cotRunContent{}, err
	}
	return cotRunContent{ThreadID: thread, RunID: run, Status: status}, nil
}

func coTStepContent(stepID, stepName string) (cotStepContent, error) {
	id, err := validateRequired("step_id", stepID, maxCoTLabelLength)
	if err != nil {
		return cotStepContent{}, err
	}
	name, err := validateRequired("step_name", stepName, maxCoTLabelLength)
	if err != nil {
		return cotStepContent{}, err
	}
	return cotStepContent{StepID: id, StepName: name}, nil
}

func newCoTEvent(eventType CoTEventType, content any, at time.Time) (CoTEvent, error) {
	if at.IsZero() {
		return CoTEvent{}, fmt.Errorf("%s event time is required", eventType)
	}
	encoded, err := json.Marshal(content)
	if err != nil {
		return CoTEvent{}, fmt.Errorf("encode %s content: %w", eventType, err)
	}
	return CoTEvent{Type: eventType, Content: string(encoded), At: at}, nil
}

// encodeCoTEvents validates the submitted events and returns a fresh wire
// slice; the caller's events are never modified.
func encodeCoTEvents(events []CoTEvent) ([]cotWireEvent, error) {
	if len(events) == 0 {
		return nil, fmt.Errorf("events is required")
	}
	if len(events) > maxCoTEventsPerRequest {
		return nil, fmt.Errorf("events exceeds %d entries", maxCoTEventsPerRequest)
	}
	encoded := make([]cotWireEvent, 0, len(events))
	for index, event := range events {
		eventType, err := validateRequired(fmt.Sprintf("events[%d].type", index), string(event.Type), maxCoTEventTypeLength)
		if err != nil {
			return nil, err
		}
		if err := validateJSONObject(fmt.Sprintf("events[%d].content", index), event.Content, maxCoTEventContentLength); err != nil {
			return nil, err
		}
		if event.At.IsZero() {
			return nil, fmt.Errorf("events[%d].at is required", index)
		}
		encoded = append(encoded, cotWireEvent{
			EventType: eventType,
			Content:   event.Content,
			Timestamp: cotEventTimestamp(event.At),
		})
	}
	return encoded, nil
}

// cotEventTimestamp encodes an event time for the message_cot wire format.
//
// Known ambiguity: the pinned SDK's MessageCot model types `timestamp` as a
// string, while the production reference implementation sends integer
// milliseconds and is proven to work against the live API. Integer milliseconds
// is what we send. This function and the `any` field it feeds are the only
// place the encoding is decided, so a probe that settles the ambiguity can flip
// it here alone.
func cotEventTimestamp(at time.Time) any { return at.UnixMilli() }

func validateCoTOutcome(name string, outcome CoTOutcome) (CoTOutcome, error) {
	switch outcome {
	case CoTOutcomeDone, CoTOutcomeError:
		return outcome, nil
	default:
		return "", fmt.Errorf("%s must be %q or %q", name, CoTOutcomeDone, CoTOutcomeError)
	}
}

func (s *ChannelSender) cotAPI() cotTransport {
	if s == nil || s.client == nil {
		return nil
	}
	return s.client
}

// callCoT issues one hand-built request and returns the decoded `data` field.
func (s *ChannelSender) callCoT(ctx context.Context, api cotTransport, operation string, req *larkcore.ApiReq) (json.RawMessage, error) {
	resp, err := api.Do(ctx, req)
	if err != nil {
		return nil, cotTransportError(operation, err)
	}
	if resp == nil {
		return nil, &CoTAPIError{Operation: operation, Class: cotClassInvalidResponse}
	}
	var envelope cotEnvelope
	if unmarshalErr := json.Unmarshal(resp.RawBody, &envelope); unmarshalErr != nil {
		return nil, &CoTAPIError{
			Operation:  operation,
			Class:      cotClassInvalidResponse,
			HTTPStatus: resp.StatusCode,
			RequestID:  resp.RequestId(),
		}
	}
	if envelope.Code != 0 {
		return nil, cotResponseError(operation, resp, envelope)
	}
	return envelope.Data, nil
}

func cotResponseError(operation string, resp *larkcore.ApiResp, envelope cotEnvelope) error {
	err := &CoTAPIError{
		Operation:  operation,
		Class:      cotClassAPIRejected,
		HTTPStatus: resp.StatusCode,
		Code:       envelope.Code,
		RequestID:  resp.RequestId(),
	}
	if isCoTTerminalStatus(envelope.Msg) {
		err.Class = cotClassAlreadyTerminal
		err.cause = ErrCoTAlreadyTerminal
	}
	return err
}

// isCoTTerminalStatus classifies the undocumented rejection that means the CoT
// message has already been completed. The API returns it as prose, so the
// marker is matched case-insensitively in exactly one place.
func isCoTTerminalStatus(message string) bool {
	return strings.Contains(strings.ToLower(message), cotTerminalStatusMarker)
}

func cotTransportError(operation string, err error) *CoTAPIError {
	switch {
	case errors.Is(err, context.Canceled):
		return &CoTAPIError{Operation: operation, Class: cotClassCanceled, cause: context.Canceled}
	case errors.Is(err, context.DeadlineExceeded):
		return &CoTAPIError{Operation: operation, Class: cotClassDeadline, cause: context.DeadlineExceeded}
	default:
		return &CoTAPIError{Operation: operation, Class: cotClassTransport}
	}
}

var _ CoTMessages = (*ChannelSender)(nil)
