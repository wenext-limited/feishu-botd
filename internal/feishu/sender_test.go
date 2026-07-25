package feishu

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	lark "github.com/larksuite/oapi-sdk-go/v3"
	larkcore "github.com/larksuite/oapi-sdk-go/v3/core"
	larkim "github.com/larksuite/oapi-sdk-go/v3/service/im/v1"

	"feishu-botd/internal/notify"
)

type messageCreateStep struct {
	resp *larkim.CreateMessageResp
	err  error
}

type messageReplyStep struct {
	resp *larkim.ReplyMessageResp
	err  error
}

type scriptedMessageAPI struct {
	createSteps []messageCreateStep
	replySteps  []messageReplyStep
	createReqs  []*larkim.CreateMessageReq
	replyReqs   []*larkim.ReplyMessageReq
}

func (s *scriptedMessageAPI) Create(_ context.Context, req *larkim.CreateMessageReq, _ ...larkcore.RequestOptionFunc) (*larkim.CreateMessageResp, error) {
	s.createReqs = append(s.createReqs, req)
	index := len(s.createReqs) - 1
	if index >= len(s.createSteps) {
		return nil, errors.New("unexpected create call")
	}
	return s.createSteps[index].resp, s.createSteps[index].err
}

func (s *scriptedMessageAPI) Reply(_ context.Context, req *larkim.ReplyMessageReq, _ ...larkcore.RequestOptionFunc) (*larkim.ReplyMessageResp, error) {
	s.replyReqs = append(s.replyReqs, req)
	index := len(s.replyReqs) - 1
	if index >= len(s.replySteps) {
		return nil, errors.New("unexpected reply call")
	}
	return s.replySteps[index].resp, s.replySteps[index].err
}

func TestChannelSenderThreadsReplyMessageID(t *testing.T) {
	messageID := "om_new"
	api := &fakeCardKitMessageAPI{replyResp: &larkim.ReplyMessageResp{
		Data: &larkim.ReplyMessageRespData{MessageId: &messageID},
	}}
	s := &ChannelSender{messageAPI: api}

	got, err := s.Send(context.Background(), "oc_test", notify.Request{
		Title:            "Release",
		Markdown:         "Ready to ship",
		ReplyToMessageID: "om_original",
	})
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	if got != messageID {
		t.Fatalf("message id = %q", got)
	}
	if api.createReq != nil {
		t.Fatal("reply unexpectedly used create API")
	}
	if api.replyReq == nil || api.replyReq.Body == nil {
		t.Fatal("reply request was not captured")
	}
	assertOrdinaryMessageBody(t, api.replyReq.Body.MsgType, api.replyReq.Body.Content, larkim.MsgTypePost, "Release", "Ready to ship")
}

func TestChannelSenderCreatesPostWhenReplyMessageIDIsAbsent(t *testing.T) {
	messageID := "om_new"
	api := &fakeCardKitMessageAPI{createResp: &larkim.CreateMessageResp{
		Data: &larkim.CreateMessageRespData{MessageId: &messageID},
	}}
	s := &ChannelSender{messageAPI: api}

	got, err := s.Send(context.Background(), "oc_test", notify.Request{
		Title:    "Release",
		Markdown: "Ready to ship",
	})
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	if got != messageID {
		t.Fatalf("message id = %q", got)
	}
	if api.replyReq != nil {
		t.Fatal("new message unexpectedly used reply API")
	}
	if api.createReq == nil || api.createReq.Body == nil {
		t.Fatal("create request was not captured")
	}
	if api.createReq.Body.ReceiveId == nil || *api.createReq.Body.ReceiveId != "oc_test" {
		t.Fatalf("receive id = %v", api.createReq.Body.ReceiveId)
	}
	assertOrdinaryMessageBody(t, api.createReq.Body.MsgType, api.createReq.Body.Content, larkim.MsgTypePost, "Release", "Ready to ship")
}

func TestChannelSenderSendsCardJSONAsInteractiveReply(t *testing.T) {
	messageID := "om_new"
	api := &fakeCardKitMessageAPI{replyResp: &larkim.ReplyMessageResp{
		Data: &larkim.ReplyMessageRespData{MessageId: &messageID},
	}}
	s := &ChannelSender{messageAPI: api}
	cardJSON := `{"schema":"2.0","body":{"elements":[{"tag":"markdown","content":"status"}]}}`

	got, err := s.Send(context.Background(), "oc_test", notify.Request{
		CardJSON:         cardJSON,
		ReplyToMessageID: "om_original",
	})
	if err != nil {
		t.Fatalf("send card: %v", err)
	}
	if got != messageID || api.replyReq == nil || api.replyReq.Body == nil {
		t.Fatalf("message id/request = %q %#v", got, api.replyReq)
	}
	if api.replyReq.Body.MsgType == nil || *api.replyReq.Body.MsgType != larkim.MsgTypeInteractive {
		t.Fatalf("message type = %v", api.replyReq.Body.MsgType)
	}
	if api.replyReq.Body.Content == nil || *api.replyReq.Body.Content != cardJSON {
		t.Fatalf("content = %v", api.replyReq.Body.Content)
	}
}

func TestChannelSenderLowLevelSDKWireShape(t *testing.T) {
	httpClient := &cardKitRecordingHTTPClient{}
	var output bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&output, &slog.HandlerOptions{Level: slog.LevelDebug}))
	client := lark.NewClient(
		"cli_test",
		"test-placeholder",
		lark.WithHttpClient(httpClient),
		lark.WithLogger(safeSDKLogger{logger: logger}),
	)
	sender := &ChannelSender{messageAPI: client.Im.V1.Message}

	createRequest := notify.Request{
		Source: "wire-test", DedupeKey: "create-post", Title: "Release", Markdown: "Ready to ship",
	}
	if _, err := sender.Send(context.Background(), "oc_test", createRequest); err != nil {
		t.Fatalf("create post: %v", err)
	}
	cardJSON := `{"schema":"2.0","body":{"elements":[{"tag":"markdown","content":"status"}]}}`
	replyRequest := notify.Request{
		Source: "wire-test", DedupeKey: "reply-card", CardJSON: cardJSON, ReplyToMessageID: "om_original",
	}
	if _, err := sender.Send(context.Background(), "oc_test", replyRequest); err != nil {
		t.Fatalf("reply card: %v", err)
	}

	requests := nonTokenRequests(httpClient.requests)
	if len(requests) != 2 {
		t.Fatalf("API request count = %d, want 2; requests = %#v", len(requests), requests)
	}
	parts, partErr := ordinaryMessageParts(notify.Request{Title: "Release", Markdown: "Ready to ship"})
	if partErr != nil || len(parts) != 1 {
		t.Fatalf("build expected post: parts=%#v err=%v", parts, partErr)
	}
	assertWireRequest(t, requests[0], http.MethodPost, "/open-apis/im/v1/messages", "receive_id_type=chat_id", map[string]any{
		"receive_id": "oc_test",
		"msg_type":   "post",
		"content":    parts[0].content,
		"uuid":       ordinaryMessageUUID([]byte("dedupe\x00wire-test\x00create-post"), 0),
	})
	assertWireRequest(t, requests[1], http.MethodPost, "/open-apis/im/v1/messages/om_original/reply", "", map[string]any{
		"msg_type": "interactive",
		"content":  cardJSON,
		"uuid":     ordinaryMessageUUID([]byte("dedupe\x00wire-test\x00reply-card"), 0),
	})
}

func TestChannelSenderRetriesLostReplyWithStableUUID(t *testing.T) {
	messageID := "om_replied"
	api := &scriptedMessageAPI{replySteps: []messageReplyStep{
		{err: errors.New("connection reset after response")},
		{resp: &larkim.ReplyMessageResp{Data: &larkim.ReplyMessageRespData{MessageId: &messageID}}},
	}}
	sender := &ChannelSender{messageAPI: api, retryMaxAttempts: 2, retryBase: time.Nanosecond}

	got, err := sender.Send(context.Background(), "oc_test", notify.Request{
		CardJSON: `{"schema":"2.0"}`, ReplyToMessageID: "om_original",
	})
	if err != nil || got != messageID {
		t.Fatalf("send result = %q, err=%v", got, err)
	}
	if len(api.replyReqs) != 2 || len(api.createReqs) != 0 {
		t.Fatalf("reply/create calls = %d/%d", len(api.replyReqs), len(api.createReqs))
	}
	assertStableMessageUUID(t, api.replyReqs[0].Body.Uuid, api.replyReqs[1].Body.Uuid)
}

func TestChannelSenderRetriesServerFailureWithStableUUID(t *testing.T) {
	messageID := "om_created"
	api := &scriptedMessageAPI{createSteps: []messageCreateStep{
		{resp: &larkim.CreateMessageResp{
			ApiResp:   &larkcore.ApiResp{StatusCode: http.StatusServiceUnavailable},
			CodeError: larkcore.CodeError{Code: 300999, Msg: "private upstream body"},
		}},
		{resp: &larkim.CreateMessageResp{Data: &larkim.CreateMessageRespData{MessageId: &messageID}}},
	}}
	sender := &ChannelSender{messageAPI: api, retryMaxAttempts: 2, retryBase: time.Nanosecond}

	got, err := sender.Send(context.Background(), "oc_test", notify.Request{
		Source: "agent", DedupeKey: "delivery-2", Title: "Status", Markdown: "Complete",
	})
	if err != nil || got != messageID {
		t.Fatalf("send result = %q, err=%v", got, err)
	}
	if len(api.createReqs) != 2 || len(api.replyReqs) != 0 {
		t.Fatalf("create/reply calls = %d/%d", len(api.createReqs), len(api.replyReqs))
	}
	assertStableMessageUUID(t, api.createReqs[0].Body.Uuid, api.createReqs[1].Body.Uuid)
}

func TestChannelSenderFrequencyLimitRetriesExactReplyWithoutCreateFallback(t *testing.T) {
	limited := func() *larkim.ReplyMessageResp {
		return &larkim.ReplyMessageResp{
			ApiResp:   &larkcore.ApiResp{StatusCode: http.StatusBadRequest},
			CodeError: larkcore.CodeError{Code: 230020, Msg: "frequency limit with private reply details"},
		}
	}
	api := &scriptedMessageAPI{replySteps: []messageReplyStep{{resp: limited()}, {resp: limited()}}}
	sender := &ChannelSender{messageAPI: api, retryMaxAttempts: 2, retryBase: time.Nanosecond}

	_, err := sender.Send(context.Background(), "oc_test", notify.Request{
		Source: "agent", DedupeKey: "delivery-rate-limit", CardJSON: `{"schema":"2.0"}`, ReplyToMessageID: "om_original",
	})
	var sendErr *MessageSendError
	if !errors.As(err, &sendErr) || sendErr.Code != 230020 || !sendErr.Retryable {
		t.Fatalf("error = %#v, want retryable 230020", err)
	}
	if len(api.replyReqs) != 2 || len(api.createReqs) != 0 {
		t.Fatalf("reply/create calls = %d/%d; reply must never fall back to create", len(api.replyReqs), len(api.createReqs))
	}
	assertStableMessageUUID(t, api.replyReqs[0].Body.Uuid, api.replyReqs[1].Body.Uuid)
	if strings.Contains(err.Error(), "private reply details") || strings.Contains(err.Error(), "om_original") {
		t.Fatalf("rate-limit error leaked private details: %v", err)
	}
}

func TestSafeSDKLoggerAndMessageErrorOmitOutboundSecrets(t *testing.T) {
	var output bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&output, &slog.HandlerOptions{Level: slog.LevelDebug}))
	sdkLogger := safeSDKLogger{logger: logger}
	const (
		privateURL       = "https://open.feishu.invalid/messages/om_private/reply?ticket=private-ticket"
		privateMessageID = "om_private"
		privateCardID    = "card_private"
		privatePayload   = `{"text":"private outbound payload"}`
		privateBody      = "private upstream error body"
	)
	transportErr := &url.Error{
		Op:  "POST",
		URL: privateURL,
		Err: fmt.Errorf("%s: %w", privateBody, context.DeadlineExceeded),
	}
	sdkLogger.Debug(context.Background(), privateURL, privateMessageID)
	sdkLogger.Info(context.Background(), privateCardID, privatePayload)
	sdkLogger.Warn(context.Background(), transportErr)
	sdkLogger.Error(context.Background(), privateURL, privateMessageID, privateCardID, privatePayload, transportErr)

	api := &fakeCardKitMessageAPI{replyErr: transportErr}
	sender := &ChannelSender{messageAPI: api, retryMaxAttempts: 1}
	_, err := sender.Send(context.Background(), "oc_private", notify.Request{
		CardJSON:         privatePayload,
		ReplyToMessageID: privateMessageID,
	})
	var sendErr *MessageSendError
	if !errors.As(err, &sendErr) || sendErr.Class != "deadline_exceeded" || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error = %#v, want sanitized deadline MessageSendError", err)
	}

	logged := output.String()
	for _, private := range []string{privateURL, "private-ticket", privateMessageID, privateCardID, privatePayload, "private outbound payload", privateBody} {
		if strings.Contains(logged, private) {
			t.Fatalf("safe SDK log leaked %q: %s", private, logged)
		}
		if strings.Contains(err.Error(), private) {
			t.Fatalf("message error leaked %q: %s", private, err)
		}
	}
	for _, safe := range []string{"event_class=sdk_debug", "event_class=sdk_info", "event_class=sdk_warn", "event_class=sdk_error"} {
		if !strings.Contains(logged, safe) {
			t.Fatalf("safe SDK log missing %q: %s", safe, logged)
		}
	}
}

func TestChannelSenderReturnsSanitizedTypedAPIError(t *testing.T) {
	api := &fakeCardKitMessageAPI{replyResp: &larkim.ReplyMessageResp{
		ApiResp: &larkcore.ApiResp{
			StatusCode: http.StatusBadRequest,
			Header:     http.Header{larkcore.HttpHeaderKeyLogId: []string{"log-safe-123"}},
			RawBody:    []byte(`{"message_id":"om_private","msg":"private error body"}`),
		},
		CodeError: larkcore.CodeError{Code: 230002, Msg: "reply om_private contained private error body"},
	}}
	sender := &ChannelSender{messageAPI: api}
	_, err := sender.Send(context.Background(), "oc_private", notify.Request{
		CardJSON:         `{"text":"private payload"}`,
		ReplyToMessageID: "om_private",
	})
	var sendErr *MessageSendError
	if !errors.As(err, &sendErr) {
		t.Fatalf("error = %v, want MessageSendError", err)
	}
	if sendErr.Operation != "message_reply" || sendErr.Class != "api_rejected" || sendErr.HTTPStatus != http.StatusBadRequest || sendErr.Code != 230002 || sendErr.RequestID != "log-safe-123" {
		t.Fatalf("typed error = %#v", sendErr)
	}
	for _, private := range []string{"om_private", "private error body", "private payload"} {
		if strings.Contains(err.Error(), private) {
			t.Fatalf("typed error leaked %q: %s", private, err)
		}
	}
}

func assertOrdinaryMessageBody(t *testing.T, messageType, content *string, wantType, wantTitle, wantMarkdown string) {
	t.Helper()
	if messageType == nil || *messageType != wantType {
		t.Fatalf("message type = %v, want %q", messageType, wantType)
	}
	if content == nil {
		t.Fatal("message content is nil")
	}
	var post ordinaryPost
	if err := json.Unmarshal([]byte(*content), &post); err != nil {
		t.Fatalf("decode post content %q: %v", *content, err)
	}
	if post.ZhCn.Title != wantTitle || len(post.ZhCn.Content) != 1 || len(post.ZhCn.Content[0]) != 1 {
		t.Fatalf("post = %#v", post)
	}
	element := post.ZhCn.Content[0][0]
	if element.Tag != "md" || element.Text != wantMarkdown {
		t.Fatalf("post element = %#v", element)
	}
}

func assertStableMessageUUID(t *testing.T, first, second *string) {
	t.Helper()
	if first == nil || second == nil || *first == "" || *first != *second {
		t.Fatalf("message UUIDs = %v / %v, want one stable identity", first, second)
	}
	if len(*first) > maxMessageUUIDLength {
		t.Fatalf("message UUID length = %d, max %d: %q", len(*first), maxMessageUUIDLength, *first)
	}
}
