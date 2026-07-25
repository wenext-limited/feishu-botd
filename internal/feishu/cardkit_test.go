package feishu

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"testing"

	lark "github.com/larksuite/oapi-sdk-go/v3"
	larkcore "github.com/larksuite/oapi-sdk-go/v3/core"
	larkcardkit "github.com/larksuite/oapi-sdk-go/v3/service/cardkit/v1"
	larkim "github.com/larksuite/oapi-sdk-go/v3/service/im/v1"
)

type recordedHTTPRequest struct {
	Method   string
	Path     string
	RawQuery string
	Body     []byte
}

type cardKitRecordingHTTPClient struct {
	requests []recordedHTTPRequest
}

func (c *cardKitRecordingHTTPClient) Do(req *http.Request) (*http.Response, error) {
	body, err := io.ReadAll(req.Body)
	if err != nil {
		return nil, err
	}
	c.requests = append(c.requests, recordedHTTPRequest{
		Method:   req.Method,
		Path:     req.URL.Path,
		RawQuery: req.URL.RawQuery,
		Body:     body,
	})

	responseBody := `{"code":0,"msg":"success"}`
	switch req.URL.Path {
	case "/open-apis/auth/v3/tenant_access_token/internal":
		responseBody = `{"code":0,"msg":"success","tenant_access_token":"t-test","expire":7200}`
	case "/open-apis/cardkit/v1/cards":
		responseBody = `{"code":0,"msg":"success","data":{"card_id":"7355372766134157313"}}`
	case "/open-apis/im/v1/messages":
		responseBody = `{"code":0,"msg":"success","data":{"message_id":"om_created"}}`
	case "/open-apis/im/v1/messages/om_original/reply":
		responseBody = `{"code":0,"msg":"success","data":{"message_id":"om_reply"}}`
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(bytes.NewBufferString(responseBody)),
		Request:    req,
	}, nil
}

type fakeCardKitCardAPI struct {
	createReq   *larkcardkit.CreateCardReq
	settingsReq *larkcardkit.SettingsCardReq
	batchReq    *larkcardkit.BatchUpdateCardReq

	createResp   *larkcardkit.CreateCardResp
	settingsResp *larkcardkit.SettingsCardResp
	batchResp    *larkcardkit.BatchUpdateCardResp

	createErr   error
	settingsErr error
	batchErr    error
}

func (f *fakeCardKitCardAPI) Create(_ context.Context, req *larkcardkit.CreateCardReq, _ ...larkcore.RequestOptionFunc) (*larkcardkit.CreateCardResp, error) {
	f.createReq = req
	return f.createResp, f.createErr
}

func (f *fakeCardKitCardAPI) Settings(_ context.Context, req *larkcardkit.SettingsCardReq, _ ...larkcore.RequestOptionFunc) (*larkcardkit.SettingsCardResp, error) {
	f.settingsReq = req
	return f.settingsResp, f.settingsErr
}

func (f *fakeCardKitCardAPI) BatchUpdate(_ context.Context, req *larkcardkit.BatchUpdateCardReq, _ ...larkcore.RequestOptionFunc) (*larkcardkit.BatchUpdateCardResp, error) {
	f.batchReq = req
	return f.batchResp, f.batchErr
}

type fakeCardKitElementAPI struct {
	req  *larkcardkit.ContentCardElementReq
	resp *larkcardkit.ContentCardElementResp
	err  error
}

func (f *fakeCardKitElementAPI) Content(_ context.Context, req *larkcardkit.ContentCardElementReq, _ ...larkcore.RequestOptionFunc) (*larkcardkit.ContentCardElementResp, error) {
	f.req = req
	return f.resp, f.err
}

type fakeCardKitMessageAPI struct {
	createReq *larkim.CreateMessageReq
	replyReq  *larkim.ReplyMessageReq

	createResp *larkim.CreateMessageResp
	replyResp  *larkim.ReplyMessageResp

	createErr error
	replyErr  error
}

func (f *fakeCardKitMessageAPI) Create(_ context.Context, req *larkim.CreateMessageReq, _ ...larkcore.RequestOptionFunc) (*larkim.CreateMessageResp, error) {
	f.createReq = req
	return f.createResp, f.createErr
}

func (f *fakeCardKitMessageAPI) Reply(_ context.Context, req *larkim.ReplyMessageReq, _ ...larkcore.RequestOptionFunc) (*larkim.ReplyMessageResp, error) {
	f.replyReq = req
	return f.replyResp, f.replyErr
}

func TestDynamicCardsCreateCardBuildsNativeCardKitRequest(t *testing.T) {
	cardID := "7355372766134157313"
	api := &fakeCardKitCardAPI{createResp: &larkcardkit.CreateCardResp{
		Data: &larkcardkit.CreateCardRespData{CardId: &cardID},
	}}
	sender := &ChannelSender{cardAPI: api}
	cardJSON := `{"schema":"2.0","config":{"streaming_mode":true,"update_multi":true},"body":{"elements":[{"tag":"markdown","element_id":"agent_answer","content":""}]}}`

	got, err := sender.CreateCard(context.Background(), cardJSON)
	if err != nil {
		t.Fatalf("create card: %v", err)
	}
	if got != cardID {
		t.Fatalf("card id = %q, want %q", got, cardID)
	}
	if api.createReq == nil || api.createReq.Body == nil {
		t.Fatal("create request body was not captured")
	}
	if api.createReq.Body.Type == nil || *api.createReq.Body.Type != "card_json" {
		t.Fatalf("card type = %v, want card_json", api.createReq.Body.Type)
	}
	if api.createReq.Body.Data == nil || *api.createReq.Body.Data != cardJSON {
		t.Fatalf("card data = %v, want exact source JSON", api.createReq.Body.Data)
	}
}

func TestDynamicCardsCreateCardRejectsNonSchemaTwoDocument(t *testing.T) {
	sender := &ChannelSender{cardAPI: &fakeCardKitCardAPI{}}
	if _, err := sender.CreateCard(context.Background(), `{"schema":"1.0"}`); err == nil {
		t.Fatal("expected schema validation error")
	}
}

func TestDynamicCardsSendCardBuildsCardEntityMessage(t *testing.T) {
	messageID := "om_created"
	api := &fakeCardKitMessageAPI{createResp: &larkim.CreateMessageResp{
		Data: &larkim.CreateMessageRespData{MessageId: &messageID},
	}}
	sender := &ChannelSender{messageAPI: api}

	got, err := sender.SendCard(context.Background(), CardSendRequest{
		ChatID: "oc_test",
		CardID: "7355372766134157313",
		UUID:   "38cc728b-9ad4-49f8-8b52-82445c054815",
	})
	if err != nil {
		t.Fatalf("send card: %v", err)
	}
	if got != messageID {
		t.Fatalf("message id = %q, want %q", got, messageID)
	}
	if api.replyReq != nil {
		t.Fatal("new card send unexpectedly used reply API")
	}
	if api.createReq == nil || api.createReq.Body == nil {
		t.Fatal("create message request body was not captured")
	}
	body := api.createReq.Body
	if body.ReceiveId == nil || *body.ReceiveId != "oc_test" {
		t.Fatalf("receive id = %v, want oc_test", body.ReceiveId)
	}
	if body.MsgType == nil || *body.MsgType != larkim.MsgTypeInteractive {
		t.Fatalf("message type = %v, want interactive", body.MsgType)
	}
	if body.Uuid == nil || *body.Uuid != "38cc728b-9ad4-49f8-8b52-82445c054815" {
		t.Fatalf("message uuid = %v", body.Uuid)
	}
	if body.Content == nil || *body.Content != `{"type":"card","data":{"card_id":"7355372766134157313"}}` {
		t.Fatalf("message content = %v", body.Content)
	}
}

func TestDynamicCardsSendCardBuildsCardEntityReply(t *testing.T) {
	messageID := "om_reply"
	api := &fakeCardKitMessageAPI{replyResp: &larkim.ReplyMessageResp{
		Data: &larkim.ReplyMessageRespData{MessageId: &messageID},
	}}
	sender := &ChannelSender{messageAPI: api}

	got, err := sender.SendCard(context.Background(), CardSendRequest{
		ReplyToMessageID: "om_original",
		CardID:           "7355372766134157313",
		UUID:             "38cc728b-9ad4-49f8-8b52-82445c054815",
	})
	if err != nil {
		t.Fatalf("reply with card: %v", err)
	}
	if got != messageID {
		t.Fatalf("message id = %q, want %q", got, messageID)
	}
	if api.createReq != nil {
		t.Fatal("card reply unexpectedly used create API")
	}
	if api.replyReq == nil || api.replyReq.Body == nil {
		t.Fatal("reply request body was not captured")
	}
	body := api.replyReq.Body
	if body.MsgType == nil || *body.MsgType != larkim.MsgTypeInteractive {
		t.Fatalf("message type = %v, want interactive", body.MsgType)
	}
	if body.Uuid == nil || *body.Uuid != "38cc728b-9ad4-49f8-8b52-82445c054815" {
		t.Fatalf("message uuid = %v", body.Uuid)
	}
	if body.Content == nil || *body.Content != `{"type":"card","data":{"card_id":"7355372766134157313"}}` {
		t.Fatalf("message content = %v", body.Content)
	}
}

func TestDynamicCardsSendCardRequiresExactlyOneDestination(t *testing.T) {
	sender := &ChannelSender{messageAPI: &fakeCardKitMessageAPI{}}
	base := CardSendRequest{
		CardID: "7355372766134157313",
		UUID:   "38cc728b-9ad4-49f8-8b52-82445c054815",
	}

	if _, err := sender.SendCard(context.Background(), base); err == nil {
		t.Fatal("expected missing destination error")
	}

	base.ChatID = "oc_test"
	base.ReplyToMessageID = "om_original"
	if _, err := sender.SendCard(context.Background(), base); err == nil {
		t.Fatal("expected ambiguous destination error")
	}
}

func TestDynamicCardsUpdateContentUsesFullSnapshotUUIDAndSequence(t *testing.T) {
	api := &fakeCardKitElementAPI{resp: &larkcardkit.ContentCardElementResp{}}
	sender := &ChannelSender{elementAPI: api}

	err := sender.UpdateContent(context.Background(), CardContentUpdate{
		CardID:    "7355372766134157313",
		ElementID: "agent_answer",
		Content:   "Complete accumulated answer",
		UUID:      "content-operation-1",
		Sequence:  7,
	})
	if err != nil {
		t.Fatalf("update content: %v", err)
	}
	if api.req == nil || api.req.Body == nil {
		t.Fatal("content request body was not captured")
	}
	body := api.req.Body
	if body.Content == nil || *body.Content != "Complete accumulated answer" {
		t.Fatalf("content = %v", body.Content)
	}
	if body.Uuid == nil || *body.Uuid != "content-operation-1" {
		t.Fatalf("uuid = %v", body.Uuid)
	}
	if body.Sequence == nil || *body.Sequence != 7 {
		t.Fatalf("sequence = %v", body.Sequence)
	}
}

func TestDynamicCardsUpdateSettingsUsesSerializedSettings(t *testing.T) {
	api := &fakeCardKitCardAPI{settingsResp: &larkcardkit.SettingsCardResp{}}
	sender := &ChannelSender{cardAPI: api}
	settings := `{"config":{"streaming_mode":false}}`

	err := sender.UpdateSettings(context.Background(), CardSettingsUpdate{
		CardID:       "7355372766134157313",
		SettingsJSON: settings,
		UUID:         "finish-operation",
		Sequence:     8,
	})
	if err != nil {
		t.Fatalf("update settings: %v", err)
	}
	if api.settingsReq == nil || api.settingsReq.Body == nil {
		t.Fatal("settings request body was not captured")
	}
	body := api.settingsReq.Body
	if body.Settings == nil || *body.Settings != settings {
		t.Fatalf("settings = %v", body.Settings)
	}
	if body.Uuid == nil || *body.Uuid != "finish-operation" {
		t.Fatalf("uuid = %v", body.Uuid)
	}
	if body.Sequence == nil || *body.Sequence != 8 {
		t.Fatalf("sequence = %v", body.Sequence)
	}
}

func TestDynamicCardsBatchUpdateUsesSerializedActionArray(t *testing.T) {
	api := &fakeCardKitCardAPI{batchResp: &larkcardkit.BatchUpdateCardResp{}}
	sender := &ChannelSender{cardAPI: api}
	actions := `[{"action":"partial_update_setting","params":{"settings":{"config":{"streaming_mode":false}}}}]`

	err := sender.BatchUpdate(context.Background(), CardBatchUpdate{
		CardID:      "7355372766134157313",
		ActionsJSON: actions,
		UUID:        "batch-operation",
		Sequence:    9,
	})
	if err != nil {
		t.Fatalf("batch update: %v", err)
	}
	if api.batchReq == nil || api.batchReq.Body == nil {
		t.Fatal("batch request body was not captured")
	}
	body := api.batchReq.Body
	if body.Actions == nil || *body.Actions != actions {
		t.Fatalf("actions = %v", body.Actions)
	}
	if body.Uuid == nil || *body.Uuid != "batch-operation" {
		t.Fatalf("uuid = %v", body.Uuid)
	}
	if body.Sequence == nil || *body.Sequence != 9 {
		t.Fatalf("sequence = %v", body.Sequence)
	}
}

func TestDynamicCardsReturnsTypedBusinessError(t *testing.T) {
	api := &fakeCardKitElementAPI{resp: &larkcardkit.ContentCardElementResp{
		ApiResp: &larkcore.ApiResp{
			StatusCode: http.StatusBadRequest,
			Header:     http.Header{larkcore.HttpHeaderKeyLogId: []string{"log-123"}},
		},
		CodeError: larkcore.CodeError{Code: 300317, Msg: "sequence did not increase"},
	}}
	sender := &ChannelSender{elementAPI: api}

	err := sender.UpdateContent(context.Background(), CardContentUpdate{
		CardID:    "7355372766134157313",
		ElementID: "agent_answer",
		Content:   "snapshot",
		UUID:      "content-operation-1",
		Sequence:  7,
	})
	var apiErr *DynamicCardAPIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("error = %v, want DynamicCardAPIError", err)
	}
	if apiErr.Code != 300317 || apiErr.HTTPStatus != http.StatusBadRequest || apiErr.RequestID != "log-123" {
		t.Fatalf("unexpected API error: %#v", apiErr)
	}
}

func TestDynamicCardsPreservesTransportError(t *testing.T) {
	want := errors.New("connection reset")
	api := &fakeCardKitCardAPI{createErr: want}
	sender := &ChannelSender{cardAPI: api}

	_, err := sender.CreateCard(context.Background(), `{"schema":"2.0"}`)
	if !errors.Is(err, want) {
		t.Fatalf("error = %v, want wrapped transport error", err)
	}
}

func TestDynamicCardsRejectsMissingSuccessIdentifiers(t *testing.T) {
	sender := &ChannelSender{messageAPI: &fakeCardKitMessageAPI{replyResp: &larkim.ReplyMessageResp{}}}
	_, err := sender.SendCard(context.Background(), CardSendRequest{
		ReplyToMessageID: "om_original",
		CardID:           "7355372766134157313",
		UUID:             "38cc728b-9ad4-49f8-8b52-82445c054815",
	})
	if err == nil {
		t.Fatal("expected missing message id error")
	}
}

func TestDynamicCardsSDKWireRequests(t *testing.T) {
	httpClient := &cardKitRecordingHTTPClient{}
	client := lark.NewClient(
		"cli_test",
		"secret",
		lark.WithHttpClient(httpClient),
	)
	sender := &ChannelSender{
		cardAPI:    client.Cardkit.V1.Card,
		elementAPI: client.Cardkit.V1.CardElement,
		messageAPI: client.Im.V1.Message,
	}
	ctx := context.Background()
	cardJSON := `{"schema":"2.0","config":{"streaming_mode":true,"update_multi":true},"body":{"elements":[{"tag":"markdown","element_id":"agent_answer","content":""}]}}`

	cardID, err := sender.CreateCard(ctx, cardJSON)
	if err != nil {
		t.Fatalf("create card: %v", err)
	}
	if _, err := sender.SendCard(ctx, CardSendRequest{
		ChatID: "oc_test",
		CardID: cardID,
		UUID:   "9b6c0104-a741-4ff0-a6c1-88705c7281f0",
	}); err != nil {
		t.Fatalf("send card: %v", err)
	}
	if _, err := sender.SendCard(ctx, CardSendRequest{
		ReplyToMessageID: "om_original",
		CardID:           cardID,
		UUID:             "1eb55ffc-ec2c-4cb2-8daf-6e7b15d3eb6c",
	}); err != nil {
		t.Fatalf("reply with card: %v", err)
	}
	if err := sender.UpdateContent(ctx, CardContentUpdate{
		CardID:    cardID,
		ElementID: "agent_answer",
		Content:   "Accumulated answer",
		UUID:      "content-operation",
		Sequence:  1,
	}); err != nil {
		t.Fatalf("update content: %v", err)
	}
	if err := sender.UpdateSettings(ctx, CardSettingsUpdate{
		CardID:       cardID,
		SettingsJSON: `{"config":{"streaming_mode":false}}`,
		UUID:         "settings-operation",
		Sequence:     2,
	}); err != nil {
		t.Fatalf("update settings: %v", err)
	}
	if err := sender.BatchUpdate(ctx, CardBatchUpdate{
		CardID:      cardID,
		ActionsJSON: `[{"action":"partial_update_setting","params":{"settings":{"config":{"streaming_mode":false}}}}]`,
		UUID:        "batch-operation",
		Sequence:    3,
	}); err != nil {
		t.Fatalf("batch update: %v", err)
	}

	requests := nonTokenRequests(httpClient.requests)
	if len(requests) != 6 {
		t.Fatalf("API request count = %d, want 6; requests = %#v", len(requests), requests)
	}
	assertWireRequest(t, requests[0], http.MethodPost, "/open-apis/cardkit/v1/cards", "", map[string]any{
		"type": "card_json",
		"data": cardJSON,
	})
	assertWireRequest(t, requests[1], http.MethodPost, "/open-apis/im/v1/messages", "receive_id_type=chat_id", map[string]any{
		"receive_id": "oc_test",
		"msg_type":   "interactive",
		"content":    `{"type":"card","data":{"card_id":"7355372766134157313"}}`,
		"uuid":       "9b6c0104-a741-4ff0-a6c1-88705c7281f0",
	})
	assertWireRequest(t, requests[2], http.MethodPost, "/open-apis/im/v1/messages/om_original/reply", "", map[string]any{
		"msg_type": "interactive",
		"content":  `{"type":"card","data":{"card_id":"7355372766134157313"}}`,
		"uuid":     "1eb55ffc-ec2c-4cb2-8daf-6e7b15d3eb6c",
	})
	assertWireRequest(t, requests[3], http.MethodPut, "/open-apis/cardkit/v1/cards/7355372766134157313/elements/agent_answer/content", "", map[string]any{
		"uuid":     "content-operation",
		"content":  "Accumulated answer",
		"sequence": float64(1),
	})
	assertWireRequest(t, requests[4], http.MethodPatch, "/open-apis/cardkit/v1/cards/7355372766134157313/settings", "", map[string]any{
		"settings": `{"config":{"streaming_mode":false}}`,
		"uuid":     "settings-operation",
		"sequence": float64(2),
	})
	assertWireRequest(t, requests[5], http.MethodPost, "/open-apis/cardkit/v1/cards/7355372766134157313/batch_update", "", map[string]any{
		"uuid":     "batch-operation",
		"sequence": float64(3),
		"actions":  `[{"action":"partial_update_setting","params":{"settings":{"config":{"streaming_mode":false}}}}]`,
	})
}

func nonTokenRequests(requests []recordedHTTPRequest) []recordedHTTPRequest {
	filtered := make([]recordedHTTPRequest, 0, len(requests))
	for _, req := range requests {
		if req.Path == "/open-apis/auth/v3/tenant_access_token/internal" {
			continue
		}
		filtered = append(filtered, req)
	}
	return filtered
}

func assertWireRequest(t *testing.T, got recordedHTTPRequest, method, path, rawQuery string, wantBody map[string]any) {
	t.Helper()
	if got.Method != method || got.Path != path || got.RawQuery != rawQuery {
		t.Fatalf("request = %s %s?%s, want %s %s?%s", got.Method, got.Path, got.RawQuery, method, path, rawQuery)
	}
	var body map[string]any
	if err := json.Unmarshal(got.Body, &body); err != nil {
		t.Fatalf("decode request body %q: %v", got.Body, err)
	}
	if len(body) != len(wantBody) {
		t.Fatalf("request body = %#v, want %#v", body, wantBody)
	}
	for key, want := range wantBody {
		if gotValue, ok := body[key]; !ok || gotValue != want {
			t.Fatalf("request body[%q] = %#v, want %#v; body = %#v", key, gotValue, want, body)
		}
	}
}
