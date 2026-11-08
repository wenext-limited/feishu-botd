package feishu

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"testing"

	lark "github.com/larksuite/oapi-sdk-go/v3"
)

// agentCardHTTPClient records every request and answers each path with a
// canned body; unknown paths succeed with an empty payload.
type agentCardHTTPClient struct {
	requests  []recordedHTTPRequest
	responses map[string]string
}

func (c *agentCardHTTPClient) Do(req *http.Request) (*http.Response, error) {
	body, err := io.ReadAll(req.Body)
	if err != nil {
		return nil, err
	}
	c.requests = append(c.requests, recordedHTTPRequest{Method: req.Method, Path: req.URL.Path, RawQuery: req.URL.RawQuery, Body: body})
	responseBody := `{"code":0,"msg":"success"}`
	if req.URL.Path == "/open-apis/auth/v3/tenant_access_token/internal" {
		responseBody = `{"code":0,"msg":"success","tenant_access_token":"t-test","expire":7200}`
	} else if canned, ok := c.responses[req.URL.Path]; ok {
		responseBody = canned
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(bytes.NewBufferString(responseBody)),
		Request:    req,
	}, nil
}

func agentCardSender(httpClient *agentCardHTTPClient) *ChannelSender {
	client := lark.NewClient("cli_test", "secret", lark.WithHttpClient(httpClient))
	return &ChannelSender{
		cardAPI:        client.Cardkit.V1.Card,
		cardReplaceAPI: client.Cardkit.V1.Card,
		messageAPI:     client.Im.V1.Message,
		pinAPI:         client.Im.V1.Pin,
	}
}

func TestAgentCardsSDKWireRequests(t *testing.T) {
	httpClient := &agentCardHTTPClient{responses: map[string]string{
		"/open-apis/im/v1/messages":                  `{"code":0,"msg":"success","data":{"message_id":"om_root"}}`,
		"/open-apis/im/v1/messages/om_question/reply": `{"code":0,"msg":"success","data":{"message_id":"om_threaded","thread_id":"omt_task"}}`,
	}}
	sender := agentCardSender(httpClient)
	ctx := context.Background()
	cardJSON := `{"schema":"2.0","config":{"update_multi":true},"body":{"elements":[]}}`

	root, err := sender.PostCard(ctx, CardPostRequest{ChatID: "oc_group", CardID: "card_1", UUID: "uuid-root"})
	if err != nil || len(root.MessageIDs) != 1 || root.MessageIDs[0] != "om_root" || root.ThreadID != "" {
		t.Fatalf("root post = %#v err=%v", root, err)
	}
	threaded, err := sender.PostCard(ctx, CardPostRequest{ReplyToMessageID: "om_question", ReplyInThread: true, CardID: "card_2", UUID: "uuid-reply"})
	if err != nil || threaded.MessageIDs[0] != "om_threaded" || threaded.ThreadID != "omt_task" {
		t.Fatalf("threaded post = %#v err=%v", threaded, err)
	}
	if err := sender.ReplaceCard(ctx, CardReplaceRequest{CardID: "card_1", CardJSON: cardJSON, UUID: "uuid-replace", Sequence: 4}); err != nil {
		t.Fatalf("replace card: %v", err)
	}
	if err := sender.PinMessage(ctx, "om_root"); err != nil {
		t.Fatalf("pin: %v", err)
	}

	requests := nonTokenRequests(httpClient.requests)
	if len(requests) != 4 {
		t.Fatalf("API request count = %d, want 4; requests = %#v", len(requests), requests)
	}
	assertWireRequest(t, requests[0], http.MethodPost, "/open-apis/im/v1/messages", "receive_id_type=chat_id", map[string]any{
		"receive_id": "oc_group",
		"msg_type":   "interactive",
		"content":    `{"type":"card","data":{"card_id":"card_1"}}`,
		"uuid":       "uuid-root",
	})
	assertWireRequest(t, requests[1], http.MethodPost, "/open-apis/im/v1/messages/om_question/reply", "", map[string]any{
		"msg_type":        "interactive",
		"content":         `{"type":"card","data":{"card_id":"card_2"}}`,
		"uuid":            "uuid-reply",
		"reply_in_thread": true,
	})
	if got := requests[2]; got.Method != http.MethodPut || got.Path != "/open-apis/cardkit/v1/cards/card_1" ||
		!bytes.Contains(got.Body, []byte(`"sequence":4`)) || !bytes.Contains(got.Body, []byte(`"uuid":"uuid-replace"`)) ||
		!bytes.Contains(got.Body, []byte(`"type":"card_json"`)) {
		t.Fatalf("replace request = %s %s %s", got.Method, got.Path, got.Body)
	}
	assertWireRequest(t, requests[3], http.MethodPost, "/open-apis/im/v1/pins", "", map[string]any{
		"message_id": "om_root",
	})
}

func TestAgentCardsReportFeishuRejections(t *testing.T) {
	httpClient := &agentCardHTTPClient{responses: map[string]string{
		"/open-apis/cardkit/v1/cards/card_old": `{"code":200750,"msg":"card expired"}`,
		"/open-apis/im/v1/pins":                `{"code":230027,"msg":"Lack of necessary permissions"}`,
	}}
	sender := agentCardSender(httpClient)
	ctx := context.Background()

	err := sender.ReplaceCard(ctx, CardReplaceRequest{CardID: "card_old", CardJSON: `{"schema":"2.0"}`, UUID: "u", Sequence: 1})
	var cardErr *DynamicCardAPIError
	if !errors.As(err, &cardErr) || cardErr.Code != 200750 {
		t.Fatalf("replace error = %v, want DynamicCardAPIError 200750", err)
	}
	err = sender.PinMessage(ctx, "om_x")
	var pinErr *MessageSendError
	if !errors.As(err, &pinErr) || pinErr.Class != "api_rejected" || pinErr.Code != 230027 || pinErr.Retryable {
		t.Fatalf("pin error = %#v, want a definitive 230027 rejection", err)
	}
	if err := sender.ReplaceCard(ctx, CardReplaceRequest{CardID: "card_1", CardJSON: `{"schema":"2.0"}`, UUID: "u"}); err == nil {
		t.Fatal("a replace without a positive sequence was sent")
	}
	if _, err := sender.PostCard(ctx, CardPostRequest{ChatID: "oc", ReplyToMessageID: "om", CardID: "c", UUID: "u"}); err == nil {
		t.Fatal("a post with two destinations was sent")
	}
}
