package feishu

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"reflect"
	"sync"
	"testing"
	"time"

	lark "github.com/larksuite/oapi-sdk-go/v3"
)

type scriptedResponse struct {
	status int
	body   string
}

// scriptedHTTPClient answers each "METHOD path" with its scripted responses in
// order, repeating the last one.
type scriptedHTTPClient struct {
	mu        sync.Mutex
	requests  []recordedHTTPRequest
	responses map[string][]scriptedResponse
}

func (c *scriptedHTTPClient) Do(req *http.Request) (*http.Response, error) {
	body, err := io.ReadAll(req.Body)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	response := scriptedResponse{status: http.StatusOK, body: `{"code":0,"msg":"success","data":{}}`}
	if req.URL.Path == "/open-apis/auth/v3/tenant_access_token/internal" {
		response.body = `{"code":0,"msg":"success","tenant_access_token":"t-test","expire":7200}`
	} else {
		c.requests = append(c.requests, recordedHTTPRequest{Method: req.Method, Path: req.URL.Path, RawQuery: req.URL.RawQuery, Body: body})
		key := req.Method + " " + req.URL.Path
		if script := c.responses[key]; len(script) > 0 {
			response = script[0]
			if len(script) > 1 {
				c.responses[key] = script[1:]
			}
		}
	}
	return &http.Response{
		StatusCode: response.status,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(bytes.NewBufferString(response.body)),
		Request:    req,
	}, nil
}

func announcementSender(httpClient *scriptedHTTPClient) *ChannelSender {
	client := lark.NewClient("cli_test", "secret", lark.WithHttpClient(httpClient))
	return &ChannelSender{
		announcementAPI:         client.Docx.V1.ChatAnnouncement,
		announcementBlockAPI:    client.Docx.V1.ChatAnnouncementBlock,
		announcementChildrenAPI: client.Docx.V1.ChatAnnouncementBlockChildren,
		announcementRetryBase:   time.Millisecond,
	}
}

const announcementBlocksPath = "/open-apis/docx/v1/chats/oc_chat/announcement/blocks"

func TestChatAnnouncementSDKWireRequests(t *testing.T) {
	httpClient := &scriptedHTTPClient{responses: map[string][]scriptedResponse{
		"GET /open-apis/docx/v1/chats/oc_chat/announcement": {{200, `{"code":0,"data":{"announcement_type":"docx","revision_id":7}}`}},
		"GET " + announcementBlocksPath: {
			{200, `{"code":0,"data":{"has_more":true,"page_token":"p2","items":[
				{"block_id":"oc_chat","block_type":1,"children":["h1","t1","b1"]},
				{"block_id":"h1","block_type":5,"parent_id":"oc_chat","heading3":{"elements":[{"text_run":{"content":"Status"}}]}}]}}`},
			{200, `{"code":0,"data":{"has_more":false,"items":[
				{"block_id":"t1","block_type":2,"text":{"elements":[{"text_run":{"content":"see ","text_element_style":{"bold":true}}},{"text_run":{"content":"x","text_element_style":{"link":{"url":"https%3A%2F%2Fexample.com%2Fa"}}}}]}},
				{"block_id":"b1","block_type":12,"bullet":{"elements":[{"text_run":{"content":"one"}}]}},
				{"block_id":"nested","block_type":2,"parent_id":"b1","text":{"elements":[]}}]}}`},
		},
		"POST " + announcementBlocksPath + "/oc_chat/children": {{200, `{"code":0,"data":{"children":[{"block_id":"new1"},{"block_id":"new2"}]}}`}},
	}}
	sender := announcementSender(httpClient)
	ctx := context.Background()

	info, err := sender.GetChatAnnouncement(ctx, "oc_chat")
	if err != nil || info != (AnnouncementInfo{Type: "docx", RevisionID: 7}) {
		t.Fatalf("get = %#v err=%v", info, err)
	}
	blocks, err := sender.ListChatAnnouncementBlocks(ctx, "oc_chat")
	if err != nil {
		t.Fatal(err)
	}
	want := []AnnouncementBlock{
		{ID: "h1", Type: AnnouncementBlockHeading3, Runs: []AnnouncementTextRun{{Content: "Status"}}},
		{ID: "t1", Type: AnnouncementBlockText, Runs: []AnnouncementTextRun{{Content: "see ", Bold: true}, {Content: "x", LinkURL: "https://example.com/a"}}},
		{ID: "b1", Type: AnnouncementBlockBullet, Runs: []AnnouncementTextRun{{Content: "one"}}},
	}
	if !reflect.DeepEqual(blocks, want) {
		t.Fatalf("blocks = %#v", blocks)
	}
	ids, err := sender.CreateChatAnnouncementBlocks(ctx, "oc_chat", 1, []AnnouncementBlock{
		{Type: AnnouncementBlockHeading3, Runs: []AnnouncementTextRun{{Content: "Agent"}}},
		{Type: AnnouncementBlockText, Runs: []AnnouncementTextRun{{Content: "c", Code: true}, {Content: "l", LinkURL: "https://e.com/?q=a b"}}},
	}, "token-create")
	if err != nil || !reflect.DeepEqual(ids, []string{"new1", "new2"}) {
		t.Fatalf("create = %v err=%v", ids, err)
	}
	if err := sender.DeleteChatAnnouncementBlocks(ctx, "oc_chat", 2, 4, "token-delete"); err != nil {
		t.Fatal(err)
	}

	requests := httpClient.requests
	if len(requests) != 5 {
		t.Fatalf("requests = %d: %#v", len(requests), requests)
	}
	if q, _ := url.ParseQuery(requests[1].RawQuery); q.Get("page_size") != "500" || q.Get("page_token") != "" {
		t.Fatalf("first list query = %s", requests[1].RawQuery)
	}
	if q, _ := url.ParseQuery(requests[2].RawQuery); q.Get("page_token") != "p2" {
		t.Fatalf("second list query = %s", requests[2].RawQuery)
	}
	create := requests[3]
	if create.Method != http.MethodPost || create.RawQuery != "client_token=token-create" {
		t.Fatalf("create request = %s %s?%s", create.Method, create.Path, create.RawQuery)
	}
	var body map[string]any
	if err := json.Unmarshal(create.Body, &body); err != nil {
		t.Fatal(err)
	}
	wantBody := map[string]any{
		"index": float64(1),
		"children": []any{
			map[string]any{"block_type": float64(5), "heading3": map[string]any{"elements": []any{
				map[string]any{"text_run": map[string]any{"content": "Agent"}},
			}}},
			map[string]any{"block_type": float64(2), "text": map[string]any{"elements": []any{
				map[string]any{"text_run": map[string]any{"content": "c", "text_element_style": map[string]any{"inline_code": true}}},
				map[string]any{"text_run": map[string]any{"content": "l", "text_element_style": map[string]any{
					"link": map[string]any{"url": "https%3A%2F%2Fe.com%2F%3Fq%3Da+b"},
				}}},
			}}},
		},
	}
	if !reflect.DeepEqual(body, wantBody) {
		t.Fatalf("create body = %s", create.Body)
	}
	del := requests[4]
	if del.Method != http.MethodDelete || del.Path != announcementBlocksPath+"/oc_chat/children/batch_delete" ||
		del.RawQuery != "client_token=token-delete" || string(del.Body) != `{"start_index":2,"end_index":4}` {
		t.Fatalf("delete request = %s %s?%s %s", del.Method, del.Path, del.RawQuery, del.Body)
	}
}

func TestChatAnnouncementRetriesRateLimitThenSucceeds(t *testing.T) {
	httpClient := &scriptedHTTPClient{responses: map[string][]scriptedResponse{
		"POST " + announcementBlocksPath + "/oc_chat/children": {
			{429, `{"code":99991400,"msg":"request trigger frequency limit"}`},
			{200, `{"code":99991400,"msg":"request trigger frequency limit"}`},
			{200, `{"code":0,"data":{"children":[{"block_id":"new1"}]}}`},
		},
		"DELETE " + announcementBlocksPath + "/oc_chat/children/batch_delete": {
			{429, `{"code":99991400,"msg":"request trigger frequency limit"}`},
			{200, `{"code":0,"data":{}}`},
		},
	}}
	sender := announcementSender(httpClient)
	block := []AnnouncementBlock{{Type: AnnouncementBlockText, Runs: []AnnouncementTextRun{{Content: "x"}}}}
	ids, err := sender.CreateChatAnnouncementBlocks(context.Background(), "oc_chat", -1, block, "tok")
	if err != nil || len(ids) != 1 {
		t.Fatalf("create = %v err=%v", ids, err)
	}
	if err := sender.DeleteChatAnnouncementBlocks(context.Background(), "oc_chat", 0, 1, "tok2"); err != nil {
		t.Fatal(err)
	}
	if len(httpClient.requests) != 5 {
		t.Fatalf("requests = %d", len(httpClient.requests))
	}
	for _, req := range httpClient.requests[:3] {
		if req.RawQuery != "client_token=tok" {
			t.Fatalf("retry changed the client token: %s", req.RawQuery)
		}
	}
}

func TestChatAnnouncementGivesUpOnRateLimitAsRetryable(t *testing.T) {
	httpClient := &scriptedHTTPClient{responses: map[string][]scriptedResponse{
		"GET /open-apis/docx/v1/chats/oc_chat/announcement": {{429, `{"code":99991400,"msg":"limit"}`}},
	}}
	sender := announcementSender(httpClient)
	sender.announcementMaxAttempts = 3
	_, err := sender.GetChatAnnouncement(context.Background(), "oc_chat")
	var sendErr *MessageSendError
	if !errors.As(err, &sendErr) || !sendErr.Retryable || !IsAnnouncementRateLimited(err) {
		t.Fatalf("err = %#v", err)
	}
	if len(httpClient.requests) != 3 {
		t.Fatalf("requests = %d, want 3", len(httpClient.requests))
	}
}

func TestChatAnnouncementDeleteDoesNotRetryAmbiguousFailures(t *testing.T) {
	httpClient := &scriptedHTTPClient{responses: map[string][]scriptedResponse{
		"DELETE " + announcementBlocksPath + "/oc_chat/children/batch_delete": {{500, `{"code":1770000,"msg":"internal"}`}},
	}}
	sender := announcementSender(httpClient)
	err := sender.DeleteChatAnnouncementBlocks(context.Background(), "oc_chat", 0, 1, "")
	var sendErr *MessageSendError
	if !errors.As(err, &sendErr) || !sendErr.Retryable || len(httpClient.requests) != 1 {
		t.Fatalf("err = %#v requests = %d", err, len(httpClient.requests))
	}
}

func TestChatAnnouncementReportsPermissionRejection(t *testing.T) {
	httpClient := &scriptedHTTPClient{responses: map[string][]scriptedResponse{
		"GET /open-apis/docx/v1/chats/oc_chat/announcement": {{400, `{"code":99991672,"msg":"Access denied. One of the following scopes is required"}`}},
	}}
	_, err := announcementSender(httpClient).GetChatAnnouncement(context.Background(), "oc_chat")
	var sendErr *MessageSendError
	if !errors.As(err, &sendErr) || sendErr.Class != "api_rejected" || sendErr.Code != 99991672 || sendErr.Retryable {
		t.Fatalf("err = %#v", err)
	}
	if len(httpClient.requests) != 1 {
		t.Fatalf("a permission rejection was retried")
	}
}

func TestChatAnnouncementListRequiresThePageBlock(t *testing.T) {
	httpClient := &scriptedHTTPClient{responses: map[string][]scriptedResponse{
		"GET " + announcementBlocksPath: {{200, `{"code":0,"data":{"items":[{"block_id":"t1","block_type":2}]}}`}},
	}}
	_, err := announcementSender(httpClient).ListChatAnnouncementBlocks(context.Background(), "oc_chat")
	var sendErr *MessageSendError
	if !errors.As(err, &sendErr) || sendErr.Class != "invalid_response" {
		t.Fatalf("err = %#v", err)
	}
}
