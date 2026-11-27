package feishu

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"testing"

	lark "github.com/larksuite/oapi-sdk-go/v3"
	larkcore "github.com/larksuite/oapi-sdk-go/v3/core"
	larkim "github.com/larksuite/oapi-sdk-go/v3/service/im/v1"
)

// keyedGetMessageAPI answers Get by exact message id, like Feishu does, and
// fails any List: a DM reply parent must never walk chat history.
type keyedGetMessageAPI struct {
	messages  map[string]*larkim.Message
	denied    map[string]bool
	gets      []string
	cardTypes []string
	lists     int
}

func (f *keyedGetMessageAPI) List(context.Context, *larkim.ListMessageReq, ...larkcore.RequestOptionFunc) (*larkim.ListMessageResp, error) {
	f.lists++
	return nil, errors.New("list must not be called on the trigger-only path")
}

func (f *keyedGetMessageAPI) Get(_ context.Context, req *larkim.GetMessageReq, _ ...larkcore.RequestOptionFunc) (*larkim.GetMessageResp, error) {
	id, cardType := getMessageReqParams(req)
	f.gets = append(f.gets, id)
	f.cardTypes = append(f.cardTypes, cardType)
	if f.denied[id] {
		// Feishu's shape for a message the bot may not read.
		return &larkim.GetMessageResp{CodeError: larkcore.CodeError{Code: 230027, Msg: "no permission"}}, nil
	}
	message, ok := f.messages[id]
	if !ok {
		return &larkim.GetMessageResp{CodeError: larkcore.CodeError{Code: 230011, Msg: "message not found"}}, nil
	}
	return &larkim.GetMessageResp{Data: &larkim.GetMessageRespData{Items: []*larkim.Message{message}}}, nil
}

func dmMessage(id, chatID, messageType, content, createTime, senderID, senderType string) *larkim.Message {
	message := threadMessage(id, messageType, content, createTime, senderID, senderType)
	message.ChatId = ptr(chatID)
	return message
}

const (
	dmChat       = "oc_dm_requester"
	reviewNotice = `{"text":"评审通知：PR #42 需要你审核，风险项：迁移脚本"}`
)

func dmReplyFixture() *keyedGetMessageAPI {
	return &keyedGetMessageAPI{messages: map[string]*larkim.Message{
		"om_notice": dmMessage("om_notice", dmChat, "text", reviewNotice, "1000", "cli_bot", "app"),
		"om_reply":  dmMessage("om_reply", dmChat, "text", `{"text":"这个风险具体指什么？"}`, "2000", "ou_requester", "user"),
	}}
}

func dmReplyRequest() AttachedContextRequest {
	return AttachedContextRequest{
		TriggerMessageID: "om_reply", TriggerCreateTime: "2000",
		ChatID: dmChat, ReplyParentMessageID: "om_notice",
	}
}

func TestAttachedContextCarriesSameChatDirectReplyParent(t *testing.T) {
	history := dmReplyFixture()
	got, err := newSDKAttachedContextLookup(history, nil).LookupAttachedContext(context.Background(), dmReplyRequest())
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if got.Status != AttachedContextFound || len(got.Messages) != 1 {
		t.Fatalf("result = %#v, want the one quoted notice", got)
	}
	if msg := got.Messages[0]; msg.AuthorType != "app" || !strings.Contains(msg.Text, "评审通知：PR #42") {
		t.Fatalf("parent message = %#v", msg)
	}
	if strings.Contains(got.Messages[0].Text, "这个风险具体指什么") {
		t.Fatal("trigger guide text leaked into the snapshot")
	}
	if len(history.gets) != 2 || history.gets[0] != "om_reply" || history.gets[1] != "om_notice" || history.lists != 0 {
		t.Fatalf("reads = gets %v lists %d, want exactly trigger then parent", history.gets, history.lists)
	}
	for _, cardType := range history.cardTypes {
		if cardType != attachedCardContentType {
			t.Fatalf("get card content type = %q, want raw card content so card notices read as text", cardType)
		}
	}
	for _, issue := range got.Issues {
		t.Fatalf("unexpected issue %v", issue)
	}
}

func TestAttachedContextOmitsUnprovenDirectReplyParent(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*keyedGetMessageAPI, *AttachedContextRequest)
		issue  bool
	}{
		{"cross chat", func(h *keyedGetMessageAPI, _ *AttachedContextRequest) {
			h.messages["om_notice"].ChatId = ptr("oc_someone_else")
		}, false},
		{"trigger fetched in another chat", func(h *keyedGetMessageAPI, _ *AttachedContextRequest) {
			h.messages["om_reply"].ChatId = ptr("oc_someone_else")
		}, false},
		{"delivery chat mismatch", func(_ *keyedGetMessageAPI, in *AttachedContextRequest) {
			in.ChatID = "oc_someone_else"
		}, false},
		{"delivery chat unknown", func(_ *keyedGetMessageAPI, in *AttachedContextRequest) {
			in.ChatID = ""
		}, false},
		{"parent chat unknown", func(h *keyedGetMessageAPI, _ *AttachedContextRequest) {
			h.messages["om_notice"].ChatId = nil
		}, false},
		{"recalled parent", func(h *keyedGetMessageAPI, _ *AttachedContextRequest) {
			h.messages["om_notice"].Deleted = ptrBool(true)
		}, false},
		{"parent is the trigger", func(_ *keyedGetMessageAPI, in *AttachedContextRequest) {
			in.ReplyParentMessageID = "om_reply"
		}, false},
		{"parent newer than trigger", func(h *keyedGetMessageAPI, _ *AttachedContextRequest) {
			h.messages["om_notice"].CreateTime = ptr("3000")
		}, false},
		{"feishu returns a different message", func(h *keyedGetMessageAPI, _ *AttachedContextRequest) {
			h.messages["om_notice"] = dmMessage("om_other", dmChat, "text", reviewNotice, "1000", "cli_bot", "app")
		}, false},
		{"all chat ids unknown", func(h *keyedGetMessageAPI, in *AttachedContextRequest) {
			in.ChatID = ""
			h.messages["om_notice"].ChatId = nil
			h.messages["om_reply"].ChatId = nil
		}, false},
		{"parent create time unknown", func(h *keyedGetMessageAPI, _ *AttachedContextRequest) {
			h.messages["om_notice"].CreateTime = nil
		}, false},
		{"trigger create time malformed", func(h *keyedGetMessageAPI, _ *AttachedContextRequest) {
			h.messages["om_reply"].CreateTime = ptr("soon")
		}, false},
		{"trigger read returns a different message", func(h *keyedGetMessageAPI, _ *AttachedContextRequest) {
			h.messages["om_reply"] = dmMessage("om_zzz", dmChat, "text", `{"text":"x"}`, "2000", "ou_requester", "user")
		}, false},
		{"bot may not read parent", func(h *keyedGetMessageAPI, _ *AttachedContextRequest) {
			h.denied = map[string]bool{"om_notice": true}
		}, true},
		{"parent missing", func(h *keyedGetMessageAPI, _ *AttachedContextRequest) {
			delete(h.messages, "om_notice")
		}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			history := dmReplyFixture()
			request := dmReplyRequest()
			tt.mutate(history, &request)
			got, err := newSDKAttachedContextLookup(history, nil).LookupAttachedContext(context.Background(), request)
			if err != nil {
				t.Fatalf("lookup: %v", err)
			}
			if len(got.Messages) != 0 || got.Status == AttachedContextFound {
				t.Fatalf("result = %#v, want parent omitted", got)
			}
			if got := hasAttachedContextIssue(got.Issues, AttachedContextIssueHistoryUnreadable); got != tt.issue {
				t.Fatalf("history_unreadable issue = %v, want %v", got, tt.issue)
			}
			if history.lists != 0 {
				t.Fatalf("trigger-only path listed chat history %d times", history.lists)
			}
		})
	}
}

// multiItemGetMessageAPI returns several items for the parent read, as a
// merge_forward does; only the exact parent may be chosen.
type multiItemGetMessageAPI struct {
	*keyedGetMessageAPI
	extra []*larkim.Message
}

func (f *multiItemGetMessageAPI) Get(ctx context.Context, req *larkim.GetMessageReq, opts ...larkcore.RequestOptionFunc) (*larkim.GetMessageResp, error) {
	resp, err := f.keyedGetMessageAPI.Get(ctx, req, opts...)
	if id, _ := getMessageReqParams(req); id == "om_notice" && resp != nil && resp.Data != nil {
		resp.Data.Items = append(append([]*larkim.Message{}, f.extra...), resp.Data.Items...)
	}
	return resp, err
}

func TestAttachedContextPicksOnlyExactParentFromMultiItemRead(t *testing.T) {
	foreign := dmMessage("om_forwarded", "oc_other_chat", "text", `{"text":"SECRET from another chat"}`, "500", "ou_x", "user")
	history := &multiItemGetMessageAPI{keyedGetMessageAPI: dmReplyFixture(), extra: []*larkim.Message{foreign}}
	got, err := newSDKAttachedContextLookup(history, nil).LookupAttachedContext(context.Background(), dmReplyRequest())
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if len(got.Messages) != 1 || strings.Contains(got.Messages[0].Text, "SECRET") || !strings.Contains(got.Messages[0].Text, "评审通知") {
		t.Fatalf("messages = %#v, want only the exact parent", got.Messages)
	}

	got, err = newSDKAttachedContextLookup(&multiItemGetMessageAPI{keyedGetMessageAPI: &keyedGetMessageAPI{messages: map[string]*larkim.Message{
		"om_reply":  dmReplyFixture().messages["om_reply"],
		"om_notice": foreign,
	}}}, nil).LookupAttachedContext(context.Background(), dmReplyRequest())
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if len(got.Messages) != 0 {
		t.Fatalf("messages = %#v, want parent omitted when no item matches", got.Messages)
	}
}

func TestAttachedContextWithoutReplyParentReadsOnlyTrigger(t *testing.T) {
	history := dmReplyFixture()
	request := dmReplyRequest()
	request.ReplyParentMessageID = ""
	got, err := newSDKAttachedContextLookup(history, nil).LookupAttachedContext(context.Background(), request)
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if got.Status != AttachedContextMissing || len(got.Messages) != 0 || len(got.Issues) != 0 {
		t.Fatalf("result = %#v, want trigger-only missing", got)
	}
	if len(history.gets) != 1 || history.gets[0] != "om_reply" {
		t.Fatalf("gets = %v, want trigger only", history.gets)
	}
}

func TestAttachedContextThreadPathIgnoresReplyParent(t *testing.T) {
	history := &fakeThreadMessageAPI{responses: []*larkim.ListMessageResp{listMessageResponse(false, "",
		threadMessage("om_reply", "text", `{"text":"guide"}`, "2000", "ou_requester", "user"),
		threadMessage("om_prior", "text", `{"text":"topic prior"}`, "1000", "ou_other", "user"),
	)}}
	request := dmReplyRequest()
	request.ThreadID = "omt_topic"
	got, err := newSDKAttachedContextLookup(history, nil).LookupAttachedContext(context.Background(), request)
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if got.Status != AttachedContextFound || len(got.Messages) != 1 || got.Messages[0].Text != "topic prior" {
		t.Fatalf("thread snapshot = %#v", got)
	}
}

func ptrBool(value bool) *bool { return &value }

// getMessageReqParams reads the SDK request's unexported path and query
// parameters; the wire-contract test below covers the real encoding.
func getMessageReqParams(req *larkim.GetMessageReq) (messageID, cardType string) {
	apiReq := reflect.ValueOf(req).Elem().FieldByName("apiReq").Elem()
	if id := apiReq.FieldByName("PathParams").MapIndex(reflect.ValueOf("message_id")); id.IsValid() {
		messageID = id.String()
	}
	if values := apiReq.FieldByName("QueryParams").MapIndex(reflect.ValueOf("card_msg_content_type")); values.IsValid() && values.Len() > 0 {
		cardType = values.Index(0).String()
	}
	return messageID, cardType
}

type dmGetHTTPClient struct{ requests []recordedHTTPRequest }

func (c *dmGetHTTPClient) Do(req *http.Request) (*http.Response, error) {
	c.requests = append(c.requests, recordedHTTPRequest{Method: req.Method, Path: req.URL.Path, RawQuery: req.URL.RawQuery})
	body := `{"code":0,"msg":"success"}`
	item := func(id, sender, senderType, createTime, text string) string {
		return `{"code":0,"msg":"success","data":{"items":[{"message_id":"` + id + `","chat_id":"` + dmChat +
			`","msg_type":"text","create_time":"` + createTime + `","deleted":false,"sender":{"id":"` + sender +
			`","sender_type":"` + senderType + `"},"body":{"content":` + strconv.Quote(text) + `}}]}}`
	}
	switch req.URL.Path {
	case "/open-apis/auth/v3/tenant_access_token/internal":
		body = `{"code":0,"msg":"success","tenant_access_token":"t-test","expire":7200}`
	case "/open-apis/im/v1/messages/om_reply":
		body = item("om_reply", "ou_requester", "user", "2000", `{"text":"这个风险具体指什么？"}`)
	case "/open-apis/im/v1/messages/om_notice":
		body = item("om_notice", "cli_bot", "app", "1000", reviewNotice)
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(body)),
		Request:    req,
	}, nil
}

func TestDirectReplyParentUsesExactGetWireContract(t *testing.T) {
	httpClient := &dmGetHTTPClient{}
	client := lark.NewClient("cli_test", "test-placeholder", lark.WithHttpClient(httpClient))
	lookup := newSDKAttachedContextLookup(client.Im.V1.Message, client.Im.V1.MessageResource)
	got, err := lookup.LookupAttachedContext(context.Background(), dmReplyRequest())
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if got.Status != AttachedContextFound || len(got.Messages) != 1 || !strings.Contains(got.Messages[0].Text, "评审通知") {
		t.Fatalf("result = %#v", got)
	}
	requests := nonTokenRequests(httpClient.requests)
	wantPaths := []string{"/open-apis/im/v1/messages/om_reply", "/open-apis/im/v1/messages/om_notice"}
	if len(requests) != len(wantPaths) {
		t.Fatalf("requests = %#v, want exactly trigger and parent reads", requests)
	}
	for i, request := range requests {
		query, _ := url.ParseQuery(request.RawQuery)
		if request.Method != "GET" || request.Path != wantPaths[i] || query.Get("card_msg_content_type") != attachedCardContentType {
			t.Fatalf("request %d = %#v", i, request)
		}
	}
}
