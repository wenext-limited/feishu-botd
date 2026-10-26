package feishu

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	lark "github.com/larksuite/oapi-sdk-go/v3"
	larkcore "github.com/larksuite/oapi-sdk-go/v3/core"
	larkim "github.com/larksuite/oapi-sdk-go/v3/service/im/v1"

	"feishu-botd/internal/notify"
)

func TestPostMessageCreatesRootAndReportsItsThread(t *testing.T) {
	messageID, threadID := "om_root_post", "omt_topic"
	api := &fakeCardKitMessageAPI{createResp: &larkim.CreateMessageResp{
		Data: &larkim.CreateMessageRespData{MessageId: &messageID, ThreadId: &threadID},
	}}
	sender := &ChannelSender{messageAPI: api}

	posted, err := sender.PostMessage(context.Background(), PostMessageRequest{
		ChatID:  "oc_refund",
		Content: notify.Request{Title: "Mirror", Markdown: "Q: why does refund retry?", Source: "agent_post", DedupeKey: "op"},
	})
	if err != nil {
		t.Fatalf("post root: %v", err)
	}
	if len(posted.MessageIDs) != 1 || posted.MessageIDs[0] != messageID || posted.ThreadID != threadID {
		t.Fatalf("posted = %#v", posted)
	}
	if api.replyReq != nil || api.createReq == nil {
		t.Fatal("root post did not use the create API")
	}
	if got := deref(api.createReq.Body.ReceiveId); got != "oc_refund" {
		t.Fatalf("receive id = %q", got)
	}
	if !strings.HasPrefix(deref(api.createReq.Body.Uuid), "notify_") {
		t.Fatalf("root post carried no idempotency uuid: %#v", api.createReq.Body)
	}
	assertOrdinaryMessageBody(t, api.createReq.Body.MsgType, api.createReq.Body.Content, larkim.MsgTypePost, "Mirror", "Q: why does refund retry?")
}

func TestPostMessageRepliesInThreadWhenAsked(t *testing.T) {
	messageID, threadID := "om_thread_reply", "omt_opened"
	api := &fakeCardKitMessageAPI{replyResp: &larkim.ReplyMessageResp{
		Data: &larkim.ReplyMessageRespData{MessageId: &messageID, ThreadId: &threadID},
	}}
	sender := &ChannelSender{messageAPI: api}

	posted, err := sender.PostMessage(context.Background(), PostMessageRequest{
		ChatID:           "oc_ignored_for_reply",
		ReplyToMessageID: "om_root_post",
		ReplyInThread:    true,
		Content:          notify.Request{Markdown: "A: idempotency key expired", MentionUserID: "ou_asker"},
	})
	if err != nil {
		t.Fatalf("post reply: %v", err)
	}
	if posted.MessageIDs[0] != messageID || posted.ThreadID != threadID {
		t.Fatalf("posted = %#v", posted)
	}
	if api.createReq != nil || api.replyReq == nil {
		t.Fatal("reply did not use the reply API")
	}
	if body := api.replyReq.Body; body.ReplyInThread == nil || !*body.ReplyInThread {
		t.Fatalf("reply_in_thread was not requested: %#v", body)
	}
	if !strings.Contains(deref(api.replyReq.Body.Content), `"user_id":"ou_asker"`) {
		t.Fatalf("native mention missing from reply: %s", deref(api.replyReq.Body.Content))
	}
}

func TestPostMessageOmitsReplyInThreadUnlessAsked(t *testing.T) {
	messageID := "om_flat_reply"
	api := &fakeCardKitMessageAPI{replyResp: &larkim.ReplyMessageResp{
		Data: &larkim.ReplyMessageRespData{MessageId: &messageID},
	}}
	sender := &ChannelSender{messageAPI: api}

	posted, err := sender.PostMessage(context.Background(), PostMessageRequest{
		ReplyToMessageID: "om_question",
		Content:          notify.Request{Markdown: "flat answer"},
	})
	if err != nil {
		t.Fatalf("post flat reply: %v", err)
	}
	if api.replyReq.Body.ReplyInThread != nil || posted.ThreadID != "" {
		t.Fatalf("flat reply = body %#v posted %#v", api.replyReq.Body, posted)
	}
}

func TestPostMessageRejectsMissingDestinationWithoutCallingFeishu(t *testing.T) {
	api := &fakeCardKitMessageAPI{}
	sender := &ChannelSender{messageAPI: api}

	_, err := sender.PostMessage(context.Background(), PostMessageRequest{Content: notify.Request{Markdown: "x"}})
	var sendErr *MessageSendError
	if !errors.As(err, &sendErr) || sendErr.Class != "invalid_destination" {
		t.Fatalf("error = %v", err)
	}
	if api.createReq != nil || api.replyReq != nil {
		t.Fatal("an undirected post reached Feishu")
	}
}

func TestPostMessageReportsRejectionAsTypedSendError(t *testing.T) {
	api := &fakeCardKitMessageAPI{replyResp: &larkim.ReplyMessageResp{
		ApiResp:   &larkcore.ApiResp{StatusCode: 400},
		CodeError: larkcore.CodeError{Code: 230011},
	}}
	sender := &ChannelSender{messageAPI: api}

	_, err := sender.PostMessage(context.Background(), PostMessageRequest{
		ReplyToMessageID: "om_recalled",
		Content:          notify.Request{Markdown: "too late"},
	})
	var sendErr *MessageSendError
	if !errors.As(err, &sendErr) || sendErr.Class != "api_rejected" || sendErr.Code != 230011 || sendErr.Retryable {
		t.Fatalf("error = %#v", err)
	}
}

func TestPostMessageSDKWireRequestRepliesInThreadToTheTarget(t *testing.T) {
	httpClient := &cardKitRecordingHTTPClient{}
	client := lark.NewClient("cli_test", "secret", lark.WithHttpClient(httpClient))
	sender := &ChannelSender{messageAPI: client.Im.V1.Message}

	posted, err := sender.PostMessage(context.Background(), PostMessageRequest{
		ReplyToMessageID: "om_original",
		ReplyInThread:    true,
		Content:          notify.Request{Markdown: "mirrored answer", Source: "agent_post", DedupeKey: "op-1"},
	})
	if err != nil {
		t.Fatalf("post reply: %v", err)
	}
	if len(posted.MessageIDs) != 1 || posted.MessageIDs[0] != "om_reply" {
		t.Fatalf("posted = %#v", posted)
	}
	var reply *recordedHTTPRequest
	for index := range httpClient.requests {
		if httpClient.requests[index].Path == "/open-apis/im/v1/messages/om_original/reply" {
			reply = &httpClient.requests[index]
		}
	}
	if reply == nil {
		t.Fatalf("no reply request to the target: %#v", httpClient.requests)
	}
	var body map[string]any
	if err := json.Unmarshal(reply.Body, &body); err != nil {
		t.Fatalf("decode reply body: %v", err)
	}
	if body["reply_in_thread"] != true || body["msg_type"] != "post" || body["uuid"] == "" {
		t.Fatalf("reply body = %s", reply.Body)
	}
}

// steppedCreateAPI answers successive create calls from a script.
type steppedCreateAPI struct {
	fakeCardKitMessageAPI
	calls int
	ids   []string
	fail  int // 1-based call that fails definitively
}

func (s *steppedCreateAPI) Create(_ context.Context, _ *larkim.CreateMessageReq, _ ...larkcore.RequestOptionFunc) (*larkim.CreateMessageResp, error) {
	s.calls++
	if s.calls == s.fail {
		return &larkim.CreateMessageResp{ApiResp: &larkcore.ApiResp{StatusCode: 400}, CodeError: larkcore.CodeError{Code: 230002}}, nil
	}
	id := s.ids[s.calls-1]
	return &larkim.CreateMessageResp{Data: &larkim.CreateMessageRespData{MessageId: &id}}, nil
}

func TestPostMessageReturnsAcceptedPartsBesideALaterFailure(t *testing.T) {
	api := &steppedCreateAPI{ids: []string{"om_part_1", "om_part_2"}, fail: 2}
	sender := &ChannelSender{messageAPI: api}

	long := strings.Repeat("refund retry analysis line\n", 400)
	posted, err := sender.PostMessage(context.Background(), PostMessageRequest{
		ChatID:  "oc_refund",
		Content: notify.Request{Markdown: long, Source: "agent_post", DedupeKey: "op"},
	})
	var sendErr *MessageSendError
	if !errors.As(err, &sendErr) || sendErr.Code != 230002 {
		t.Fatalf("error = %v", err)
	}
	if len(posted.MessageIDs) != 1 || posted.MessageIDs[0] != "om_part_1" {
		t.Fatalf("accepted parts = %#v", posted)
	}
}
