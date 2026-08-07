package feishu

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/url"
	"strconv"
	"strings"
	"testing"

	lark "github.com/larksuite/oapi-sdk-go/v3"
	larkcore "github.com/larksuite/oapi-sdk-go/v3/core"
	larkim "github.com/larksuite/oapi-sdk-go/v3/service/im/v1"
)

func TestAttachedContextLookupUsesThreadHistoryWireContract(t *testing.T) {
	httpClient := &cardKitRecordingHTTPClient{}
	client := lark.NewClient("cli_test", "test-placeholder", lark.WithHttpClient(httpClient))
	lookup := newSDKAttachedContextLookup(client.Im.V1.Message, client.Im.V1.MessageResource)

	_, err := lookup.LookupAttachedContext(context.Background(), AttachedContextRequest{
		ThreadID: "omt_private_thread", TriggerMessageID: "om_private_trigger", TriggerCreateTime: "2345",
	})
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	requests := nonTokenRequests(httpClient.requests)
	if len(requests) != 1 {
		t.Fatalf("history request count = %d, requests = %#v", len(requests), requests)
	}
	request := requests[0]
	if request.Method != "GET" || request.Path != "/open-apis/im/v1/messages" {
		t.Fatalf("history request = %#v", request)
	}
	query, err := url.ParseQuery(request.RawQuery)
	if err != nil {
		t.Fatalf("parse history query: %v", err)
	}
	want := map[string]string{
		"container_id_type": "thread",
		"container_id":      "omt_private_thread",
		"sort_type":         "ByCreateTimeDesc",
		"page_size":         strconv.Itoa(attachedContextPageSize),
		"end_time":          "2",
	}
	for key, value := range want {
		if got := query.Get(key); got != value {
			t.Fatalf("query[%q] = %q, want %q; query=%v", key, got, value, query)
		}
	}
}

type fakeThreadMessageAPI struct {
	responses []*larkim.ListMessageResp
	err       error
	calls     int
}

func (f *fakeThreadMessageAPI) List(context.Context, *larkim.ListMessageReq, ...larkcore.RequestOptionFunc) (*larkim.ListMessageResp, error) {
	f.calls++
	if f.err != nil {
		return nil, f.err
	}
	if len(f.responses) == 0 {
		return nil, nil
	}
	response := f.responses[0]
	f.responses = f.responses[1:]
	return response, nil
}

func TestAttachedContextLookupReturnsSnapshotBeforeTriggerOldestFirst(t *testing.T) {
	triggerID := "om_guide"
	history := &fakeThreadMessageAPI{responses: []*larkim.ListMessageResp{listMessageResponse(false, "",
		threadMessage("om_later", "text", `{"text":"posted later"}`, "3000", "ou_later", "user"),
		threadMessage(triggerID, "text", `{"text":"@Nous 看看这个问题"}`, "2000", "ou_guide", "user"),
		threadMessage("om_second", "text", `{"text":"second detail"}`, "1500", "ou_two", "user"),
		threadMessage("om_first", "text", `{"text":"first detail"}`, "1000", "ou_one", "user"),
	)}}
	lookup := newSDKAttachedContextLookup(history, &orderedFakeMessageResourceAPI{})

	got, err := lookup.LookupAttachedContext(context.Background(), AttachedContextRequest{
		ThreadID: "omt_thread", TriggerMessageID: triggerID, TriggerCreateTime: "2000",
	})
	if err != nil {
		t.Fatalf("lookup attached context: %v", err)
	}
	if got.Status != AttachedContextFound || len(got.Messages) != 2 {
		t.Fatalf("context = %#v", got)
	}
	if got.Messages[0].Text != "first detail" || got.Messages[1].Text != "second detail" {
		t.Fatalf("messages are not a pre-trigger oldest-first snapshot: %#v", got.Messages)
	}
	if got.Messages[0].AuthorLabel != "participant-1" || got.Messages[1].AuthorLabel != "participant-2" {
		t.Fatalf("participant labels = %#v", got.Messages)
	}
	for _, message := range got.Messages {
		if strings.Contains(message.AuthorLabel, "ou_") {
			t.Fatalf("author label leaked provider identity: %#v", message)
		}
	}
}

func TestAttachedContextLookupFailsClosedWhenTriggerSearchCapIsExhausted(t *testing.T) {
	messages := make([]*larkim.Message, 0, attachedContextMaxBoundaryScan)
	for index := 0; index < attachedContextMaxBoundaryScan; index++ {
		messages = append(messages, threadMessage(
			"om_after_"+strconv.Itoa(index), "text", `{"text":"later"}`, "3000", "ou_later", "user",
		))
	}
	history := &fakeThreadMessageAPI{responses: []*larkim.ListMessageResp{
		listMessageResponse(true, "next-page", messages...),
	}}
	lookup := newSDKAttachedContextLookup(history, &orderedFakeMessageResourceAPI{})

	got, err := lookup.LookupAttachedContext(context.Background(), AttachedContextRequest{
		ThreadID: "omt_thread", TriggerMessageID: "om_guide", TriggerCreateTime: "3000",
	})
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if got.Status != AttachedContextUnreadable || !got.Truncated || len(got.Messages) != 0 {
		t.Fatalf("context = %#v", got)
	}
	for _, issue := range []AttachedContextIssueCode{AttachedContextIssueBoundaryScanLimit, AttachedContextIssueBoundaryNotFound} {
		if !hasAttachedContextIssue(got.Issues, issue) {
			t.Fatalf("missing issue %q in %#v", issue, got.Issues)
		}
	}
}

func TestAttachedContextLookupRetainsTriggerImagesButNotGuideText(t *testing.T) {
	png := append([]byte("\x89PNG\r\n\x1a\n"), bytes.Repeat([]byte{0}, 32)...)
	history := &fakeThreadMessageAPI{responses: []*larkim.ListMessageResp{listMessageResponse(false, "",
		threadMessage("om_guide", "post", `{"title":"guide title","content":[[{"tag":"text","text":"@Nous 看看这个问题"},{"tag":"img","image_key":"img_guide"},{"tag":"media","file_key":"video_guide"}]]}`, "2000", "ou_guide", "user"),
		threadMessage("om_before", "post", `{"title":"Crash","content":[[{"tag":"text","text":"happens on launch, reported by "},{"tag":"at","user_name":"Private Display Name"},{"tag":"img","image_key":"img_before"}]]}`, "1000", "ou_reporter", "user"),
	)}}
	resources := &orderedFakeMessageResourceAPI{data: [][]byte{png, png}}
	lookup := newSDKAttachedContextLookup(history, resources)

	got, err := lookup.LookupAttachedContext(context.Background(), AttachedContextRequest{
		ThreadID: "omt_thread", TriggerMessageID: "om_guide", TriggerCreateTime: "2000",
	})
	if err != nil {
		t.Fatalf("lookup attached context: %v", err)
	}
	if got.Status != AttachedContextFound || len(got.Messages) != 2 {
		t.Fatalf("context = %#v", got)
	}
	if got.Messages[0].Text != "Crash\nhappens on launch, reported by @participant" || len(got.Messages[0].Images) != 1 {
		t.Fatalf("prior rich message = %#v", got.Messages[0])
	}
	if strings.Contains(got.Messages[0].Text, "Private Display Name") {
		t.Fatalf("structured mention leaked provider display name: %#v", got.Messages[0])
	}
	if got.Messages[1].Text != "" || len(got.Messages[1].Images) != 1 {
		t.Fatalf("trigger guide text was retained or media lost: %#v", got.Messages[1])
	}
	if !hasAttachedContextIssue(got.Issues, AttachedContextIssueVideoOmitted) {
		t.Fatalf("trigger video omission was not reported: %#v", got.Issues)
	}
	for _, message := range got.Messages {
		for _, image := range message.Images {
			if image.MediaType != "image/png" || len(image.Data) != len(png) {
				t.Fatalf("image = %#v", image)
			}
		}
	}
}

type orderedFakeMessageResourceAPI struct {
	data  [][]byte
	errs  []error
	calls int
}

func (f *orderedFakeMessageResourceAPI) Get(context.Context, *larkim.GetMessageResourceReq, ...larkcore.RequestOptionFunc) (*larkim.GetMessageResourceResp, error) {
	index := f.calls
	f.calls++
	if index < len(f.errs) && f.errs[index] != nil {
		return nil, f.errs[index]
	}
	if index >= len(f.data) {
		return nil, errors.New("missing fake resource")
	}
	return &larkim.GetMessageResourceResp{File: bytes.NewReader(f.data[index])}, nil
}

func TestAttachedContextLookupDistinguishesMissingAndUnreadable(t *testing.T) {
	tests := []struct {
		name    string
		request AttachedContextRequest
		history *fakeThreadMessageAPI
		status  AttachedContextStatus
		issue   AttachedContextIssueCode
	}{
		{
			name: "no thread", request: AttachedContextRequest{TriggerMessageID: "om_guide"},
			history: &fakeThreadMessageAPI{}, status: AttachedContextMissing, issue: AttachedContextIssueNoThread,
		},
		{
			name:    "permission or api failure",
			request: AttachedContextRequest{ThreadID: "omt_thread", TriggerMessageID: "om_guide"},
			history: &fakeThreadMessageAPI{err: errors.New("private provider error")},
			status:  AttachedContextUnreadable, issue: AttachedContextIssueHistoryUnreadable,
		},
		{
			name:    "trigger boundary absent",
			request: AttachedContextRequest{ThreadID: "omt_thread", TriggerMessageID: "om_missing"},
			history: &fakeThreadMessageAPI{responses: []*larkim.ListMessageResp{listMessageResponse(false, "",
				threadMessage("om_other", "text", `{"text":"not safely bounded"}`, "1000", "ou_one", "user"),
			)}},
			status: AttachedContextUnreadable, issue: AttachedContextIssueBoundaryNotFound,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			lookup := newSDKAttachedContextLookup(test.history, &orderedFakeMessageResourceAPI{})
			got, err := lookup.LookupAttachedContext(context.Background(), test.request)
			if err != nil {
				t.Fatalf("lookup: %v", err)
			}
			if got.Status != test.status || !hasAttachedContextIssue(got.Issues, test.issue) {
				t.Fatalf("context = %#v", got)
			}
		})
	}
}

func TestAttachedContextLookupReportsPartialImageAndVideoFailures(t *testing.T) {
	history := &fakeThreadMessageAPI{responses: []*larkim.ListMessageResp{listMessageResponse(false, "",
		threadMessage("om_guide", "text", `{"text":"guide"}`, "3000", "ou_guide", "user"),
		threadMessage("om_video", "media", `{"file_key":"file_video"}`, "2000", "ou_one", "user"),
		threadMessage("om_image", "image", `{"image_key":"img_broken"}`, "1000", "ou_one", "user"),
	)}}
	resources := &orderedFakeMessageResourceAPI{errs: []error{errors.New("download failed")}}
	lookup := newSDKAttachedContextLookup(history, resources)

	got, err := lookup.LookupAttachedContext(context.Background(), AttachedContextRequest{
		ThreadID: "omt_thread", TriggerMessageID: "om_guide", TriggerCreateTime: "3000",
	})
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if got.Status != AttachedContextFound {
		t.Fatalf("status = %v, want found: the video degrades to a placeholder row, not a refusal", got.Status)
	}
	if len(got.Messages) != 1 || got.Messages[0].Text != placeholderVideo {
		t.Fatalf("messages = %#v, want one video placeholder row", got.Messages)
	}
	for _, issue := range []AttachedContextIssueCode{AttachedContextIssueVideoOmitted, AttachedContextIssueImageUnreadable} {
		if !hasAttachedContextIssue(got.Issues, issue) {
			t.Fatalf("missing issue %q in %#v", issue, got.Issues)
		}
	}
}

func TestAttachedContextLookupDegradesUnsupportedMessagesToPlaceholderRows(t *testing.T) {
	history := &fakeThreadMessageAPI{responses: []*larkim.ListMessageResp{listMessageResponse(false, "",
		threadMessage("om_guide", "text", `{"text":"@Nous 看看这个"}`, "5000", "ou_guide", "user"),
		threadMessage("om_card", "interactive", `{"elements":[]}`, "4000", "ou_bot", "app"),
		threadMessage("om_file", "file", `{"file_key":"file_doc","file_name":"crash报告.pdf"}`, "3000", "ou_one", "user"),
		threadMessage("om_audio", "audio", `{"file_key":"file_voice"}`, "2000", "ou_one", "user"),
		threadMessage("om_post", "post", `{"title":"","content":[[{"tag":"text","text":"复现视频："},{"tag":"media","file_key":"video_repro"},{"tag":"text","text":"，登录后必现"}]]}`, "1000", "ou_two", "user"),
	)}}
	lookup := newSDKAttachedContextLookup(history, &orderedFakeMessageResourceAPI{})

	got, err := lookup.LookupAttachedContext(context.Background(), AttachedContextRequest{
		ThreadID: "omt_thread", TriggerMessageID: "om_guide", TriggerCreateTime: "5000",
	})
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if got.Status != AttachedContextFound || len(got.Messages) != 4 {
		t.Fatalf("context = %#v", got)
	}
	wantTexts := []string{
		"复现视频：" + placeholderVideo + "，登录后必现",
		placeholderAudio,
		"[unsupported file: crash报告.pdf]",
		placeholderUnsupported,
	}
	for index, want := range wantTexts {
		if got.Messages[index].Text != want {
			t.Fatalf("messages[%d].Text = %q, want %q", index, got.Messages[index].Text, want)
		}
	}
	if !hasAttachedContextIssue(got.Issues, AttachedContextIssueVideoOmitted) ||
		!hasAttachedContextIssue(got.Issues, AttachedContextIssueUnsupportedMessage) {
		t.Fatalf("issues = %#v", got.Issues)
	}
}

func TestAttachedContextLookupEnforcesAndReportsCaps(t *testing.T) {
	largeText := strings.Repeat("界", attachedContextMaxTextBytes)
	largeImage := io.LimitReader(strings.NewReader(strings.Repeat("x", attachedContextMaxImageBytes+1)), int64(attachedContextMaxImageBytes+1))
	largeImageBytes, err := io.ReadAll(largeImage)
	if err != nil {
		t.Fatalf("make image: %v", err)
	}
	history := &fakeThreadMessageAPI{responses: []*larkim.ListMessageResp{listMessageResponse(false, "",
		threadMessage("om_guide", "text", `{"text":"guide"}`, "3000", "ou_guide", "user"),
		threadMessage("om_image", "image", `{"image_key":"img_large"}`, "2000", "ou_one", "user"),
		threadMessage("om_text", "text", `{"text":"`+largeText+`"}`, "1000", "ou_one", "user"),
	)}}
	resources := &orderedFakeMessageResourceAPI{data: [][]byte{largeImageBytes}}
	lookup := newSDKAttachedContextLookup(history, resources)

	got, err := lookup.LookupAttachedContext(context.Background(), AttachedContextRequest{
		ThreadID: "omt_thread", TriggerMessageID: "om_guide", TriggerCreateTime: "3000",
	})
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if got.Status != AttachedContextFound || !got.Truncated {
		t.Fatalf("context = %#v", got)
	}
	if !hasAttachedContextIssue(got.Issues, AttachedContextIssueTextLimit) || !hasAttachedContextIssue(got.Issues, AttachedContextIssueImageTooLarge) {
		t.Fatalf("cap issues = %#v", got.Issues)
	}
	if len(got.Messages) != 1 || len(got.Messages[0].Text) > attachedContextMaxTextBytes {
		t.Fatalf("bounded messages = %#v", got.Messages)
	}
}

// Received post content is the FLAT shape ({"title":…,"content":[[…]]});
// the locale wrapper only exists in the send format. Both parse, because a
// parser that only knew the send shape silently classified every real
// rich-text bug report as malformed — the 2026-08-07 「看看这个」 incident.
func TestLocalizedAttachedPostAcceptsReceivedAndSendShapes(t *testing.T) {
	flat := `{"title":"Bug 报告","content":[[{"tag":"text","text":"问题描述"}]],"content_v2":[[{"tag":"md","text":"问题描述"}]]}`
	wrapped := `{"zh_cn":{"title":"Bug 报告","content":[[{"tag":"text","text":"问题描述"}]]}}`
	for name, raw := range map[string]string{"received flat": flat, "send wrapped": wrapped} {
		post, ok := localizedAttachedPost(raw)
		if !ok || post.Title != "Bug 报告" || len(post.Content) != 1 {
			t.Fatalf("%s shape did not parse: ok=%v post=%#v", name, ok, post)
		}
	}
	for name, raw := range map[string]string{
		"not json":   "not json",
		"empty":      "{}",
		"wrong type": `{"title":42}`,
	} {
		if _, ok := localizedAttachedPost(raw); ok {
			t.Fatalf("%s shape must not parse", name)
		}
	}
}

func TestAttachedContextLookupSkipsRecalledAndSystemMessages(t *testing.T) {
	yes := true
	recalled := threadMessage("om_recalled", "text", `{"text":"taken back"}`, "1500", "ou_one", "user")
	recalled.Deleted = &yes
	history := &fakeThreadMessageAPI{responses: []*larkim.ListMessageResp{listMessageResponse(false, "",
		threadMessage("om_guide", "text", `{"text":"@Nous 看看这个"}`, "4000", "ou_guide", "user"),
		threadMessage("om_system", "system", `{"template":"joined the topic"}`, "3000", "ou_sys", "app"),
		recalled,
		threadMessage("om_report", "post", `{"title":"","content":[[{"tag":"text","text":"问题描述：游戏中卡住"},{"tag":"media","file_key":"video_1"}]]}`, "1000", "ou_one", "user"),
	)}}
	lookup := newSDKAttachedContextLookup(history, &orderedFakeMessageResourceAPI{})

	got, err := lookup.LookupAttachedContext(context.Background(), AttachedContextRequest{
		ThreadID: "omt_thread", TriggerMessageID: "om_guide", TriggerCreateTime: "4000",
	})
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if got.Status != AttachedContextFound || len(got.Messages) != 1 {
		t.Fatalf("context = %#v", got)
	}
	if got.Messages[0].Text != "问题描述：游戏中卡住"+placeholderVideo {
		t.Fatalf("report row = %#v", got.Messages[0])
	}
	for _, message := range got.Messages {
		if strings.Contains(message.Text, "taken back") {
			t.Fatalf("a recalled message resurfaced: %#v", message)
		}
	}
	if hasAttachedContextIssue(got.Issues, AttachedContextIssueUnsupportedMessage) ||
		hasAttachedContextIssue(got.Issues, AttachedContextIssueMalformedMessage) {
		t.Fatalf("chrome must not be reported as an issue: %#v", got.Issues)
	}
}

func listMessageResponse(hasMore bool, pageToken string, messages ...*larkim.Message) *larkim.ListMessageResp {
	return &larkim.ListMessageResp{Data: &larkim.ListMessageRespData{
		HasMore: &hasMore, PageToken: ptr(pageToken), Items: messages,
	}}
}

func threadMessage(id, messageType, content, createTime, senderID, senderType string) *larkim.Message {
	return &larkim.Message{
		MessageId: ptr(id), MsgType: ptr(messageType), CreateTime: ptr(createTime),
		Sender: &larkim.Sender{Id: ptr(senderID), SenderType: ptr(senderType)},
		Body:   &larkim.MessageBody{Content: ptr(content)},
	}
}

func hasAttachedContextIssue(issues []AttachedContextIssue, code AttachedContextIssueCode) bool {
	for _, issue := range issues {
		if issue.Code == code {
			return true
		}
	}
	return false
}
