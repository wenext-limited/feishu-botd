package feishu

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	larkim "github.com/larksuite/oapi-sdk-go/v3/service/im/v1"
)

// rawCardContent wraps a rendered card tree the way the history API returns
// it under card_msg_content_type=raw_card_content.
func rawCardContent(t *testing.T, card string) string {
	t.Helper()
	encoded, err := json.Marshal(map[string]any{
		"json_card":       card,
		"json_attachment": map[string]any{},
		"card_schema":     2,
	})
	if err != nil {
		t.Fatal(err)
	}
	return string(encoded)
}

// Shape captured from a production iBot answer (CardKit, schema 2.0).
const iBotAnswerCard = `{"body":{"id":"_2","property":{"elements":[{"id":"agent_answer","property":{"elements":[` +
	`{"id":"a0","property":{"content":"要查哪些项目、哪个分支？比如 ","textAlign":"left"},"tag":"plain_text"},` +
	`{"id":"a1","property":{"content":"ludo、lama / develop"},"tag":"code_span"},` +
	`{"id":"a2","property":{"content":"，也可以回「默认分支」。","textAlign":"left"},"tag":"plain_text"},` +
	`{"id":"a3","property":{},"tag":"br"},{"id":"a4","property":{},"tag":"br"},` +
	`{"id":"a5","property":{"content":"范围一敲定就开始。","textAlign":"left"},"tag":"plain_text"},` +
	`{"id":"a6","property":{"contents":[{"contents":[{"content":"const batches = [\n  [\"ludo\"],\n];\n","contentType":"variable"}]}],"language":"plain_text"},"tag":"code_block"},` +
	`{"id":"a7","property":{"content":"a25d19c · project.pbxproj:808","url":{"url":"https://example.com/x"}},"tag":"link"},` +
	`{"id":"a8","property":{"columns":[{"dataType":"markdown","displayName":"项目","name":"0"},{"dataType":"markdown","displayName":"build","name":"1"}],"rows":[` +
	`{"0":{"data":{"id":"_4","property":{"elements":[{"id":"c0","property":{"content":"Ludo"},"tag":"plain_text"}]},"tag":"markdown"}},` +
	`"1":{"data":{"id":"_5","property":{"elements":[{"id":"c1","property":{"content":"194"},"tag":"plain_text"}]},"tag":"markdown"}}}` +
	`]},"tag":"table"}` +
	`],"markdownElements":[],"originTag":"markdown"},"tag":"markdown"}]},"tag":"body"},` +
	`"config":{"summary":{"content":"iBot 回复"}},"schema":"2.0","source":"json"}`

func TestParseAttachedCardFlattensRawCardKitAnswer(t *testing.T) {
	got := parseAttachedCard(rawCardContent(t, iBotAnswerCard))
	want := "要查哪些项目、哪个分支？比如 `ludo、lama / develop`，也可以回「默认分支」。\n\n" +
		"范围一敲定就开始。\n```\nconst batches = [\n  [\"ludo\"],\n];\n```\n" +
		"a25d19c · project.pbxproj:808\n项目 | build\nLudo | 194"
	if got != want {
		t.Fatalf("card text =\n%s\nwant\n%s", got, want)
	}
}

func TestParseAttachedCardReadsSimplifiedShapeAndDropsUpgradeStub(t *testing.T) {
	simplified := `{"title":"部署结果","elements":[[{"tag":"text","text":"已完成 "},{"tag":"at","user_id":"ou_x","user_name":"张三"}],[{"tag":"img","image_key":"img_1"}]]}`
	if got := parseAttachedCard(simplified); got != "部署结果\n已完成 @participant" {
		t.Fatalf("simplified card text = %q", got)
	}
	stub := `{"title":null,"elements":[[{"tag":"img","image_key":"img_1"},{"tag":"text","text":"请升级至最新版本客户端，以查看内容"},{"tag":"text","text":""}]]}`
	if got := parseAttachedCard(stub); got != "" {
		t.Fatalf("upgrade stub yielded %q, want no text", got)
	}
	for _, malformed := range []string{`not json`, `{"json_card":"{broken"}`, `{"elements":[]}`} {
		if got := parseAttachedCard(malformed); got != "" {
			t.Fatalf("parseAttachedCard(%q) = %q, want empty", malformed, got)
		}
	}
}

func TestAttachedContextCarriesAgentCardAnswersAsText(t *testing.T) {
	history := &fakeThreadMessageAPI{responses: []*larkim.ListMessageResp{listMessageResponse(false, "",
		threadMessage("om_followup", "text", `{"text":"展示下你具体调用的 script"}`, "4000", "ou_user", "user"),
		threadMessage("om_answer", "interactive", rawCardContent(t, iBotAnswerCard), "3000", "cli_bot", "app"),
		threadMessage("om_status", "post", `{"title":"","content":[[{"tag":"text","text":"Completed"}]]}`, "2001", "cli_bot", "app"),
		threadMessage("om_ask", "text", `{"text":"使用 script 获取 build number"}`, "1000", "ou_user", "user"),
	)}}
	lookup := newSDKAttachedContextLookup(history, &orderedFakeMessageResourceAPI{})

	got, err := lookup.LookupAttachedContext(context.Background(), AttachedContextRequest{
		ThreadID: "omt_thread", TriggerMessageID: "om_followup", TriggerCreateTime: "4000",
	})
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if got.Status != AttachedContextFound || len(got.Issues) != 0 || got.Truncated {
		t.Fatalf("context = %#v, want a complete snapshot", got)
	}
	found := false
	for _, message := range got.Messages {
		if message.Text == placeholderUnsupported {
			t.Fatalf("card answer degraded to the placeholder: %#v", got.Messages)
		}
		found = found || strings.HasPrefix(message.Text, "要查哪些项目")
	}
	if !found {
		t.Fatalf("messages = %#v, want the card answer's text", got.Messages)
	}
}
