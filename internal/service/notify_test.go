package service

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"feishu-botd/internal/config"
	"feishu-botd/internal/dedupe"
	"feishu-botd/internal/notify"
)

type fakeSender struct {
	messageID string
	err       error
	readyErr  error
	calls     int
	chatID    string
	request   notify.Request
	started   chan struct{} // closed when Send begins (optional)
	release   chan struct{} // blocks Send until closed (optional)
}

func (f *fakeSender) Ready(_ context.Context) error { return f.readyErr }

func (f *fakeSender) Send(_ context.Context, chatID string, req notify.Request) (string, error) {
	f.calls++
	f.chatID = chatID
	f.request = req
	if f.started != nil {
		close(f.started)
	}
	if f.release != nil {
		<-f.release
	}
	if f.err != nil {
		return "", f.err
	}
	return f.messageID, nil
}

func newTestService(sender *fakeSender) *Service {
	cfg := config.Config{
		AppID:       "cli_test",
		AppSecret:   "secret",
		Channels:    map[string]string{"ops": "oc_test", "ci": "oc_ci"},
		Services:    map[string]config.ServiceConfig{"jenkins": {DefaultChannel: "ci"}},
		DedupeTTL:   time.Hour,
		SendTimeout: time.Second,
	}
	return NewService(cfg, sender, dedupe.NewMemoryStore(time.Hour), slog.Default())
}

func validRequest() notify.Request {
	return notify.Request{
		Source:        "xipe",
		SourceEventID: "evt_1",
		DedupeKey:     "xipe:evt_1:ops",
		Severity:      "critical",
		Title:         "Title",
		Markdown:      "**Body**",
		Target:        notify.Target{Channel: "ops"},
		Metadata:      map[string]string{"trigger": "reauth_required"},
	}
}

func TestSendNotificationSuccessAndDuplicate(t *testing.T) {
	sender := &fakeSender{messageID: "om_1"}
	svc := newTestService(sender)

	resp, apiErr := svc.SendNotification(context.Background(), validRequest())
	if apiErr != nil {
		t.Fatalf("first send error: %v", apiErr)
	}
	if resp.Duplicate {
		t.Fatal("first send was marked duplicate")
	}
	if resp.MessageID != "om_1" || resp.Provider != "feishu" || resp.Status != "sent" {
		t.Fatalf("unexpected response: %#v", resp)
	}
	if sender.chatID != "oc_test" {
		t.Fatalf("sender chat id = %q", sender.chatID)
	}
	if sender.request.Title != "Title" || sender.request.Markdown != "**Body**" {
		t.Fatalf("sender request = %#v", sender.request)
	}

	dup, apiErr := svc.SendNotification(context.Background(), validRequest())
	if apiErr != nil {
		t.Fatalf("second send error: %v", apiErr)
	}
	if !dup.Duplicate {
		t.Fatal("second send was not marked duplicate")
	}
	if sender.calls != 1 {
		t.Fatalf("sender calls = %d, want 1", sender.calls)
	}
}

func TestSendNotificationKeepsMarkdownContract(t *testing.T) {
	sender := &fakeSender{messageID: "om_1"}
	svc := newTestService(sender)
	req := validRequest()
	req.CardJSON = `{"type":"template"}`

	if _, apiErr := svc.SendNotification(context.Background(), req); apiErr != nil {
		t.Fatalf("send error: %v", apiErr)
	}
	if sender.request.CardJSON != "" || sender.request.Markdown != req.Markdown {
		t.Fatalf("notification drifted from markdown contract: %#v", sender.request)
	}
}

func TestSendNotificationDedupeConflict(t *testing.T) {
	svc := newTestService(&fakeSender{messageID: "om_1"})
	if _, apiErr := svc.SendNotification(context.Background(), validRequest()); apiErr != nil {
		t.Fatalf("first send error: %v", apiErr)
	}
	changed := validRequest()
	changed.Title = "Different"
	_, apiErr := svc.SendNotification(context.Background(), changed)
	if apiErr == nil || apiErr.Status != 409 || apiErr.Code != "dedupe_conflict" {
		t.Fatalf("expected dedupe_conflict 409, got %v", apiErr)
	}
}

func TestSendNotificationProviderFailureAbortsReservation(t *testing.T) {
	sender := &fakeSender{err: errors.New("boom secret token")}
	svc := newTestService(sender)

	_, apiErr := svc.SendNotification(context.Background(), validRequest())
	if apiErr == nil || apiErr.Status != 502 || apiErr.Code != "feishu_unavailable" || !apiErr.Retryable {
		t.Fatalf("expected retryable feishu_unavailable 502, got %v", apiErr)
	}

	// Abort must have freed the reservation: a retry attempts the send again.
	sender.err = nil
	sender.messageID = "om_retry"
	resp, apiErr := svc.SendNotification(context.Background(), validRequest())
	if apiErr != nil {
		t.Fatalf("retry error: %v", apiErr)
	}
	if resp.Duplicate || resp.MessageID != "om_retry" {
		t.Fatalf("retry not re-sent: %#v", resp)
	}
	if sender.calls != 2 {
		t.Fatalf("sender calls = %d, want 2", sender.calls)
	}
}

func TestSendNotificationUnknownChannel(t *testing.T) {
	svc := newTestService(&fakeSender{messageID: "om_1"})
	req := validRequest()
	req.Target.Channel = "nope"
	_, apiErr := svc.SendNotification(context.Background(), req)
	if apiErr == nil || apiErr.Status != 404 || apiErr.Code != "unknown_channel" {
		t.Fatalf("expected unknown_channel 404, got %v", apiErr)
	}
}

func TestSendNotificationUsesServiceDefaultChannel(t *testing.T) {
	sender := &fakeSender{messageID: "om_1"}
	svc := newTestService(sender)
	req := validRequest()
	req.Source = "jenkins"
	req.Target.Channel = ""

	if _, apiErr := svc.SendNotification(context.Background(), req); apiErr != nil {
		t.Fatalf("send error: %v", apiErr)
	}
	if sender.chatID != "oc_ci" || sender.request.Target.Channel != "ci" {
		t.Fatalf("expected jenkins default channel, chat=%q req=%#v", sender.chatID, sender.request)
	}
}

func TestSendNotificationValidationRejectsBadSeverity(t *testing.T) {
	svc := newTestService(&fakeSender{messageID: "om_1"})
	req := validRequest()
	req.Severity = "fatal"
	_, apiErr := svc.SendNotification(context.Background(), req)
	if apiErr == nil || apiErr.Status != 400 || apiErr.Code != "invalid_severity" {
		t.Fatalf("expected invalid_severity 400, got %v", apiErr)
	}
}

func TestSendMessageMarkdown(t *testing.T) {
	sender := &fakeSender{messageID: "om_msg"}
	svc := newTestService(sender)
	res, apiErr := svc.SendMessage(context.Background(), MessageInput{
		Channel:  "ops",
		Title:    "Hi",
		Markdown: "**hello**",
	})
	if apiErr != nil {
		t.Fatalf("send message error: %v", apiErr)
	}
	if res.MessageID != "om_msg" || res.Duplicate {
		t.Fatalf("unexpected result: %#v", res)
	}
	if sender.chatID != "oc_test" || sender.request.Markdown != "**hello**" {
		t.Fatalf("unexpected sender state: chat=%q req=%#v", sender.chatID, sender.request)
	}
}

func TestSendMessageUsesServiceDefaultChannel(t *testing.T) {
	sender := &fakeSender{messageID: "om_msg"}
	svc := newTestService(sender)
	res, apiErr := svc.SendMessage(context.Background(), MessageInput{
		Source:   "jenkins",
		Markdown: "**hello**",
	})
	if apiErr != nil {
		t.Fatalf("send message error: %v", apiErr)
	}
	if res.MessageID != "om_msg" || sender.chatID != "oc_ci" || sender.request.Target.Channel != "ci" {
		t.Fatalf("expected jenkins default channel, result=%#v chat=%q req=%#v", res, sender.chatID, sender.request)
	}
}

func TestSendMessageExplicitChannelOverridesServiceDefault(t *testing.T) {
	sender := &fakeSender{messageID: "om_msg"}
	svc := newTestService(sender)
	_, apiErr := svc.SendMessage(context.Background(), MessageInput{
		Source:   "jenkins",
		Channel:  "ops",
		Markdown: "**hello**",
	})
	if apiErr != nil {
		t.Fatalf("send message error: %v", apiErr)
	}
	if sender.chatID != "oc_test" || sender.request.Target.Channel != "ops" {
		t.Fatalf("expected explicit channel, chat=%q req=%#v", sender.chatID, sender.request)
	}
}

func TestSendMessageCard(t *testing.T) {
	sender := &fakeSender{messageID: "om_card"}
	svc := newTestService(sender)
	cardJSON := `{"type":"template","data":{"template_id":"tpl","template_variable":{"title":"Build"}}}`

	res, apiErr := svc.SendMessage(context.Background(), MessageInput{
		Channel:  "ops",
		Source:   "jenkins",
		CardJSON: cardJSON,
	})
	if apiErr != nil {
		t.Fatalf("send card error: %v", apiErr)
	}
	if res.MessageID != "om_card" || res.Duplicate {
		t.Fatalf("unexpected result: %#v", res)
	}
	if sender.chatID != "oc_test" || sender.request.CardJSON != cardJSON || sender.request.Markdown != "" {
		t.Fatalf("unexpected sender state: chat=%q req=%#v", sender.chatID, sender.request)
	}
}

func TestSendMessageRejectsInvalidCardJSON(t *testing.T) {
	svc := newTestService(&fakeSender{messageID: "om_msg"})
	_, apiErr := svc.SendMessage(context.Background(), MessageInput{
		Channel:  "ops",
		CardJSON: "not-json",
	})
	if apiErr == nil || apiErr.Code != "invalid_card_json" {
		t.Fatalf("expected invalid_card_json, got %v", apiErr)
	}
}

func TestSendMessageRejectsMultipleContents(t *testing.T) {
	svc := newTestService(&fakeSender{messageID: "om_msg"})
	_, apiErr := svc.SendMessage(context.Background(), MessageInput{
		Channel:  "ops",
		Markdown: "hello",
		CardJSON: `{"type":"template"}`,
	})
	if apiErr == nil || apiErr.Code != "invalid_content" {
		t.Fatalf("expected invalid_content, got %v", apiErr)
	}
}

func TestSendMessageDedupes(t *testing.T) {
	sender := &fakeSender{messageID: "om_msg"}
	svc := newTestService(sender)
	in := MessageInput{Channel: "ops", Source: "svc", DedupeKey: "k1", Markdown: "x"}

	if _, apiErr := svc.SendMessage(context.Background(), in); apiErr != nil {
		t.Fatalf("first send message error: %v", apiErr)
	}
	res, apiErr := svc.SendMessage(context.Background(), in)
	if apiErr != nil {
		t.Fatalf("second send message error: %v", apiErr)
	}
	if !res.Duplicate {
		t.Fatal("second send message was not a duplicate")
	}
	if sender.calls != 1 {
		t.Fatalf("sender calls = %d, want 1", sender.calls)
	}
}

func TestSendMessageUnknownChannel(t *testing.T) {
	svc := newTestService(&fakeSender{messageID: "om_msg"})
	_, apiErr := svc.SendMessage(context.Background(), MessageInput{Channel: "nope", Markdown: "x"})
	if apiErr == nil || apiErr.Status != 404 || apiErr.Code != "unknown_channel" {
		t.Fatalf("expected unknown_channel 404, got %v", apiErr)
	}
}

func TestSendMessageRejectsOversizedDedupeKey(t *testing.T) {
	svc := newTestService(&fakeSender{messageID: "om_msg"})
	_, apiErr := svc.SendMessage(context.Background(), MessageInput{
		Channel:   "ops",
		DedupeKey: strings.Repeat("k", 241),
		Markdown:  "x",
	})
	if apiErr == nil || apiErr.Code != "field_too_large" {
		t.Fatalf("expected field_too_large, got %v", apiErr)
	}
}

// TestSendNotificationInFlight drives a real in-flight reservation through
// service.deliver: while the first send is blocked inside the sender, a second
// send with the same key must observe inFlight==true and return the retryable
// 409 dedupe_in_flight error.
func TestSendNotificationInFlight(t *testing.T) {
	sender := &fakeSender{messageID: "om_1", started: make(chan struct{}), release: make(chan struct{})}
	svc := newTestService(sender)

	done := make(chan struct{})
	go func() {
		defer close(done)
		if _, apiErr := svc.SendNotification(context.Background(), validRequest()); apiErr != nil {
			t.Errorf("first send error: %v", apiErr)
		}
	}()
	<-sender.started // first send is in flight; the reservation is held inFlight

	_, apiErr := svc.SendNotification(context.Background(), validRequest())
	if apiErr == nil || apiErr.Status != 409 || apiErr.Code != "dedupe_in_flight" || !apiErr.Retryable {
		t.Fatalf("expected retryable dedupe_in_flight 409, got %v", apiErr)
	}

	close(sender.release)
	<-done
	if sender.calls != 1 {
		t.Fatalf("sender calls = %d, want 1", sender.calls)
	}
}
