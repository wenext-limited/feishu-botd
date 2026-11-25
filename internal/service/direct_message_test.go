package service

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"feishu-botd/internal/config"
	"feishu-botd/internal/dedupe"
	"feishu-botd/internal/feishu"
	"feishu-botd/internal/notify"
)

func newDirectMessageService(sender *fakeSender) *Service {
	cfg := config.Config{
		AppID:     "cli_test",
		AppSecret: "secret",
		Channels:  map[string]string{"ops": "oc_test"},
		Services:  map[string]config.ServiceConfig{"bridge": {DefaultChannel: "ops"}},
		DirectMessages: config.DirectMessagesConfig{
			AllowedEmailDomains: []string{"example.com"},
			AllowedSources:      []string{"bridge"},
		},
		DedupeTTL:   time.Hour,
		SendTimeout: time.Second,
	}
	return NewService(cfg, sender, dedupe.NewMemoryStore(time.Hour), slog.Default())
}

func TestSendMessageDirectMessageByEmail(t *testing.T) {
	sender := &fakeSender{messageID: "om_dm"}
	svc := newDirectMessageService(sender)
	in := MessageInput{UserEmail: " Dev@Example.COM ", Source: "bridge", DedupeKey: "pr-1", Title: "Review", Markdown: "done"}
	res, apiErr := svc.SendMessage(context.Background(), in)
	if apiErr != nil || res.MessageID != "om_dm" {
		t.Fatalf("send = %#v, %v", res, apiErr)
	}
	// The domain is normalized; the receive id is the email, never the
	// source's default channel.
	if sender.chatID != "Dev@example.com" || sender.request.Target != (notify.Target{UserEmail: "Dev@example.com"}) {
		t.Fatalf("sender got chat=%q target=%#v", sender.chatID, sender.request.Target)
	}
	again, apiErr := svc.SendMessage(context.Background(), in)
	if apiErr != nil || !again.Duplicate || sender.calls != 1 {
		t.Fatalf("dedupe: %#v %v calls=%d", again, apiErr, sender.calls)
	}
	// The same dedupe key to a different person is a conflict, not a replay.
	in.UserEmail = "other@example.com"
	if _, apiErr := svc.SendMessage(context.Background(), in); apiErr == nil || apiErr.Code != "dedupe_conflict" {
		t.Fatalf("other recipient with same key = %v, want dedupe_conflict", apiErr)
	}
}

func TestSendMessageDirectMessageGate(t *testing.T) {
	cases := []struct {
		name string
		in   MessageInput
		code string
	}{
		{"source not allowed", MessageInput{UserEmail: "dev@example.com", Source: "jenkins"}, "direct_message_denied"},
		{"no source", MessageInput{UserEmail: "dev@example.com"}, "direct_message_denied"},
		{"domain not allowed", MessageInput{UserEmail: "dev@evil.com", Source: "bridge"}, "direct_message_denied"},
		{"subdomain not allowed", MessageInput{UserEmail: "dev@mail.example.com", Source: "bridge"}, "direct_message_denied"},
		{"lookalike suffix", MessageInput{UserEmail: "dev@notexample.com", Source: "bridge"}, "direct_message_denied"},
		{"not an email", MessageInput{UserEmail: "example.com", Source: "bridge"}, "invalid_user_email"},
		{"two ats", MessageInput{UserEmail: "a@b@example.com", Source: "bridge"}, "invalid_user_email"},
		{"whitespace", MessageInput{UserEmail: "a b@example.com", Source: "bridge"}, "invalid_user_email"},
		{"with channel", MessageInput{UserEmail: "dev@example.com", Channel: "ops", Source: "bridge"}, "invalid_target"},
		{"as reply", MessageInput{UserEmail: "dev@example.com", Source: "bridge", ReplyToMessageID: "om_x"}, "invalid_target"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sender := &fakeSender{messageID: "om_dm"}
			tc.in.Markdown = "hi"
			_, apiErr := newDirectMessageService(sender).SendMessage(context.Background(), tc.in)
			if apiErr == nil || apiErr.Code != tc.code {
				t.Fatalf("error = %v, want %s", apiErr, tc.code)
			}
			if sender.calls != 0 {
				t.Fatalf("sender called %d times for a refused DM", sender.calls)
			}
		})
	}
}

func TestSendMessageDirectMessageDisabledByDefault(t *testing.T) {
	sender := &fakeSender{messageID: "om_dm"}
	_, apiErr := newTestService(sender).SendMessage(context.Background(), MessageInput{
		UserEmail: "dev@example.com", Source: "jenkins", Markdown: "hi",
	})
	if apiErr == nil || apiErr.Code != "direct_message_denied" || sender.calls != 0 {
		t.Fatalf("error = %v calls=%d, want direct_message_denied and no send", apiErr, sender.calls)
	}
}

func TestSendNotificationRejectsUserEmailInsteadOfDefaultChannel(t *testing.T) {
	sender := &fakeSender{messageID: "om_dm"}
	req := validRequest()
	req.Source = "bridge"
	req.Target = notify.Target{UserEmail: "dev@example.com"}
	_, apiErr := newDirectMessageService(sender).SendNotification(context.Background(), req)
	if apiErr == nil || apiErr.Code != "unsupported_target" || sender.calls != 0 {
		t.Fatalf("error = %v calls=%d, want unsupported_target and no send", apiErr, sender.calls)
	}
}

func TestSendMessageDirectMessageRecipientRejected(t *testing.T) {
	sender := &fakeSender{err: &feishu.MessageSendError{Operation: "message_create", Class: "api_rejected", Code: 230013}}
	svc := newDirectMessageService(sender)
	in := MessageInput{UserEmail: "gone@example.com", Source: "bridge", DedupeKey: "k", Markdown: "hi"}
	_, apiErr := svc.SendMessage(context.Background(), in)
	if apiErr == nil || apiErr.Code != "direct_message_rejected" || apiErr.Retryable {
		t.Fatalf("error = %v, want non-retryable direct_message_rejected", apiErr)
	}
	// A transient failure stays the retryable feishu_unavailable, and the
	// aborted reservation lets the retry through.
	sender.err = errors.New("boom")
	if _, apiErr := svc.SendMessage(context.Background(), in); apiErr == nil || apiErr.Code != "feishu_unavailable" || !apiErr.Retryable {
		t.Fatalf("transient error = %v", apiErr)
	}
}
