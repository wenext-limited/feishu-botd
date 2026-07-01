package feishu

import (
	"context"
	"testing"

	channeltypes "github.com/larksuite/oapi-sdk-go/v3/channel/types"

	"feishu-botd/internal/notify"
)

// fakeChannel embeds a nil channeltypes.Channel so it only needs to implement
// Send; any other method call would panic, but ChannelSender.Send never
// calls them.
type fakeChannel struct {
	channeltypes.Channel
	lastInput *channeltypes.SendInput
	result    *channeltypes.SendResult
	err       error
}

func (f *fakeChannel) Send(_ context.Context, input *channeltypes.SendInput) (*channeltypes.SendResult, error) {
	f.lastInput = input
	if f.err != nil {
		return nil, f.err
	}
	return f.result, nil
}

func TestChannelSenderThreadsReplyMessageID(t *testing.T) {
	fc := &fakeChannel{result: &channeltypes.SendResult{MessageID: "om_new"}}
	s := &ChannelSender{channel: fc}

	messageID, err := s.Send(context.Background(), "oc_test", notify.Request{
		Title:            "t",
		Markdown:         "m",
		ReplyToMessageID: "om_original",
	})
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	if messageID != "om_new" {
		t.Fatalf("message id = %q", messageID)
	}
	if fc.lastInput.ChatID != "oc_test" {
		t.Fatalf("chat id = %q", fc.lastInput.ChatID)
	}
	if fc.lastInput.ReplyMessageID != "om_original" {
		t.Fatalf("reply message id = %q, want om_original", fc.lastInput.ReplyMessageID)
	}
}

func TestChannelSenderOmitsReplyMessageIDWhenAbsent(t *testing.T) {
	fc := &fakeChannel{result: &channeltypes.SendResult{MessageID: "om_new"}}
	s := &ChannelSender{channel: fc}

	if _, err := s.Send(context.Background(), "oc_test", notify.Request{Markdown: "m"}); err != nil {
		t.Fatalf("send: %v", err)
	}
	if fc.lastInput.ReplyMessageID != "" {
		t.Fatalf("reply message id = %q, want empty", fc.lastInput.ReplyMessageID)
	}
}
