package feishu

import (
	"context"
	"errors"
	"testing"
	"time"

	larkim "github.com/larksuite/oapi-sdk-go/v3/service/im/v1"
)

type fakeChatTitleLookup struct {
	title string
	err   error
	calls int
}

func (f *fakeChatTitleLookup) Title(_ context.Context, _ string) (string, error) {
	f.calls++
	return f.title, f.err
}

func TestGroupTitleIsCachedAndRefreshesAfterTTL(t *testing.T) {
	lookup := &fakeChatTitleLookup{title: " Yoki QA "}
	var handled []InboundCommand
	r := NewCommandReceiver(CommandReceiverConfig{
		AppID:           "app",
		AppSecret:       "secret",
		Channels:        map[string]string{"qa": "oc_group"},
		BotOpenID:       "ou_bot",
		ChatTitleLookup: lookup,
	}, func(_ context.Context, command InboundCommand) error {
		handled = append(handled, command)
		return nil
	}, nil)
	now := time.Unix(1_700_000_000, 0)
	r.titleCache.now = func() time.Time { return now }

	mention := &larkim.MentionEvent{Key: ptr("@_bot"), MentionedType: ptr("app")}
	if err := r.handleMessage(context.Background(), messageEvent("evt_1", "om_1", "oc_group", "@_bot why login fails", mention)); err != nil {
		t.Fatalf("first handle: %v", err)
	}
	lookup.title = "Yoki Renamed"
	if err := r.handleMessage(context.Background(), messageEvent("evt_2", "om_2", "oc_group", "@_bot why signup fails", mention)); err != nil {
		t.Fatalf("cached handle: %v", err)
	}
	if lookup.calls != 1 || len(handled) != 2 || handled[0].ConversationTitle != "Yoki QA" || handled[1].ConversationTitle != "Yoki QA" {
		t.Fatalf("cached titles calls=%d handled=%#v", lookup.calls, handled)
	}

	now = now.Add(chatTitleTTL + time.Second)
	if err := r.handleMessage(context.Background(), messageEvent("evt_3", "om_3", "oc_group", "@_bot why reset fails", mention)); err != nil {
		t.Fatalf("refreshed handle: %v", err)
	}
	if lookup.calls != 2 || handled[2].ConversationTitle != "Yoki Renamed" {
		t.Fatalf("refreshed title calls=%d handled=%#v", lookup.calls, handled)
	}
}

func TestGroupTitleLookupFailureIsFailSoftAndDirectMessagesSkipLookup(t *testing.T) {
	lookup := &fakeChatTitleLookup{err: errors.New("private sdk failure")}
	var handled []InboundCommand
	r := NewCommandReceiver(CommandReceiverConfig{
		AppID:           "app",
		AppSecret:       "secret",
		Channels:        map[string]string{"qa": "oc_group"},
		BotOpenID:       "ou_bot",
		ChatTitleLookup: lookup,
	}, func(_ context.Context, command InboundCommand) error {
		handled = append(handled, command)
		return nil
	}, nil)
	mention := &larkim.MentionEvent{Key: ptr("@_bot"), MentionedType: ptr("app")}
	if err := r.handleMessage(context.Background(), messageEvent("evt_1", "om_1", "oc_group", "@_bot question", mention)); err != nil {
		t.Fatalf("group handle: %v", err)
	}
	direct := messageEvent("evt_2", "om_2", "oc_direct", "question")
	*direct.Event.Message.ChatType = "p2p"
	if err := r.handleMessage(context.Background(), direct); err != nil {
		t.Fatalf("direct handle: %v", err)
	}
	if lookup.calls != 1 || len(handled) != 2 || handled[0].ConversationTitle != "" || handled[1].ConversationTitle != "" {
		t.Fatalf("fail-soft titles calls=%d handled=%#v", lookup.calls, handled)
	}
}
