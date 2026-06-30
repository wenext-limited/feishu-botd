package feishu

import (
	"testing"

	larkevent "github.com/larksuite/oapi-sdk-go/v3/event"
	larkim "github.com/larksuite/oapi-sdk-go/v3/service/im/v1"
)

func TestCommandFromEventParsesMentionedTextCommand(t *testing.T) {
	r := NewCommandReceiver(CommandReceiverConfig{
		AppID:     "cli_test",
		AppSecret: "secret",
		Channels:  map[string]string{"ops": "oc_ops"},
	}, nil, nil)

	cmd, ok := r.CommandFromEvent(messageEvent("evt_1", "om_1", "oc_ops", "@_user_1 status prod now", &larkim.MentionEvent{
		Key:           ptr("@_user_1"),
		MentionedType: ptr("app"),
	}))
	if !ok {
		t.Fatal("expected command")
	}
	if cmd.DeliveryID != "evt_1" || cmd.Command != "status" || cmd.Text != "prod now" || cmd.ChatAlias != "ops" || cmd.SenderID != "ou_sender" {
		t.Fatalf("command = %#v", cmd)
	}
	if cmd.Metadata["message_id"] != "om_1" || cmd.Metadata["chat_type"] != "group" {
		t.Fatalf("metadata = %#v", cmd.Metadata)
	}
	if _, leaked := cmd.Metadata["chat_id"]; leaked {
		t.Fatalf("metadata leaked raw chat id: %#v", cmd.Metadata)
	}
}

func TestCommandFromEventMatchesConfiguredBotName(t *testing.T) {
	r := NewCommandReceiver(CommandReceiverConfig{
		AppID:     "cli_test",
		AppSecret: "secret",
		Channels:  map[string]string{"ops": "oc_ops"},
		BotNames:  []string{"BuildBot"},
	}, nil, nil)

	cmd, ok := r.CommandFromEvent(messageEvent("evt_1", "om_1", "oc_ops", "@_user_1 /Deploy main", &larkim.MentionEvent{
		Key:  ptr("@_user_1"),
		Name: ptr("buildbot"),
	}))
	if !ok {
		t.Fatal("expected command")
	}
	if cmd.Command != "deploy" || cmd.Text != "main" {
		t.Fatalf("command = %#v", cmd)
	}
}

func TestCommandFromEventSkipsUnknownAndAmbiguousChats(t *testing.T) {
	unknown := NewCommandReceiver(CommandReceiverConfig{
		AppID:     "cli_test",
		AppSecret: "secret",
		Channels:  map[string]string{"ops": "oc_ops"},
	}, nil, nil)
	if _, ok := unknown.CommandFromEvent(messageEvent("evt_1", "om_1", "oc_missing", "@_user_1 status", &larkim.MentionEvent{
		Key:           ptr("@_user_1"),
		MentionedType: ptr("app"),
	})); ok {
		t.Fatal("unknown chat produced a command")
	}

	ambiguous := NewCommandReceiver(CommandReceiverConfig{
		AppID:     "cli_test",
		AppSecret: "secret",
		Channels:  map[string]string{"ops": "oc_same", "ci": "oc_same"},
	}, nil, nil)
	if _, ok := ambiguous.CommandFromEvent(messageEvent("evt_1", "om_1", "oc_same", "@_user_1 status", &larkim.MentionEvent{
		Key:           ptr("@_user_1"),
		MentionedType: ptr("app"),
	})); ok {
		t.Fatal("ambiguous chat produced a command")
	}
}

func TestCommandFromEventSkipsMessagesWithoutBotMention(t *testing.T) {
	r := NewCommandReceiver(CommandReceiverConfig{
		AppID:     "cli_test",
		AppSecret: "secret",
		Channels:  map[string]string{"ops": "oc_ops"},
		BotOpenID: "ou_bot",
	}, nil, nil)
	if _, ok := r.CommandFromEvent(messageEvent("evt_1", "om_1", "oc_ops", "@_user_1 status", &larkim.MentionEvent{
		Key: ptr("@_user_1"),
		Id:  &larkim.UserId{OpenId: ptr("ou_someone_else")},
	})); ok {
		t.Fatal("non-bot mention produced a command")
	}
}

func TestCommandFromEventRequiresConfiguredBotNameWhenSet(t *testing.T) {
	r := NewCommandReceiver(CommandReceiverConfig{
		AppID:     "cli_test",
		AppSecret: "secret",
		Channels:  map[string]string{"ops": "oc_ops"},
		BotNames:  []string{"BuildBot"},
	}, nil, nil)
	if _, ok := r.CommandFromEvent(messageEvent("evt_1", "om_1", "oc_ops", "@_user_1 status", &larkim.MentionEvent{
		Key:           ptr("@_user_1"),
		MentionedType: ptr("app"),
		Name:          ptr("OtherBot"),
	})); ok {
		t.Fatal("non-configured app mention produced a command")
	}
}

func messageEvent(eventID, messageID, chatID, text string, mentions ...*larkim.MentionEvent) *larkim.P2MessageReceiveV1 {
	messageType := "text"
	chatType := "group"
	content := `{"text":` + quote(text) + `}`
	return &larkim.P2MessageReceiveV1{
		EventV2Base: &larkevent.EventV2Base{
			Header: &larkevent.EventHeader{EventID: eventID},
		},
		Event: &larkim.P2MessageReceiveV1Data{
			Sender: &larkim.EventSender{
				SenderId: &larkim.UserId{OpenId: ptr("ou_sender")},
			},
			Message: &larkim.EventMessage{
				MessageId:   ptr(messageID),
				ChatId:      ptr(chatID),
				ChatType:    ptr(chatType),
				MessageType: &messageType,
				Content:     &content,
				Mentions:    mentions,
			},
		},
	}
}

func ptr(s string) *string {
	return &s
}

func quote(s string) string {
	out := `"`
	for _, r := range s {
		if r == '"' || r == '\\' {
			out += `\`
		}
		out += string(r)
	}
	return out + `"`
}
