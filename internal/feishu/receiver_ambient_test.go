package feishu

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	larkim "github.com/larksuite/oapi-sdk-go/v3/service/im/v1"
)

func ambientReceiver(ambientChats ...string) *CommandReceiver {
	granted := make(map[string]struct{}, len(ambientChats))
	for _, alias := range ambientChats {
		granted[alias] = struct{}{}
	}
	return NewCommandReceiver(CommandReceiverConfig{
		AppAlias:  "support",
		AppID:     "cli_test",
		AppSecret: "secret",
		Channels:  map[string]string{"ops": "oc_ops", "refund-backend": "oc_refund"},
		BotOpenID: "ou_bot",
		AmbientChat: func(chatAlias string) bool {
			_, ok := granted[chatAlias]
			return ok
		},
	}, nil, nil)
}

func fromHuman(event *larkim.P2MessageReceiveV1) *larkim.P2MessageReceiveV1 {
	event.Event.Sender.SenderType = ptr("user")
	return event
}

func TestCommandFromEventAdmitsUnaddressedHumanMessageOnlyInAmbientChats(t *testing.T) {
	r := ambientReceiver("refund-backend")

	cmd, ok := r.CommandFromEvent(fromHuman(messageEvent("evt_ambient", "om_ambient", "oc_refund", "the refund job is stuck again")))
	if !ok {
		t.Fatal("unaddressed human message in an ambient chat was discarded")
	}
	if !cmd.AmbientOnly || !cmd.HumanSender || cmd.UnmentionedReply {
		t.Fatalf("ambient candidate markers = %#v", cmd)
	}
	if cmd.Prompt != "the refund job is stuck again" || cmd.NamedPrompt != cmd.Prompt || cmd.ChatAlias != "refund-backend" {
		t.Fatalf("ambient candidate = %#v", cmd)
	}
	body, err := json.Marshal(cmd)
	if err != nil {
		t.Fatalf("marshal candidate: %v", err)
	}
	if strings.Contains(string(body), "AmbientOnly") || strings.Contains(string(body), "HumanSender") {
		t.Fatalf("serialized command leaked private ambient state: %s", body)
	}

	if _, ok := r.CommandFromEvent(fromHuman(messageEvent("evt_other", "om_other", "oc_ops", "chatter elsewhere"))); ok {
		t.Fatal("unaddressed message in a chat without an ambient grant was admitted")
	}
}

func TestCommandFromEventNeverAdmitsBotAuthoredAmbientMessages(t *testing.T) {
	r := ambientReceiver("refund-backend")

	appEvent := messageEvent("evt_app", "om_app", "oc_refund", "automated status line")
	appEvent.Event.Sender.SenderType = ptr("app")
	if _, ok := r.CommandFromEvent(appEvent); ok {
		t.Fatal("an app-authored message became an ambient candidate")
	}
	untyped := messageEvent("evt_untyped", "om_untyped", "oc_refund", "no sender type")
	if _, ok := r.CommandFromEvent(untyped); ok {
		t.Fatal("a sender of unknown type was assumed human")
	}
	self := fromHuman(messageEvent("evt_self", "om_self", "oc_refund", "this app's own identity"))
	self.Event.Sender.SenderId = &larkim.UserId{OpenId: ptr("ou_bot")}
	if _, ok := r.CommandFromEvent(self); ok {
		t.Fatal("a message carrying the bot's own identity became an ambient candidate")
	}
}

func TestCommandFromEventKeepsAddressedMessagesAddressedInAmbientChats(t *testing.T) {
	r := ambientReceiver("refund-backend")

	mentioned, ok := r.CommandFromEvent(fromHuman(messageEvent("evt_mention", "om_mention", "oc_refund", "@_user_1 status", &larkim.MentionEvent{
		Key: ptr("@_user_1"), MentionedType: ptr("app"),
	})))
	if !ok || mentioned.AmbientOnly || mentioned.UnmentionedReply {
		t.Fatalf("mentioned message in an ambient chat = %#v ok=%t", mentioned, ok)
	}
	reply := fromHuman(messageEvent("evt_reply", "om_reply", "oc_refund", "following up"))
	reply.Event.Message.ParentId = ptr("om_parent")
	candidate, ok := r.CommandFromEvent(reply)
	if !ok || candidate.AmbientOnly || !candidate.UnmentionedReply {
		t.Fatalf("unmentioned reply in an ambient chat = %#v ok=%t", candidate, ok)
	}
}

func TestCommandFromEventRendersOtherPeoplesTextMentionsByNameOnlyInNamedPrompt(t *testing.T) {
	r := ambientReceiver()

	event := fromHuman(messageEvent("evt_names", "om_names", "oc_ops", "@_user_1 ask @_user_10 and @_user_2, then @_user_10 again @_user_3",
		&larkim.MentionEvent{Key: ptr("@_user_1"), MentionedType: ptr("app")},
		&larkim.MentionEvent{Key: ptr("@_user_2"), Name: ptr("Alice"), MentionedType: ptr("user"), Id: &larkim.UserId{OpenId: ptr("ou_alice")}},
		&larkim.MentionEvent{Key: ptr("@_user_10"), Name: ptr("Bob"), MentionedType: ptr("user"), Id: &larkim.UserId{OpenId: ptr("ou_bob")}},
		&larkim.MentionEvent{Key: ptr("@_user_3"), Name: ptr("Deploy Bot"), MentionedType: ptr("bot"), Id: &larkim.UserId{OpenId: ptr("ou_other_bot")}},
	))
	cmd, ok := r.CommandFromEvent(event)
	if !ok {
		t.Fatal("mentioned message was discarded")
	}
	// The legacy prompt, and therefore the legacy command view, keeps its
	// historical rendering byte-for-byte — including stripping the bot's
	// "@_user_1" out of "@_user_10" — and never carries a display name.
	if cmd.Prompt != "ask 0 and @_user_2, then 0 again @_user_3" || cmd.Command != "ask" {
		t.Fatalf("legacy prompt changed: %#v", cmd)
	}
	if cmd.NamedPrompt != "ask @Bob and @Alice, then @Bob again @Deploy Bot" {
		t.Fatalf("named prompt = %q", cmd.NamedPrompt)
	}
	want := []Mention{{UserID: "ou_bob", Name: "Bob"}, {UserID: "ou_alice", Name: "Alice"}}
	if !reflect.DeepEqual(cmd.Mentions, want) {
		t.Fatalf("mentions = %#v, want %#v (bots excluded, appearance order, no repeats)", cmd.Mentions, want)
	}
}

func TestCommandFromEventRendersPostMentionsByNameOnlyInNamedPrompt(t *testing.T) {
	r := ambientReceiver()

	content := `{"title":"","content":[[` +
		`{"tag":"at","user_id":"@_user_1","user_name":"Nous"},` +
		`{"tag":"text","text":" loop in "},` +
		`{"tag":"at","user_id":"@_user_2","user_name":"Stale Label"},` +
		`{"tag":"text","text":" and "},` +
		`{"tag":"at","user_id":"ou_unlisted","user_name":"Carol"}]]}`
	event := fromHuman(postMessageEvent("evt_post_names", "om_post_names", "oc_ops", content,
		&larkim.MentionEvent{Key: ptr("@_user_1"), MentionedType: ptr("app")},
		&larkim.MentionEvent{Key: ptr("@_user_2"), Name: ptr("Alice"), MentionedType: ptr("user"), Id: &larkim.UserId{OpenId: ptr("ou_alice")}},
	))
	cmd, ok := r.CommandFromEvent(event)
	if !ok {
		t.Fatal("mentioned post was discarded")
	}
	if cmd.Prompt != "loop in @participant and @participant" {
		t.Fatalf("legacy post prompt changed: %q", cmd.Prompt)
	}
	if cmd.NamedPrompt != "loop in @Alice and @Carol" {
		t.Fatalf("named post prompt = %q", cmd.NamedPrompt)
	}
	want := []Mention{{UserID: "ou_alice", Name: "Alice"}, {UserID: "ou_unlisted", Name: "Carol"}}
	if !reflect.DeepEqual(cmd.Mentions, want) {
		t.Fatalf("post mentions = %#v, want %#v", cmd.Mentions, want)
	}
}

func TestCommandFromEventWithoutOtherMentionsHasIdenticalNamedPrompt(t *testing.T) {
	r := ambientReceiver()

	cmd, ok := r.CommandFromEvent(fromHuman(messageEvent("evt_plain_mention", "om_plain_mention", "oc_ops", "@_user_1 status prod", &larkim.MentionEvent{
		Key: ptr("@_user_1"), MentionedType: ptr("app"),
	})))
	if !ok || cmd.NamedPrompt != cmd.Prompt || cmd.Prompt != "status prod" || cmd.Mentions != nil {
		t.Fatalf("plain mentioned prompt = %#v", cmd)
	}
}

func TestNamedPromptStripsTheBotNameOnlyAsAWholeWord(t *testing.T) {
	r := NewCommandReceiver(CommandReceiverConfig{
		AppID:     "cli_test",
		AppSecret: "secret",
		Channels:  map[string]string{"ops": "oc_ops"},
		BotNames:  []string{"Al"},
	}, nil, nil)

	cmd, ok := r.CommandFromEvent(fromHuman(messageEvent("evt_al", "om_al", "oc_ops", "@_user_1 @_user_2 please look",
		&larkim.MentionEvent{Key: ptr("@_user_1"), Name: ptr("Al"), MentionedType: ptr("bot")},
		&larkim.MentionEvent{Key: ptr("@_user_2"), Name: ptr("Alice"), MentionedType: ptr("user"), Id: &larkim.UserId{OpenId: ptr("ou_alice")}},
	)))
	if !ok {
		t.Fatal("mentioned message was discarded")
	}
	if cmd.NamedPrompt != "@Alice please look" {
		t.Fatalf("named prompt = %q, want the person's name intact", cmd.NamedPrompt)
	}
}

func TestPostOnlyMentionsNeverListTheAllPseudoMention(t *testing.T) {
	r := ambientReceiver()

	content := `{"title":"","content":[[` +
		`{"tag":"at","user_id":"@_user_1","user_name":"Nous"},` +
		`{"tag":"text","text":" heads up "},` +
		`{"tag":"at","user_id":"all","user_name":"Everyone"}]]}`
	cmd, ok := r.CommandFromEvent(fromHuman(postMessageEvent("evt_all", "om_all", "oc_ops", content,
		&larkim.MentionEvent{Key: ptr("@_user_1"), MentionedType: ptr("app")},
	)))
	if !ok {
		t.Fatal("mentioned post was discarded")
	}
	if cmd.Mentions != nil {
		t.Fatalf("@all became a person: %#v", cmd.Mentions)
	}
}
