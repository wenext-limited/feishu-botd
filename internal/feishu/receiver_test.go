package feishu

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"

	larkevent "github.com/larksuite/oapi-sdk-go/v3/event"
	larkcallback "github.com/larksuite/oapi-sdk-go/v3/event/dispatcher/callback"
	larkim "github.com/larksuite/oapi-sdk-go/v3/service/im/v1"
)

func TestCommandFromEventParsesMentionedTextCommand(t *testing.T) {
	r := NewCommandReceiver(CommandReceiverConfig{
		AppID:     "cli_test",
		AppSecret: "secret",
		Channels:  map[string]string{"ops": "oc_ops"},
		BotOpenID: "ou_bot",
	}, nil, nil)

	cmd, ok := r.CommandFromEvent(messageEvent("evt_1", "om_1", "oc_ops", "@_user_1 status prod now", &larkim.MentionEvent{
		Key:           ptr("@_user_1"),
		MentionedType: ptr("app"),
	}))
	if !ok {
		t.Fatal("expected command")
	}
	if cmd.DeliveryID != expectedMessageEventDeliveryID("evt_1") || cmd.Command != "status" || cmd.Text != "prod now" || cmd.Prompt != "status prod now" || cmd.ChatAlias != "ops" || cmd.SenderID != "ou_sender" {
		t.Fatalf("command = %#v", cmd)
	}
	if cmd.ChatID != "oc_ops" || cmd.ConversationID != expectedConversationID("oc_ops") {
		t.Fatalf("private route/conversation = %#v", cmd)
	}
	if cmd.Metadata["message_id"] != "om_1" || cmd.Metadata["chat_type"] != "group" {
		t.Fatalf("metadata = %#v", cmd.Metadata)
	}
	if cmd.Metadata["create_time"] != "1754380800123" {
		t.Fatalf("trigger boundary metadata = %#v", cmd.Metadata)
	}
	if cmd.UnmentionedReply {
		t.Fatal("mentioned group message was marked as an unmentioned reply")
	}
	if _, leaked := cmd.Metadata["chat_id"]; leaked {
		t.Fatalf("metadata leaked raw chat id: %#v", cmd.Metadata)
	}
}

func TestCommandFromEventAcceptsUnmentionedGroupReplyAsPrivateCandidate(t *testing.T) {
	r := NewCommandReceiver(CommandReceiverConfig{
		AppAlias:  "support",
		AppID:     "cli_test",
		AppSecret: "secret",
		Channels:  map[string]string{"ops": "oc_ops"},
		BotOpenID: "ou_bot",
	}, nil, nil)

	event := messageEvent("evt_reply", "om_reply", "oc_ops", "what about retries?")
	event.Event.Message.ParentId = ptr("om_agent_answer")
	cmd, ok := r.CommandFromEvent(event)
	if !ok {
		t.Fatal("unmentioned reply to a group message was discarded before ownership lookup")
	}
	if !cmd.UnmentionedReply {
		t.Fatal("unmentioned group reply did not retain its private routing marker")
	}
	if cmd.Metadata["parent_id"] != "om_agent_answer" {
		t.Fatalf("parent metadata = %#v", cmd.Metadata)
	}
	if cmd.Prompt != "what about retries?" || cmd.ChatAlias != "ops" {
		t.Fatalf("reply candidate = %#v", cmd)
	}
	body, err := json.Marshal(cmd)
	if err != nil {
		t.Fatalf("marshal reply candidate: %v", err)
	}
	if strings.Contains(string(body), "UnmentionedReply") {
		t.Fatalf("serialized command leaked private reply routing state: %s", body)
	}
}

func TestCommandFromEventStillRejectsUnmentionedGroupMessageWithoutParent(t *testing.T) {
	r := NewCommandReceiver(CommandReceiverConfig{
		AppID:     "cli_test",
		AppSecret: "secret",
		Channels:  map[string]string{"ops": "oc_ops"},
		BotOpenID: "ou_bot",
	}, nil, nil)

	if _, ok := r.CommandFromEvent(messageEvent("evt_plain", "om_plain", "oc_ops", "not for the bot")); ok {
		t.Fatal("ordinary unmentioned group message was accepted")
	}
}

func TestCommandHandlerFailureLogOmitsInboundAndErrorContent(t *testing.T) {
	var output bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&output, nil))
	const (
		privateChannel = "private-routing-alias"
		privateEvent   = "evt_private_correlation"
		privatePrompt  = "private-command customer-secret"
		privateError   = "handler echoed customer-secret"
	)
	handled := false
	r := NewCommandReceiver(CommandReceiverConfig{
		AppID:     "cli_test",
		AppSecret: "secret",
		Channels:  map[string]string{privateChannel: "oc_private_route"},
		BotOpenID: "ou_bot",
	}, func(context.Context, InboundCommand) error {
		handled = true
		return errors.New(privateError)
	}, logger)

	err := r.handleMessage(context.Background(), messageEvent(
		privateEvent,
		"om_private_route",
		"oc_private_route",
		"@_user_1 "+privatePrompt,
		&larkim.MentionEvent{Key: ptr("@_user_1"), MentionedType: ptr("app")},
	))
	if err != nil || !handled {
		t.Fatalf("handle message err=%v handled=%v", err, handled)
	}
	logged := output.String()
	for _, private := range []string{privateChannel, privateEvent, privatePrompt, "customer-secret", privateError, "oc_private_route", "om_private_route"} {
		if strings.Contains(logged, private) {
			t.Fatalf("command handler failure log leaked %q: %s", private, logged)
		}
	}
	for _, safe := range []string{"command handler failed", "operation=command", "error_class=handler_error"} {
		if !strings.Contains(logged, safe) {
			t.Fatalf("command handler failure log missing %q: %s", safe, logged)
		}
	}
}

func TestEventDispatcherLogOmitsEventHeadersBodyAndErrors(t *testing.T) {
	var output bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&output, nil))
	r := NewCommandReceiver(CommandReceiverConfig{
		AppID:     "cli_test",
		AppSecret: "secret",
	}, nil, logger)

	const (
		privateHeader = "private-event-header"
		privateBody   = "private-event-body"
		privatePath   = "private-event-path"
	)
	response := r.client.EventHandler().Handle(context.Background(), &larkevent.EventReq{
		Header:     map[string][]string{"X-Private": {privateHeader}},
		Body:       []byte(privateBody),
		RequestURI: "/" + privatePath,
	})
	if response == nil {
		t.Fatal("expected invalid event response")
	}

	logged := output.String()
	for _, private := range []string{privateHeader, privateBody, privatePath} {
		if strings.Contains(logged, private) {
			t.Fatalf("event dispatcher log leaked %q: %s", private, logged)
		}
	}
	if !strings.Contains(logged, "event_class=sdk_error") {
		t.Fatalf("event dispatcher log omitted safe error classification: %s", logged)
	}
}

func TestReceiverHandlerErrorClassUsesFixedVocabulary(t *testing.T) {
	for _, test := range []struct {
		name string
		err  error
		want string
	}{
		{name: "handler", err: errors.New("private handler details"), want: "handler_error"},
		{name: "canceled", err: context.Canceled, want: "context_canceled"},
		{name: "deadline", err: context.DeadlineExceeded, want: "deadline_exceeded"},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := receiverHandlerErrorClass(test.err); got != test.want {
				t.Fatalf("error class = %q, want %q", got, test.want)
			}
		})
	}
}

func TestSafeWebSocketStartErrorNeverReturnsSDKDetails(t *testing.T) {
	const privateError = "websocket failed at wss://example.invalid?ticket=private-ticket"
	err := safeWebSocketStartError(errors.New(privateError))
	if err != errCommandReceiverUnavailable || strings.Contains(err.Error(), "private-ticket") {
		t.Fatalf("sanitized websocket error = %v", err)
	}
	if !errors.Is(safeWebSocketStartError(context.Canceled), context.Canceled) {
		t.Fatal("context cancellation classification was not preserved")
	}
	if !errors.Is(safeWebSocketStartError(context.DeadlineExceeded), context.DeadlineExceeded) {
		t.Fatal("context deadline classification was not preserved")
	}
}

func TestCommandFromEventAcceptsP2PWithoutMentionAndPreservesPrompt(t *testing.T) {
	r := NewCommandReceiver(CommandReceiverConfig{
		AppID:     "cli_test",
		AppSecret: "secret",
		Channels:  map[string]string{"ops": "oc_ops"},
	}, nil, nil)

	event := messageEvent("evt_direct", "om_direct", "oc_direct", "  /Ask  first line\n second   line  ")
	*event.Event.Message.ChatType = "p2p"
	cmd, ok := r.CommandFromEvent(event)
	if !ok {
		t.Fatal("expected direct command")
	}
	if cmd.DeliveryID != expectedMessageEventDeliveryID("evt_direct") || cmd.Command != "ask" || cmd.Text != "first line second line" {
		t.Fatalf("compatible command fields = %#v", cmd)
	}
	if cmd.Prompt != "/Ask  first line\n second   line" {
		t.Fatalf("prompt = %q", cmd.Prompt)
	}
	if cmd.ChatAlias != "direct" || cmd.ChatID != "oc_direct" {
		t.Fatalf("direct route = %#v", cmd)
	}
	if cmd.ConversationID != expectedConversationID("oc_direct") || strings.Contains(cmd.ConversationID, "oc_direct") {
		t.Fatalf("conversation_id = %q", cmd.ConversationID)
	}
	if cmd.Metadata["chat_type"] != "p2p" {
		t.Fatalf("metadata = %#v", cmd.Metadata)
	}
	body, err := json.Marshal(cmd)
	if err != nil {
		t.Fatalf("marshal command: %v", err)
	}
	if strings.Contains(string(body), "oc_direct") {
		t.Fatalf("serialized command leaked raw chat id: %s", body)
	}
}

func TestCommandFromEventAcceptsMentionedPostTriggerWithPlaceholders(t *testing.T) {
	r := NewCommandReceiver(CommandReceiverConfig{
		AppID:     "cli_test",
		AppSecret: "secret",
		Channels:  map[string]string{"ops": "oc_ops"},
		BotOpenID: "ou_bot",
	}, nil, nil)

	content := `{"title":"","content":[[` +
		`{"tag":"at","user_id":"ou_bot","user_name":"Nous"},` +
		`{"tag":"text","text":"看看这个"},` +
		`{"tag":"media","file_key":"video_1"}],[` +
		`{"tag":"at","user_id":"ou_colleague","user_name":"Private Name"},` +
		`{"tag":"text","text":"reported it"},` +
		`{"tag":"img","image_key":"img_1"}]]}`
	event := postMessageEvent("evt_post", "om_post", "oc_ops", content, &larkim.MentionEvent{
		Key:           ptr("@_user_1"),
		MentionedType: ptr("app"),
	})
	cmd, ok := r.CommandFromEvent(event)
	if !ok {
		t.Fatal("expected a rich-text trigger to be accepted")
	}
	want := "看看这个" + placeholderVideo + "\n@participant" + "reported it" + placeholderImage
	if cmd.Prompt != want {
		t.Fatalf("prompt = %q, want %q", cmd.Prompt, want)
	}
	if strings.Contains(cmd.Prompt, "Private Name") || strings.Contains(cmd.Prompt, "ou_colleague") {
		t.Fatalf("prompt leaked a mention identity: %q", cmd.Prompt)
	}
	if cmd.Metadata["message_type"] != "post" || cmd.ChatAlias != "ops" {
		t.Fatalf("command = %#v", cmd)
	}
}

func TestCommandFromEventAcceptsP2PPostTrigger(t *testing.T) {
	r := NewCommandReceiver(CommandReceiverConfig{
		AppID:     "cli_test",
		AppSecret: "secret",
		Channels:  map[string]string{"ops": "oc_ops"},
	}, nil, nil)

	content := `{"title":"","content":[[{"tag":"text","text":"这个视频里的问题帮我查一下"},{"tag":"media","file_key":"video_1"}]]}`
	event := postMessageEvent("evt_direct_post", "om_direct_post", "oc_direct", content)
	*event.Event.Message.ChatType = "p2p"
	cmd, ok := r.CommandFromEvent(event)
	if !ok {
		t.Fatal("expected a direct rich-text trigger to be accepted")
	}
	if cmd.Prompt != "这个视频里的问题帮我查一下"+placeholderVideo {
		t.Fatalf("prompt = %q", cmd.Prompt)
	}
	if cmd.ChatAlias != "direct" {
		t.Fatalf("route = %#v", cmd)
	}
}

func TestCommandFromEventStillRejectsUnmentionedGroupPost(t *testing.T) {
	r := NewCommandReceiver(CommandReceiverConfig{
		AppID:     "cli_test",
		AppSecret: "secret",
		Channels:  map[string]string{"ops": "oc_ops"},
		BotOpenID: "ou_bot",
	}, nil, nil)

	content := `{"title":"","content":[[{"tag":"text","text":"just chatting"},{"tag":"media","file_key":"video_1"}]]}`
	if _, ok := r.CommandFromEvent(postMessageEvent("evt_plain_post", "om_plain_post", "oc_ops", content)); ok {
		t.Fatal("unmentioned group post must not become a command")
	}
}

func TestCommandFromEventRejectsPostTriggerWithNoUsableContent(t *testing.T) {
	r := NewCommandReceiver(CommandReceiverConfig{
		AppID:     "cli_test",
		AppSecret: "secret",
		Channels:  map[string]string{"ops": "oc_ops"},
		BotOpenID: "ou_bot",
	}, nil, nil)

	// Only the bot's own mention: nothing remains to ask.
	content := `{"title":"","content":[[{"tag":"at","user_id":"ou_bot","user_name":"Nous"}]]}`
	event := postMessageEvent("evt_empty_post", "om_empty_post", "oc_ops", content, &larkim.MentionEvent{
		Key:           ptr("@_user_1"),
		MentionedType: ptr("app"),
	})
	if _, ok := r.CommandFromEvent(event); ok {
		t.Fatal("a mention-only post must not become a command")
	}
}

func TestCommandFromEventUsesOpaqueStableDeliveryIDWhenEventIDIsMissing(t *testing.T) {
	r := NewCommandReceiver(CommandReceiverConfig{
		AppID:     "cli_test",
		AppSecret: "secret",
		Channels:  map[string]string{"ops": "oc_ops"},
		BotOpenID: "ou_bot",
	}, nil, nil)

	const rawMessageID = "om_private_message_route"
	event := messageEvent("", rawMessageID, "oc_ops", "@_user_1 status", &larkim.MentionEvent{
		Key:           ptr("@_user_1"),
		MentionedType: ptr("app"),
	})
	cmd, ok := r.CommandFromEvent(event)
	if !ok {
		t.Fatal("expected command with message-id fallback")
	}
	want := expectedMessageDeliveryID(rawMessageID)
	if cmd.DeliveryID != want {
		t.Fatalf("delivery_id = %q, want %q", cmd.DeliveryID, want)
	}
	if strings.Contains(cmd.DeliveryID, rawMessageID) {
		t.Fatalf("delivery_id leaked raw message id: %q", cmd.DeliveryID)
	}

	retry, ok := r.CommandFromEvent(event)
	if !ok || retry.DeliveryID != cmd.DeliveryID {
		t.Fatalf("retry delivery_id = %q, want stable %q", retry.DeliveryID, cmd.DeliveryID)
	}
	other := messageEvent("", "om_other_message", "oc_ops", "@_user_1 status", &larkim.MentionEvent{
		Key:           ptr("@_user_1"),
		MentionedType: ptr("app"),
	})
	otherCommand, ok := r.CommandFromEvent(other)
	if !ok || otherCommand.DeliveryID == cmd.DeliveryID {
		t.Fatalf("distinct message delivery_id = %q, first = %q", otherCommand.DeliveryID, cmd.DeliveryID)
	}
	missing := messageEvent("", "   ", "oc_ops", "@_user_1 status", &larkim.MentionEvent{
		Key:           ptr("@_user_1"),
		MentionedType: ptr("app"),
	})
	if _, ok := r.CommandFromEvent(missing); ok {
		t.Fatal("command without event id or usable message id was accepted")
	}
}

func TestCommandFromEventHashesEventIDAsStableDeliveryID(t *testing.T) {
	r := NewCommandReceiver(CommandReceiverConfig{
		AppID:     "cli_test",
		AppSecret: "secret",
		Channels:  map[string]string{"ops": "oc_ops"},
		BotOpenID: "ou_bot",
	}, nil, nil)

	cmd, ok := r.CommandFromEvent(messageEvent("evt_original", "om_private_message_route", "oc_ops", "@_user_1 status", &larkim.MentionEvent{
		Key:           ptr("@_user_1"),
		MentionedType: ptr("app"),
	}))
	if !ok {
		t.Fatal("expected command")
	}
	want := expectedMessageEventDeliveryID("evt_original")
	if cmd.DeliveryID != want || strings.Contains(cmd.DeliveryID, "evt_original") {
		t.Fatalf("delivery_id = %q, want opaque stable id %q", cmd.DeliveryID, want)
	}
}

func TestConversationIDIsStableAndChatScoped(t *testing.T) {
	first := conversationID("oc_same")
	if first == "" || first != conversationID(" oc_same ") {
		t.Fatalf("conversation id not stable: %q", first)
	}
	if first == conversationID("oc_other") {
		t.Fatalf("different chats shared conversation id: %q", first)
	}
	if len(first) != len("conv_")+sha256.Size*2 || strings.Contains(first, "oc_same") {
		t.Fatalf("conversation id is not an opaque SHA-256 id: %q", first)
	}
}

func TestCommandFromEventScopesGroupConversationToThread(t *testing.T) {
	r := NewCommandReceiver(CommandReceiverConfig{
		AppID: "app_test", AppSecret: "test-placeholder",
		Channels:  map[string]string{"ops": "oc_ops"},
		BotOpenID: "ou_bot",
	}, nil, nil)
	first := messageEvent("evt_1", "om_1", "oc_ops", "@_bot ask one", &larkim.MentionEvent{Key: ptr("@_bot"), MentionedType: ptr("app")})
	second := messageEvent("evt_2", "om_2", "oc_ops", "@_bot ask two", &larkim.MentionEvent{Key: ptr("@_bot"), MentionedType: ptr("app")})
	first.Event.Message.ThreadId = ptr("thread_one")
	second.Event.Message.ThreadId = ptr("thread_two")

	one, ok := r.CommandFromEvent(first)
	if !ok {
		t.Fatal("first thread was not accepted")
	}
	two, ok := r.CommandFromEvent(second)
	if !ok {
		t.Fatal("second thread was not accepted")
	}
	if one.ConversationID == two.ConversationID || strings.Contains(one.ConversationID, "thread_one") {
		t.Fatalf("thread-scoped conversation ids are not opaque/distinct: %q %q", one.ConversationID, two.ConversationID)
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
		Key:           ptr("@_user_1"),
		Name:          ptr("buildbot"),
		MentionedType: ptr("bot"),
	}))
	if !ok {
		t.Fatal("expected command")
	}
	if cmd.Command != "deploy" || cmd.Text != "main" {
		t.Fatalf("command = %#v", cmd)
	}
}

func TestCommandFromEventRejectsNameOnlyNonBotMention(t *testing.T) {
	r := NewCommandReceiver(CommandReceiverConfig{
		AppID:     "cli_test",
		AppSecret: "secret",
		Channels:  map[string]string{"ops": "oc_ops"},
		BotNames:  []string{"BuildBot"},
	}, nil, nil)

	for _, mentionedType := range []string{"", "user", "app", "unknown"} {
		t.Run("type_"+mentionedType, func(t *testing.T) {
			if _, ok := r.CommandFromEvent(messageEvent("evt_1", "om_1", "oc_ops", "@_user_1 status", &larkim.MentionEvent{
				Key:           ptr("@_user_1"),
				Name:          ptr("BuildBot"),
				MentionedType: ptr(mentionedType),
			})); ok {
				t.Fatalf("name-only %q mention produced a command", mentionedType)
			}
		})
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

func TestCommandFromEventAllowsMentionedUnconfiguredGroupWhenEnabled(t *testing.T) {
	r := NewCommandReceiver(CommandReceiverConfig{
		AppAlias:                    "nous",
		AppID:                       "cli_test",
		AppSecret:                   "secret",
		Channels:                    map[string]string{"ops": "oc_ops"},
		AllowUnconfiguredGroupChats: true,
		BotOpenID:                   "ou_bot",
	}, nil, nil)

	event := messageEvent("evt_1", "om_1", "oc_unconfigured", "@_bot explain this", &larkim.MentionEvent{
		Key:           ptr("@_bot"),
		MentionedType: ptr("app"),
	})
	cmd, ok := r.CommandFromEvent(event)
	if !ok {
		t.Fatal("mentioned unconfigured group was not accepted")
	}
	if !cmd.UnconfiguredGroup || cmd.ChatID != "oc_unconfigured" || cmd.Command != "explain" || cmd.Text != "this" {
		t.Fatalf("unconfigured group command = %#v", cmd)
	}
	if cmd.ChatAlias == "" || strings.Contains(cmd.ChatAlias, "oc_unconfigured") {
		t.Fatalf("unconfigured group alias exposed or omitted the raw route: %q", cmd.ChatAlias)
	}
	retry, ok := r.CommandFromEvent(event)
	if !ok || retry.ChatAlias != cmd.ChatAlias {
		t.Fatalf("unconfigured group alias is unstable: first=%q retry=%q", cmd.ChatAlias, retry.ChatAlias)
	}
	if cmd.ChatAlias == unconfiguredGroupAlias("other-app", "oc_unconfigured") ||
		cmd.ChatAlias == unconfiguredGroupAlias("nous", "oc_other") {
		t.Fatal("unconfigured group alias is not scoped by both app and chat")
	}
	body, err := json.Marshal(cmd)
	if err != nil {
		t.Fatalf("marshal command: %v", err)
	}
	if strings.Contains(string(body), "oc_unconfigured") || strings.Contains(string(body), "UnconfiguredGroup") {
		t.Fatalf("serialized command leaked daemon-private routing state: %s", body)
	}

	configured, ok := r.CommandFromEvent(messageEvent("evt_2", "om_2", "oc_ops", "@_bot status", &larkim.MentionEvent{
		Key:           ptr("@_bot"),
		MentionedType: ptr("app"),
	}))
	if !ok || configured.ChatAlias != "ops" || configured.UnconfiguredGroup {
		t.Fatalf("configured channel did not retain precedence: %#v, ok=%t", configured, ok)
	}

	if _, ok := r.CommandFromEvent(messageEvent("evt_3", "om_3", "oc_other", "status")); ok {
		t.Fatal("unmentioned unconfigured group was accepted")
	}
	if _, ok := r.CommandFromEvent(messageEvent("evt_4", "om_4", "oc_other", "@_other status", &larkim.MentionEvent{
		Key: ptr("@_other"),
		Id:  &larkim.UserId{OpenId: ptr("ou_other")},
	})); ok {
		t.Fatal("unconfigured group with a different mention identity was accepted")
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

func TestCommandFromEventStrongBotIDOverridesMatchingDisplayName(t *testing.T) {
	r := NewCommandReceiver(CommandReceiverConfig{
		AppID: "cli_test", AppSecret: "secret", Channels: map[string]string{"ops": "oc_ops"},
		BotOpenID: "ou_bot", BotNames: []string{"BuildBot"},
	}, nil, nil)
	if _, ok := r.CommandFromEvent(messageEvent("evt_1", "om_1", "oc_ops", "@_user_1 status", &larkim.MentionEvent{
		Key: ptr("@_user_1"), Name: ptr("BuildBot"), Id: &larkim.UserId{OpenId: ptr("ou_other")},
	})); ok {
		t.Fatal("same-name mention with a mismatching strong id produced a command")
	}
}

func TestCommandFromEventStrongBotIDRemainsAuthoritative(t *testing.T) {
	r := NewCommandReceiver(CommandReceiverConfig{
		AppID: "cli_test", AppSecret: "secret", Channels: map[string]string{"ops": "oc_ops"},
		BotOpenID: "ou_bot", BotNames: []string{"BuildBot"},
	}, nil, nil)
	cmd, ok := r.CommandFromEvent(messageEvent("evt_1", "om_1", "oc_ops", "@_user_1 status", &larkim.MentionEvent{
		Key: ptr("@_user_1"), Name: ptr("NotTheBotName"), MentionedType: ptr("user"),
		Id: &larkim.UserId{OpenId: ptr("ou_bot")},
	}))
	if !ok || cmd.Command != "status" {
		t.Fatalf("matching strong bot id was not authoritative: %#v, ok=%v", cmd, ok)
	}
}

func TestCommandFromEventRejectsUnverifiedGroupMention(t *testing.T) {
	r := NewCommandReceiver(CommandReceiverConfig{
		AppID: "cli_test", AppSecret: "secret", Channels: map[string]string{"ops": "oc_ops"},
	}, nil, nil)
	if _, ok := r.CommandFromEvent(messageEvent("evt_1", "om_1", "oc_ops", "@_user_1 status", &larkim.MentionEvent{
		Key: ptr("@_user_1"), MentionedType: ptr("app"), Id: &larkim.UserId{OpenId: ptr("ou_bot")},
	})); ok {
		t.Fatal("group mention without a configured bot identity produced a command")
	}
}

func TestCardActionFromEventPreservesTypedJSONWithoutCallbackSecrets(t *testing.T) {
	r := NewCommandReceiver(CommandReceiverConfig{AppID: "cli_test", AppSecret: "secret"}, nil, nil)
	event := cardActionEvent()

	action, ok := r.CardActionFromEvent(event)
	if !ok {
		t.Fatal("expected card action")
	}
	if action.DeliveryID != expectedCardActionDeliveryID("evt_action") || action.MessageID != "om_card" || action.SenderID != "ou_actor" {
		t.Fatalf("action identity = %#v", action)
	}
	if action.ConversationID != expectedConversationID("oc_secret") {
		t.Fatalf("conversation id = %q", action.ConversationID)
	}
	if action.Tag != "button" || action.Name != "approve" || action.Option != "primary" || action.Timezone != "Asia/Shanghai" || action.InputValue != "reason" || !action.Checked {
		t.Fatalf("action fields = %#v", action)
	}
	if action.ValueJSON != `{"action":"approve","confirmed":true,"nested":{"count":2}}` {
		t.Fatalf("value_json = %s", action.ValueJSON)
	}
	if action.FormValueJSON != `{"comment":"ship it","labels":["safe","reviewed"]}` {
		t.Fatalf("form_value_json = %s", action.FormValueJSON)
	}
	if strings.Join(action.Options, ",") != "one,two" {
		t.Fatalf("options = %#v", action.Options)
	}
	event.Event.Action.Options[0] = "mutated"
	if action.Options[0] != "one" {
		t.Fatalf("action options alias SDK memory: %#v", action.Options)
	}

	body, err := json.Marshal(action)
	if err != nil {
		t.Fatalf("marshal action: %v", err)
	}
	for _, secret := range []string{"card_update_token", "evt_action", "oc_secret", "om_card"} {
		if strings.Contains(string(body), secret) {
			t.Fatalf("serialized action leaked %q: %s", secret, body)
		}
	}
}

func TestCardActionHandlerIsRegisteredAndAcknowledgesFailureWithToast(t *testing.T) {
	r := NewCommandReceiver(CommandReceiverConfig{AppID: "cli_test", AppSecret: "secret"}, nil, nil)
	var handled InboundCardAction
	r.SetCardActionHandler(func(_ context.Context, action InboundCardAction) error {
		handled = action
		return errors.New("queue unavailable")
	})

	payload := []byte(`{
		"schema":"2.0",
		"header":{"event_id":"evt_registered","event_type":"card.action.trigger"},
		"event":{
			"operator":{"open_id":"ou_actor"},
			"token":"must-not-escape",
			"context":{"open_message_id":"om_registered","open_chat_id":"oc_registered"},
			"action":{"tag":"button","name":"retry","value":{"action":"retry"}}
		}
	}`)
	resp, err := r.client.EventHandler().Do(context.Background(), payload)
	if err != nil {
		t.Fatalf("dispatch card action: %v", err)
	}
	typed, ok := resp.(*larkcallback.CardActionTriggerResponse)
	if !ok || typed == nil || typed.Toast == nil {
		t.Fatalf("callback response = %#v, want error toast acknowledgement", resp)
	}
	if typed.Toast.Type != "error" || typed.Toast.Content != "Agent unavailable. Please retry." {
		t.Fatalf("callback toast = %#v", typed.Toast)
	}
	if handled.DeliveryID != expectedCardActionDeliveryID("evt_registered") || handled.Name != "retry" || handled.MessageID != "om_registered" {
		t.Fatalf("handled action = %#v", handled)
	}
}

func TestCardActionHandlerAcknowledgesSuccessWithoutMutatingCard(t *testing.T) {
	r := NewCommandReceiver(CommandReceiverConfig{AppID: "cli_test", AppSecret: "secret"}, nil, nil)
	r.SetCardActionHandler(func(context.Context, InboundCardAction) error { return nil })
	resp, err := r.handleCardAction(context.Background(), cardActionEvent())
	if err != nil || resp != nil {
		t.Fatalf("success acknowledgement response=%#v err=%v", resp, err)
	}
}

func TestCardActionHandlerFailureLogOmitsCallbackAndErrorContent(t *testing.T) {
	var output bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&output, nil))
	r := NewCommandReceiver(CommandReceiverConfig{AppID: "cli_test", AppSecret: "secret"}, nil, logger)
	const (
		maliciousAction = "approve user-secret-from-callback"
		maliciousError  = "handler echoed private form value"
	)
	r.SetCardActionHandler(func(context.Context, InboundCardAction) error {
		return errors.New(maliciousError)
	})
	event := cardActionEvent()
	event.Event.Action.Name = maliciousAction

	resp, err := r.handleCardAction(context.Background(), event)
	if err != nil || resp == nil || resp.Toast == nil || resp.Toast.Type != "error" {
		t.Fatalf("failure acknowledgement response=%#v err=%v", resp, err)
	}
	logged := output.String()
	for _, private := range []string{maliciousAction, maliciousError} {
		if strings.Contains(logged, private) {
			t.Fatalf("card action failure log leaked %q: %s", private, logged)
		}
	}
	for _, safe := range []string{"card action handler failed", "operation=card_action", "error_class=handler_error"} {
		if !strings.Contains(logged, safe) {
			t.Fatalf("card action failure log missing %q: %s", safe, logged)
		}
	}
}

func TestCardActionFromEventRejectsMissingEventID(t *testing.T) {
	r := NewCommandReceiver(CommandReceiverConfig{AppID: "cli_test", AppSecret: "secret"}, nil, nil)
	event := cardActionEvent()
	event.EventV2Base.Header.EventID = ""
	if _, ok := r.CardActionFromEvent(event); ok {
		t.Fatal("card action without event id was accepted")
	}
}

func messageEvent(eventID, messageID, chatID, text string, mentions ...*larkim.MentionEvent) *larkim.P2MessageReceiveV1 {
	contentBytes, _ := json.Marshal(map[string]string{"text": text})
	return rawMessageEvent(eventID, messageID, chatID, "text", string(contentBytes), mentions...)
}

func postMessageEvent(eventID, messageID, chatID, content string, mentions ...*larkim.MentionEvent) *larkim.P2MessageReceiveV1 {
	return rawMessageEvent(eventID, messageID, chatID, "post", content, mentions...)
}

func rawMessageEvent(eventID, messageID, chatID, messageType, content string, mentions ...*larkim.MentionEvent) *larkim.P2MessageReceiveV1 {
	chatType := "group"
	for _, mention := range mentions {
		if mention != nil && mention.Id == nil && strings.EqualFold(deref(mention.MentionedType), "app") {
			mention.Id = &larkim.UserId{OpenId: ptr("ou_bot")}
		}
	}
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
				CreateTime:  ptr("1754380800123"),
				ChatId:      ptr(chatID),
				ChatType:    ptr(chatType),
				MessageType: &messageType,
				Content:     &content,
				Mentions:    mentions,
			},
		},
	}
}

func cardActionEvent() *larkcallback.CardActionTriggerEvent {
	return &larkcallback.CardActionTriggerEvent{
		EventV2Base: &larkevent.EventV2Base{
			Header: &larkevent.EventHeader{EventID: "evt_action"},
		},
		Event: &larkcallback.CardActionTriggerRequest{
			Operator: &larkcallback.Operator{OpenID: "ou_actor"},
			Token:    "card_update_token",
			Host:     "im_message",
			Context: &larkcallback.Context{
				OpenMessageID: "om_card",
				OpenChatID:    "oc_secret",
			},
			Action: &larkcallback.CallBackAction{
				Tag:        "button",
				Name:       "approve",
				Option:     "primary",
				Timezone:   "Asia/Shanghai",
				InputValue: "reason",
				Options:    []string{"one", "two"},
				Checked:    true,
				Value: map[string]interface{}{
					"action":    "approve",
					"confirmed": true,
					"nested":    map[string]interface{}{"count": 2},
				},
				FormValue: map[string]interface{}{
					"comment": "ship it",
					"labels":  []interface{}{"safe", "reviewed"},
				},
			},
		},
	}
}

func expectedConversationID(chatID string) string {
	sum := sha256.Sum256([]byte("feishu-botd/conversation/v1\x00" + strings.TrimSpace(chatID)))
	return "conv_" + hex.EncodeToString(sum[:])
}

func expectedMessageDeliveryID(messageID string) string {
	sum := sha256.Sum256([]byte("feishu-botd/message-delivery/v1\x00" + strings.TrimSpace(messageID)))
	return "delivery_msg_" + hex.EncodeToString(sum[:])
}

func expectedMessageEventDeliveryID(eventID string) string {
	sum := sha256.Sum256([]byte("feishu-botd/message-event-delivery/v1\x00" + strings.TrimSpace(eventID)))
	return "delivery_event_" + hex.EncodeToString(sum[:])
}

func expectedCardActionDeliveryID(eventID string) string {
	sum := sha256.Sum256([]byte("feishu-botd/card-action-delivery/v1\x00" + strings.TrimSpace(eventID)))
	return "delivery_action_" + hex.EncodeToString(sum[:])
}

func ptr(s string) *string {
	return &s
}
