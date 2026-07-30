package feishu

import (
	"strings"
	"testing"

	"feishu-botd/internal/config"

	larkim "github.com/larksuite/oapi-sdk-go/v3/service/im/v1"
)

func TestDefaultAppConversationIDDerivationPinned(t *testing.T) {
	tests := []struct {
		name   string
		alias  string
		chatID string
		thread string
		want   string
	}{
		{
			name:   "empty alias flat chat",
			chatID: "oc_known_chat",
			want:   "conv_0281624435ef00d83d7ad4c0991b974b0465dd437cecb34bf65747c26e853231",
		},
		{
			name:   "reserved default flat chat",
			alias:  config.DefaultAppAlias,
			chatID: "oc_known_chat",
			want:   "conv_0281624435ef00d83d7ad4c0991b974b0465dd437cecb34bf65747c26e853231",
		},
		{
			name:   "reserved default thread",
			alias:  config.DefaultAppAlias,
			chatID: "oc_known_chat",
			thread: "omt_known_thread",
			want:   "conv_c90b2c43dab0674af7c357c69cfeda47d2b59405b17f91583517c491d44c6914",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			receiver := NewCommandReceiver(CommandReceiverConfig{
				AppAlias:  test.alias,
				AppID:     "cli_test",
				AppSecret: "secret",
				Channels:  map[string]string{"ops": test.chatID},
				BotOpenID: "ou_bot",
			}, nil, nil)
			event := messageEvent(
				"evt_known",
				"om_known",
				test.chatID,
				"@_user_1 status",
				&larkim.MentionEvent{Key: ptr("@_user_1"), MentionedType: ptr("app")},
			)
			if test.thread != "" {
				event.Event.Message.ThreadId = ptr(test.thread)
			}
			command, ok := receiver.CommandFromEvent(event)
			if !ok {
				t.Fatal("default receiver rejected pinned event")
			}
			if command.ConversationID != test.want {
				t.Fatalf("conversation id = %q, want pinned %q", command.ConversationID, test.want)
			}
		})
	}
}

func TestDefaultAppDeliveryIDDerivationsPinned(t *testing.T) {
	tests := []struct {
		name  string
		alias string
		got   func(string, string) string
		value string
		want  string
	}{
		{
			name:  "message event empty alias",
			got:   messageEventDeliveryIDForApp,
			value: "evt_known",
			want:  "delivery_event_8ebf851bd313aaf919d2657ea10bf801b2ddabdeaa766ffa160853e2ffcd5456",
		},
		{
			name:  "message event default alias",
			alias: config.DefaultAppAlias,
			got:   messageEventDeliveryIDForApp,
			value: "evt_known",
			want:  "delivery_event_8ebf851bd313aaf919d2657ea10bf801b2ddabdeaa766ffa160853e2ffcd5456",
		},
		{
			name:  "message fallback default alias",
			alias: config.DefaultAppAlias,
			got:   messageDeliveryIDForApp,
			value: "om_known",
			want:  "delivery_msg_dda476c12a71568f6c7bb737a07ffa1d8fa8613e3e58d1c27a39f3dd9678d4ba",
		},
		{
			name:  "card action default alias",
			alias: config.DefaultAppAlias,
			got:   cardActionDeliveryIDForApp,
			value: "evt_action_known",
			want:  "delivery_action_498bcc5cffe652f276fbd8851638816d4d8af7540417eed3ff223d6f744431aa",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := test.got(test.alias, test.value); got != test.want {
				t.Fatalf("delivery id = %q, want pinned %q", got, test.want)
			}
		})
	}
}

func TestMissingEventHeaderFallbackDeliveryIsStableAndAppScoped(t *testing.T) {
	newReceiver := func(alias string) *CommandReceiver {
		return NewCommandReceiver(CommandReceiverConfig{
			AppAlias:  alias,
			AppID:     "cli_test",
			AppSecret: "secret",
			Channels:  map[string]string{"ops": "oc_known_chat"},
			BotOpenID: "ou_bot",
		}, nil, nil)
	}
	const rawMessageID = "om_known"
	event := messageEvent(
		"ignored_event",
		rawMessageID,
		"oc_known_chat",
		"@_user_1 status",
		&larkim.MentionEvent{Key: ptr("@_user_1"), MentionedType: ptr("app")},
	)
	event.EventV2Base.Header = nil

	parse := func(alias string) InboundCommand {
		t.Helper()
		command, ok := newReceiver(alias).CommandFromEvent(event)
		if !ok {
			t.Fatalf("receiver %q rejected no-header event", alias)
		}
		return command
	}
	defaultCommand := parse("")
	defaultRetry := parse(config.DefaultAppAlias)
	betaCommand := parse("beta")
	betaRetry := parse("beta")

	const pinnedDefault = "delivery_msg_dda476c12a71568f6c7bb737a07ffa1d8fa8613e3e58d1c27a39f3dd9678d4ba"
	if defaultCommand.DeliveryID != pinnedDefault || defaultRetry.DeliveryID != pinnedDefault {
		t.Fatalf(
			"default no-header delivery ids = %q / %q, want pinned %q",
			defaultCommand.DeliveryID,
			defaultRetry.DeliveryID,
			pinnedDefault,
		)
	}
	if betaCommand.DeliveryID == pinnedDefault || betaCommand.DeliveryID != betaRetry.DeliveryID {
		t.Fatalf(
			"beta no-header delivery ids are not distinct and stable: default=%q beta=%q retry=%q",
			pinnedDefault,
			betaCommand.DeliveryID,
			betaRetry.DeliveryID,
		)
	}
	if strings.Contains(betaCommand.DeliveryID, "beta") || strings.Contains(betaCommand.DeliveryID, rawMessageID) {
		t.Fatalf("beta no-header delivery id exposed private routing input: %q", betaCommand.DeliveryID)
	}
}

func TestAdditionalAppsNamespaceInboundMessageHandles(t *testing.T) {
	newReceiver := func(alias string) *CommandReceiver {
		return NewCommandReceiver(CommandReceiverConfig{
			AppAlias:  alias,
			AppID:     "cli_test",
			AppSecret: "secret",
			Channels:  map[string]string{"ops": "oc_known_chat"},
			BotOpenID: "ou_bot",
		}, nil, nil)
	}
	event := messageEvent(
		"evt_known",
		"om_known",
		"oc_known_chat",
		"@_user_1 status",
		&larkim.MentionEvent{Key: ptr("@_user_1"), MentionedType: ptr("app")},
	)

	defaultCommand, ok := newReceiver("").CommandFromEvent(event)
	if !ok {
		t.Fatal("default receiver rejected event")
	}
	blueCommand, ok := newReceiver("blue").CommandFromEvent(event)
	if !ok {
		t.Fatal("blue receiver rejected event")
	}
	blueRetry, ok := newReceiver("blue").CommandFromEvent(event)
	if !ok {
		t.Fatal("blue receiver rejected retry")
	}
	greenCommand, ok := newReceiver("green").CommandFromEvent(event)
	if !ok {
		t.Fatal("green receiver rejected event")
	}

	if defaultCommand.AppAlias != config.DefaultAppAlias || defaultCommand.Metadata["app_alias"] != config.DefaultAppAlias {
		t.Fatalf("default app tag = %#v", defaultCommand)
	}
	if blueCommand.AppAlias != "blue" || blueCommand.Metadata["app_alias"] != "blue" {
		t.Fatalf("blue app tag = %#v", blueCommand)
	}
	if blueCommand.DeliveryID == defaultCommand.DeliveryID || blueCommand.ConversationID == defaultCommand.ConversationID {
		t.Fatalf("additional app reused default handles: default=%#v blue=%#v", defaultCommand, blueCommand)
	}
	for _, handle := range []string{blueCommand.DeliveryID, blueCommand.ConversationID} {
		for _, private := range []string{"blue", "evt_known", "om_known", "oc_known_chat"} {
			if strings.Contains(handle, private) {
				t.Fatalf("additional app handle %q exposed %q", handle, private)
			}
		}
	}
	if blueCommand.DeliveryID != blueRetry.DeliveryID || blueCommand.ConversationID != blueRetry.ConversationID {
		t.Fatalf("additional app handles are unstable: first=%#v retry=%#v", blueCommand, blueRetry)
	}
	if greenCommand.DeliveryID == blueCommand.DeliveryID || greenCommand.ConversationID == blueCommand.ConversationID {
		t.Fatalf("additional app aliases share handles: blue=%#v green=%#v", blueCommand, greenCommand)
	}
}

func TestAdditionalAppsNamespaceCardActionHandles(t *testing.T) {
	event := cardActionEvent()
	parse := func(alias string) InboundCardAction {
		t.Helper()
		receiver := NewCommandReceiver(CommandReceiverConfig{
			AppAlias:  alias,
			AppID:     "cli_test",
			AppSecret: "secret",
		}, nil, nil)
		action, ok := receiver.CardActionFromEvent(event)
		if !ok {
			t.Fatalf("receiver %q rejected card action", alias)
		}
		return action
	}

	defaultAction := parse("")
	blueAction := parse("blue")
	blueRetry := parse("blue")
	greenAction := parse("green")
	if defaultAction.AppAlias != config.DefaultAppAlias || blueAction.AppAlias != "blue" {
		t.Fatalf("app tags: default=%#v blue=%#v", defaultAction, blueAction)
	}
	if blueAction.DeliveryID == defaultAction.DeliveryID || blueAction.ConversationID == defaultAction.ConversationID {
		t.Fatalf("additional app reused default action handles: default=%#v blue=%#v", defaultAction, blueAction)
	}
	for _, handle := range []string{blueAction.DeliveryID, blueAction.ConversationID} {
		for _, private := range []string{"blue", "evt_action", "oc_secret"} {
			if strings.Contains(handle, private) {
				t.Fatalf("additional app action handle %q exposed %q", handle, private)
			}
		}
	}
	if blueAction.DeliveryID != blueRetry.DeliveryID || blueAction.ConversationID != blueRetry.ConversationID {
		t.Fatalf("additional app action handles are unstable: first=%#v retry=%#v", blueAction, blueRetry)
	}
	if greenAction.DeliveryID == blueAction.DeliveryID || greenAction.ConversationID == blueAction.ConversationID {
		t.Fatalf("additional app aliases share action handles: blue=%#v green=%#v", blueAction, greenAction)
	}
}

func TestCommandReceiverReportsFixedConnectionStates(t *testing.T) {
	var aliases []string
	var states []ConnectionState
	receiver := NewCommandReceiver(CommandReceiverConfig{
		AppID:     "cli_test",
		AppSecret: "secret",
		ConnectionStateChanged: func(appAlias string, state ConnectionState) {
			aliases = append(aliases, appAlias)
			states = append(states, state)
		},
	}, nil, nil)

	select {
	case <-receiver.InitialReady():
		t.Fatal("receiver reported initial readiness before connecting")
	default:
	}
	for _, state := range []ConnectionState{
		ConnectionStateStarting,
		ConnectionStateConnected,
		ConnectionStateReconnecting,
		ConnectionStateDisconnected,
		ConnectionStateUnavailable,
		ConnectionStateUnavailable, // duplicate state must not emit twice
	} {
		receiver.setConnectionState(state)
	}
	select {
	case <-receiver.InitialReady():
	default:
		t.Fatal("receiver did not report initial readiness after connecting")
	}

	wantStates := []ConnectionState{
		ConnectionStateStarting,
		ConnectionStateConnected,
		ConnectionStateReconnecting,
		ConnectionStateDisconnected,
		ConnectionStateUnavailable,
	}
	if len(states) != len(wantStates) {
		t.Fatalf("states = %#v, want %#v", states, wantStates)
	}
	for index, want := range wantStates {
		if states[index] != want {
			t.Fatalf("state[%d] = %q, want %q", index, states[index], want)
		}
		if aliases[index] != config.DefaultAppAlias {
			t.Fatalf("alias[%d] = %q, want %q", index, aliases[index], config.DefaultAppAlias)
		}
	}
	if got := receiver.ConnectionState(); got != ConnectionStateUnavailable {
		t.Fatalf("current state = %q, want %q", got, ConnectionStateUnavailable)
	}
}
