package service

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"feishu-botd/internal/config"
	"feishu-botd/internal/dedupe"
	"feishu-botd/internal/feishu"
	"feishu-botd/internal/notify"
)

type multiAppTestSend struct {
	chatID  string
	request notify.Request
}

type multiAppTestBackend struct {
	mu sync.Mutex

	alias     string
	messageID string
	readyErr  error
	readySeen chan<- string
	readyWait <-chan struct{}
	sendSeen  chan<- string
	sendWait  <-chan struct{}

	ordinary       []multiAppTestSend
	createdCards   []string
	sentCards      []feishu.CardSendRequest
	contentUpdates []feishu.CardContentUpdate
	settings       []feishu.CardSettingsUpdate
}

func newMultiAppTestBackend(alias string) *multiAppTestBackend {
	return &multiAppTestBackend{alias: alias, messageID: "om_" + alias}
}

func (b *multiAppTestBackend) Ready(context.Context) error {
	b.mu.Lock()
	alias, readyErr, readySeen, readyWait := b.alias, b.readyErr, b.readySeen, b.readyWait
	b.mu.Unlock()
	if readySeen != nil {
		readySeen <- alias
	}
	if readyWait != nil {
		<-readyWait
	}
	return readyErr
}

func (b *multiAppTestBackend) Send(_ context.Context, chatID string, req notify.Request) (string, error) {
	b.mu.Lock()
	alias, messageID, sendSeen, sendWait := b.alias, b.messageID, b.sendSeen, b.sendWait
	b.mu.Unlock()
	if sendSeen != nil {
		sendSeen <- alias
	}
	if sendWait != nil {
		<-sendWait
	}
	b.mu.Lock()
	b.ordinary = append(b.ordinary, multiAppTestSend{chatID: chatID, request: req})
	b.mu.Unlock()
	return messageID, nil
}

func (b *multiAppTestBackend) CreateCard(_ context.Context, cardJSON string) (string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.createdCards = append(b.createdCards, cardJSON)
	return "card_" + b.alias, nil
}

func (b *multiAppTestBackend) SendCard(_ context.Context, req feishu.CardSendRequest) (string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.sentCards = append(b.sentCards, req)
	return b.messageID, nil
}

func (b *multiAppTestBackend) UpdateContent(_ context.Context, req feishu.CardContentUpdate) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.contentUpdates = append(b.contentUpdates, req)
	return nil
}

func (b *multiAppTestBackend) UpdateSettings(_ context.Context, req feishu.CardSettingsUpdate) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.settings = append(b.settings, req)
	return nil
}

func (b *multiAppTestBackend) BatchUpdate(context.Context, feishu.CardBatchUpdate) error {
	return nil
}

func (b *multiAppTestBackend) counts() (ordinary, cards, content, settings int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.ordinary), len(b.sentCards), len(b.contentUpdates), len(b.settings)
}

func newMultiAppServiceFixture(
	t *testing.T,
	providers map[string]config.AgentProviderConfig,
) (*Service, *multiAppTestBackend, *multiAppTestBackend) {
	t.Helper()
	alpha := newMultiAppTestBackend(config.DefaultAppAlias)
	beta := newMultiAppTestBackend("beta")
	cfg := config.Config{
		AppID:     "app-default",
		AppSecret: "secret-default",
		Commands:  config.CommandConfig{Enabled: true},
		Channels: map[string]string{
			"ops":     "oc_default",
			"support": "oc_beta",
		},
		Apps: map[string]config.AppConfig{
			config.DefaultAppAlias: {
				AppID: "app-default", AppSecret: "secret-default",
				Commands: config.CommandConfig{Enabled: true},
				Channels: map[string]string{"ops": "oc_default"},
			},
			"beta": {
				AppID: "app-beta", AppSecret: "secret-beta",
				Commands: config.CommandConfig{Enabled: true},
				Channels: map[string]string{"support": "oc_beta"},
			},
		},
		ChannelRoutes: map[string]config.ChannelRoute{
			"ops":     {AppAlias: config.DefaultAppAlias, ChatID: "oc_default"},
			"support": {AppAlias: "beta", ChatID: "oc_beta"},
		},
		AgentProviders: providers,
		DedupeTTL:      2 * time.Hour,
		SendTimeout:    time.Second,
	}
	svc := NewMultiAppService(
		cfg,
		map[string]feishu.Sender{config.DefaultAppAlias: alpha, "beta": beta},
		dedupe.NewMemoryStore(2*time.Hour),
		slog.Default(),
	)
	return svc, alpha, beta
}

func dispatchMultiAppPrompt(
	t *testing.T,
	svc *Service,
	appAlias, deliveryID, conversationID, chatAlias, messageID string,
) {
	t.Helper()
	if _, apiErr := svc.DispatchCommand(context.Background(), CommandInput{
		AppAlias:       appAlias,
		DeliveryID:     deliveryID,
		Command:        "ask",
		Text:           "status",
		Prompt:         "ask status",
		ConversationID: conversationID,
		ChatAlias:      chatAlias,
		SenderID:       "ou_sender",
		Metadata: map[string]string{
			"chat_type": "group", "message_type": "text", "message_id": messageID,
		},
	}); apiErr != nil {
		t.Fatalf("dispatch %s prompt: %v", appAlias, apiErr)
	}
}

func TestDefaultAppInternalDerivationsArePinned(t *testing.T) {
	tests := []struct {
		name string
		in   CommandInput
		want string
	}{
		{
			name: "flat alias route",
			in:   CommandInput{ChatAlias: "ops"},
			want: "conv_f2f06f2b316ef8dc00f6d8f3decf208c",
		},
		{
			name: "threaded private route",
			in: CommandInput{
				ChatAlias: "ops", ChatID: "oc_private_route",
				Metadata: map[string]string{"thread_id": "thread_one"},
			},
			want: "conv_e67493a40f5e6103e2b94dccca1a4a9c",
		},
		{
			name: "root scoped private route",
			in: CommandInput{
				ChatAlias: "ops", ChatID: "oc_private_route",
				Metadata: map[string]string{"root_id": "om_root"},
			},
			want: "conv_a331e961bfe5ac4f403d83f09a088b19",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := fallbackConversationID(tt.in); got != tt.want {
				t.Fatalf("default fallback conversation id = %q, want pinned %q", got, tt.want)
			}
			tt.in.AppAlias = "beta"
			if got := fallbackConversationID(tt.in); got == tt.want {
				t.Fatalf("named app reused default conversation id %q", got)
			}
		})
	}

	if got := agentMessageDedupeKey("", "om_same_message"); got != "200bd3323e6766b1f78068d0a3345b43172aeb4d44458caade7072a616516fff" {
		t.Fatalf("default message dedupe key = %q", got)
	}
	if got := appScopedDedupeSource("", "svc"); got != "svc" {
		t.Fatalf("default notification dedupe source = %q, want byte-identical svc", got)
	}
}

func TestMultiAppAgentEventsAndResponsesUseResolvedBackend(t *testing.T) {
	svc, alpha, beta := newMultiAppServiceFixture(t, nil)
	sub := mustSubscribeAgent(t, svc, AgentSubscribeOptions{
		Provider: "agent", Commands: []string{"ask"}, IncludeCardActions: true,
	})

	dispatchMultiAppPrompt(t, svc, config.DefaultAppAlias, "delivery_default", "conv_default", "ops", "om_default_in")
	defaultEvent := receiveAgentEvent(t, sub)
	if defaultEvent.Metadata["app_alias"] != config.DefaultAppAlias {
		t.Fatalf("default event metadata = %#v", defaultEvent.Metadata)
	}
	defaultReceipt := startAgentResponse(t, svc, "agent", defaultEvent.DeliveryID, AgentResponseContent{
		Markdown: "default", Actions: []AgentResponseAction{{ActionID: "go", Label: "Go", PayloadJSON: `{}`}},
	})

	dispatchMultiAppPrompt(t, svc, "beta", "delivery_beta", "conv_beta", "support", "om_beta_in")
	betaEvent := receiveAgentEvent(t, sub)
	if betaEvent.Metadata["app_alias"] != "beta" {
		t.Fatalf("beta event metadata = %#v", betaEvent.Metadata)
	}
	betaReceipt, apiErr := svc.StartAgentResponse(context.Background(), StartAgentResponseInput{
		Provider: "agent", DeliveryID: betaEvent.DeliveryID, OperationID: "start-1",
		Content: AgentResponseContent{
			Markdown: "beta", Actions: []AgentResponseAction{{ActionID: "go", Label: "Go", PayloadJSON: `{}`}},
		},
	})
	if apiErr != nil {
		t.Fatalf("start beta response: %v", apiErr)
	}
	if betaReceipt.ResponseID == defaultReceipt.ResponseID {
		t.Fatal("apps shared a response handle")
	}

	if _, apiErr := svc.UpdateAgentResponse(context.Background(), UpdateAgentResponseInput{
		Provider: "agent", ResponseID: betaReceipt.ResponseID, OperationID: "update-1",
		ExpectedRevision: 1, Markdown: "beta updated",
	}); apiErr != nil {
		t.Fatalf("update beta response: %v", apiErr)
	}
	if _, apiErr := svc.FinishAgentResponse(context.Background(), FinishAgentResponseInput{
		Provider: "agent", ResponseID: defaultReceipt.ResponseID, OperationID: "finish-default",
		ExpectedRevision: 1, Outcome: AgentResponseOutcomeCompleted, Markdown: "default",
	}); apiErr != nil {
		t.Fatalf("finish default response: %v", apiErr)
	}
	if _, apiErr := svc.FinishAgentResponse(context.Background(), FinishAgentResponseInput{
		Provider: "agent", ResponseID: betaReceipt.ResponseID, OperationID: "finish-beta",
		ExpectedRevision: 2, Outcome: AgentResponseOutcomeCompleted, Markdown: "beta final",
	}); apiErr != nil {
		t.Fatalf("finish beta response: %v", apiErr)
	}
	_, alphaCards, alphaUpdates, alphaSettings := alpha.counts()
	_, betaCards, betaUpdates, betaSettings := beta.counts()
	if alphaCards != 1 || betaCards != 1 || alphaUpdates != 0 || betaUpdates != 2 ||
		alphaSettings != 1 || betaSettings != 1 {
		t.Fatalf(
			"backend card routing: default cards/content/settings=%d/%d/%d beta=%d/%d/%d",
			alphaCards, alphaUpdates, alphaSettings, betaCards, betaUpdates, betaSettings,
		)
	}
}

func TestMultiAppNotificationDedupeIsAppScoped(t *testing.T) {
	svc, alpha, beta := newMultiAppServiceFixture(t, nil)
	request := notify.Request{
		Source: "monitor", SourceEventID: "event-1", DedupeKey: "same-key", Severity: "info",
		Title: "same title", Markdown: "same body", Target: notify.Target{Channel: "ops"},
	}
	first, apiErr := svc.SendNotification(context.Background(), request)
	if apiErr != nil || first.Duplicate {
		t.Fatalf("default notification = %#v, %v", first, apiErr)
	}
	request.Target.Channel = "support"
	second, apiErr := svc.SendNotification(context.Background(), request)
	if apiErr != nil || second.Duplicate {
		t.Fatalf("beta notification = %#v, %v", second, apiErr)
	}
	replay, apiErr := svc.SendNotification(context.Background(), request)
	if apiErr != nil || !replay.Duplicate {
		t.Fatalf("beta replay = %#v, %v", replay, apiErr)
	}
	alphaOrdinary, _, _, _ := alpha.counts()
	betaOrdinary, _, _, _ := beta.counts()
	if alphaOrdinary != 1 || betaOrdinary != 1 {
		t.Fatalf("notification sends default=%d beta=%d", alphaOrdinary, betaOrdinary)
	}

	// This string was the former variable-length named-app namespace and is
	// itself a valid caller source. It must not collide with beta/source=x.
	named := request
	named.Source = "x"
	named.SourceEventID = "event-collision"
	named.DedupeKey = "collision-key"
	if _, apiErr := svc.SendNotification(context.Background(), named); apiErr != nil {
		t.Fatalf("named-app adversarial fixture: %v", apiErr)
	}
	defaultRequest := named
	defaultRequest.Source = "feishu-botd/app-dedupe/v1\x00beta\x00x"
	defaultRequest.Target.Channel = "ops"
	if _, apiErr := svc.SendNotification(context.Background(), defaultRequest); apiErr != nil {
		t.Fatalf("default caller collided with named internal namespace: %v", apiErr)
	}
}

func TestMultiAppMetadataProvenanceAndShortOverlappingRedaction(t *testing.T) {
	svc, _, _ := newMultiAppServiceFixture(t, nil)
	sub := mustSubscribeAgent(t, svc, AgentSubscribeOptions{Provider: "agent", Commands: []string{"ask"}})
	if _, apiErr := svc.DispatchCommand(context.Background(), CommandInput{
		DeliveryID: "spoofed", Command: "ask", ChatAlias: "ops",
		Metadata: map[string]string{"app_alias": config.DefaultAppAlias},
	}); apiErr != nil {
		t.Fatalf("dispatch spoofed metadata fixture: %v", apiErr)
	}
	event := receiveAgentEvent(t, sub)
	if _, exposed := event.Metadata["app_alias"]; exposed {
		t.Fatalf("metadata-only app alias was trusted: %#v", event.Metadata)
	}

	cfg := config.Config{
		Apps: map[string]config.AppConfig{
			"tiny": {
				AppID: "abc", AppSecret: "abc-private",
				Channels: map[string]string{"tiny": "xy"},
			},
		},
		Channels:      map[string]string{"tiny": "xy"},
		ChannelRoutes: map[string]config.ChannelRoute{"tiny": {AppAlias: "tiny", ChatID: "xy"}},
	}
	redacted := newRedactor(cfg).redact(errors.New("abc-private abc xy"))
	for _, private := range []string{"abc-private", "abc", "xy"} {
		if strings.Contains(redacted, private) {
			t.Fatalf("redactor leaked %q in %q", private, redacted)
		}
	}
}

func TestMultiAppAllowedAppsFanoutAndFallbackArbitration(t *testing.T) {
	svc, _, _ := newMultiAppServiceFixture(t, nil)
	exactDefault := mustSubscribeAgent(t, svc, AgentSubscribeOptions{
		Provider: "exact-default", Commands: []string{"ask"},
		AllowedApps: []string{config.DefaultAppAlias}, AllowedAppsConfigured: true,
	})
	fallbackBeta := mustSubscribeAgent(t, svc, AgentSubscribeOptions{
		Provider: "fallback-beta", IncludeUnmatchedMessages: true,
		AllowedApps: []string{"beta"}, AllowedAppsConfigured: true,
	})
	explicitNone := mustSubscribeAgent(t, svc, AgentSubscribeOptions{
		Provider: "explicit-none", Commands: []string{"ask"},
		AllowedAppsConfigured: true,
	})
	unrestricted := mustSubscribeAgent(t, svc, AgentSubscribeOptions{
		Provider: "unrestricted", Commands: []string{"free"},
	})

	dispatchMultiAppPrompt(t, svc, "beta", "allowed_beta", "conv_allowed_beta", "support", "om_allowed_beta")
	betaEvent := receiveAgentEvent(t, fallbackBeta)
	if betaEvent.DeliveryID != "allowed_beta" {
		t.Fatalf("beta fallback event = %#v", betaEvent)
	}
	assertNoAgentEvent(t, exactDefault)
	assertNoAgentEvent(t, explicitNone)

	dispatchMultiAppPrompt(t, svc, config.DefaultAppAlias, "allowed_default", "conv_allowed_default", "ops", "om_allowed_default")
	defaultEvent := receiveAgentEvent(t, exactDefault)
	if defaultEvent.DeliveryID != "allowed_default" {
		t.Fatalf("default exact event = %#v", defaultEvent)
	}
	assertNoAgentEvent(t, fallbackBeta)
	assertNoAgentEvent(t, explicitNone)

	if _, apiErr := svc.DispatchCommand(context.Background(), CommandInput{
		AppAlias: "beta", DeliveryID: "allowed_absent", Command: "free",
		Prompt: "free", ConversationID: "conv_allowed_absent", ChatAlias: "support",
		Metadata: map[string]string{"chat_type": "group", "message_type": "text", "message_id": "om_allowed_absent"},
	}); apiErr != nil {
		t.Fatalf("dispatch absent allowlist fixture: %v", apiErr)
	}
	if event := receiveAgentEvent(t, unrestricted); event.DeliveryID != "allowed_absent" {
		t.Fatalf("absent allowlist event = %#v", event)
	}

	legacyDefault, apiErr := svc.SubscribeCommandsForApps(context.Background(), CommandSubscribeOptions{
		Provider: "legacy-default", Commands: []string{"status"},
		AllowedApps: []string{config.DefaultAppAlias}, AllowedAppsConfigured: true,
	})
	if apiErr != nil {
		t.Fatalf("subscribe scoped legacy provider: %v", apiErr)
	}
	defer legacyDefault.Close()
	legacyAll, apiErr := svc.SubscribeCommandsForApps(context.Background(), CommandSubscribeOptions{
		Provider: "legacy-all", Commands: []string{"status"},
	})
	if apiErr != nil {
		t.Fatalf("subscribe unrestricted legacy provider: %v", apiErr)
	}
	defer legacyAll.Close()
	if delivered, apiErr := svc.DispatchCommand(context.Background(), CommandInput{
		AppAlias: "beta", DeliveryID: "legacy_allowed_beta", Command: "status",
		Prompt: "status", ConversationID: "conv_legacy_allowed_beta", ChatAlias: "support",
		Metadata: map[string]string{"chat_type": "group", "message_type": "text", "message_id": "om_legacy_allowed_beta"},
	}); apiErr != nil || delivered != 1 {
		t.Fatalf("scoped legacy dispatch = %d, %v", delivered, apiErr)
	}
	select {
	case event := <-legacyAll.C:
		if event.DeliveryID != "legacy_allowed_beta" {
			t.Fatalf("unrestricted legacy event = %#v", event)
		}
	case <-time.After(time.Second):
		t.Fatal("unrestricted legacy provider did not receive beta event")
	}
	assertNoCommandEvent(t, legacyDefault)
}

func TestMultiAppAllowedAppsFiltersCardActionFanout(t *testing.T) {
	svc, _, beta := newMultiAppServiceFixture(t, nil)
	defaultOnly := mustSubscribeAgent(t, svc, AgentSubscribeOptions{
		Provider: "agent", Commands: []string{"ask"}, IncludeCardActions: true,
		AllowedApps: []string{config.DefaultAppAlias}, AllowedAppsConfigured: true,
	})
	betaAllowed := mustSubscribeAgent(t, svc, AgentSubscribeOptions{
		Provider: "agent", Commands: []string{"ask"}, IncludeCardActions: true,
		AllowedApps: []string{"beta"}, AllowedAppsConfigured: true,
	})
	wrongProvider := mustSubscribeAgent(t, svc, AgentSubscribeOptions{
		Provider: "other-agent", Commands: []string{"ask"}, IncludeCardActions: true,
		AllowedApps: []string{"beta"}, AllowedAppsConfigured: true,
	})

	dispatchMultiAppPrompt(t, svc, "beta", "action_scope_beta", "conv_action_scope_beta", "support", "om_action_scope_beta")
	betaPrompt := receiveAgentEvent(t, betaAllowed)
	_ = receiveAgentEvent(t, wrongProvider)
	assertNoAgentEvent(t, defaultOnly)
	response := startAgentResponse(t, svc, "agent", betaPrompt.DeliveryID, AgentResponseContent{
		Markdown: "beta action",
		Actions: []AgentResponseAction{{
			ActionID: "approve", Label: "Approve", PayloadJSON: `{}`,
		}},
	})

	beta.mu.Lock()
	responseMessageID := beta.messageID
	beta.mu.Unlock()
	if apiErr := svc.DispatchAgentCardAction(context.Background(), AgentCardActionInput{
		AppAlias: "beta", DeliveryID: "action_scope_delivery",
		MessageID: responseMessageID, SenderID: "ou_actor", ActionID: "approve",
		PayloadJSON: `{}`, ActionPayloadJSON: `{}`,
	}); apiErr != nil {
		t.Fatalf("dispatch beta card action: %v", apiErr)
	}
	action := receiveAgentEvent(t, betaAllowed)
	if action.CardAction == nil || action.CardAction.ResponseID != response.ResponseID ||
		action.Metadata["app_alias"] != "beta" {
		t.Fatalf("beta card action = %#v", action)
	}
	assertNoAgentEvent(t, defaultOnly)
	assertNoAgentEvent(t, wrongProvider)
}

func TestMultiAppLegacyCommandReplyUsesDeliveryApp(t *testing.T) {
	svc, alpha, beta := newMultiAppServiceFixture(t, nil)
	sub, apiErr := svc.SubscribeCommandsForApps(context.Background(), CommandSubscribeOptions{
		Provider: "legacy", Commands: []string{"status"},
	})
	if apiErr != nil {
		t.Fatalf("subscribe legacy: %v", apiErr)
	}
	defer sub.Close()

	if _, apiErr := svc.DispatchCommand(context.Background(), CommandInput{
		AppAlias: "beta", DeliveryID: "legacy_beta", Command: "status", Text: "now",
		ChatAlias: "support", Metadata: map[string]string{
			"chat_type": "group", "message_type": "text", "message_id": "om_legacy_beta",
		},
	}); apiErr != nil {
		t.Fatalf("dispatch beta legacy command: %v", apiErr)
	}
	command := <-sub.C
	if command.AppAlias != "" || command.Metadata["app_alias"] != "" {
		t.Fatalf("legacy command exposed app internals: %#v", command)
	}
	if apiErr := svc.RespondCommand(context.Background(), CommandResponse{
		Provider: "legacy", DeliveryID: command.DeliveryID, Markdown: "beta reply",
	}); apiErr != nil {
		t.Fatalf("respond beta command: %v", apiErr)
	}
	alphaOrdinary, _, _, _ := alpha.counts()
	betaOrdinary, _, _, _ := beta.counts()
	if alphaOrdinary != 0 || betaOrdinary != 1 {
		t.Fatalf("legacy reply egress default=%d beta=%d", alphaOrdinary, betaOrdinary)
	}
	beta.mu.Lock()
	defer beta.mu.Unlock()
	if got := beta.ordinary[0]; got.chatID != "oc_beta" || got.request.ReplyToMessageID != "om_legacy_beta" {
		t.Fatalf("beta legacy reply = %#v", got)
	}
}

func TestInternalScriptPrincipalDoesNotCollideWithExternalProvider(t *testing.T) {
	tests := []struct {
		name             string
		externalApps     []string
		externalReceives bool
		wantDelivered    int
	}{
		{
			name: "external provider is restricted away",
			externalApps: []string{
				config.DefaultAppAlias,
			},
			wantDelivered: 1,
		},
		{
			name:             "external provider is independently allowed",
			externalApps:     []string{"beta"},
			externalReceives: true,
			wantDelivered:    2,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc, alpha, beta := newMultiAppServiceFixture(t, map[string]config.AgentProviderConfig{
				"script-executor": {
					AllowedApps: tt.externalApps, AllowedAppsConfigured: true,
				},
			})
			internal, apiErr := svc.SubscribeInternalCommandsForApps(context.Background(), CommandSubscribeOptions{
				Provider: "script-executor", Commands: []string{"status"},
				AllowedApps: []string{"beta"}, AllowedAppsConfigured: true,
			})
			if apiErr != nil {
				t.Fatalf("subscribe internal script executor: %v", apiErr)
			}
			defer internal.Close()
			external, apiErr := svc.SubscribeCommandsForApps(context.Background(), CommandSubscribeOptions{
				Provider: "script-executor", Commands: []string{"status"},
				AllowedApps: tt.externalApps, AllowedAppsConfigured: true,
			})
			if apiErr != nil {
				t.Fatalf("subscribe external provider: %v", apiErr)
			}
			defer external.Close()

			delivered, apiErr := svc.DispatchCommand(context.Background(), CommandInput{
				AppAlias: "beta", DeliveryID: "script_identity", Command: "status",
				Prompt: "status", ConversationID: "conv_script_identity", ChatAlias: "support",
				Metadata: map[string]string{
					"chat_type": "group", "message_type": "text", "message_id": "om_script_identity",
				},
			})
			if apiErr != nil || delivered != tt.wantDelivered {
				t.Fatalf("script identity dispatch = %d, %v", delivered, apiErr)
			}
			select {
			case event := <-internal.C:
				if event.DeliveryID != "script_identity" {
					t.Fatalf("internal event = %#v", event)
				}
			case <-time.After(time.Second):
				t.Fatal("internal script executor did not receive its event")
			}
			if tt.externalReceives {
				select {
				case event := <-external.C:
					if event.DeliveryID != "script_identity" {
						t.Fatalf("external event = %#v", event)
					}
				case <-time.After(time.Second):
					t.Fatal("independently allowed external provider did not receive its event")
				}
				if apiErr := svc.RespondCommand(context.Background(), CommandResponse{
					Provider: "script-executor", DeliveryID: "script_identity", Markdown: "external",
				}); apiErr != nil {
					t.Fatalf("allowed external response: %v", apiErr)
				}
				if apiErr := svc.RespondInternalCommand(context.Background(), CommandResponse{
					Provider: "script-executor", DeliveryID: "script_identity", Markdown: "internal",
				}); apiErr == nil || apiErr.Code != "already_responded" {
					t.Fatalf("internal replay after external response = %#v", apiErr)
				}
			} else {
				assertNoCommandEvent(t, external)
				if apiErr := svc.RespondCommand(context.Background(), CommandResponse{
					Provider: "script-executor", DeliveryID: "script_identity", Markdown: "external",
				}); apiErr == nil || apiErr.Code != "unknown_delivery" {
					t.Fatalf("restricted external response = %#v", apiErr)
				}
				if apiErr := svc.RespondInternalCommand(context.Background(), CommandResponse{
					Provider: "script-executor", DeliveryID: "script_identity", Markdown: "internal",
				}); apiErr != nil {
					t.Fatalf("internal response was affected by external config: %v", apiErr)
				}
			}
			alphaOrdinary, _, _, _ := alpha.counts()
			betaOrdinary, _, _, _ := beta.counts()
			if alphaOrdinary != 0 || betaOrdinary != 1 {
				t.Fatalf("script response egress default=%d beta=%d", alphaOrdinary, betaOrdinary)
			}
		})
	}
}

func TestMultiAppRawMessageAndActionKeysAreIsolated(t *testing.T) {
	svc, alpha, beta := newMultiAppServiceFixture(t, nil)
	alpha.messageID = "om_shared_response"
	beta.messageID = "om_shared_response"
	sub := mustSubscribeAgent(t, svc, AgentSubscribeOptions{
		Provider: "agent", Commands: []string{"ask"}, IncludeCardActions: true,
	})

	dispatchMultiAppPrompt(t, svc, config.DefaultAppAlias, "delivery_raw_default", "conv_raw_default", "ops", "om_shared_inbound")
	defaultEvent := receiveAgentEvent(t, sub)
	dispatchMultiAppPrompt(t, svc, "beta", "delivery_raw_beta", "conv_raw_beta", "support", "om_shared_inbound")
	betaEvent := receiveAgentEvent(t, sub)
	defaultResponse := startAgentResponse(t, svc, "agent", defaultEvent.DeliveryID, AgentResponseContent{
		Markdown: "default", Actions: []AgentResponseAction{{ActionID: "go", Label: "Go", PayloadJSON: `{}`}},
	})
	betaResponse, apiErr := svc.StartAgentResponse(context.Background(), StartAgentResponseInput{
		Provider: "agent", DeliveryID: betaEvent.DeliveryID, OperationID: "start-1",
		Content: AgentResponseContent{
			Markdown: "beta", Actions: []AgentResponseAction{{ActionID: "go", Label: "Go", PayloadJSON: `{}`}},
		},
	})
	if apiErr != nil {
		t.Fatalf("start beta response: %v", apiErr)
	}

	for _, input := range []AgentCardActionInput{
		{
			AppAlias: config.DefaultAppAlias, DeliveryID: "same_action_delivery",
			MessageID: "om_shared_response", SenderID: "ou_actor", ActionID: "go",
			PayloadJSON: `{}`, ActionPayloadJSON: `{}`,
		},
		{
			AppAlias: "beta", DeliveryID: "same_action_delivery",
			MessageID: "om_shared_response", SenderID: "ou_actor", ActionID: "go",
			PayloadJSON: `{}`, ActionPayloadJSON: `{}`,
		},
	} {
		if apiErr := svc.DispatchAgentCardAction(context.Background(), input); apiErr != nil {
			t.Fatalf("dispatch %s action: %v", input.AppAlias, apiErr)
		}
		action := receiveAgentEvent(t, sub)
		wantResponse := defaultResponse.ResponseID
		if input.AppAlias == "beta" {
			wantResponse = betaResponse.ResponseID
		}
		if action.CardAction == nil || action.CardAction.ResponseID != wantResponse ||
			action.Metadata["app_alias"] != input.AppAlias {
			t.Fatalf("%s action routed as %#v", input.AppAlias, action)
		}
	}

	// The retry is deduplicated only inside the default app's action namespace.
	if apiErr := svc.DispatchAgentCardAction(context.Background(), AgentCardActionInput{
		AppAlias: config.DefaultAppAlias, DeliveryID: "same_action_delivery",
		MessageID: "om_shared_response", SenderID: "ou_actor", ActionID: "go",
		PayloadJSON: `{}`, ActionPayloadJSON: `{}`,
	}); apiErr != nil {
		t.Fatalf("retry default action: %v", apiErr)
	}
	assertNoAgentEvent(t, sub)
}

func TestMultiAppFollowUpsAndProviderOperationReceiptsAreIsolated(t *testing.T) {
	svc, alpha, beta := newMultiAppServiceFixture(t, nil)
	first := mustSubscribeAgent(t, svc, AgentSubscribeOptions{Provider: "first", Commands: []string{"ask"}})
	second := mustSubscribeAgent(t, svc, AgentSubscribeOptions{Provider: "second", Commands: []string{"ask"}})

	dispatchMultiAppPrompt(t, svc, config.DefaultAppAlias, "follow_default", "conv_follow_default", "ops", "om_follow_default")
	_ = receiveAgentEvent(t, first)
	_ = receiveAgentEvent(t, second)
	dispatchMultiAppPrompt(t, svc, "beta", "follow_beta", "conv_follow_beta", "support", "om_follow_beta")
	_ = receiveAgentEvent(t, first)
	_ = receiveAgentEvent(t, second)

	defaultReceipt, apiErr := svc.SendAgentFollowUp(context.Background(), SendAgentFollowUpInput{
		Provider: "first", ConversationID: "conv_follow_default", OperationID: "same-op", Markdown: "default",
	})
	if apiErr != nil {
		t.Fatalf("default follow-up: %v", apiErr)
	}
	betaReceipt, apiErr := svc.SendAgentFollowUp(context.Background(), SendAgentFollowUpInput{
		Provider: "first", ConversationID: "conv_follow_beta", OperationID: "same-op", Markdown: "beta",
	})
	if apiErr != nil {
		t.Fatalf("beta follow-up: %v", apiErr)
	}
	if defaultReceipt.FollowUpID == betaReceipt.FollowUpID {
		t.Fatal("cross-app follow-ups shared a receipt")
	}

	otherProvider, apiErr := svc.SendAgentFollowUp(context.Background(), SendAgentFollowUpInput{
		Provider: "second", ConversationID: "conv_follow_default", OperationID: "same-op", Markdown: "default",
	})
	if apiErr != nil {
		t.Fatalf("second-provider follow-up: %v", apiErr)
	}
	if otherProvider.FollowUpID == defaultReceipt.FollowUpID || otherProvider.Duplicate {
		t.Fatalf("providers shared a follow-up receipt: first=%#v second=%#v", defaultReceipt, otherProvider)
	}
	alphaOrdinary, _, _, _ := alpha.counts()
	betaOrdinary, _, _, _ := beta.counts()
	if alphaOrdinary != 2 || betaOrdinary != 1 {
		t.Fatalf("follow-up egress default=%d beta=%d", alphaOrdinary, betaOrdinary)
	}
}

func TestMultiAppConversationHandleCollisionFailsClosedBeforeFanout(t *testing.T) {
	svc, _, _ := newMultiAppServiceFixture(t, nil)
	sub := mustSubscribeAgent(t, svc, AgentSubscribeOptions{Provider: "agent", Commands: []string{"ask"}})
	dispatchMultiAppPrompt(t, svc, config.DefaultAppAlias, "collision_default", "same_conversation", "ops", "om_collision_default")
	_ = receiveAgentEvent(t, sub)
	dispatchMultiAppPrompt(t, svc, "beta", "collision_beta", "same_conversation", "support", "om_collision_beta")
	assertNoAgentEvent(t, sub)
}

func TestMultiAppAllowedAppsIsRecheckedAtEveryHandleMutation(t *testing.T) {
	providerConfig := config.AgentProviderConfig{
		AllowedApps: []string{"beta"}, AllowedAppsConfigured: true,
	}
	svc, alpha, beta := newMultiAppServiceFixture(t, map[string]config.AgentProviderConfig{
		"agent": providerConfig,
	})
	agentSub := mustSubscribeAgent(t, svc, AgentSubscribeOptions{
		Provider: "agent", Commands: []string{"ask"},
		AllowedApps: []string{"beta"}, AllowedAppsConfigured: true,
	})
	legacySub, apiErr := svc.SubscribeCommandsForApps(context.Background(), CommandSubscribeOptions{
		Provider: "agent", Commands: []string{"status"},
		AllowedApps: []string{"beta"}, AllowedAppsConfigured: true,
	})
	if apiErr != nil {
		t.Fatalf("subscribe legacy mutation fixture: %v", apiErr)
	}
	defer legacySub.Close()

	dispatchMultiAppPrompt(t, svc, "beta", "mutation_started", "conv_mutation_beta", "support", "om_mutation_started")
	startedEvent := receiveAgentEvent(t, agentSub)
	response := startAgentResponse(t, svc, "agent", startedEvent.DeliveryID, AgentResponseContent{Markdown: "started"})

	dispatchMultiAppPrompt(t, svc, "beta", "mutation_unstarted", "conv_mutation_unstarted", "support", "om_mutation_unstarted")
	_ = receiveAgentEvent(t, agentSub)
	if _, apiErr := svc.DispatchCommand(context.Background(), CommandInput{
		AppAlias: "beta", DeliveryID: "mutation_legacy", Command: "status",
		Prompt: "status", ConversationID: "conv_mutation_legacy", ChatAlias: "support",
		Metadata: map[string]string{"chat_type": "group", "message_type": "text", "message_id": "om_mutation_legacy"},
	}); apiErr != nil {
		t.Fatalf("dispatch legacy mutation fixture: %v", apiErr)
	}
	select {
	case <-legacySub.C:
	case <-time.After(time.Second):
		t.Fatal("legacy mutation fixture was not delivered")
	}

	messageKey := agentMessageDedupeKey("beta", "om_mutation_unstarted")
	nearExpiry := time.Now().Add(time.Minute)
	svc.agentBroker.mu.Lock()
	unstarted := svc.agentBroker.deliveries["mutation_unstarted"]
	if unstarted == nil {
		svc.agentBroker.mu.Unlock()
		t.Fatal("unstarted delivery was not recorded")
	}
	unstarted.expiresAt = nearExpiry
	svc.agentBroker.seenMessages[messageKey] = nearExpiry
	svc.agentBroker.mu.Unlock()
	svc.inboundRoutes.mu.Lock()
	svc.inboundRoutes.deliveries["mutation_unstarted"] = nearExpiry
	svc.inboundRoutes.messages[messageKey] = nearExpiry
	svc.inboundRoutes.mu.Unlock()

	svc.cfg.AgentProviders["agent"] = config.AgentProviderConfig{
		AllowedApps: []string{config.DefaultAppAlias}, AllowedAppsConfigured: true,
	}
	beforeAlphaOrdinary, beforeAlphaCards, beforeAlphaContent, beforeAlphaSettings := alpha.counts()
	beforeBetaOrdinary, beforeBetaCards, beforeBetaContent, beforeBetaSettings := beta.counts()

	if _, apiErr := svc.StartAgentResponse(context.Background(), StartAgentResponseInput{
		Provider: "agent", DeliveryID: "mutation_unstarted", OperationID: "denied-start",
		Content: AgentResponseContent{Markdown: "must not send"},
	}); apiErr == nil || apiErr.Status != 404 || apiErr.Code != "unknown_delivery" {
		t.Fatalf("denied start error = %#v", apiErr)
	}
	if _, apiErr := svc.UpdateAgentResponse(context.Background(), UpdateAgentResponseInput{
		Provider: "agent", ResponseID: response.ResponseID, OperationID: "denied-update",
		ExpectedRevision: 1, Markdown: "must not update",
	}); apiErr == nil || apiErr.Status != 404 || apiErr.Code != "unknown_response" {
		t.Fatalf("denied update error = %#v", apiErr)
	}
	if _, apiErr := svc.FinishAgentResponse(context.Background(), FinishAgentResponseInput{
		Provider: "agent", ResponseID: response.ResponseID, OperationID: "denied-finish",
		ExpectedRevision: 1, Outcome: AgentResponseOutcomeCompleted, Markdown: "must not finish",
	}); apiErr == nil || apiErr.Status != 404 || apiErr.Code != "unknown_response" {
		t.Fatalf("denied finish error = %#v", apiErr)
	}
	if _, apiErr := svc.SendAgentFollowUp(context.Background(), SendAgentFollowUpInput{
		Provider: "agent", ConversationID: "conv_mutation_beta",
		OperationID: "denied-follow-up", Markdown: "must not follow up",
	}); apiErr == nil || apiErr.Status != 404 || apiErr.Code != "unknown_conversation" {
		t.Fatalf("denied follow-up error = %#v", apiErr)
	}
	if apiErr := svc.RespondCommand(context.Background(), CommandResponse{
		Provider: "agent", DeliveryID: "mutation_legacy", Markdown: "must not reply",
	}); apiErr == nil || apiErr.Status != 404 || apiErr.Code != "unknown_delivery" {
		t.Fatalf("denied legacy response error = %#v", apiErr)
	}

	svc.agentBroker.mu.Lock()
	gotDeliveryExpiry := unstarted.expiresAt
	gotSeenExpiry := svc.agentBroker.seenMessages[messageKey]
	svc.agentBroker.mu.Unlock()
	svc.inboundRoutes.mu.Lock()
	gotRouteDeliveryExpiry := svc.inboundRoutes.deliveries["mutation_unstarted"]
	gotRouteMessageExpiry := svc.inboundRoutes.messages[messageKey]
	svc.inboundRoutes.mu.Unlock()
	for name, got := range map[string]time.Time{
		"agent delivery": gotDeliveryExpiry,
		"agent message":  gotSeenExpiry,
		"route delivery": gotRouteDeliveryExpiry,
		"route message":  gotRouteMessageExpiry,
	} {
		if !got.Equal(nearExpiry) {
			t.Fatalf("denied mutation refreshed %s expiry: got %v want %v", name, got, nearExpiry)
		}
	}
	svc.commandBroker.mu.Lock()
	legacyState := svc.commandBroker.deliveries["mutation_legacy"].state
	svc.commandBroker.mu.Unlock()
	if legacyState != commandDeliveryOpen {
		t.Fatalf("denied legacy response changed delivery state to %v", legacyState)
	}

	afterAlphaOrdinary, afterAlphaCards, afterAlphaContent, afterAlphaSettings := alpha.counts()
	afterBetaOrdinary, afterBetaCards, afterBetaContent, afterBetaSettings := beta.counts()
	if [4]int{afterAlphaOrdinary, afterAlphaCards, afterAlphaContent, afterAlphaSettings} !=
		[4]int{beforeAlphaOrdinary, beforeAlphaCards, beforeAlphaContent, beforeAlphaSettings} ||
		[4]int{afterBetaOrdinary, afterBetaCards, afterBetaContent, afterBetaSettings} !=
			[4]int{beforeBetaOrdinary, beforeBetaCards, beforeBetaContent, beforeBetaSettings} {
		t.Fatalf(
			"denied mutations reached a backend: default %v -> %v, beta %v -> %v",
			[4]int{beforeAlphaOrdinary, beforeAlphaCards, beforeAlphaContent, beforeAlphaSettings},
			[4]int{afterAlphaOrdinary, afterAlphaCards, afterAlphaContent, afterAlphaSettings},
			[4]int{beforeBetaOrdinary, beforeBetaCards, beforeBetaContent, beforeBetaSettings},
			[4]int{afterBetaOrdinary, afterBetaCards, afterBetaContent, afterBetaSettings},
		)
	}
}

func TestMultiAppReadinessTracksEachConnectionAndBoundsAuthChecks(t *testing.T) {
	svc, alpha, beta := newMultiAppServiceFixture(t, nil)
	ready, checks := svc.Ready(context.Background())
	if ready || checks["feishu_connection.default"] != AppConnectionStarting ||
		checks["feishu_connection.beta"] != AppConnectionStarting {
		t.Fatalf("initial multi-app readiness = %t, %#v", ready, checks)
	}

	svc.SetAppConnectionState(config.DefaultAppAlias, AppConnectionConnected)
	svc.SetAppConnectionState("beta", AppConnectionConnected)
	ready, checks = svc.Ready(context.Background())
	if !ready || checks["feishu_auth.default"] != "ok" || checks["feishu_auth.beta"] != "ok" {
		t.Fatalf("connected multi-app readiness = %t, %#v", ready, checks)
	}
	svc.SetAppConnectionState("beta", AppConnectionUnavailable)
	ready, checks = svc.Ready(context.Background())
	if ready || checks["feishu_connection.beta"] != AppConnectionUnavailable {
		t.Fatalf("unavailable app readiness = %t, %#v", ready, checks)
	}

	svc.SetAppConnectionState("beta", AppConnectionConnected)
	started := make(chan string, 2)
	release := make(chan struct{})
	defer close(release)
	alpha.mu.Lock()
	alpha.readySeen, alpha.readyWait = started, release
	alpha.mu.Unlock()
	beta.mu.Lock()
	beta.readySeen, beta.readyWait = started, release
	beta.mu.Unlock()
	svc.cfg.SendTimeout = 30 * time.Millisecond
	began := time.Now()
	ready, checks = svc.Ready(context.Background())
	if elapsed := time.Since(began); elapsed > 500*time.Millisecond {
		t.Fatalf("readiness exceeded its auth deadline: %v", elapsed)
	}
	if ready || checks["feishu_auth"] != "unavailable" ||
		checks["feishu_auth.default"] != "unavailable" ||
		checks["feishu_auth.beta"] != "unavailable" {
		t.Fatalf("timed-out multi-app readiness = %t, %#v", ready, checks)
	}
	if len(started) != 2 {
		t.Fatalf("auth checks did not start concurrently: started=%d", len(started))
	}
}

func TestMultiAppFollowUpPinsRouteAcrossInFlightPruneAndReplay(t *testing.T) {
	svc, alpha, beta := newMultiAppServiceFixture(t, nil)
	sub := mustSubscribeAgent(t, svc, AgentSubscribeOptions{
		Provider: "agent", Commands: []string{"ask"},
	})
	dispatchMultiAppPrompt(t, svc, config.DefaultAppAlias, "pin_default", "conv_pin", "ops", "om_pin_default")
	_ = receiveAgentEvent(t, sub)
	route := followUpRoute(t, svc, "conv_pin")
	originalExpiry := time.Now().Add(time.Minute)
	route.mu.Lock()
	route.expiresAt = originalExpiry
	route.mu.Unlock()

	sendStarted := make(chan string, 1)
	sendRelease := make(chan struct{})
	alpha.mu.Lock()
	alpha.sendSeen, alpha.sendWait = sendStarted, sendRelease
	alpha.mu.Unlock()
	type followUpResult struct {
		receipt AgentFollowUpReceipt
		apiErr  *notify.APIError
	}
	resultCh := make(chan followUpResult, 1)
	go func() {
		receipt, apiErr := svc.SendAgentFollowUp(context.Background(), SendAgentFollowUpInput{
			Provider: "agent", ConversationID: "conv_pin",
			OperationID: "pin-op", Markdown: "pinned",
		})
		resultCh <- followUpResult{receipt: receipt, apiErr: apiErr}
	}()
	select {
	case alias := <-sendStarted:
		if alias != config.DefaultAppAlias {
			t.Fatalf("follow-up started on app %q", alias)
		}
	case <-time.After(time.Second):
		t.Fatal("follow-up send did not start")
	}

	kept := svc.agentBroker.lookupConversation("conv_pin", originalExpiry.Add(time.Second))
	if kept != route {
		t.Fatalf("in-flight route was pruned or rebound: got %p want %p", kept, route)
	}
	dispatchMultiAppPrompt(t, svc, "beta", "pin_beta_collision", "conv_pin", "support", "om_pin_beta")
	assertNoAgentEvent(t, sub)

	close(sendRelease)
	result := <-resultCh
	if result.apiErr != nil || result.receipt.Duplicate {
		t.Fatalf("initial pinned follow-up = %#v, %v", result.receipt, result.apiErr)
	}
	replay, apiErr := svc.SendAgentFollowUp(context.Background(), SendAgentFollowUpInput{
		Provider: "agent", ConversationID: "conv_pin",
		OperationID: "pin-op", Markdown: "pinned",
	})
	if apiErr != nil || !replay.Duplicate || replay.FollowUpID != result.receipt.FollowUpID {
		t.Fatalf("pinned follow-up replay = %#v, %v; first=%#v", replay, apiErr, result.receipt)
	}
	alphaOrdinary, _, _, _ := alpha.counts()
	betaOrdinary, _, _, _ := beta.counts()
	if alphaOrdinary != 1 || betaOrdinary != 0 {
		t.Fatalf("pinned follow-up egress default=%d beta=%d", alphaOrdinary, betaOrdinary)
	}
}
