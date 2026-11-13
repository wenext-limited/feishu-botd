package main

import (
	"context"
	"errors"
	"log/slog"
	"reflect"
	"sort"
	"sync"
	"time"

	"feishu-botd/internal/config"
	"feishu-botd/internal/feishu"
	"feishu-botd/internal/service"
)

// liveReceiver is a receiver whose routing settings and handlers a reload
// can change without reconnecting.
type liveReceiver interface {
	appReceiver
	UpdateSettings(feishu.CommandReceiverConfig)
	SetCommandHandler(feishu.CommandHandler)
	SetCardActionHandler(feishu.CardActionHandler)
	SetMessageReactionHandler(feishu.MessageReactionHandler)
}

// appRuntime is one app's running Feishu connection and script executor.
type appRuntime struct {
	receiver liveReceiver
	app      config.AppConfig
	scripts  *service.CommandSubscription
}

// appManager owns every app's runtime. A reload starts, stops, or replaces
// one app's runtime without touching the others.
type appManager struct {
	ctx         context.Context
	svc         *service.Service
	logger      *slog.Logger
	componentCh chan<- componentResult
	timeout     time.Duration
	// newReceiver builds a receiver; tests replace it.
	newReceiver func(alias string, cfg feishu.CommandReceiverConfig) liveReceiver

	mu   sync.Mutex
	apps map[string]*appRuntime
}

func newAppManager(
	ctx context.Context,
	svc *service.Service,
	logger *slog.Logger,
	componentCh chan<- componentResult,
	timeout time.Duration,
) *appManager {
	return &appManager{
		ctx:         ctx,
		svc:         svc,
		logger:      logger,
		componentCh: componentCh,
		timeout:     timeout,
		newReceiver: func(_ string, cfg feishu.CommandReceiverConfig) liveReceiver {
			return feishu.NewCommandReceiver(cfg, nil, logger)
		},
		apps: make(map[string]*appRuntime),
	}
}

// build creates alias's receiver, wired to the service, and registers it as
// the app's current runtime. The caller starts it.
func (m *appManager) build(alias string, app config.AppConfig) liveReceiver {
	var receiver liveReceiver
	receiverCfg := m.receiverConfig(alias, app)
	// A receiver that has been replaced or removed may still report a late
	// disconnect; only the current one speaks for the app.
	receiverCfg.ConnectionStateChanged = func(appAlias string, state feishu.ConnectionState) {
		if m.isCurrent(receiver) {
			m.svc.SetAppConnectionState(appAlias, string(state))
		}
	}
	receiver = m.newReceiver(alias, receiverCfg)
	m.installHandlers(alias, receiver, app)

	m.mu.Lock()
	m.apps[alias] = &appRuntime{receiver: receiver, app: app}
	m.mu.Unlock()
	return receiver
}

func (m *appManager) receiverConfig(alias string, app config.AppConfig) feishu.CommandReceiverConfig {
	svc := m.svc
	return feishu.CommandReceiverConfig{
		AppAlias:                    alias,
		AppID:                       app.AppID,
		AppSecret:                   app.AppSecret,
		Channels:                    app.Channels,
		AllowUnconfiguredGroupChats: app.Commands.AllowUnconfiguredGroupChats,
		BotOpenID:                   app.Commands.BotOpenID,
		BotUserID:                   app.Commands.BotUserID,
		BotUnionID:                  app.Commands.BotUnionID,
		BotNames:                    app.Commands.BotNames,
		AmbientChat: func(chatAlias string) bool {
			return svc.Config().AmbientChatEnabled(alias, chatAlias)
		},
		RuntimeChatAlias: func(chatID string) (string, bool) {
			grant, ok := svc.Config().RuntimeChats.ByChat(alias, chatID)
			return grant.ChatAlias, ok
		},
	}
}

// installHandlers points the receiver at the service. Commands and card
// actions flow only while the app has commands enabled.
func (m *appManager) installHandlers(alias string, receiver liveReceiver, app config.AppConfig) {
	svc := m.svc
	if app.Commands.Enabled {
		receiver.SetCommandHandler(func(ctx context.Context, cmd feishu.InboundCommand) error {
			_, apiErr := svc.DispatchCommand(ctx, service.CommandInput{
				AppAlias:          cmd.AppAlias,
				DeliveryID:        cmd.DeliveryID,
				Command:           cmd.Command,
				Text:              cmd.Text,
				Prompt:            cmd.Prompt,
				ConversationID:    cmd.ConversationID,
				ChatAlias:         cmd.ChatAlias,
				SenderID:          cmd.SenderID,
				Metadata:          cmd.Metadata,
				ConversationTitle: cmd.ConversationTitle,
				ChatID:            cmd.ChatID,
				UnconfiguredGroup: cmd.UnconfiguredGroup,
				UnmentionedReply:  cmd.UnmentionedReply,
				AmbientOnly:       cmd.AmbientOnly,
				HumanSender:       cmd.HumanSender,
				NamedPrompt:       cmd.NamedPrompt,
				Mentions:          agentMentions(cmd.Mentions),
			})
			if apiErr != nil {
				return apiErr
			}
			return nil
		})
		receiver.SetCardActionHandler(func(ctx context.Context, action feishu.InboundCardAction) error {
			if apiErr := svc.DispatchInboundCardAction(ctx, action); apiErr != nil {
				return apiErr
			}
			return nil
		})
	} else {
		receiver.SetCommandHandler(nil)
		receiver.SetCardActionHandler(nil)
	}
	receiver.SetMessageReactionHandler(func(ctx context.Context, reaction feishu.InboundMessageReaction) error {
		operation := service.MessageReactionUnspecified
		switch reaction.Operation {
		case feishu.MessageReactionAdded:
			operation = service.MessageReactionAdded
		case feishu.MessageReactionRemoved:
			operation = service.MessageReactionRemoved
		}
		if apiErr := svc.DispatchAgentMessageReaction(ctx, service.AgentMessageReactionInput{
			AppAlias:     reaction.AppAlias,
			DeliveryID:   reaction.DeliveryID,
			MessageRef:   reaction.MessageRef,
			SenderID:     reaction.SenderID,
			ReactionType: reaction.ReactionType,
			Operation:    operation,
		}); apiErr != nil {
			return apiErr
		}
		return nil
	})
}

// isCurrent reports whether receiver is still some app's live receiver.
func (m *appManager) isCurrent(receiver appReceiver) bool {
	if receiver == nil {
		return false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, runtime := range m.apps {
		if appReceiver(runtime.receiver) == receiver {
			return true
		}
	}
	return false
}

// receivers returns the current receivers by alias.
func (m *appManager) receivers() map[string]appReceiver {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[string]appReceiver, len(m.apps))
	for alias, runtime := range m.apps {
		out[alias] = runtime.receiver
	}
	return out
}

// startScripts subscribes alias's script executor when its config enables
// one. Any previous executor for the app must already be stopped.
func (m *appManager) startScripts(alias string) error {
	m.mu.Lock()
	runtime := m.apps[alias]
	m.mu.Unlock()
	if runtime == nil {
		return nil
	}
	subs, err := startScriptExecutors(m.ctx, map[string]config.AppConfig{alias: runtime.app}, []string{alias}, m.svc, m.logger)
	if err != nil {
		return err
	}
	if len(subs) > 0 {
		m.mu.Lock()
		runtime.scripts = subs[0]
		m.mu.Unlock()
	}
	return nil
}

// launch starts receiver and waits, up to the startup timeout, for its
// first connection. A later unexpected exit reaches componentCh like any
// startup receiver's.
func (m *appManager) launch(alias string, receiver liveReceiver) error {
	startupCh := startAppReceivers(m.ctx, []string{alias}, map[string]appReceiver{alias: receiver}, m.componentCh)
	return waitForAppReceivers(m.ctx, m.timeout, 1, startupCh)
}

// stop closes alias's runtime and forgets it.
func (m *appManager) stop(alias string) {
	m.mu.Lock()
	runtime := m.apps[alias]
	delete(m.apps, alias)
	m.mu.Unlock()
	if runtime == nil {
		return
	}
	if runtime.scripts != nil {
		runtime.scripts.Close()
	}
	runtime.receiver.Close()
}

// closeAll closes every runtime at shutdown.
func (m *appManager) closeAll() {
	m.mu.Lock()
	aliases := make([]string, 0, len(m.apps))
	for alias := range m.apps {
		aliases = append(aliases, alias)
	}
	m.mu.Unlock()
	sort.Strings(aliases)
	for _, alias := range aliases {
		m.stop(alias)
	}
}

// appChangeResult is what apply did, for the reload log line.
type appChangeResult struct {
	started  []string
	stopped  []string
	replaced []string
	updated  []string
	failed   []string
}

// apply brings the running apps in line with cfg. Apps the plan adds or
// re-credits get a new connection; a re-credited app's old connection
// closes once the new one is up, so no event window opens. Every other app
// keeps its connection and takes the new settings in place.
func (m *appManager) apply(cfg config.Config, plan config.ReloadPlan) appChangeResult {
	var result appChangeResult
	apps := cfg.EffectiveApps()

	for _, alias := range plan.AppsRemoved {
		m.stop(alias)
		result.stopped = append(result.stopped, alias)
	}

	for _, alias := range cfg.AppAliases() {
		app := apps[alias]
		switch {
		case contains(plan.AppsAdded, alias):
			if err := m.startApp(alias, app); err != nil {
				m.logger.Error("config reload: app connection failed", "app_alias", alias)
				result.failed = append(result.failed, alias)
				continue
			}
			result.started = append(result.started, alias)
		case contains(plan.AppsRecredited, alias):
			if err := m.replaceApp(alias, app); err != nil {
				m.logger.Error("config reload: app connection failed", "app_alias", alias)
				result.failed = append(result.failed, alias)
				continue
			}
			result.replaced = append(result.replaced, alias)
		default:
			if m.updateApp(alias, app) {
				result.updated = append(result.updated, alias)
			}
		}
	}
	return result
}

func (m *appManager) startApp(alias string, app config.AppConfig) error {
	receiver := m.build(alias, app)
	err := m.launch(alias, receiver)
	if scriptErr := m.startScripts(alias); scriptErr != nil {
		err = errors.Join(err, scriptErr)
	}
	return err
}

func (m *appManager) replaceApp(alias string, app config.AppConfig) error {
	m.mu.Lock()
	old := m.apps[alias]
	m.mu.Unlock()
	if old != nil && old.scripts != nil {
		old.scripts.Close()
	}
	// build makes the new receiver current, so the old one's disconnect is
	// ignored from here on.
	err := m.startApp(alias, app)
	if old != nil {
		old.receiver.Close()
	}
	return err
}

// updateApp applies an app's new settings to its running connection. It
// reports whether anything changed.
func (m *appManager) updateApp(alias string, app config.AppConfig) bool {
	m.mu.Lock()
	runtime := m.apps[alias]
	m.mu.Unlock()
	if runtime == nil {
		return false
	}
	if reflect.DeepEqual(runtime.app, app) {
		return false
	}
	scriptsChanged := !reflect.DeepEqual(runtime.app.Commands.Scripts, app.Commands.Scripts)
	runtime.receiver.UpdateSettings(m.receiverConfig(alias, app))
	m.installHandlers(alias, runtime.receiver, app)
	m.mu.Lock()
	runtime.app = app
	oldScripts := runtime.scripts
	if scriptsChanged {
		runtime.scripts = nil
	}
	m.mu.Unlock()
	if scriptsChanged {
		if oldScripts != nil {
			oldScripts.Close()
		}
		if err := m.startScripts(alias); err != nil {
			m.logger.Error("config reload: script executor failed", "app_alias", alias)
		}
	}
	return true
}

func contains(list []string, value string) bool {
	for _, item := range list {
		if item == value {
			return true
		}
	}
	return false
}
