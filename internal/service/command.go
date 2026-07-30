package service

import (
	"context"
	"strings"
	"sync"
	"time"

	"feishu-botd/internal/notify"
)

const commandSubscriptionBuffer = 32

// CommandInput is the transport-neutral form of an inbound bot command.
// ChatAlias must already be resolved from daemon configuration; raw Feishu chat
// ids never enter the public command stream.
type CommandInput struct {
	// AppAlias is daemon-private ingress routing state. An empty value is the
	// legacy default app and preserves all existing direct service callers.
	AppAlias       string `json:"-"`
	DeliveryID     string
	Command        string
	Text           string
	Prompt         string
	ConversationID string
	ChatAlias      string
	SenderID       string
	Metadata       map[string]string

	// ChatID is daemon-private ingress routing state. It is required for direct
	// messages and is deliberately never copied into either public protobuf
	// stream.
	ChatID string `json:"-"`
}

// CommandResponse is a provider reply to a previously delivered command.
type CommandResponse struct {
	Provider   string
	DeliveryID string
	Title      string
	Markdown   string
	CardJSON   string
}

// CommandSubscription is an active provider stream. Close must be called when
// the stream exits so the broker can stop delivering to it.
type CommandSubscription struct {
	C     <-chan CommandInput
	close func()
}

type inboundRouteRegistry struct {
	mu         sync.Mutex
	ttl        time.Duration
	deliveries map[string]time.Time
	messages   map[string]time.Time
}

func newInboundRouteRegistry(ttl time.Duration) *inboundRouteRegistry {
	if ttl <= 0 {
		ttl = time.Hour
	}
	return &inboundRouteRegistry{
		ttl:        ttl,
		deliveries: make(map[string]time.Time),
		messages:   make(map[string]time.Time),
	}
}

func (s *CommandSubscription) Close() {
	if s != nil && s.close != nil {
		s.close()
	}
}

type commandBroker struct {
	mu          sync.Mutex
	nextSubID   uint64
	ttl         time.Duration
	subscribers map[uint64]*commandSubscriber
	deliveries  map[string]*commandDelivery
}

type commandSubscriber struct {
	id                    uint64
	principal             commandPrincipal
	commands              map[string]struct{}
	allowedApps           map[string]struct{}
	allowedAppsConfigured bool
	ch                    chan CommandInput
}

// commandPrincipal separates process-internal consumers from authenticated
// wire providers even when they intentionally use the same display name. The
// internal bit never crosses a transport or enters a provider-visible event.
type commandPrincipal struct {
	provider string
	internal bool
}

type commandDelivery struct {
	appAlias         string
	chatAlias        string
	messageID        string
	allowedProviders map[commandPrincipal]struct{}
	expiresAt        time.Time
	state            commandDeliveryState
}

type commandDeliveryState int

const (
	commandDeliveryOpen commandDeliveryState = iota
	commandDeliveryResponding
	commandDeliveryResponded
)

func newCommandBroker(ttl time.Duration) *commandBroker {
	if ttl <= 0 {
		ttl = time.Hour
	}
	return &commandBroker{
		ttl:         ttl,
		subscribers: make(map[uint64]*commandSubscriber),
		deliveries:  make(map[string]*commandDelivery),
	}
}

func (s *Service) SubscribeCommands(ctx context.Context, provider string, commands []string) (*CommandSubscription, *notify.APIError) {
	return s.SubscribeCommandsForApps(ctx, CommandSubscribeOptions{
		Provider: provider,
		Commands: commands,
	})
}

// CommandSubscribeOptions adds daemon-private application authorization to the
// unchanged legacy command stream. Absent allowed_apps means all apps; an
// explicitly configured empty list means none.
type CommandSubscribeOptions struct {
	Provider              string
	Commands              []string
	AllowedApps           []string
	AllowedAppsConfigured bool
}

// SubscribeCommandsForApps registers an app-filtered legacy command consumer.
// External callers still use the unchanged protobuf Subscribe request; the
// authenticated transport supplies this internal scope from configuration.
func (s *Service) SubscribeCommandsForApps(ctx context.Context, in CommandSubscribeOptions) (*CommandSubscription, *notify.APIError) {
	return s.subscribeCommandsForApps(ctx, in, false)
}

// SubscribeInternalCommandsForApps registers a trusted in-process consumer.
// Its broker identity is distinct from any authenticated wire provider with the
// same name, while its explicit application allowlist is enforced normally.
func (s *Service) SubscribeInternalCommandsForApps(ctx context.Context, in CommandSubscribeOptions) (*CommandSubscription, *notify.APIError) {
	return s.subscribeCommandsForApps(ctx, in, true)
}

func (s *Service) subscribeCommandsForApps(
	ctx context.Context,
	in CommandSubscribeOptions,
	internal bool,
) (*CommandSubscription, *notify.APIError) {
	_ = ctx
	sub, apiErr := s.commandBroker.subscribe(in, internal)
	if apiErr != nil {
		return nil, apiErr
	}
	return &CommandSubscription{
		C: sub.ch,
		close: func() {
			s.commandBroker.unsubscribe(sub.id)
		},
	}, nil
}

// DispatchCommand publishes an inbound command to currently connected matching
// providers. It is intentionally non-blocking so Feishu event acknowledgement is
// not coupled to local provider health.
func (s *Service) DispatchCommand(ctx context.Context, in CommandInput) (int, *notify.APIError) {
	_ = ctx
	explicitAppAlias := strings.TrimSpace(in.AppAlias)
	in.AppAlias = effectiveAppAlias(in.AppAlias)
	if explicitAppAlias != "" {
		in.Metadata = cloneStringMap(in.Metadata)
		if in.Metadata == nil {
			in.Metadata = make(map[string]string)
		}
		in.Metadata["app_alias"] = in.AppAlias
	} else if _, supplied := in.Metadata["app_alias"]; supplied {
		in.Metadata = cloneStringMap(in.Metadata)
		delete(in.Metadata, "app_alias")
	}
	in.DeliveryID = strings.TrimSpace(in.DeliveryID)
	in.Command = normalizeCommand(in.Command)
	in.Text = strings.TrimSpace(in.Text)
	in.Prompt = strings.TrimSpace(in.Prompt)
	in.ConversationID = strings.TrimSpace(in.ConversationID)
	in.ChatAlias = strings.TrimSpace(in.ChatAlias)
	in.SenderID = strings.TrimSpace(in.SenderID)
	in.ChatID = strings.TrimSpace(in.ChatID)
	if in.DeliveryID == "" {
		return 0, notify.BadRequest("missing_delivery_id", "delivery_id is required")
	}
	if in.Command == "" {
		return 0, notify.BadRequest("missing_command", "command is required")
	}
	if in.ChatAlias == "" {
		return 0, notify.BadRequest("missing_channel", "chat_alias is required")
	}
	isDirect := in.ChatAlias == "direct" && in.Metadata["chat_type"] == "p2p"
	if !isDirect {
		appAlias, _, ok := s.cfg.ResolveChannel(in.ChatAlias)
		if !ok || appAlias != in.AppAlias {
			return 0, notify.NewAPIError(404, "unknown_channel", "unknown channel", false)
		}
	} else if in.ChatID == "" {
		return 0, notify.BadRequest("missing_direct_route", "direct message route is required")
	} else if _, ok := s.backendForApp(in.AppAlias); !ok {
		return 0, notify.NewAPIError(404, "unknown_channel", "unknown channel", false)
	}
	if in.Prompt == "" {
		in.Prompt = strings.TrimSpace(strings.Join([]string{in.Command, in.Text}, " "))
	}
	if in.ConversationID == "" {
		in.ConversationID = fallbackConversationID(in)
	}
	if len(in.DeliveryID) > 160 || len(in.Command) > 64 || len(in.Text) > 8000 || len(in.Prompt) > 32*1024 || len(in.ConversationID) > 160 || len(in.SenderID) > 160 || len(in.ChatID) > 160 || len(in.Metadata["message_id"]) > 160 {
		return 0, notify.BadRequest("field_too_large", "one or more fields are too large")
	}

	legacyDelivered, agentDelivered := s.dispatchInboundRoute(in, isDirect)
	s.logger.Info("command dispatched",
		"correlation", opaqueLogCorrelationID("command", in.DeliveryID),
		"legacy_subscribers", legacyDelivered,
		"agent_subscribers", agentDelivered,
	)
	return legacyDelivered, nil
}

func (s *Service) dispatchInboundRoute(in CommandInput, isDirect bool) (legacyDelivered, agentDelivered int) {
	routes := s.inboundRoutes
	routes.mu.Lock()
	defer routes.mu.Unlock()
	now := time.Now()
	routes.pruneLocked(now)
	messageKey := agentMessageDedupeKey(in.AppAlias, in.Metadata["message_id"])
	if _, duplicate := routes.deliveries[in.DeliveryID]; duplicate {
		return 0, 0
	}
	if messageKey != "" {
		if _, duplicate := routes.messages[messageKey]; duplicate {
			return 0, 0
		}
	}

	handled := false
	if !isDirect {
		legacyDelivered, handled = s.commandBroker.dispatch(in)
	}
	if !handled {
		agentDelivered, handled = s.agentBroker.dispatchMessage(in)
	}
	if handled {
		expiresAt := now.Add(routes.ttl)
		routes.deliveries[in.DeliveryID] = expiresAt
		if messageKey != "" {
			routes.messages[messageKey] = expiresAt
		}
	}
	return legacyDelivered, agentDelivered
}

func (r *inboundRouteRegistry) pruneLocked(now time.Time) {
	for deliveryID, expiresAt := range r.deliveries {
		if now.After(expiresAt) {
			delete(r.deliveries, deliveryID)
		}
	}
	for messageKey, expiresAt := range r.messages {
		if now.After(expiresAt) {
			delete(r.messages, messageKey)
		}
	}
}

func (s *Service) RespondCommand(ctx context.Context, in CommandResponse) *notify.APIError {
	return s.respondCommand(ctx, in, false)
}

// RespondInternalCommand replies as a trusted in-process consumer registered
// through SubscribeInternalCommandsForApps. Configured provider ACLs do not
// apply to this separate principal; delivery ownership and the subscription's
// explicit application allowlist still do.
func (s *Service) RespondInternalCommand(ctx context.Context, in CommandResponse) *notify.APIError {
	return s.respondCommand(ctx, in, true)
}

func (s *Service) respondCommand(ctx context.Context, in CommandResponse, internal bool) *notify.APIError {
	in.Provider = strings.TrimSpace(in.Provider)
	in.DeliveryID = strings.TrimSpace(in.DeliveryID)
	if len(s.cfg.AgentProviders) > 0 && in.Provider == "" {
		return notify.BadRequest("missing_provider", "provider is required")
	}
	if in.DeliveryID == "" {
		return notify.BadRequest("missing_delivery_id", "delivery_id is required")
	}
	chatAlias, messageID, appAlias, apiErr := s.commandBroker.beginResponse(
		in.DeliveryID,
		commandPrincipal{provider: in.Provider, internal: internal},
		func(resolvedApp string) bool {
			return internal || s.appAllowed(in.Provider, resolvedApp)
		},
	)
	if apiErr != nil {
		return apiErr
	}
	resolvedApp, _, ok := s.cfg.ResolveChannel(chatAlias)
	if !ok || resolvedApp != appAlias {
		s.commandBroker.finishResponse(in.DeliveryID, false)
		return notify.NewAPIError(404, "unknown_delivery", "unknown delivery", false)
	}

	_, apiErr = s.SendMessage(ctx, MessageInput{
		Channel:          chatAlias,
		Source:           "command",
		DedupeKey:        "command:" + in.DeliveryID,
		Title:            in.Title,
		Markdown:         in.Markdown,
		CardJSON:         in.CardJSON,
		ReplyToMessageID: messageID,
	})
	if apiErr != nil {
		s.commandBroker.finishResponse(in.DeliveryID, false)
		return apiErr
	}
	s.commandBroker.finishResponse(in.DeliveryID, true)
	return nil
}

func (b *commandBroker) subscribe(in CommandSubscribeOptions, internal bool) (*commandSubscriber, *notify.APIError) {
	provider := strings.TrimSpace(in.Provider)
	if provider == "" {
		return nil, notify.BadRequest("missing_provider", "provider is required")
	}
	if len(provider) > 64 {
		return nil, notify.BadRequest("field_too_large", "one or more fields are too large")
	}

	commandSet := make(map[string]struct{}, len(in.Commands))
	for _, command := range in.Commands {
		command = normalizeCommand(command)
		if command == "" {
			continue
		}
		if len(command) > 64 {
			return nil, notify.BadRequest("field_too_large", "one or more fields are too large")
		}
		commandSet[command] = struct{}{}
	}
	if len(commandSet) == 0 {
		return nil, notify.BadRequest("missing_command", "at least one command is required")
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	b.nextSubID++
	sub := &commandSubscriber{
		id:                    b.nextSubID,
		principal:             commandPrincipal{provider: provider, internal: internal},
		commands:              commandSet,
		allowedApps:           normalizedAppAllowlist(in.AllowedApps),
		allowedAppsConfigured: in.AllowedAppsConfigured,
		ch:                    make(chan CommandInput, commandSubscriptionBuffer),
	}
	b.subscribers[sub.id] = sub
	return sub, nil
}

func (b *commandBroker) unsubscribe(id uint64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	sub, ok := b.subscribers[id]
	if !ok {
		return
	}
	delete(b.subscribers, id)
	close(sub.ch)
}

func (b *commandBroker) dispatch(in CommandInput) (delivered int, handled bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.pruneLocked(time.Now())
	if _, ok := b.deliveries[in.DeliveryID]; ok {
		return 0, true
	}

	publicInput := publicCommandInput(in)
	allowedProviders := make(map[commandPrincipal]struct{})
	providerSent := make(map[commandPrincipal]struct{})
	for _, sub := range b.subscribers {
		if !sub.allowsApp(in.AppAlias) {
			continue
		}
		if _, ok := sub.commands[in.Command]; !ok {
			continue
		}
		if _, duplicateProvider := providerSent[sub.principal]; duplicateProvider {
			continue
		}
		select {
		case sub.ch <- cloneCommandInput(publicInput):
			delivered++
			providerSent[sub.principal] = struct{}{}
			allowedProviders[sub.principal] = struct{}{}
		default:
		}
	}
	if delivered > 0 {
		b.deliveries[in.DeliveryID] = &commandDelivery{
			appAlias:         in.AppAlias,
			chatAlias:        in.ChatAlias,
			messageID:        in.Metadata["message_id"],
			allowedProviders: allowedProviders,
			expiresAt:        time.Now().Add(b.ttl),
			state:            commandDeliveryOpen,
		}
	}
	return delivered, delivered > 0
}

func (b *commandBroker) beginResponse(
	deliveryID string,
	principal commandPrincipal,
	appAllowed func(string) bool,
) (chatAlias, messageID, appAlias string, apiErr *notify.APIError) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.pruneLocked(time.Now())
	delivery, ok := b.deliveries[deliveryID]
	if !ok {
		return "", "", "", notify.NewAPIError(404, "unknown_delivery", "unknown delivery", false)
	}
	if principal.provider != "" {
		_, received := delivery.allowedProviders[principal]
		if !received || appAllowed != nil && !appAllowed(delivery.appAlias) {
			return "", "", "", notify.NewAPIError(404, "unknown_delivery", "unknown delivery", false)
		}
	}
	switch delivery.state {
	case commandDeliveryResponded:
		return "", "", "", notify.NewAPIError(409, "already_responded", "delivery already has a response", false)
	case commandDeliveryResponding:
		return "", "", "", notify.NewAPIError(409, "response_in_flight", "delivery response is already being sent", true)
	}
	delivery.state = commandDeliveryResponding
	return delivery.chatAlias, delivery.messageID, delivery.appAlias, nil
}

func (b *commandBroker) finishResponse(deliveryID string, sent bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	delivery, ok := b.deliveries[deliveryID]
	if !ok {
		return
	}
	if sent {
		delivery.state = commandDeliveryResponded
		return
	}
	delivery.state = commandDeliveryOpen
}

func (b *commandBroker) pruneLocked(now time.Time) {
	for deliveryID, delivery := range b.deliveries {
		if now.After(delivery.expiresAt) {
			delete(b.deliveries, deliveryID)
		}
	}
}

func normalizeCommand(command string) string {
	command = strings.TrimSpace(command)
	command = strings.TrimLeft(command, "/")
	return strings.ToLower(command)
}

func cloneCommandInput(in CommandInput) CommandInput {
	out := in
	if len(in.Metadata) > 0 {
		out.Metadata = make(map[string]string, len(in.Metadata))
		for k, v := range in.Metadata {
			out.Metadata[k] = v
		}
	}
	return out
}

func publicCommandInput(in CommandInput) CommandInput {
	out := cloneCommandInput(in)
	out.Metadata = legacyPublicMetadata(in.Metadata)
	out.AppAlias = ""
	out.ChatID = ""
	return out
}

func legacyPublicMetadata(in map[string]string) map[string]string {
	out := make(map[string]string, 2)
	for _, key := range []string{"chat_type", "message_type"} {
		if value := strings.TrimSpace(in[key]); value != "" {
			out[key] = value
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func normalizedAppAllowlist(apps []string) map[string]struct{} {
	if len(apps) == 0 {
		return nil
	}
	out := make(map[string]struct{}, len(apps))
	for _, alias := range apps {
		alias = effectiveAppAlias(alias)
		out[alias] = struct{}{}
	}
	return out
}

func appAllowedByList(appAlias string, allowed map[string]struct{}, configured bool) bool {
	if !configured {
		return true
	}
	_, ok := allowed[effectiveAppAlias(appAlias)]
	return ok
}

func (s *commandSubscriber) allowsApp(appAlias string) bool {
	return appAllowedByList(appAlias, s.allowedApps, s.allowedAppsConfigured)
}
