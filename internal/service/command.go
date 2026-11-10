package service

import (
	"context"
	"strings"
	"sync"
	"time"

	"feishu-botd/internal/feishu"
	"feishu-botd/internal/notify"
)

const (
	commandSubscriptionBuffer = 32
	maxCommandBytes           = 64
	maxCommandTextBytes       = 8000
	maxInboundPromptBytes     = 32 * 1024
)

// CommandInput is the transport-neutral form of an inbound bot command.
// ChatAlias is either a configured alias or an opaque alias generated for an
// explicitly allowed unconfigured group. Raw Feishu chat ids never enter the
// public command stream.
type CommandInput struct {
	// AppAlias is daemon-private ingress routing state. An empty value is the
	// legacy default app and preserves all existing direct service callers.
	AppAlias          string `json:"-"`
	DeliveryID        string
	Command           string
	Text              string
	Prompt            string
	ConversationID    string
	ChatAlias         string
	SenderID          string
	Metadata          map[string]string
	ConversationTitle string
	// SenderEmail is the sender's company address, resolved by the daemon
	// for providers granted allow_sender_email (see resolveSenderEmail) and
	// handed only to those. Personal data: never logged, never serialized.
	SenderEmail string `json:"-"`

	// ChatID, UnconfiguredGroup, and UnmentionedReply are daemon-private ingress
	// routing state.
	// ChatID is required for direct messages and allowed unconfigured groups;
	// none of these fields is copied into a public provider stream.
	ChatID            string `json:"-"`
	UnconfiguredGroup bool   `json:"-"`
	// UnmentionedReply requires an ownership lookup before any provider may see
	// the message. It is set only for group replies without a matching bot mention.
	UnmentionedReply bool `json:"-"`
	// AmbientOnly marks a group message that neither mentions the bot nor
	// replies to anything. It can reach only ambient subscriptions, and only
	// when HumanSender is also set. Both are daemon-private ingress state.
	AmbientOnly bool `json:"-"`
	HumanSender bool `json:"-"`

	// NamedPrompt is Prompt with other people's mentions rendered inline as
	// "@<name>". Mentions lists those people. Both reach only a provider that
	// holds the ambient grant; every other provider keeps the legacy prompt.
	NamedPrompt string
	Mentions    []AgentMention
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
	// A caller cannot supply an address; only resolveSenderEmail sets one.
	in.SenderEmail = ""
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
	in.NamedPrompt = strings.TrimSpace(in.NamedPrompt)
	in.Mentions = boundedAgentMentions(in.Mentions)
	if !isGroupChatType(in.Metadata["chat_type"]) {
		// Only a group has bystanders to observe.
		in.AmbientOnly = false
	}
	if in.DeliveryID == "" {
		return 0, notify.BadRequest("missing_delivery_id", "delivery_id is required")
	}
	if in.ChatAlias == "" {
		return 0, notify.BadRequest("missing_channel", "chat_alias is required")
	}
	isDirect := in.ChatAlias == "direct" && in.Metadata["chat_type"] == "p2p"
	isUnconfiguredGroup := s.acceptsUnconfiguredGroup(in)
	switch {
	case isDirect:
		if in.ChatID == "" {
			return 0, notify.BadRequest("missing_direct_route", "direct message route is required")
		}
		if _, ok := s.backendForApp(in.AppAlias); !ok {
			return 0, notify.NewAPIError(404, "unknown_channel", "unknown channel", false)
		}
	case in.UnconfiguredGroup:
		if !isUnconfiguredGroup {
			return 0, notify.NewAPIError(404, "unknown_channel", "unknown channel", false)
		}
	default:
		appAlias, _, ok := s.cfg.ResolveChannel(in.ChatAlias)
		if !ok || appAlias != in.AppAlias {
			return 0, notify.NewAPIError(404, "unknown_channel", "unknown channel", false)
		}
	}
	if in.Prompt == "" {
		in.Prompt = strings.TrimSpace(strings.Join([]string{in.Command, in.Text}, " "))
	}
	if in.Prompt == "" {
		return 0, notify.BadRequest("missing_command", "command or prompt is required")
	}
	if in.ConversationID == "" {
		in.ConversationID = fallbackConversationID(in)
	}
	if len(in.NamedPrompt) > maxInboundPromptBytes {
		// Display names can make the named rendering longer than the prompt
		// itself. That must never cost the message: fall back to the prompt.
		in.NamedPrompt = ""
	}
	if len(in.DeliveryID) > 160 || len(in.Prompt) > maxInboundPromptBytes || len(in.ConversationID) > 160 || len(in.SenderID) > 160 || len(in.ChatID) > 160 || len(in.Metadata["message_id"]) > 160 {
		return 0, notify.BadRequest("field_too_large", "one or more fields are too large")
	}
	projectionDropped := false
	in.Command, in.Text, projectionDropped = boundedCommandProjection(in.Command, in.Text)
	replyProvider, ownedReply, apiErr := s.resolveAgentReplyOwner(in)
	if apiErr != nil {
		return 0, apiErr
	}
	// A message nobody addressed — no mention, and either no parent or a parent
	// no provider authored — can still be an ambient observation, but never a
	// prompt. Without an ambient grant for this chat it is discarded exactly as
	// before.
	ambientOnly := in.AmbientOnly || in.UnmentionedReply && !ownedReply
	if ambientOnly && !s.ambientCandidate(in) {
		if in.UnmentionedReply {
			s.logger.Info("unowned group reply ignored",
				"correlation", opaqueLogCorrelationID("command", in.DeliveryID),
			)
		}
		return 0, nil
	}

	// Direct messages and wildcard groups are agent-only. This preserves the
	// configured-channel allowlists of legacy command providers and local script
	// executors while allowing conversational agents to opt into broader ingress.
	// A reply to an owned message is also agent-only and stays pinned to the
	// provider that authored the parent, regardless of first-word command matches.
	s.resolveSenderEmail(ctx, &in, replyProvider)
	// A reply route is retained for every proactive provider that could
	// receive the message before any of them can see it, then narrowed to the
	// ones that did. Both writes happen outside the ingress lock.
	routeGrantees := s.retainInboundMessageRoute(in, ambientOnly)
	legacyDelivered, agentDelivered, ambientDelivered := s.dispatchInboundRoute(in, isDirect || isUnconfiguredGroup || ownedReply, replyProvider, ambientOnly)
	s.narrowInboundMessageRoute(in, routeGrantees)
	s.logger.Info("command dispatched",
		"correlation", opaqueLogCorrelationID("command", in.DeliveryID),
		"legacy_subscribers", legacyDelivered,
		"agent_subscribers", agentDelivered,
		"ambient_subscribers", ambientDelivered,
		"command_projection_dropped", projectionDropped,
		"owned_reply", ownedReply,
	)
	return legacyDelivered, nil
}

// ambientCandidate reports whether any provider allowed to see this app holds
// the ambient grant for this group chat, and the message came from a person.
func (s *Service) ambientCandidate(in CommandInput) bool {
	return in.HumanSender && isGroupChatType(in.Metadata["chat_type"]) &&
		s.cfg.AmbientChatEnabled(in.AppAlias, in.ChatAlias)
}

func isGroupChatType(chatType string) bool {
	return chatType == "group" || chatType == "topic_group"
}

const (
	maxAgentMentions          = 64
	maxAgentMentionNameBytes  = 256
	maxAgentMentionUserIDSize = 160
)

// boundedAgentMentions applies the receiver's own bounds at the service
// boundary too, so a direct caller cannot put an unbounded list on the wire.
// Entries without a usable id are dropped; an over-long name is dropped to
// empty rather than cut mid-character.
func boundedAgentMentions(in []AgentMention) []AgentMention {
	if len(in) == 0 {
		return nil
	}
	out := make([]AgentMention, 0, len(in))
	for _, mention := range in {
		mention.UserID = strings.TrimSpace(mention.UserID)
		mention.Name = strings.TrimSpace(mention.Name)
		if mention.UserID == "" || len(mention.UserID) > maxAgentMentionUserIDSize {
			continue
		}
		if len(mention.Name) > maxAgentMentionNameBytes {
			mention.Name = ""
		}
		out = append(out, mention)
		if len(out) == maxAgentMentions {
			break
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// resolveAgentReplyOwner proves that an inbound parent is an agent-authored
// message and returns its provider. Unknown, expired, or app-disallowed parents
// are intentionally indistinguishable from unowned messages.
// resolveAgentReplyOwner names the provider an unmentioned group reply
// belongs to: the author of the message it replies to, or else the provider
// whose response card opened the thread it was posted in. Inside a thread
// Feishu points a plain message at the thread's root — the person's question,
// not the card — so the thread is what says the conversation is the agent's.
func (s *Service) resolveAgentReplyOwner(in CommandInput) (string, bool, *notify.APIError) {
	if s.agentOwners == nil {
		return "", false, nil
	}
	refs := []string{
		feishu.MessageRefForApp(in.AppAlias, in.Metadata["parent_id"]),
		feishu.ThreadRefForApp(in.AppAlias, in.Metadata["thread_id"]),
	}
	now := time.Now()
	for _, ref := range refs {
		if ref == "" {
			continue
		}
		owner, ok, err := s.agentOwners.Lookup(ref, now)
		if err != nil {
			return "", false, notify.NewAPIError(500, "ownership_store_unavailable", "could not resolve response ownership", true)
		}
		provider := strings.TrimSpace(owner.Provider)
		if ok && provider != "" && s.appAllowed(provider, in.AppAlias) {
			return provider, true, nil
		}
	}
	return "", false, nil
}

func (s *Service) acceptsUnconfiguredGroup(in CommandInput) bool {
	if !in.UnconfiguredGroup || !s.cfg.AllowsUnconfiguredGroupChats(in.AppAlias) || in.ChatID == "" {
		return false
	}
	switch in.Metadata["chat_type"] {
	case "group", "topic_group":
	default:
		return false
	}
	_, ok := s.backendForApp(in.AppAlias)
	return ok
}

func (s *Service) dispatchInboundRoute(in CommandInput, agentOnly bool, replyProvider string, ambientOnly bool) (legacyDelivered, agentDelivered, ambientDelivered int) {
	routes := s.inboundRoutes
	routes.mu.Lock()
	defer routes.mu.Unlock()
	now := time.Now()
	routes.pruneLocked(now)
	messageKey := agentMessageDedupeKey(in.AppAlias, in.Metadata["message_id"])
	if _, duplicate := routes.deliveries[in.DeliveryID]; duplicate {
		return 0, 0, 0
	}
	if messageKey != "" {
		if _, duplicate := routes.messages[messageKey]; duplicate {
			return 0, 0, 0
		}
	}
	handled := false
	var addressed map[string]struct{}
	var legacyProviders map[string]struct{}
	if !ambientOnly {
		if !agentOnly {
			legacyDelivered, handled, legacyProviders = s.commandBroker.dispatch(in)
		}
		if !handled {
			if replyProvider == "" {
				agentDelivered, handled, addressed = s.agentBroker.dispatchMessage(in)
			} else {
				agentDelivered, handled, addressed = s.agentBroker.dispatchMessageToProvider(in, replyProvider)
			}
		}
		if handled {
			expiresAt := now.Add(routes.ttl)
			routes.deliveries[in.DeliveryID] = expiresAt
			if messageKey != "" {
				routes.messages[messageKey] = expiresAt
			}
		}
	}
	// Ambient observation is independent of who answers: a provider that got
	// the addressed variant is skipped, everyone else granted this chat sees
	// it, even when a legacy command consumer or another agent owns the reply.
	if s.ambientCandidate(in) {
		skip := make(map[string]struct{}, len(addressed)+len(legacyProviders))
		for provider := range addressed {
			skip[provider] = struct{}{}
		}
		// A provider that took this message as a legacy command already has
		// it, addressed; it does not also get an ambient copy.
		for provider := range legacyProviders {
			skip[provider] = struct{}{}
		}
		ambientDelivered = s.agentBroker.dispatchAmbient(in, skip, replyProvider)
	}
	return legacyDelivered, agentDelivered, ambientDelivered
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
		if len(command) > maxCommandBytes {
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

// dispatch also reports the authenticated wire providers that received the
// command; trusted in-process consumers are not providers and are omitted.
func (b *commandBroker) dispatch(in CommandInput) (delivered int, handled bool, providers map[string]struct{}) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.pruneLocked(time.Now())
	if _, ok := b.deliveries[in.DeliveryID]; ok {
		return 0, true, nil
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
	for principal := range allowedProviders {
		if !principal.internal {
			if providers == nil {
				providers = make(map[string]struct{})
			}
			providers[principal.provider] = struct{}{}
		}
	}
	return delivered, delivered > 0, providers
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

// boundedCommandProjection keeps the complete prompt authoritative. Command
// and Text are a compatibility view for exact legacy/agent command matching;
// if that derived view cannot fit its wire limits, omit it atomically so an
// otherwise valid conversational prompt can still reach unmatched agents.
func boundedCommandProjection(command, text string) (string, string, bool) {
	if command == "" {
		return "", "", text != ""
	}
	if len(command) > maxCommandBytes || len(text) > maxCommandTextBytes {
		return "", "", true
	}
	return command, text, false
}

func cloneCommandInput(in CommandInput) CommandInput {
	out := in
	if len(in.Metadata) > 0 {
		out.Metadata = make(map[string]string, len(in.Metadata))
		for k, v := range in.Metadata {
			out.Metadata[k] = v
		}
	}
	out.Mentions = cloneAgentMentions(in.Mentions)
	return out
}

func publicCommandInput(in CommandInput) CommandInput {
	out := cloneCommandInput(in)
	out.Metadata = legacyPublicMetadata(in.Metadata)
	out.AppAlias = ""
	out.ChatID = ""
	out.UnconfiguredGroup = false
	out.UnmentionedReply = false
	out.AmbientOnly = false
	out.HumanSender = false
	// The legacy command stream predates mention identities and keeps its
	// exact historical shape.
	out.NamedPrompt = ""
	out.Mentions = nil
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
