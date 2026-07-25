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
	id       uint64
	provider string
	commands map[string]struct{}
	ch       chan CommandInput
}

type commandDelivery struct {
	chatAlias        string
	messageID        string
	allowedProviders map[string]struct{}
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
	_ = ctx
	sub, apiErr := s.commandBroker.subscribe(provider, commands)
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
		if _, ok := s.cfg.Channels[in.ChatAlias]; !ok {
			return 0, notify.NewAPIError(404, "unknown_channel", "unknown channel", false)
		}
	} else if in.ChatID == "" {
		return 0, notify.BadRequest("missing_direct_route", "direct message route is required")
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
	messageKey := agentMessageDedupeKey(in.Metadata["message_id"])
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
	in.Provider = strings.TrimSpace(in.Provider)
	in.DeliveryID = strings.TrimSpace(in.DeliveryID)
	if len(s.cfg.AgentProviders) > 0 && in.Provider == "" {
		return notify.BadRequest("missing_provider", "provider is required")
	}
	if in.DeliveryID == "" {
		return notify.BadRequest("missing_delivery_id", "delivery_id is required")
	}
	chatAlias, messageID, apiErr := s.commandBroker.beginResponse(in.DeliveryID, in.Provider)
	if apiErr != nil {
		return apiErr
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

func (b *commandBroker) subscribe(provider string, commands []string) (*commandSubscriber, *notify.APIError) {
	provider = strings.TrimSpace(provider)
	if provider == "" {
		return nil, notify.BadRequest("missing_provider", "provider is required")
	}
	if len(provider) > 64 {
		return nil, notify.BadRequest("field_too_large", "one or more fields are too large")
	}

	commandSet := make(map[string]struct{}, len(commands))
	for _, command := range commands {
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
		id:       b.nextSubID,
		provider: provider,
		commands: commandSet,
		ch:       make(chan CommandInput, commandSubscriptionBuffer),
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
	allowedProviders := make(map[string]struct{})
	providerSent := make(map[string]struct{})
	for _, sub := range b.subscribers {
		if _, ok := sub.commands[in.Command]; !ok {
			continue
		}
		if _, duplicateProvider := providerSent[sub.provider]; duplicateProvider {
			continue
		}
		select {
		case sub.ch <- cloneCommandInput(publicInput):
			delivered++
			providerSent[sub.provider] = struct{}{}
			allowedProviders[sub.provider] = struct{}{}
		default:
		}
	}
	if delivered > 0 {
		b.deliveries[in.DeliveryID] = &commandDelivery{
			chatAlias:        in.ChatAlias,
			messageID:        in.Metadata["message_id"],
			allowedProviders: allowedProviders,
			expiresAt:        time.Now().Add(b.ttl),
			state:            commandDeliveryOpen,
		}
	}
	return delivered, delivered > 0
}

func (b *commandBroker) beginResponse(deliveryID, provider string) (chatAlias, messageID string, apiErr *notify.APIError) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.pruneLocked(time.Now())
	delivery, ok := b.deliveries[deliveryID]
	if !ok {
		return "", "", notify.NewAPIError(404, "unknown_delivery", "unknown delivery", false)
	}
	if provider != "" {
		if _, allowed := delivery.allowedProviders[provider]; !allowed {
			return "", "", notify.NewAPIError(404, "unknown_delivery", "unknown delivery", false)
		}
	}
	switch delivery.state {
	case commandDeliveryResponded:
		return "", "", notify.NewAPIError(409, "already_responded", "delivery already has a response", false)
	case commandDeliveryResponding:
		return "", "", notify.NewAPIError(409, "response_in_flight", "delivery response is already being sent", true)
	}
	delivery.state = commandDeliveryResponding
	return delivery.chatAlias, delivery.messageID, nil
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
	out.Metadata = agentPublicMetadata(in.Metadata)
	out.ChatID = ""
	return out
}
