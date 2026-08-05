package service

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"time"

	"feishu-botd/internal/feishu"
	"feishu-botd/internal/notify"
)

const (
	agentSubscriptionBuffer = 64
	agentContentElementID   = "agent_answer"
	// The timeline panel and the markdown element nested inside it. Feishu caps
	// a developer-defined element_id at 20 characters.
	agentTimelinePanelElementID = "agent_timeline"
	agentTimelineElementID      = "agent_timeline_body"
	agentPlaceholderMarkdown    = "Thinking…"
	maxAgentCardBytes           = 30 * 1024
	maxAgentTitleBytes          = 200
	minCardMutationInterval     = 125 * time.Millisecond
	messageUUIDDedupeWindow     = time.Hour
)

// AgentSubscribeOptions selects a full-fidelity agent event stream. Exact
// command subscribers win; IncludeUnmatchedMessages is the natural chat-agent
// fallback for P2P prompts, otherwise-unhandled group mentions, and replies
// pinned to a message the provider authored.
type AgentSubscribeOptions struct {
	Provider                 string
	Commands                 []string
	IncludeUnmatchedMessages bool
	IncludeCardActions       bool
	IncludeMessageReactions  bool
	AllowedApps              []string
	AllowedAppsConfigured    bool
}

type AgentEvent struct {
	DeliveryID      string
	ConversationID  string
	ChatAlias       string
	SenderID        string
	Metadata        map[string]string
	Message         *AgentMessage
	CardAction      *AgentCardAction
	MessageReaction *AgentMessageReaction
}

type AgentMessage struct {
	Text              string
	Command           string
	CommandText       string
	ReplyToMessageRef string
	ConversationTitle string
}

type AgentCardAction struct {
	ResponseID  string
	ActionID    string
	PayloadJSON string
}

type MessageReactionOperation int

const (
	MessageReactionUnspecified MessageReactionOperation = iota
	MessageReactionAdded
	MessageReactionRemoved
)

type AgentMessageReaction struct {
	MessageRef   string
	ReactionType string
	Operation    MessageReactionOperation
}

type AgentSubscription struct {
	C     <-chan AgentEvent
	close func()
}

func (s *AgentSubscription) Close() {
	if s != nil && s.close != nil {
		s.close()
	}
}

type AgentResponseActionStyle int

const (
	AgentResponseActionStyleUnspecified AgentResponseActionStyle = iota
	AgentResponseActionStyleDefault
	AgentResponseActionStylePrimary
	AgentResponseActionStyleDanger
)

type AgentResponseAction struct {
	ActionID    string
	Label       string
	PayloadJSON string
	Style       AgentResponseActionStyle
}

type AgentResponseContent struct {
	Title    string
	Markdown string
	Actions  []AgentResponseAction
	// TimelineMarkdown and TimelineTitle are optional. Either one makes the
	// response carry a collapsible timeline panel for its whole lifetime; a
	// provider that sends neither gets exactly the card it got before.
	TimelineMarkdown string
	TimelineTitle    string
}

// agentTimelineParts are the timeline halves of an Update or Finish request.
// An empty field means "leave that part of the panel unchanged", so a provider
// can advance the collapsed header without resending the body, or the reverse.
type agentTimelineParts struct {
	Markdown string
	Title    string
}

type AgentResponsePhase int

const (
	AgentResponsePhaseUnspecified AgentResponsePhase = iota
	AgentResponsePhaseStreaming
	AgentResponsePhaseCompleted
	AgentResponsePhaseFailed
	AgentResponsePhaseCancelled
)

type AgentResponseOutcome int

const (
	AgentResponseOutcomeUnspecified AgentResponseOutcome = iota
	AgentResponseOutcomeCompleted
	AgentResponseOutcomeFailed
	AgentResponseOutcomeCancelled
)

type AgentResponseReceipt struct {
	ResponseID string
	Revision   uint64
	Phase      AgentResponsePhase
	Duplicate  bool
	MessageRef string
}

type StartAgentResponseInput struct {
	Provider    string
	DeliveryID  string
	OperationID string
	Content     AgentResponseContent
}

type UpdateAgentResponseInput struct {
	Provider         string
	ResponseID       string
	OperationID      string
	ExpectedRevision uint64
	Markdown         string
	TimelineMarkdown string
	TimelineTitle    string
}

type FinishAgentResponseInput struct {
	Provider         string
	ResponseID       string
	OperationID      string
	ExpectedRevision uint64
	Outcome          AgentResponseOutcome
	Markdown         string
	Summary          string
	TimelineMarkdown string
	TimelineTitle    string
}

// AgentCardActionInput is the daemon-private callback handoff. MessageID is
// used only to resolve the response owner and is never forwarded to a provider.
type AgentCardActionInput struct {
	AppAlias          string `json:"-"`
	DeliveryID        string
	MessageID         string `json:"-"`
	SenderID          string
	ActionID          string
	PayloadJSON       string
	ActionPayloadJSON string `json:"-"`
}

// AgentMessageReactionInput is the provider-safe reaction handoff. Ownership
// has already been reduced to MessageRef, so this type never accepts a raw
// Feishu message id.
type AgentMessageReactionInput struct {
	AppAlias     string
	DeliveryID   string
	MessageRef   string
	SenderID     string
	ReactionType string
	Operation    MessageReactionOperation
}

// DispatchInboundCardAction normalizes the SDK callback into the provider-safe
// action contract. Ownership is resolved from MessageID in daemon state; no
// callback-supplied response/provider handle is trusted.
func (s *Service) DispatchInboundCardAction(ctx context.Context, in feishu.InboundCardAction) *notify.APIError {
	var value map[string]any
	if err := json.Unmarshal([]byte(in.ValueJSON), &value); err != nil || value == nil {
		return notify.BadRequest("invalid_action_payload", "card action value must be a JSON object")
	}
	actionID, ok := value["action_id"].(string)
	actionID = strings.TrimSpace(actionID)
	if !ok || actionID == "" {
		return notify.BadRequest("invalid_action_payload", "card action value must contain action_id")
	}
	actionPayload, ok := value["payload_json"].(string)
	if !ok {
		return notify.BadRequest("invalid_action_payload", "card action value must contain payload_json")
	}
	actionPayload, apiErr := normalizeJSONObjectRaw(actionPayload, "invalid_action_payload")
	if apiErr != nil {
		return apiErr
	}
	formJSON, formErr := normalizeJSONObjectRaw(in.FormValueJSON, "invalid_action_payload")
	if formErr != nil {
		formJSON = "{}"
	}
	payload := map[string]any{
		"tag": in.Tag,
		// Feishu's action name identifies a form component independently from
		// the provider-defined action_id carried in value. Preserve it without
		// trusting it for authorization or ownership.
		"name":        strings.TrimSpace(in.Name),
		"option":      in.Option,
		"timezone":    in.Timezone,
		"input_value": in.InputValue,
		"options":     append([]string(nil), in.Options...),
		"checked":     in.Checked,
		"value": map[string]any{
			"action_id": actionID,
			"payload":   json.RawMessage(actionPayload),
		},
		"form_value": json.RawMessage(formJSON),
	}
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return notify.NewAPIError(500, "internal", "could not encode card action", false)
	}
	return s.DispatchAgentCardAction(ctx, AgentCardActionInput{
		AppAlias:          in.AppAlias,
		DeliveryID:        in.DeliveryID,
		MessageID:         in.MessageID,
		SenderID:          in.SenderID,
		ActionID:          actionID,
		PayloadJSON:       string(payloadJSON),
		ActionPayloadJSON: actionPayload,
	})
}

type agentBroker struct {
	mu                   sync.Mutex
	nextSubID            uint64
	ttl                  time.Duration
	subscribers          map[uint64]*agentSubscriber
	deliveries           map[string]*agentDelivery
	responses            map[string]*agentResponse
	responsesByMessageID map[appStateKey]*agentResponse
	conversations        map[string]*agentConversationRoute
	seenMessages         map[string]time.Time
	seenActions          map[appStateKey]time.Time
	seenReactions        map[appStateKey]time.Time
}

type agentSubscriber struct {
	id                       uint64
	provider                 string
	commands                 map[string]struct{}
	allowedApps              map[string]struct{}
	allowedAppsConfigured    bool
	includeUnmatchedMessages bool
	includeCardActions       bool
	includeMessageReactions  bool
	ch                       chan AgentEvent
}

type agentDeliveryState int

const (
	agentDeliveryOpen agentDeliveryState = iota
	agentDeliveryStarting
	agentDeliveryRetryable
	agentDeliveryStreaming
)

type agentDelivery struct {
	mu sync.Mutex

	appAlias         string
	input            CommandInput
	allowedProviders map[string]struct{}
	expiresAt        time.Time
	state            agentDeliveryState
	provider         string
	operationID      string
	fingerprint      string
	responseID       string
	messageUUID      string
	cardJSON         string
	cardID           string
	messageID        string
	messageDedupeKey string
	actionDigests    map[string]string
	sendAmbiguousAt  time.Time
	sendRetryClosed  bool
	sendRetryCode    string
	response         *agentResponse
}

type agentOperation struct {
	kind              string
	fingerprint       string
	revision          uint64
	phase             AgentResponsePhase
	content           string
	contentUUID       string
	contentSeq        int32
	contentDone       bool
	contentAmbiguous  bool
	contentClosed     bool
	timeline          string
	timelineUUID      string
	timelineSeq       int32
	timelineDone      bool
	timelineAmbiguous bool
	timelineClosed    bool
	panelTitle        string
	panelJSON         string
	panelUUID         string
	panelSeq          int32
	panelDone         bool
	panelAmbiguous    bool
	panelClosed       bool
	settingsJSON      string
	settingsUUID      string
	settingsSeq       int32
	settingsDone      bool
	settingsAmbiguous bool
	settingsClosed    bool
	complete          bool
}

type agentResponse struct {
	mu sync.Mutex

	responseID     string
	provider       string
	appAlias       string
	deliveryID     string
	conversationID string
	chatAlias      string
	cardID         string
	messageID      string
	messageRef     string
	actionDigests  map[string]string
	revision       uint64
	phase          AgentResponsePhase
	markdown       string
	timeline       agentTimelineState
	nextSequence   int32
	lastMutationAt time.Time
	pendingOp      string
	operations     map[string]*agentOperation
	expiresAt      time.Time
}

type appStateKey struct {
	appAlias string
	id       string
}

func newAppStateKey(appAlias, id string) appStateKey {
	return appStateKey{appAlias: effectiveAppAlias(appAlias), id: strings.TrimSpace(id)}
}

func newAgentBroker(ttl time.Duration) *agentBroker {
	if ttl <= 0 {
		ttl = time.Hour
	}
	return &agentBroker{
		ttl:                  ttl,
		subscribers:          make(map[uint64]*agentSubscriber),
		deliveries:           make(map[string]*agentDelivery),
		responses:            make(map[string]*agentResponse),
		responsesByMessageID: make(map[appStateKey]*agentResponse),
		conversations:        make(map[string]*agentConversationRoute),
		seenMessages:         make(map[string]time.Time),
		seenActions:          make(map[appStateKey]time.Time),
		seenReactions:        make(map[appStateKey]time.Time),
	}
}

func (s *Service) SubscribeAgentEvents(ctx context.Context, in AgentSubscribeOptions) (*AgentSubscription, *notify.APIError) {
	_ = ctx
	sub, apiErr := s.agentBroker.subscribe(in)
	if apiErr != nil {
		return nil, apiErr
	}
	return &AgentSubscription{
		C: sub.ch,
		close: func() {
			s.agentBroker.unsubscribe(sub.id)
		},
	}, nil
}

func (b *agentBroker) subscribe(in AgentSubscribeOptions) (*agentSubscriber, *notify.APIError) {
	provider := strings.TrimSpace(in.Provider)
	if provider == "" {
		return nil, notify.BadRequest("missing_provider", "provider is required")
	}
	if len(provider) > 64 {
		return nil, notify.BadRequest("field_too_large", "one or more fields are too large")
	}
	commands := make(map[string]struct{}, len(in.Commands))
	for _, command := range in.Commands {
		command = normalizeCommand(command)
		if command == "" {
			continue
		}
		if len(command) > maxCommandBytes {
			return nil, notify.BadRequest("field_too_large", "one or more fields are too large")
		}
		commands[command] = struct{}{}
	}
	if len(commands) == 0 && !in.IncludeUnmatchedMessages && !in.IncludeCardActions && !in.IncludeMessageReactions {
		return nil, notify.BadRequest("missing_subscription", "at least one agent event kind is required")
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	b.nextSubID++
	sub := &agentSubscriber{
		id:                       b.nextSubID,
		provider:                 provider,
		commands:                 commands,
		allowedApps:              normalizedAppAllowlist(in.AllowedApps),
		allowedAppsConfigured:    in.AllowedAppsConfigured,
		includeUnmatchedMessages: in.IncludeUnmatchedMessages,
		includeCardActions:       in.IncludeCardActions,
		includeMessageReactions:  in.IncludeMessageReactions,
		ch:                       make(chan AgentEvent, agentSubscriptionBuffer),
	}
	b.subscribers[sub.id] = sub
	return sub, nil
}

func (b *agentBroker) unsubscribe(id uint64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	sub, ok := b.subscribers[id]
	if !ok {
		return
	}
	delete(b.subscribers, id)
	close(sub.ch)
}

func (b *agentBroker) dispatchMessage(in CommandInput) (delivered int, handled bool) {
	return b.dispatchMessageForProvider(in, "")
}

// dispatchMessageToProvider preserves reply ownership: only subscriptions for
// the provider that authored the parent message can receive this prompt.
func (b *agentBroker) dispatchMessageToProvider(in CommandInput, provider string) (delivered int, handled bool) {
	return b.dispatchMessageForProvider(in, strings.TrimSpace(provider))
}

func (b *agentBroker) dispatchMessageForProvider(in CommandInput, provider string) (delivered int, handled bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := time.Now()
	b.pruneLocked(now)
	if _, exists := b.deliveries[in.DeliveryID]; exists {
		return 0, true
	}
	messageDedupeKey := agentMessageDedupeKey(in.AppAlias, in.Metadata["message_id"])
	if messageDedupeKey != "" {
		if _, seen := b.seenMessages[messageDedupeKey]; seen {
			return 0, true
		}
	}
	if b.conversationAppConflictLocked(in.ConversationID, in.AppAlias) {
		// The wire handle carries no app field, so one opaque conversation id
		// can never safely reverse-resolve to two applications. Fail closed
		// before enqueueing an event that could later egress through the other
		// app's route.
		return 0, true
	}

	exact := make([]*agentSubscriber, 0)
	fallback := make([]*agentSubscriber, 0)
	for _, sub := range b.subscribers {
		if provider != "" && sub.provider != provider {
			continue
		}
		// Application authorization participates in candidate selection. A
		// disallowed exact subscriber must not suppress an allowed unmatched
		// subscriber for this app.
		if !sub.allowsApp(in.AppAlias) {
			continue
		}
		if _, ok := sub.commands[in.Command]; ok {
			exact = append(exact, sub)
			continue
		}
		if sub.includeUnmatchedMessages {
			fallback = append(fallback, sub)
		}
	}
	candidates := exact
	if len(candidates) == 0 {
		candidates = fallback
	}

	event := AgentEvent{
		DeliveryID:     in.DeliveryID,
		ConversationID: in.ConversationID,
		ChatAlias:      in.ChatAlias,
		SenderID:       in.SenderID,
		Metadata:       agentPublicMetadata(in.Metadata, in.AppAlias),
		Message: &AgentMessage{
			Text:              in.Prompt,
			Command:           in.Command,
			CommandText:       in.Text,
			ReplyToMessageRef: feishu.MessageRefForApp(in.AppAlias, in.Metadata["parent_id"]),
			ConversationTitle: in.ConversationTitle,
		},
	}
	allowed := make(map[string]struct{})
	providerSent := make(map[string]struct{})
	for _, sub := range candidates {
		if _, duplicateProvider := providerSent[sub.provider]; duplicateProvider {
			continue
		}
		select {
		case sub.ch <- cloneAgentEvent(event):
			providerSent[sub.provider] = struct{}{}
			allowed[sub.provider] = struct{}{}
			delivered++
		default:
		}
	}
	if delivered > 0 {
		b.deliveries[in.DeliveryID] = &agentDelivery{
			appAlias:         in.AppAlias,
			input:            cloneCommandInput(in),
			allowedProviders: allowed,
			expiresAt:        now.Add(b.ttl),
			state:            agentDeliveryOpen,
			messageDedupeKey: messageDedupeKey,
		}
		b.recordConversationLocked(in, allowed, now)
		if messageDedupeKey != "" {
			b.seenMessages[messageDedupeKey] = now.Add(b.ttl)
		}
	}
	return delivered, delivered > 0
}

func (s *Service) StartAgentResponse(ctx context.Context, in StartAgentResponseInput) (AgentResponseReceipt, *notify.APIError) {
	// Preserve the legacy single-sender capability check: an implementation
	// with no CardKit backend reports Unimplemented before consulting handles.
	if s.legacyCapabilityCheck && !s.hasDynamicCards() {
		return AgentResponseReceipt{}, notify.NotImplemented("agent_streaming_unavailable", "agent streaming is unavailable for this sender")
	}
	provider, deliveryID, operationID, apiErr := validateAgentIdentity(in.Provider, in.DeliveryID, in.OperationID)
	if apiErr != nil {
		return AgentResponseReceipt{}, apiErr
	}
	in.Provider, in.DeliveryID, in.OperationID = provider, deliveryID, operationID
	card, apiErr := buildAgentCard(in.Content)
	if apiErr != nil {
		return AgentResponseReceipt{}, apiErr
	}
	fingerprint := hashJSON(struct {
		Provider   string
		DeliveryID string
		CardJSON   string
	}{provider, deliveryID, card.json})

	b := s.agentBroker
	now := time.Now()
	delivery, ok := s.lookupAndPinAgentDelivery(
		provider,
		deliveryID,
		now,
		now.Add(messageUUIDDedupeWindow+s.cfg.SendTimeout),
	)
	if !ok {
		return AgentResponseReceipt{}, notify.NewAPIError(404, "unknown_delivery", "unknown delivery", false)
	}

	delivery.mu.Lock()
	defer delivery.mu.Unlock()
	if _, allowed := delivery.allowedProviders[provider]; !allowed || !s.appAllowed(provider, delivery.appAlias) {
		return AgentResponseReceipt{}, notify.NewAPIError(404, "unknown_delivery", "unknown delivery", false)
	}
	backend, ok := s.backendForApp(delivery.appAlias)
	if !ok || backend.dynamicCards == nil {
		return AgentResponseReceipt{}, notify.NotImplemented("agent_streaming_unavailable", "agent streaming is unavailable for this sender")
	}
	dynamicCards := backend.dynamicCards
	if delivery.operationID != "" {
		if delivery.operationID != operationID {
			return AgentResponseReceipt{}, notify.NewAPIError(409, "already_responded", "delivery already has a response", false)
		}
		if delivery.fingerprint != fingerprint {
			return AgentResponseReceipt{}, notify.NewAPIError(409, "operation_conflict", "operation id reused with different content", false)
		}
		if delivery.response != nil {
			delivery.response.mu.Lock()
			receipt := receiptFor(delivery.response, true)
			delivery.response.mu.Unlock()
			if apiErr := s.persistAgentOwner(delivery.response); apiErr != nil {
				return AgentResponseReceipt{}, apiErr
			}
			return receipt, nil
		}
		if delivery.state == agentDeliveryStarting {
			return AgentResponseReceipt{}, notify.NewAPIError(409, "response_in_flight", "delivery response is already being sent", true)
		}
	} else {
		responseID, err := randomOpaqueID("resp_")
		if err != nil {
			return AgentResponseReceipt{}, notify.NewAPIError(500, "internal", "could not create response handle", true)
		}
		delivery.provider = provider
		delivery.operationID = operationID
		delivery.fingerprint = fingerprint
		delivery.responseID = responseID
		delivery.messageUUID = operationUUID("msg", responseID, operationID, 50)
		delivery.cardJSON = card.json
		delivery.actionDigests = cloneStringMap(card.actionDigests)
	}
	if delivery.sendRetryClosed {
		return AgentResponseReceipt{}, closedAgentSendError(delivery.sendRetryCode)
	}
	if !delivery.sendAmbiguousAt.IsZero() && messageRetryWindowExhausted(time.Now(), s.cfg.SendTimeout, delivery.sendAmbiguousAt) {
		delivery.sendRetryClosed = true
		delivery.sendRetryCode = "send_retry_expired"
		return AgentResponseReceipt{}, closedAgentSendError(delivery.sendRetryCode)
	}
	delivery.state = agentDeliveryStarting

	callCtx, cancel := context.WithTimeout(ctx, s.cfg.SendTimeout)
	defer cancel()
	if delivery.cardID == "" {
		cardID, err := dynamicCards.CreateCard(callCtx, delivery.cardJSON)
		if err != nil {
			s.logAgentCardFailure("create", delivery.responseID, err)
			if isCardAPIRejection(err) && !isRetryableCardFailure(err) {
				delivery.abortStartAttempt()
			} else {
				delivery.state = agentDeliveryRetryable
			}
			return AgentResponseReceipt{}, agentCardCallError(err, "Feishu card creation failed")
		}
		delivery.cardID = cardID
	}
	if delivery.messageID == "" {
		chatID := ""
		if delivery.input.ChatAlias == "direct" || delivery.input.UnconfiguredGroup {
			chatID = delivery.input.ChatID
			if chatID == "" {
				delivery.abortStartAttempt()
				return AgentResponseReceipt{}, notify.NewAPIError(404, "unknown_delivery", "unknown delivery", false)
			}
		} else {
			routeApp, routeChatID, exists := s.cfg.ResolveChannel(delivery.input.ChatAlias)
			if !exists || routeApp != delivery.appAlias {
				delivery.abortStartAttempt()
				return AgentResponseReceipt{}, notify.NewAPIError(404, "unknown_delivery", "unknown delivery", false)
			}
			chatID = routeChatID
		}
		replyToMessageID := delivery.input.Metadata["message_id"]
		sendRequest := feishu.CardSendRequest{
			ReplyToMessageID: replyToMessageID,
			CardID:           delivery.cardID,
			UUID:             delivery.messageUUID,
		}
		if replyToMessageID == "" {
			sendRequest.ChatID = chatID
		}
		sendAttemptStartedAt := time.Now()
		messageID, err := dynamicCards.SendCard(callCtx, sendRequest)
		if err != nil {
			s.logAgentCardFailure("send", delivery.responseID, err)
			if isCardAPIRejection(err) && !isRetryableCardFailure(err) {
				if !delivery.sendAmbiguousAt.IsZero() {
					delivery.state = agentDeliveryRetryable
					delivery.sendRetryClosed = true
					delivery.sendRetryCode = "send_state_unknown"
					return AgentResponseReceipt{}, closedAgentSendError(delivery.sendRetryCode)
				}
				delivery.abortStartAttempt()
			} else {
				if isAmbiguousCardFailure(err) && delivery.sendAmbiguousAt.IsZero() {
					delivery.sendAmbiguousAt = sendAttemptStartedAt
				}
				delivery.state = agentDeliveryRetryable
			}
			return AgentResponseReceipt{}, agentCardCallError(err, "Feishu card send failed")
		}
		delivery.messageID = messageID
	}

	messageRef := feishu.MessageRefForApp(delivery.appAlias, delivery.messageID)
	if messageRef == "" {
		return AgentResponseReceipt{}, notify.NewAPIError(500, "internal", "could not derive response message reference", true)
	}
	response := &agentResponse{
		responseID:     delivery.responseID,
		provider:       provider,
		appAlias:       delivery.appAlias,
		deliveryID:     deliveryID,
		conversationID: delivery.input.ConversationID,
		chatAlias:      delivery.input.ChatAlias,
		cardID:         delivery.cardID,
		messageID:      delivery.messageID,
		messageRef:     messageRef,
		actionDigests:  cloneStringMap(delivery.actionDigests),
		revision:       1,
		phase:          AgentResponsePhaseStreaming,
		markdown:       normalizedInitialMarkdown(in.Content.Markdown),
		timeline:       card.timeline,
		operations:     make(map[string]*agentOperation),
		expiresAt:      time.Now().Add(b.ttl),
	}
	delivery.response = response
	delivery.state = agentDeliveryStreaming
	delivery.compactAfterStart()
	b.mu.Lock()
	b.responses[response.responseID] = response
	b.responsesByMessageID[newAppStateKey(response.appAlias, response.messageID)] = response
	b.mu.Unlock()
	if apiErr := s.persistAgentOwner(response); apiErr != nil {
		return AgentResponseReceipt{}, apiErr
	}
	return receiptFor(response, false), nil
}

func (s *Service) persistAgentOwner(response *agentResponse) *notify.APIError {
	if s.agentOwners == nil {
		return nil
	}
	if err := s.agentOwners.Put(response.messageRef, response.provider, time.Now()); err != nil {
		return notify.NewAPIError(500, "ownership_store_unavailable", "could not persist response ownership", true)
	}
	return nil
}

func (s *Service) UpdateAgentResponse(ctx context.Context, in UpdateAgentResponseInput) (AgentResponseReceipt, *notify.APIError) {
	provider, responseID, operationID, apiErr := validateAgentIdentity(in.Provider, in.ResponseID, in.OperationID)
	if apiErr != nil {
		return AgentResponseReceipt{}, apiErr
	}
	if in.ExpectedRevision == 0 {
		return AgentResponseReceipt{}, notify.BadRequest("invalid_revision", "expected_revision must be positive")
	}
	in.Markdown = strings.TrimSpace(in.Markdown)
	if in.Markdown == "" {
		return AgentResponseReceipt{}, notify.BadRequest("missing_markdown", "markdown is required")
	}
	timeline, apiErr := normalizedTimelineParts(in.TimelineMarkdown, in.TimelineTitle)
	if apiErr != nil {
		return AgentResponseReceipt{}, apiErr
	}
	if len(in.Markdown) > maxAgentCardBytes {
		return AgentResponseReceipt{}, notify.BadRequest("field_too_large", "one or more fields are too large")
	}
	response := s.agentBroker.lookupResponse(provider, responseID)
	if response == nil || !s.appAllowed(provider, response.appAlias) {
		return AgentResponseReceipt{}, notify.NewAPIError(404, "unknown_response", "unknown response", false)
	}
	backend, ok := s.backendForApp(response.appAlias)
	if !ok || backend.dynamicCards == nil {
		return AgentResponseReceipt{}, notify.NotImplemented("agent_streaming_unavailable", "agent streaming is unavailable for this sender")
	}
	fingerprint := hashJSON(struct {
		Expected uint64
		Markdown string
		Timeline agentTimelineParts
	}{in.ExpectedRevision, in.Markdown, timeline})
	return s.applyAgentUpdate(ctx, backend.dynamicCards, response, operationID, fingerprint, in.ExpectedRevision, in.Markdown, timeline)
}

func (s *Service) applyAgentUpdate(
	ctx context.Context,
	dynamicCards feishu.DynamicCards,
	response *agentResponse,
	operationID, fingerprint string,
	expected uint64,
	markdown string,
	timeline agentTimelineParts,
) (AgentResponseReceipt, *notify.APIError) {
	response.mu.Lock()
	defer response.mu.Unlock()
	if apiErr := response.checkCardBudget(markdown, timeline); apiErr != nil {
		return AgentResponseReceipt{}, apiErr
	}
	op, apiErr := response.beginOperation(operationID, "update", fingerprint, expected)
	if apiErr != nil {
		return AgentResponseReceipt{}, apiErr
	}
	if op.complete {
		return operationReceipt(response, op, true), nil
	}
	if op.contentSeq == 0 {
		op.content = markdown
		op.contentSeq = response.nextSequence + 1
		op.contentUUID = operationUUID("content", response.responseID, operationID, 64)
		response.planTimelineCalls(op, operationID, op.contentSeq, timeline)
	}
	callCtx, cancel := context.WithTimeout(ctx, s.cfg.SendTimeout)
	defer cancel()
	if !op.contentDone {
		if apiErr := s.applyAgentContent(callCtx, dynamicCards, response, op, operationID, "update"); apiErr != nil {
			return AgentResponseReceipt{}, apiErr
		}
	}
	if apiErr := s.applyAgentTimeline(callCtx, dynamicCards, response, op, operationID); apiErr != nil {
		return AgentResponseReceipt{}, apiErr
	}
	op.complete = true
	response.revision++
	op.revision = response.revision
	op.phase = response.phase
	response.pendingOp = ""
	op.compact()
	return operationReceipt(response, op, false), nil
}

// applyAgentContent writes the accumulated answer snapshot to the card's
// markdown element and commits it to the response before returning, so a retry
// of a multi-call operation never repeats a call Feishu already accepted.
func (s *Service) applyAgentContent(
	ctx context.Context,
	dynamicCards feishu.DynamicCards,
	response *agentResponse,
	op *agentOperation,
	operationID, logOperation string,
) *notify.APIError {
	if err := response.waitForMutation(ctx); err != nil {
		return notify.NewAPIError(502, "feishu_unavailable", "Feishu card update was cancelled", true)
	}
	if err := dynamicCards.UpdateContent(ctx, feishu.CardContentUpdate{
		CardID: response.cardID, ElementID: agentContentElementID, Content: op.content,
		UUID: op.contentUUID, Sequence: op.contentSeq,
	}); err != nil {
		s.logAgentCardFailure(logOperation, response.responseID, err)
		if isCardAPIRejection(err) && !isRetryableCardFailure(err) {
			if op.contentAmbiguous {
				op.contentClosed = true
				return operationStateUnknownError()
			}
			response.abortOperation(operationID)
		} else if isAmbiguousCardFailure(err) {
			op.contentAmbiguous = true
		}
		return agentCardCallError(err, "Feishu card update failed")
	}
	op.contentDone = true
	response.nextSequence = op.contentSeq
	response.markdown = op.content
	return nil
}

// applyAgentTimeline runs the timeline half of an operation: the panel body
// first, then its collapsed header. That order keeps a partially applied
// operation readable — a header may lag the steps it summarizes, but a step
// never appears in the header before it exists in the body.
func (s *Service) applyAgentTimeline(
	ctx context.Context,
	dynamicCards feishu.DynamicCards,
	response *agentResponse,
	op *agentOperation,
	operationID string,
) *notify.APIError {
	if op.timelineSeq != 0 && !op.timelineDone {
		if err := response.waitForMutation(ctx); err != nil {
			return notify.NewAPIError(502, "feishu_unavailable", "Feishu card timeline update was cancelled", true)
		}
		if err := dynamicCards.UpdateContent(ctx, feishu.CardContentUpdate{
			CardID: response.cardID, ElementID: agentTimelineElementID, Content: op.timeline,
			UUID: op.timelineUUID, Sequence: op.timelineSeq,
		}); err != nil {
			s.logAgentCardFailure("timeline content", response.responseID, err)
			if isCardAPIRejection(err) && !isRetryableCardFailure(err) {
				if op.timelineAmbiguous {
					op.timelineClosed = true
					return operationStateUnknownError()
				}
				response.abortOperation(operationID)
			} else if isAmbiguousCardFailure(err) {
				op.timelineAmbiguous = true
			}
			return agentCardCallError(err, "Feishu card timeline update failed")
		}
		op.timelineDone = true
		response.nextSequence = op.timelineSeq
		response.timeline.markdown = op.timeline
	}
	if op.panelSeq != 0 && !op.panelDone {
		if err := response.waitForMutation(ctx); err != nil {
			return notify.NewAPIError(502, "feishu_unavailable", "Feishu card timeline update was cancelled", true)
		}
		if err := dynamicCards.BatchUpdate(ctx, feishu.CardBatchUpdate{
			CardID: response.cardID, ActionsJSON: op.panelJSON,
			UUID: op.panelUUID, Sequence: op.panelSeq,
		}); err != nil {
			s.logAgentCardFailure("timeline header", response.responseID, err)
			if isCardAPIRejection(err) && !isRetryableCardFailure(err) {
				if op.panelAmbiguous {
					op.panelClosed = true
					return operationStateUnknownError()
				}
				response.abortOperation(operationID)
			} else if isAmbiguousCardFailure(err) {
				op.panelAmbiguous = true
			}
			return agentCardCallError(err, "Feishu card timeline update failed")
		}
		op.panelDone = true
		response.nextSequence = op.panelSeq
		response.timeline.title = op.panelTitle
	}
	return nil
}

// planTimelineCalls reserves the sequence numbers and idempotency UUIDs for an
// operation's timeline calls exactly once, on its first attempt. Freezing the
// plan is what makes a retry replay the same Feishu operations rather than
// recomputing them against state the earlier attempt already advanced.
//
// after is the last sequence the caller has reserved so far, or 0 when it is
// making no earlier call. The last reserved sequence is returned so a caller
// with a trailing call of its own can continue the card's one sequence domain.
func (r *agentResponse) planTimelineCalls(
	op *agentOperation, operationID string, after int32, timeline agentTimelineParts,
) int32 {
	next := after
	if next == 0 {
		next = r.nextSequence
	}
	if !r.timeline.present {
		// A response whose Start carried no timeline has no panel to address.
		// Timeline fields on it are ignored rather than rejected, so a provider
		// can send them unconditionally across both card shapes.
		return next
	}
	if timeline.Markdown != "" && timeline.Markdown != r.timeline.markdown {
		next++
		op.timeline = timeline.Markdown
		op.timelineSeq = next
		op.timelineUUID = operationUUID("timeline", r.responseID, operationID, 64)
	}
	if timeline.Title != "" && timeline.Title != r.timeline.title {
		next++
		op.panelTitle = timeline.Title
		op.panelSeq = next
		op.panelUUID = operationUUID("panel", r.responseID, operationID, 64)
		op.panelJSON = agentTimelineHeaderPatch(timeline.Title)
	}
	return next
}

// checkCardBudget holds the answer and the timeline to one shared ceiling,
// because Feishu's size limit applies to the rendered card rather than to any
// single element. Parts the request leaves empty keep their current value.
func (r *agentResponse) checkCardBudget(markdown string, timeline agentTimelineParts) *notify.APIError {
	if !r.timeline.present {
		return nil
	}
	effective := r.timeline.markdown
	if timeline.Markdown != "" {
		effective = timeline.Markdown
	}
	if len(markdown)+len(effective) > maxAgentCardBytes {
		return notify.BadRequest("field_too_large", "one or more fields are too large")
	}
	return nil
}

func normalizedTimelineParts(markdown, title string) (agentTimelineParts, *notify.APIError) {
	parts := agentTimelineParts{
		Markdown: strings.TrimSpace(markdown),
		Title:    strings.TrimSpace(title),
	}
	if len(parts.Markdown) > maxAgentCardBytes || len(parts.Title) > maxAgentTitleBytes {
		return agentTimelineParts{}, notify.BadRequest("field_too_large", "one or more fields are too large")
	}
	return parts, nil
}

func (s *Service) FinishAgentResponse(ctx context.Context, in FinishAgentResponseInput) (AgentResponseReceipt, *notify.APIError) {
	provider, responseID, operationID, apiErr := validateAgentIdentity(in.Provider, in.ResponseID, in.OperationID)
	if apiErr != nil {
		return AgentResponseReceipt{}, apiErr
	}
	if in.ExpectedRevision == 0 {
		return AgentResponseReceipt{}, notify.BadRequest("invalid_revision", "expected_revision must be positive")
	}
	phase := phaseForOutcome(in.Outcome)
	if phase == AgentResponsePhaseUnspecified {
		return AgentResponseReceipt{}, notify.BadRequest("invalid_outcome", "outcome must be completed, failed, or cancelled")
	}
	in.Markdown = strings.TrimSpace(in.Markdown)
	if in.Markdown == "" {
		return AgentResponseReceipt{}, notify.BadRequest("missing_markdown", "markdown is required")
	}
	in.Summary = strings.TrimSpace(in.Summary)
	if len(in.Markdown) > maxAgentCardBytes || len(in.Summary) > maxAgentTitleBytes {
		return AgentResponseReceipt{}, notify.BadRequest("field_too_large", "one or more fields are too large")
	}
	timeline, apiErr := normalizedTimelineParts(in.TimelineMarkdown, in.TimelineTitle)
	if apiErr != nil {
		return AgentResponseReceipt{}, apiErr
	}
	response := s.agentBroker.lookupResponse(provider, responseID)
	if response == nil || !s.appAllowed(provider, response.appAlias) {
		return AgentResponseReceipt{}, notify.NewAPIError(404, "unknown_response", "unknown response", false)
	}
	backend, ok := s.backendForApp(response.appAlias)
	if !ok || backend.dynamicCards == nil {
		return AgentResponseReceipt{}, notify.NotImplemented("agent_streaming_unavailable", "agent streaming is unavailable for this sender")
	}
	fingerprint := hashJSON(struct {
		Expected uint64
		Phase    AgentResponsePhase
		Markdown string
		Summary  string
		Timeline agentTimelineParts
	}{in.ExpectedRevision, phase, in.Markdown, in.Summary, timeline})
	return s.applyAgentFinish(ctx, backend.dynamicCards, response, operationID, fingerprint, in.ExpectedRevision, phase, in.Markdown, in.Summary, timeline)
}

func (s *Service) applyAgentFinish(
	ctx context.Context,
	dynamicCards feishu.DynamicCards,
	response *agentResponse,
	operationID, fingerprint string,
	expected uint64,
	phase AgentResponsePhase,
	markdown, summary string,
	timeline agentTimelineParts,
) (AgentResponseReceipt, *notify.APIError) {
	response.mu.Lock()
	defer response.mu.Unlock()
	if apiErr := response.checkCardBudget(markdown, timeline); apiErr != nil {
		return AgentResponseReceipt{}, apiErr
	}
	op, apiErr := response.beginOperation(operationID, "finish", fingerprint, expected)
	if apiErr != nil {
		return AgentResponseReceipt{}, apiErr
	}
	if op.complete {
		return operationReceipt(response, op, true), nil
	}
	if op.settingsJSON == "" {
		if markdown != response.markdown {
			op.content = markdown
			op.contentSeq = response.nextSequence + 1
			op.contentUUID = operationUUID("content", response.responseID, operationID, 64)
		}
		// Streaming mode is disabled last, after every content call this
		// operation plans, so nothing is written to a card already closed.
		op.settingsSeq = response.planTimelineCalls(op, operationID, op.contentSeq, timeline) + 1
		op.settingsUUID = operationUUID("finish", response.responseID, operationID, 64)
		op.settingsJSON = agentFinishSettings(summary)
		op.phase = phase
	}
	callCtx, cancel := context.WithTimeout(ctx, s.cfg.SendTimeout)
	defer cancel()
	if op.contentSeq != 0 && !op.contentDone {
		if apiErr := s.applyAgentContent(callCtx, dynamicCards, response, op, operationID, "finish content"); apiErr != nil {
			return AgentResponseReceipt{}, apiErr
		}
	}
	if apiErr := s.applyAgentTimeline(callCtx, dynamicCards, response, op, operationID); apiErr != nil {
		return AgentResponseReceipt{}, apiErr
	}
	if !op.settingsDone {
		if err := response.waitForMutation(callCtx); err != nil {
			return AgentResponseReceipt{}, notify.NewAPIError(502, "feishu_unavailable", "Feishu card finalization was cancelled", true)
		}
		if err := dynamicCards.UpdateSettings(callCtx, feishu.CardSettingsUpdate{
			CardID: response.cardID, SettingsJSON: op.settingsJSON,
			UUID: op.settingsUUID, Sequence: op.settingsSeq,
		}); err != nil {
			s.logAgentCardFailure("finish settings", response.responseID, err)
			if isCardAPIRejection(err) && !isRetryableCardFailure(err) {
				if op.settingsAmbiguous {
					op.settingsClosed = true
					return AgentResponseReceipt{}, operationStateUnknownError()
				}
				response.abortOperation(operationID)
			} else if isAmbiguousCardFailure(err) {
				op.settingsAmbiguous = true
			}
			return AgentResponseReceipt{}, agentCardCallError(err, "Feishu card finalization failed")
		}
		op.settingsDone = true
		response.nextSequence = op.settingsSeq
	}
	op.complete = true
	response.markdown = markdown
	response.phase = phase
	response.revision++
	op.revision = response.revision
	response.pendingOp = ""
	response.markdown = ""
	op.compact()
	return operationReceipt(response, op, false), nil
}

func (r *agentResponse) waitForMutation(ctx context.Context) error {
	wait := minCardMutationInterval - time.Since(r.lastMutationAt)
	if wait > 0 {
		timer := time.NewTimer(wait)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}
	}
	// Feishu counts attempted calls, not just successful mutations. Holding the
	// response mutex makes this one card-wide rate clock and sequence domain.
	r.lastMutationAt = time.Now()
	return nil
}

func (r *agentResponse) beginOperation(operationID, kind, fingerprint string, expected uint64) (*agentOperation, *notify.APIError) {
	if existing, ok := r.operations[operationID]; ok {
		if existing.kind != kind || existing.fingerprint != fingerprint {
			return nil, notify.NewAPIError(409, "operation_conflict", "operation id reused with different content", false)
		}
		if existing.outcomeUnknown() {
			return nil, operationStateUnknownError()
		}
		return existing, nil
	}
	if r.phase != AgentResponsePhaseStreaming {
		return nil, notify.NewAPIError(412, "response_closed", "response is already closed", false)
	}
	if r.pendingOp != "" {
		if pending := r.operations[r.pendingOp]; pending != nil && pending.outcomeUnknown() {
			return nil, operationStateUnknownError()
		}
		return nil, notify.NewAPIError(409, "operation_in_flight", "another response operation is in flight", true)
	}
	if expected != r.revision {
		return nil, notify.NewAPIError(409, "revision_conflict", "expected_revision does not match the current revision", true)
	}
	op := &agentOperation{kind: kind, fingerprint: fingerprint}
	r.operations[operationID] = op
	r.pendingOp = operationID
	return op, nil
}

func (r *agentResponse) abortOperation(operationID string) {
	delete(r.operations, operationID)
	if r.pendingOp == operationID {
		r.pendingOp = ""
	}
}

func (op *agentOperation) compactContent() {
	op.content = ""
	op.contentUUID = ""
	op.contentSeq = 0
	op.contentDone = false
	op.contentAmbiguous = false
	op.contentClosed = false
}

func (op *agentOperation) compact() {
	op.compactContent()
	op.timeline = ""
	op.timelineUUID = ""
	op.timelineSeq = 0
	op.timelineDone = false
	op.timelineAmbiguous = false
	op.timelineClosed = false
	op.panelTitle = ""
	op.panelJSON = ""
	op.panelUUID = ""
	op.panelSeq = 0
	op.panelDone = false
	op.panelAmbiguous = false
	op.panelClosed = false
	op.settingsJSON = ""
	op.settingsUUID = ""
	op.settingsSeq = 0
	op.settingsDone = false
	op.settingsAmbiguous = false
	op.settingsClosed = false
}

func (op *agentOperation) outcomeUnknown() bool {
	return op.contentClosed || op.timelineClosed || op.panelClosed || op.settingsClosed
}

func (d *agentDelivery) abortStartAttempt() {
	d.state = agentDeliveryOpen
	d.provider = ""
	d.operationID = ""
	d.fingerprint = ""
	d.responseID = ""
	d.messageUUID = ""
	d.cardJSON = ""
	d.cardID = ""
	d.messageID = ""
	d.actionDigests = nil
	d.sendAmbiguousAt = time.Time{}
	d.sendRetryClosed = false
	d.sendRetryCode = ""
	d.response = nil
}

func (d *agentDelivery) compactAfterStart() {
	d.input = CommandInput{}
	d.messageUUID = ""
	d.cardJSON = ""
	d.cardID = ""
	d.messageID = ""
	d.actionDigests = nil
	d.sendAmbiguousAt = time.Time{}
	d.sendRetryClosed = false
	d.sendRetryCode = ""
}

func isCardAPIRejection(err error) bool {
	var apiErr *feishu.DynamicCardAPIError
	return errors.As(err, &apiErr)
}

func isRetryableCardFailure(err error) bool {
	var apiErr *feishu.DynamicCardAPIError
	if !errors.As(err, &apiErr) {
		// A transport error is ambiguous, so callers must retry the exact operation.
		return true
	}
	return apiErr.HTTPStatus == 429 || apiErr.HTTPStatus >= 500 || apiErr.Code == 200810 || apiErr.Code == 230020 || apiErr.Code == 300120
}

func isAmbiguousCardFailure(err error) bool {
	var apiErr *feishu.DynamicCardAPIError
	if !errors.As(err, &apiErr) {
		return true
	}
	// A server error can be emitted after the mutation commits. A rate-limit
	// rejection is explicit and does not create that same outcome ambiguity.
	return apiErr.HTTPStatus >= 500 || apiErr.Code == 300120
}

func messageRetryWindowExhausted(now time.Time, sendTimeout time.Duration, firstAmbiguousAttempt time.Time) bool {
	if firstAmbiguousAttempt.IsZero() {
		return false
	}
	if sendTimeout < 0 {
		sendTimeout = 0
	}
	return !now.Add(sendTimeout).Before(firstAmbiguousAttempt.Add(messageUUIDDedupeWindow))
}

func closedAgentSendError(code string) *notify.APIError {
	if code == "send_retry_expired" {
		return notify.NewAPIError(412, code, "the safe Feishu message retry window has expired", false)
	}
	return notify.NewAPIError(412, "send_state_unknown", "the Feishu message outcome is unknown and cannot be retried safely", false)
}

func operationStateUnknownError() *notify.APIError {
	return notify.NewAPIError(412, "operation_state_unknown", "the Feishu card operation outcome is unknown and cannot be replaced safely", false)
}

func agentCardCallError(err error, message string) *notify.APIError {
	if isRetryableCardFailure(err) {
		return notify.NewAPIError(502, "feishu_unavailable", message, true)
	}
	return notify.NewAPIError(412, "feishu_rejected", message, false)
}

func (b *agentBroker) lookupResponse(provider, responseID string) *agentResponse {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.pruneLocked(time.Now())
	response := b.responses[responseID]
	if response == nil || response.provider != provider {
		return nil
	}
	return response
}

func (b *agentBroker) lookupAndPinDelivery(
	provider, deliveryID string,
	appAllowed func(string) bool,
	now, expiresAt time.Time,
) (*agentDelivery, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.pruneLocked(now)
	delivery, ok := b.deliveries[deliveryID]
	if !ok {
		return nil, false
	}
	if _, received := delivery.allowedProviders[provider]; !received ||
		appAllowed != nil && !appAllowed(delivery.appAlias) {
		return nil, false
	}
	if delivery.expiresAt.Before(expiresAt) {
		delivery.expiresAt = expiresAt
	}
	if delivery.messageDedupeKey != "" && b.seenMessages[delivery.messageDedupeKey].Before(expiresAt) {
		b.seenMessages[delivery.messageDedupeKey] = expiresAt
	}
	return delivery, true
}

func (s *Service) lookupAndPinAgentDelivery(provider, deliveryID string, now, expiresAt time.Time) (*agentDelivery, bool) {
	routes := s.inboundRoutes
	routes.mu.Lock()
	defer routes.mu.Unlock()
	routes.pruneLocked(now)
	delivery, ok := s.agentBroker.lookupAndPinDelivery(
		provider,
		deliveryID,
		func(appAlias string) bool { return s.appAllowed(provider, appAlias) },
		now,
		expiresAt,
	)
	if !ok {
		return nil, false
	}
	routes.deliveries[deliveryID] = expiresAt
	if delivery.messageDedupeKey != "" {
		routes.messages[delivery.messageDedupeKey] = expiresAt
	}
	return delivery, true
}

func (s *Service) DispatchAgentCardAction(ctx context.Context, in AgentCardActionInput) *notify.APIError {
	_ = ctx
	in.AppAlias = strings.TrimSpace(in.AppAlias)
	in.DeliveryID = strings.TrimSpace(in.DeliveryID)
	in.MessageID = strings.TrimSpace(in.MessageID)
	in.SenderID = strings.TrimSpace(in.SenderID)
	in.ActionID = strings.TrimSpace(in.ActionID)
	in.PayloadJSON = strings.TrimSpace(in.PayloadJSON)
	in.ActionPayloadJSON = strings.TrimSpace(in.ActionPayloadJSON)
	if in.DeliveryID == "" || in.MessageID == "" {
		return notify.BadRequest("missing_action_identity", "action delivery and message identity are required")
	}
	if in.SenderID == "" {
		return notify.BadRequest("missing_action_actor", "card action actor identity is required")
	}
	if in.ActionID == "" {
		return notify.BadRequest("missing_action_id", "action_id is required")
	}
	if len(in.DeliveryID) > 160 || len(in.MessageID) > 160 || len(in.SenderID) > 160 || len(in.ActionID) > 64 || len(in.PayloadJSON) > 16*1024 {
		return notify.BadRequest("field_too_large", "one or more fields are too large")
	}
	if apiErr := validateJSONObject(in.PayloadJSON, "invalid_action_payload"); apiErr != nil {
		return apiErr
	}
	var apiErr *notify.APIError
	in.ActionPayloadJSON, apiErr = normalizeJSONObjectRaw(in.ActionPayloadJSON, "invalid_action_payload")
	if apiErr != nil {
		return apiErr
	}
	return s.agentBroker.dispatchAction(in)
}

// DispatchAgentMessageReaction routes one native reaction only to the provider
// that authored the target message. Unknown ownership is never broadcast.
func (s *Service) DispatchAgentMessageReaction(ctx context.Context, in AgentMessageReactionInput) *notify.APIError {
	_ = ctx
	in.AppAlias = strings.TrimSpace(in.AppAlias)
	in.DeliveryID = strings.TrimSpace(in.DeliveryID)
	in.MessageRef = strings.TrimSpace(in.MessageRef)
	in.SenderID = strings.TrimSpace(in.SenderID)
	in.ReactionType = strings.TrimSpace(in.ReactionType)
	if in.DeliveryID == "" || in.MessageRef == "" {
		return notify.BadRequest("missing_reaction_identity", "reaction delivery and message reference are required")
	}
	if in.SenderID == "" {
		return notify.BadRequest("missing_reaction_actor", "reaction actor identity is required")
	}
	if in.ReactionType == "" || in.Operation == MessageReactionUnspecified {
		return notify.BadRequest("missing_reaction", "reaction type and operation are required")
	}
	if len(in.DeliveryID) > 160 || len(in.MessageRef) > 160 || len(in.SenderID) > 160 || len(in.ReactionType) > 64 {
		return notify.BadRequest("field_too_large", "one or more fields are too large")
	}
	if s.agentOwners == nil {
		return nil
	}
	owner, ok, err := s.agentOwners.Lookup(in.MessageRef, time.Now())
	if err != nil {
		return notify.NewAPIError(500, "ownership_store_unavailable", "could not resolve response ownership", true)
	}
	if !ok || !s.appAllowed(owner.Provider, in.AppAlias) {
		return nil
	}
	return s.agentBroker.dispatchReaction(owner.Provider, in)
}

func (b *agentBroker) dispatchReaction(provider string, in AgentMessageReactionInput) *notify.APIError {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := time.Now()
	b.pruneLocked(now)
	reactionKey := newAppStateKey(in.AppAlias, in.DeliveryID)
	if _, duplicate := b.seenReactions[reactionKey]; duplicate {
		return nil
	}
	var target *agentSubscriber
	for _, sub := range b.subscribers {
		if sub.provider != provider || !sub.includeMessageReactions || !sub.allowsApp(in.AppAlias) {
			continue
		}
		if target == nil || sub.id < target.id {
			target = sub
		}
	}
	if target == nil {
		return nil
	}
	event := AgentEvent{
		DeliveryID: in.DeliveryID,
		SenderID:   in.SenderID,
		Metadata:   appAliasMetadata(in.AppAlias),
		MessageReaction: &AgentMessageReaction{
			MessageRef:   in.MessageRef,
			ReactionType: in.ReactionType,
			Operation:    in.Operation,
		},
	}
	select {
	case target.ch <- event:
		b.seenReactions[reactionKey] = now.Add(b.ttl)
		return nil
	default:
		return notify.NewAPIError(429, "agent_queue_full", "agent provider queue is full", true)
	}
}

func (b *agentBroker) dispatchAction(in AgentCardActionInput) *notify.APIError {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := time.Now()
	b.pruneLocked(now)
	actionKey := newAppStateKey(in.AppAlias, in.DeliveryID)
	if _, duplicate := b.seenActions[actionKey]; duplicate {
		return nil
	}
	response := b.responsesByMessageID[newAppStateKey(in.AppAlias, in.MessageID)]
	if response == nil {
		return notify.NewAPIError(404, "unknown_response", "unknown response", false)
	}
	expectedDigest, allowed := response.actionDigests[in.ActionID]
	if !allowed || expectedDigest != actionPayloadDigest(in.ActionPayloadJSON) {
		return notify.NewAPIError(404, "unknown_action", "unknown card action", false)
	}
	var target *agentSubscriber
	for _, sub := range b.subscribers {
		if sub.provider != response.provider || !sub.includeCardActions || !sub.allowsApp(response.appAlias) {
			continue
		}
		if target == nil || sub.id < target.id {
			target = sub
		}
	}
	if target == nil {
		return notify.NewAPIError(503, "agent_unavailable", "agent provider is unavailable", true)
	}
	event := AgentEvent{
		DeliveryID:     in.DeliveryID,
		ConversationID: response.conversationID,
		ChatAlias:      response.chatAlias,
		SenderID:       in.SenderID,
		Metadata:       appAliasMetadata(in.AppAlias),
		CardAction: &AgentCardAction{
			ResponseID:  response.responseID,
			ActionID:    in.ActionID,
			PayloadJSON: in.PayloadJSON,
		},
	}
	select {
	case target.ch <- event:
		b.seenActions[actionKey] = now.Add(b.ttl)
		b.refreshConversationLocked(response.conversationID, response.provider, now)
		return nil
	default:
		return notify.NewAPIError(429, "agent_queue_full", "agent provider queue is full", true)
	}
}

func (b *agentBroker) pruneLocked(now time.Time) {
	for deliveryID, delivery := range b.deliveries {
		if now.After(delivery.expiresAt) {
			delete(b.deliveries, deliveryID)
		}
	}
	for responseID, response := range b.responses {
		if now.After(response.expiresAt) {
			delete(b.responses, responseID)
			delete(b.responsesByMessageID, newAppStateKey(response.appAlias, response.messageID))
		}
	}
	for deliveryID, expiresAt := range b.seenActions {
		if now.After(expiresAt) {
			delete(b.seenActions, deliveryID)
		}
	}
	for deliveryID, expiresAt := range b.seenReactions {
		if now.After(expiresAt) {
			delete(b.seenReactions, deliveryID)
		}
	}
	for messageID, expiresAt := range b.seenMessages {
		if now.After(expiresAt) {
			delete(b.seenMessages, messageID)
		}
	}
	for conversationID, route := range b.conversations {
		if route.expired(now) {
			delete(b.conversations, conversationID)
		}
	}
}

func validateAgentIdentity(provider, target, operationID string) (string, string, string, *notify.APIError) {
	provider = strings.TrimSpace(provider)
	target = strings.TrimSpace(target)
	operationID = strings.TrimSpace(operationID)
	if provider == "" {
		return "", "", "", notify.BadRequest("missing_provider", "provider is required")
	}
	if target == "" {
		return "", "", "", notify.BadRequest("missing_response_identity", "delivery_id or response_id is required")
	}
	if operationID == "" {
		return "", "", "", notify.BadRequest("missing_operation_id", "operation_id is required")
	}
	if len(provider) > 64 || len(target) > 160 || len(operationID) > 64 {
		return "", "", "", notify.BadRequest("field_too_large", "one or more fields are too large")
	}
	return provider, target, operationID, nil
}

func validateJSONObject(raw, code string) *notify.APIError {
	_, apiErr := normalizeJSONObjectRaw(raw, code)
	return apiErr
}

func normalizeJSONObjectRaw(raw, code string) (string, *notify.APIError) {
	raw = strings.TrimSpace(raw)
	if raw == "" || raw[0] != '{' || !json.Valid([]byte(raw)) {
		return "", notify.BadRequest(code, "payload must be a JSON object")
	}
	return raw, nil
}

func actionPayloadDigest(raw string) string {
	sum := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(sum[:])
}

func phaseForOutcome(outcome AgentResponseOutcome) AgentResponsePhase {
	switch outcome {
	case AgentResponseOutcomeCompleted:
		return AgentResponsePhaseCompleted
	case AgentResponseOutcomeFailed:
		return AgentResponsePhaseFailed
	case AgentResponseOutcomeCancelled:
		return AgentResponsePhaseCancelled
	default:
		return AgentResponsePhaseUnspecified
	}
}

func receiptFor(response *agentResponse, duplicate bool) AgentResponseReceipt {
	return AgentResponseReceipt{ResponseID: response.responseID, Revision: response.revision, Phase: response.phase, Duplicate: duplicate, MessageRef: response.messageRef}
}

func operationReceipt(response *agentResponse, op *agentOperation, duplicate bool) AgentResponseReceipt {
	return AgentResponseReceipt{ResponseID: response.responseID, Revision: op.revision, Phase: op.phase, Duplicate: duplicate, MessageRef: response.messageRef}
}

func randomOpaqueID(prefix string) (string, error) {
	raw := make([]byte, 24)
	if _, err := rand.Read(raw); err != nil {
		return "", err
	}
	return prefix + base64.RawURLEncoding.EncodeToString(raw), nil
}

func operationUUID(kind, responseID, operationID string, maxLen int) string {
	sum := sha256.Sum256([]byte(kind + "\x00" + responseID + "\x00" + operationID))
	value := "botd_" + base64.RawURLEncoding.EncodeToString(sum[:])
	if len(value) > maxLen {
		return value[:maxLen]
	}
	return value
}

func fallbackConversationID(in CommandInput) string {
	route := in.ChatAlias
	if in.ChatID != "" {
		route = in.ChatID
	}
	thread := in.Metadata["thread_id"]
	if thread == "" {
		thread = in.Metadata["root_id"]
	}
	seed := route + "\x00" + thread
	if !isDefaultAppAlias(in.AppAlias) {
		seed = "feishu-botd/fallback-conversation/v2\x00" +
			effectiveAppAlias(in.AppAlias) + "\x00" + seed
	}
	sum := sha256.Sum256([]byte(seed))
	return "conv_" + hex.EncodeToString(sum[:16])
}

func hashJSON(value any) string {
	data, _ := json.Marshal(value)
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func cloneStringMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func agentPublicMetadata(in map[string]string, appAlias string) map[string]string {
	out := make(map[string]string, 3)
	for _, key := range []string{"chat_type", "message_type"} {
		if value := strings.TrimSpace(in[key]); value != "" {
			out[key] = value
		}
	}
	if strings.TrimSpace(in["app_alias"]) != "" {
		// The private route tag, not metadata supplied by a provider, is the
		// authority. Metadata only records that ingress explicitly identified an
		// app and therefore opts into this additive public key.
		out["app_alias"] = effectiveAppAlias(appAlias)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func appAliasMetadata(appAlias string) map[string]string {
	if strings.TrimSpace(appAlias) == "" {
		return nil
	}
	return map[string]string{"app_alias": effectiveAppAlias(appAlias)}
}

func (s *agentSubscriber) allowsApp(appAlias string) bool {
	return appAllowedByList(appAlias, s.allowedApps, s.allowedAppsConfigured)
}

func normalizedInitialMarkdown(markdown string) string {
	markdown = strings.TrimSpace(markdown)
	if markdown == "" {
		return agentPlaceholderMarkdown
	}
	return markdown
}

func cloneAgentEvent(in AgentEvent) AgentEvent {
	out := in
	out.Metadata = cloneStringMap(in.Metadata)
	if in.Message != nil {
		message := *in.Message
		out.Message = &message
	}
	if in.CardAction != nil {
		action := *in.CardAction
		out.CardAction = &action
	}
	return out
}

func (s *Service) logAgentCardFailure(operation, responseID string, err error) {
	s.logFeishuFailure(operation, "agent", responseID, err)
}

func (s *Service) logFeishuFailure(operation, correlationKind, correlationValue string, err error) {
	correlationID := opaqueLogCorrelationID(correlationKind, correlationValue)
	var apiErr *feishu.DynamicCardAPIError
	if errors.As(err, &apiErr) {
		s.logger.Warn("agent card operation failed",
			"operation", operation,
			"correlation", correlationID,
			"http_status", apiErr.HTTPStatus,
			"code", apiErr.Code,
			"request_id", apiErr.RequestID,
		)
		return
	}
	errorClass := "transport"
	switch {
	case errors.Is(err, context.Canceled):
		errorClass = "context_canceled"
	case errors.Is(err, context.DeadlineExceeded):
		errorClass = "deadline_exceeded"
	}
	s.logger.Warn("agent card operation failed",
		"operation", operation,
		"correlation", correlationID,
		"error_class", errorClass,
	)
}

func opaqueLogCorrelationID(kind, value string) string {
	sum := sha256.Sum256([]byte("feishu-botd/log-correlation/v1\x00" + kind + "\x00" + value))
	return kind + "_" + hex.EncodeToString(sum[:8])
}

func agentMessageDedupeKey(appAlias, messageID string) string {
	messageID = strings.TrimSpace(messageID)
	if messageID == "" {
		return ""
	}
	seed := "feishu-botd/agent-message-dedupe/v1\x00" + messageID
	if !isDefaultAppAlias(appAlias) {
		seed = "feishu-botd/agent-message-dedupe/v2\x00" +
			effectiveAppAlias(appAlias) + "\x00" + messageID
	}
	sum := sha256.Sum256([]byte(seed))
	return hex.EncodeToString(sum[:])
}
