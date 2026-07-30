package service

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"feishu-botd/internal/feishu"
	"feishu-botd/internal/notify"
)

const (
	agentSubscriptionBuffer = 64
	agentContentElementID   = "agent_answer"
	maxAgentCardBytes       = 30 * 1024
	minCardMutationInterval = 125 * time.Millisecond
	messageUUIDDedupeWindow = time.Hour
)

// AgentSubscribeOptions selects a full-fidelity agent event stream. Exact
// command subscribers win; IncludeUnmatchedMessages is the natural chat-agent
// fallback for P2P prompts and otherwise-unhandled group mentions.
type AgentSubscribeOptions struct {
	Provider                 string
	Commands                 []string
	IncludeUnmatchedMessages bool
	IncludeCardActions       bool
}

type AgentEvent struct {
	DeliveryID     string
	ConversationID string
	ChatAlias      string
	SenderID       string
	Metadata       map[string]string
	Message        *AgentMessage
	CardAction     *AgentCardAction
}

type AgentMessage struct {
	Text        string
	Command     string
	CommandText string
}

type AgentCardAction struct {
	ResponseID  string
	ActionID    string
	PayloadJSON string
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
}

type FinishAgentResponseInput struct {
	Provider         string
	ResponseID       string
	OperationID      string
	ExpectedRevision uint64
	Outcome          AgentResponseOutcome
	Markdown         string
	Summary          string
}

// AgentCardActionInput is the daemon-private callback handoff. MessageID is
// used only to resolve the response owner and is never forwarded to a provider.
type AgentCardActionInput struct {
	DeliveryID        string
	MessageID         string `json:"-"`
	SenderID          string
	ActionID          string
	PayloadJSON       string
	ActionPayloadJSON string `json:"-"`
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
	responsesByMessageID map[string]*agentResponse
	conversations        map[string]*agentConversationRoute
	seenMessages         map[string]time.Time
	seenActions          map[string]time.Time
}

type agentSubscriber struct {
	id                       uint64
	provider                 string
	commands                 map[string]struct{}
	includeUnmatchedMessages bool
	includeCardActions       bool
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
	deliveryID     string
	conversationID string
	chatAlias      string
	cardID         string
	messageID      string
	actionDigests  map[string]string
	revision       uint64
	phase          AgentResponsePhase
	markdown       string
	nextSequence   int32
	lastMutationAt time.Time
	pendingOp      string
	operations     map[string]*agentOperation
	expiresAt      time.Time
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
		responsesByMessageID: make(map[string]*agentResponse),
		conversations:        make(map[string]*agentConversationRoute),
		seenMessages:         make(map[string]time.Time),
		seenActions:          make(map[string]time.Time),
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
		if len(command) > 64 {
			return nil, notify.BadRequest("field_too_large", "one or more fields are too large")
		}
		commands[command] = struct{}{}
	}
	if len(commands) == 0 && !in.IncludeUnmatchedMessages && !in.IncludeCardActions {
		return nil, notify.BadRequest("missing_subscription", "at least one agent event kind is required")
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	b.nextSubID++
	sub := &agentSubscriber{
		id:                       b.nextSubID,
		provider:                 provider,
		commands:                 commands,
		includeUnmatchedMessages: in.IncludeUnmatchedMessages,
		includeCardActions:       in.IncludeCardActions,
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
	b.mu.Lock()
	defer b.mu.Unlock()
	now := time.Now()
	b.pruneLocked(now)
	if _, exists := b.deliveries[in.DeliveryID]; exists {
		return 0, true
	}
	messageDedupeKey := agentMessageDedupeKey(in.Metadata["message_id"])
	if messageDedupeKey != "" {
		if _, seen := b.seenMessages[messageDedupeKey]; seen {
			return 0, true
		}
	}

	exact := make([]*agentSubscriber, 0)
	fallback := make([]*agentSubscriber, 0)
	for _, sub := range b.subscribers {
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
		Metadata:       agentPublicMetadata(in.Metadata),
		Message: &AgentMessage{
			Text:        in.Prompt,
			Command:     in.Command,
			CommandText: in.Text,
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
	if s.dynamicCards == nil {
		return AgentResponseReceipt{}, notify.NotImplemented("agent_streaming_unavailable", "agent streaming is unavailable for this sender")
	}
	provider, deliveryID, operationID, apiErr := validateAgentIdentity(in.Provider, in.DeliveryID, in.OperationID)
	if apiErr != nil {
		return AgentResponseReceipt{}, apiErr
	}
	in.Provider, in.DeliveryID, in.OperationID = provider, deliveryID, operationID
	cardJSON, actionDigests, apiErr := buildAgentCard(in.Content)
	if apiErr != nil {
		return AgentResponseReceipt{}, apiErr
	}
	fingerprint := hashJSON(struct {
		Provider   string
		DeliveryID string
		CardJSON   string
	}{provider, deliveryID, cardJSON})

	b := s.agentBroker
	now := time.Now()
	delivery, ok := s.lookupAndPinAgentDelivery(
		deliveryID,
		now,
		now.Add(messageUUIDDedupeWindow+s.cfg.SendTimeout),
	)
	if !ok {
		return AgentResponseReceipt{}, notify.NewAPIError(404, "unknown_delivery", "unknown delivery", false)
	}

	delivery.mu.Lock()
	defer delivery.mu.Unlock()
	if _, allowed := delivery.allowedProviders[provider]; !allowed {
		return AgentResponseReceipt{}, notify.NewAPIError(404, "unknown_delivery", "unknown delivery", false)
	}
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
		delivery.cardJSON = cardJSON
		delivery.actionDigests = cloneStringMap(actionDigests)
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
		cardID, err := s.dynamicCards.CreateCard(callCtx, delivery.cardJSON)
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
		chatID := s.cfg.Channels[delivery.input.ChatAlias]
		if delivery.input.ChatAlias == "direct" {
			chatID = delivery.input.ChatID
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
		messageID, err := s.dynamicCards.SendCard(callCtx, sendRequest)
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

	response := &agentResponse{
		responseID:     delivery.responseID,
		provider:       provider,
		deliveryID:     deliveryID,
		conversationID: delivery.input.ConversationID,
		chatAlias:      delivery.input.ChatAlias,
		cardID:         delivery.cardID,
		messageID:      delivery.messageID,
		actionDigests:  cloneStringMap(delivery.actionDigests),
		revision:       1,
		phase:          AgentResponsePhaseStreaming,
		markdown:       normalizedInitialMarkdown(in.Content.Markdown),
		operations:     make(map[string]*agentOperation),
		expiresAt:      time.Now().Add(b.ttl),
	}
	delivery.response = response
	delivery.state = agentDeliveryStreaming
	delivery.compactAfterStart()
	b.mu.Lock()
	b.responses[response.responseID] = response
	b.responsesByMessageID[response.messageID] = response
	b.mu.Unlock()
	return receiptFor(response, false), nil
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
	if len(in.Markdown) > maxAgentCardBytes {
		return AgentResponseReceipt{}, notify.BadRequest("field_too_large", "one or more fields are too large")
	}
	response := s.agentBroker.lookupResponse(provider, responseID)
	if response == nil {
		return AgentResponseReceipt{}, notify.NewAPIError(404, "unknown_response", "unknown response", false)
	}
	fingerprint := hashJSON(struct {
		Expected uint64
		Markdown string
	}{in.ExpectedRevision, in.Markdown})
	return s.applyAgentUpdate(ctx, response, operationID, fingerprint, in.ExpectedRevision, in.Markdown)
}

func (s *Service) applyAgentUpdate(ctx context.Context, response *agentResponse, operationID, fingerprint string, expected uint64, markdown string) (AgentResponseReceipt, *notify.APIError) {
	response.mu.Lock()
	defer response.mu.Unlock()
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
	}
	callCtx, cancel := context.WithTimeout(ctx, s.cfg.SendTimeout)
	defer cancel()
	if err := response.waitForMutation(callCtx); err != nil {
		return AgentResponseReceipt{}, notify.NewAPIError(502, "feishu_unavailable", "Feishu card update was cancelled", true)
	}
	if err := s.dynamicCards.UpdateContent(callCtx, feishu.CardContentUpdate{
		CardID: response.cardID, ElementID: agentContentElementID, Content: op.content,
		UUID: op.contentUUID, Sequence: op.contentSeq,
	}); err != nil {
		s.logAgentCardFailure("update", response.responseID, err)
		if isCardAPIRejection(err) && !isRetryableCardFailure(err) {
			if op.contentAmbiguous {
				op.contentClosed = true
				return AgentResponseReceipt{}, operationStateUnknownError()
			}
			response.abortOperation(operationID)
		} else if isAmbiguousCardFailure(err) {
			op.contentAmbiguous = true
		}
		return AgentResponseReceipt{}, agentCardCallError(err, "Feishu card update failed")
	}
	op.contentDone = true
	op.complete = true
	response.nextSequence = op.contentSeq
	response.markdown = markdown
	response.revision++
	op.revision = response.revision
	op.phase = response.phase
	response.pendingOp = ""
	op.compact()
	return operationReceipt(response, op, false), nil
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
	if len(in.Markdown) > maxAgentCardBytes || len(in.Summary) > 200 {
		return AgentResponseReceipt{}, notify.BadRequest("field_too_large", "one or more fields are too large")
	}
	response := s.agentBroker.lookupResponse(provider, responseID)
	if response == nil {
		return AgentResponseReceipt{}, notify.NewAPIError(404, "unknown_response", "unknown response", false)
	}
	fingerprint := hashJSON(struct {
		Expected uint64
		Phase    AgentResponsePhase
		Markdown string
		Summary  string
	}{in.ExpectedRevision, phase, in.Markdown, in.Summary})
	return s.applyAgentFinish(ctx, response, operationID, fingerprint, in.ExpectedRevision, phase, in.Markdown, in.Summary)
}

func (s *Service) applyAgentFinish(ctx context.Context, response *agentResponse, operationID, fingerprint string, expected uint64, phase AgentResponsePhase, markdown, summary string) (AgentResponseReceipt, *notify.APIError) {
	response.mu.Lock()
	defer response.mu.Unlock()
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
		next := response.nextSequence + 1
		if op.contentSeq != 0 {
			next = op.contentSeq + 1
		}
		op.settingsSeq = next
		op.settingsUUID = operationUUID("finish", response.responseID, operationID, 64)
		op.settingsJSON = agentFinishSettings(summary)
		op.phase = phase
	}
	callCtx, cancel := context.WithTimeout(ctx, s.cfg.SendTimeout)
	defer cancel()
	if op.contentSeq != 0 && !op.contentDone {
		if err := response.waitForMutation(callCtx); err != nil {
			return AgentResponseReceipt{}, notify.NewAPIError(502, "feishu_unavailable", "Feishu card update was cancelled", true)
		}
		if err := s.dynamicCards.UpdateContent(callCtx, feishu.CardContentUpdate{
			CardID: response.cardID, ElementID: agentContentElementID, Content: op.content,
			UUID: op.contentUUID, Sequence: op.contentSeq,
		}); err != nil {
			s.logAgentCardFailure("finish content", response.responseID, err)
			if isCardAPIRejection(err) && !isRetryableCardFailure(err) {
				if op.contentAmbiguous {
					op.contentClosed = true
					return AgentResponseReceipt{}, operationStateUnknownError()
				}
				response.abortOperation(operationID)
			} else if isAmbiguousCardFailure(err) {
				op.contentAmbiguous = true
			}
			return AgentResponseReceipt{}, agentCardCallError(err, "Feishu card update failed")
		}
		op.contentDone = true
		response.nextSequence = op.contentSeq
		response.markdown = op.content
		op.compactContent()
	}
	if !op.settingsDone {
		if err := response.waitForMutation(callCtx); err != nil {
			return AgentResponseReceipt{}, notify.NewAPIError(502, "feishu_unavailable", "Feishu card finalization was cancelled", true)
		}
		if err := s.dynamicCards.UpdateSettings(callCtx, feishu.CardSettingsUpdate{
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
	op.settingsJSON = ""
	op.settingsUUID = ""
	op.settingsSeq = 0
	op.settingsDone = false
	op.settingsAmbiguous = false
	op.settingsClosed = false
}

func (op *agentOperation) outcomeUnknown() bool {
	return op.contentClosed || op.settingsClosed
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

func (b *agentBroker) lookupAndPinDelivery(deliveryID string, now, expiresAt time.Time) (*agentDelivery, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.pruneLocked(now)
	delivery, ok := b.deliveries[deliveryID]
	if !ok {
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

func (s *Service) lookupAndPinAgentDelivery(deliveryID string, now, expiresAt time.Time) (*agentDelivery, bool) {
	routes := s.inboundRoutes
	routes.mu.Lock()
	defer routes.mu.Unlock()
	routes.pruneLocked(now)
	delivery, ok := s.agentBroker.lookupAndPinDelivery(deliveryID, now, expiresAt)
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

func (b *agentBroker) dispatchAction(in AgentCardActionInput) *notify.APIError {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := time.Now()
	b.pruneLocked(now)
	if _, duplicate := b.seenActions[in.DeliveryID]; duplicate {
		return nil
	}
	response := b.responsesByMessageID[in.MessageID]
	if response == nil {
		return notify.NewAPIError(404, "unknown_response", "unknown response", false)
	}
	expectedDigest, allowed := response.actionDigests[in.ActionID]
	if !allowed || expectedDigest != actionPayloadDigest(in.ActionPayloadJSON) {
		return notify.NewAPIError(404, "unknown_action", "unknown card action", false)
	}
	var target *agentSubscriber
	for _, sub := range b.subscribers {
		if sub.provider != response.provider || !sub.includeCardActions {
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
		CardAction: &AgentCardAction{
			ResponseID:  response.responseID,
			ActionID:    in.ActionID,
			PayloadJSON: in.PayloadJSON,
		},
	}
	select {
	case target.ch <- event:
		b.seenActions[in.DeliveryID] = now.Add(b.ttl)
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
			delete(b.responsesByMessageID, response.messageID)
		}
	}
	for deliveryID, expiresAt := range b.seenActions {
		if now.After(expiresAt) {
			delete(b.seenActions, deliveryID)
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

func buildAgentCard(content AgentResponseContent) (string, map[string]string, *notify.APIError) {
	content.Title = strings.TrimSpace(content.Title)
	content.Markdown = strings.TrimSpace(content.Markdown)
	if len(content.Title) > 200 || len(content.Markdown) > maxAgentCardBytes || len(content.Actions) > 8 {
		return "", nil, notify.BadRequest("field_too_large", "one or more fields are too large")
	}
	if content.Markdown == "" {
		content.Markdown = "Thinking…"
	}

	type text struct {
		Tag     string `json:"tag"`
		Content string `json:"content"`
	}
	type callbackBehavior struct {
		Type  string         `json:"type"`
		Value map[string]any `json:"value"`
	}
	type button struct {
		Tag       string             `json:"tag"`
		ElementID string             `json:"element_id"`
		Text      text               `json:"text"`
		Type      string             `json:"type,omitempty"`
		Behaviors []callbackBehavior `json:"behaviors"`
	}
	type element struct {
		Tag       string             `json:"tag"`
		ElementID string             `json:"element_id,omitempty"`
		Content   string             `json:"content,omitempty"`
		Text      *text              `json:"text,omitempty"`
		Type      string             `json:"type,omitempty"`
		Behaviors []callbackBehavior `json:"behaviors,omitempty"`
	}
	elements := []element{{Tag: "markdown", ElementID: agentContentElementID, Content: content.Markdown}}
	actionDigests := make(map[string]string, len(content.Actions))
	if len(content.Actions) > 0 {
		seen := make(map[string]struct{}, len(content.Actions))
		for index, action := range content.Actions {
			action.ActionID = strings.TrimSpace(action.ActionID)
			action.Label = strings.TrimSpace(action.Label)
			if action.ActionID == "" || action.Label == "" {
				return "", nil, notify.BadRequest("invalid_action", "action_id and label are required")
			}
			if len(action.ActionID) > 64 || len(action.Label) > 100 || len(action.PayloadJSON) > 8*1024 {
				return "", nil, notify.BadRequest("field_too_large", "one or more fields are too large")
			}
			if _, duplicate := seen[action.ActionID]; duplicate {
				return "", nil, notify.BadRequest("duplicate_action", "action ids must be unique")
			}
			switch action.Style {
			case AgentResponseActionStyleUnspecified, AgentResponseActionStyleDefault,
				AgentResponseActionStylePrimary, AgentResponseActionStyleDanger:
			default:
				return "", nil, notify.BadRequest("invalid_action_style", "action style is not supported")
			}
			seen[action.ActionID] = struct{}{}
			payloadJSON := strings.TrimSpace(action.PayloadJSON)
			if payloadJSON == "" {
				payloadJSON = "{}"
			}
			var apiErr *notify.APIError
			payloadJSON, apiErr = normalizeJSONObjectRaw(payloadJSON, "invalid_action_payload")
			if apiErr != nil {
				return "", nil, apiErr
			}
			actionDigests[action.ActionID] = actionPayloadDigest(payloadJSON)
			// The pinned Feishu SDK decodes callback values through interface{},
			// which would round large JSON integers to float64. Carry the provider
			// payload as a JSON string across Feishu and reconstruct it losslessly
			// when normalizing the callback for the provider.
			value := map[string]any{"action_id": action.ActionID, "payload_json": payloadJSON}
			button := button{
				Tag:       "button",
				ElementID: fmt.Sprintf("agent_action_%d", index+1),
				Text:      text{Tag: "plain_text", Content: action.Label},
				Type:      cardButtonStyle(action.Style),
				Behaviors: []callbackBehavior{{Type: "callback", Value: value}},
			}
			elements = append(elements, element{
				Tag: button.Tag, ElementID: button.ElementID, Text: &button.Text,
				Type: button.Type, Behaviors: button.Behaviors,
			})
		}
	}
	cardConfig := map[string]any{
		"streaming_mode": true,
		"update_multi":   true,
	}
	if content.Title != "" {
		cardConfig["summary"] = map[string]any{"content": content.Title}
	}
	card := map[string]any{
		"schema": "2.0",
		"config": cardConfig,
		"body":   map[string]any{"elements": elements},
	}
	if content.Title != "" {
		card["header"] = map[string]any{"title": map[string]any{"tag": "plain_text", "content": content.Title}}
	}
	data, err := json.Marshal(card)
	if err != nil {
		return "", nil, notify.NewAPIError(500, "internal", "could not encode agent card", false)
	}
	if len(data) > maxAgentCardBytes {
		return "", nil, notify.BadRequest("field_too_large", "agent card exceeds the daemon size limit")
	}
	return string(data), actionDigests, nil
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

func cardButtonStyle(style AgentResponseActionStyle) string {
	switch style {
	case AgentResponseActionStylePrimary:
		return "primary"
	case AgentResponseActionStyleDanger:
		return "danger"
	default:
		return "default"
	}
}

func agentFinishSettings(summary string) string {
	config := map[string]any{"streaming_mode": false}
	if summary != "" {
		config["summary"] = map[string]any{"content": summary}
	}
	data, _ := json.Marshal(map[string]any{"config": config})
	return string(data)
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
	return AgentResponseReceipt{ResponseID: response.responseID, Revision: response.revision, Phase: response.phase, Duplicate: duplicate}
}

func operationReceipt(response *agentResponse, op *agentOperation, duplicate bool) AgentResponseReceipt {
	return AgentResponseReceipt{ResponseID: response.responseID, Revision: op.revision, Phase: op.phase, Duplicate: duplicate}
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
	sum := sha256.Sum256([]byte(route + "\x00" + thread))
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

func agentPublicMetadata(in map[string]string) map[string]string {
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

func normalizedInitialMarkdown(markdown string) string {
	markdown = strings.TrimSpace(markdown)
	if markdown == "" {
		return "Thinking…"
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

func agentMessageDedupeKey(messageID string) string {
	messageID = strings.TrimSpace(messageID)
	if messageID == "" {
		return ""
	}
	sum := sha256.Sum256([]byte("feishu-botd/agent-message-dedupe/v1\x00" + messageID))
	return hex.EncodeToString(sum[:])
}
