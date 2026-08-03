package service

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"feishu-botd/internal/feishu"
	"feishu-botd/internal/notify"
)

// agentFollowUpSource names this path in the outbound message UUID seed. Feishu
// deduplicates an IM message UUID for one hour, so a retry that reuses the same
// operation id also reuses the same UUID and cannot post the message twice.
const agentFollowUpSource = "agent_follow_up"

// SendAgentFollowUpInput posts one later message into a conversation that has
// already delivered an agent event to this provider. It is an ordinary message
// rather than a CardKit entity: there is no revision and no way to edit it
// afterwards.
type SendAgentFollowUpInput struct {
	Provider       string
	ConversationID string
	OperationID    string
	Markdown       string
	Summary        string
}

type AgentFollowUpReceipt struct {
	FollowUpID string
	Duplicate  bool
}

// agentConversationRoute is the daemon-private reverse map from an opaque
// conversation identity back to a concrete Feishu route. It exists so a provider
// can address a later message without ever holding a raw chat, thread, or
// message id, and it records which providers were actually spoken to there.
type agentConversationRoute struct {
	mu sync.Mutex

	appAlias          string
	chatAlias         string
	chatID            string
	unconfiguredGroup bool
	// threadReplyTo is set only for a thread-scoped conversation. A follow-up
	// into a flat chat is a new top-level message, not a reply threaded under a
	// prompt the user has long since scrolled past.
	threadReplyTo string
	// providers maps each provider that received an agent event here to the
	// moment its grant lapses. Scope follows the conversation, not the daemon:
	// a provider can only follow up where it was spoken to.
	providers  map[string]time.Time
	operations map[string]*agentFollowUpOperation
	expiresAt  time.Time
}

type agentFollowUpOperation struct {
	provider    string
	fingerprint string
	followUpID  string
	pending     bool
	complete    bool
	// ambiguousAt is the first attempt whose outcome Feishu never confirmed. It
	// starts the clock on the one-hour message-UUID deduplication window.
	ambiguousAt time.Time
}

// SendAgentFollowUp delivers a standalone message into an existing conversation.
// Authorization is deliberately narrow: the caller must already be an
// authenticated provider (enforced by the transport), must be allowed to send
// follow-ups at all, and must have received an agent event for this exact
// conversation inside the dedupe TTL.
func (s *Service) SendAgentFollowUp(ctx context.Context, in SendAgentFollowUpInput) (AgentFollowUpReceipt, *notify.APIError) {
	provider, conversationID, operationID, apiErr := validateAgentFollowUpIdentity(in.Provider, in.ConversationID, in.OperationID)
	if apiErr != nil {
		return AgentFollowUpReceipt{}, apiErr
	}
	markdown := strings.TrimSpace(in.Markdown)
	summary := strings.TrimSpace(in.Summary)
	if markdown == "" {
		return AgentFollowUpReceipt{}, notify.BadRequest("missing_markdown", "markdown is required")
	}
	if len(markdown) > maxAgentCardBytes || len(summary) > 200 {
		return AgentFollowUpReceipt{}, notify.BadRequest("field_too_large", "one or more fields are too large")
	}

	now := time.Now()
	route, appAlias, chatAlias, privateChatID, replyToMessageID, unconfiguredGroup, ok := s.agentBroker.lookupAndPinConversation(
		conversationID,
		provider,
		operationID,
		func(resolvedApp string) bool { return s.appAllowed(provider, resolvedApp) },
		now,
		s.cfg.SendTimeout,
	)
	if !ok {
		return AgentFollowUpReceipt{}, unknownConversationError()
	}
	chatID := s.followUpChatID(appAlias, chatAlias, privateChatID, unconfiguredGroup)
	if chatID == "" {
		return AgentFollowUpReceipt{}, unknownConversationError()
	}
	backend, ok := s.backendForApp(appAlias)
	if !ok {
		return AgentFollowUpReceipt{}, unknownConversationError()
	}

	fingerprint := hashJSON(struct {
		Conversation string
		Markdown     string
		Summary      string
	}{conversationID, markdown, summary})
	followUpID, replay, apiErr := route.beginFollowUp(provider, operationID, fingerprint, now, s.cfg.SendTimeout)
	if apiErr != nil {
		return AgentFollowUpReceipt{}, apiErr
	}
	if replay {
		return AgentFollowUpReceipt{FollowUpID: followUpID, Duplicate: true}, nil
	}

	sendCtx, cancel := context.WithTimeout(ctx, s.cfg.SendTimeout)
	defer cancel()
	if _, err := backend.sender.Send(sendCtx, chatID, notify.Request{
		Source:           agentFollowUpSource,
		DedupeKey:        followUpID,
		Title:            summary,
		Markdown:         markdown,
		ReplyToMessageID: replyToMessageID,
	}); err != nil {
		s.logFeishuFailure("agent follow-up", "follow_up", followUpID, err)
		route.abortFollowUp(provider, operationID, err, time.Now())
		return AgentFollowUpReceipt{}, agentMessageCallError(err, "Feishu follow-up send failed")
	}
	route.commitFollowUp(provider, operationID)
	return AgentFollowUpReceipt{FollowUpID: followUpID}, nil
}

// followUpChatID resolves the send destination the same way StartAgentResponse
// does: a configured group routes through its alias, while a direct message or
// explicitly allowed unconfigured group uses the private ingress route captured
// at dispatch. Removing a configured alias leaves that conversation
// unaddressable.
func (s *Service) followUpChatID(appAlias, chatAlias, privateChatID string, unconfiguredGroup bool) string {
	if chatAlias == "direct" || unconfiguredGroup {
		return privateChatID
	}
	resolvedApp, chatID, ok := s.cfg.ResolveChannel(chatAlias)
	if !ok || resolvedApp != effectiveAppAlias(appAlias) {
		return ""
	}
	return chatID
}

func (r *agentConversationRoute) target() (appAlias, chatAlias, chatID, replyToMessageID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.appAlias, r.chatAlias, r.chatID, r.threadReplyTo
}

func (r *agentConversationRoute) expired(now time.Time) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return now.After(r.expiresAt)
}

// beginFollowUp checks the provider's conversation grant and claims the
// operation id. A replayed operation returns its recorded handle instead of
// sending again.
func (r *agentConversationRoute) beginFollowUp(
	provider, operationID, fingerprint string, now time.Time, sendTimeout time.Duration,
) (followUpID string, replay bool, apiErr *notify.APIError) {
	r.mu.Lock()
	defer r.mu.Unlock()

	operationKey := r.followUpOperationKeyLocked(provider, operationID)
	op, exists := r.operations[operationKey]
	if exists {
		if op.provider == "" {
			op.provider = provider
		}
		if op.fingerprint != fingerprint {
			return "", false, notify.NewAPIError(409, "operation_conflict", "operation id reused with different content", false)
		}
		if op.complete {
			return op.followUpID, true, nil
		}
		if op.pending {
			return "", false, notify.NewAPIError(409, "operation_in_flight", "another attempt for this follow-up is in flight", true)
		}
		if messageRetryWindowExhausted(now, sendTimeout, op.ambiguousAt) {
			return "", false, closedAgentSendError("send_retry_expired")
		}
	} else {
		grantedUntil, granted := r.providers[provider]
		if !granted || !now.Before(grantedUntil) {
			return "", false, unknownConversationError()
		}
		handle, err := randomOpaqueID("fup_")
		if err != nil {
			return "", false, notify.NewAPIError(500, "internal", "could not create follow-up handle", true)
		}
		op = &agentFollowUpOperation{provider: provider, fingerprint: fingerprint, followUpID: handle}
		r.operations[operationKey] = op
	}
	op.pending = true
	return op.followUpID, false, nil
}

func (r *agentConversationRoute) followUpOperationKeyLocked(provider, operationID string) string {
	scoped := "feishu-botd/internal-follow-up-operation/v1:" +
		hashJSON([2]string{provider, operationID})
	if _, exists := r.operations[scoped]; exists {
		return scoped
	}
	if existing := r.operations[operationID]; existing == nil || existing.provider == "" || existing.provider == provider {
		return operationID
	}
	return scoped
}

func (r *agentConversationRoute) commitFollowUp(provider, operationID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := r.followUpOperationKeyLocked(provider, operationID)
	if op, ok := r.operations[key]; ok {
		op.pending = false
		op.complete = true
	}
}

func (r *agentConversationRoute) abortFollowUp(provider, operationID string, err error, now time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := r.followUpOperationKeyLocked(provider, operationID)
	op, ok := r.operations[key]
	if !ok {
		return
	}
	op.pending = false
	switch {
	case isAmbiguousMessageFailure(err):
		if op.ambiguousAt.IsZero() {
			op.ambiguousAt = now
		}
	case isRetryableMessageFailure(err):
		// A definitive temporary rejection retains the exact operation identity
		// so the retry reuses the same Feishu message UUID.
	case op.ambiguousAt.IsZero():
		// Nothing was delivered and no earlier attempt is in doubt, so release
		// the id and let a corrected request reuse it.
		delete(r.operations, key)
	default:
		// A definitive rejection after an ambiguous attempt cannot prove the
		// earlier attempt failed. Keep the record pinned to its fingerprint so
		// only a byte-identical retry — which reuses the UUID — is possible.
	}
}

func (b *agentBroker) lookupConversation(conversationID string, now time.Time) *agentConversationRoute {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.pruneLocked(now)
	return b.conversations[conversationID]
}

// lookupAndPinConversation resolves an opaque conversation handle while holding
// the broker lock that also governs pruning. Once authorized, the exact route
// is retained for at least one state TTL and one send timeout so an in-flight
// follow-up cannot be pruned and rebound to another app. An existing operation
// remains replayable after the original conversation grant expires, but a new
// operation still requires a live grant.
func (b *agentBroker) lookupAndPinConversation(
	conversationID, provider, operationID string,
	appAllowed func(string) bool,
	now time.Time,
	sendTimeout time.Duration,
) (
	route *agentConversationRoute,
	appAlias, chatAlias, chatID, replyToMessageID string,
	unconfiguredGroup bool,
	ok bool,
) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.pruneLocked(now)
	route = b.conversations[strings.TrimSpace(conversationID)]
	if route == nil {
		return nil, "", "", "", "", false, false
	}

	route.mu.Lock()
	defer route.mu.Unlock()
	if appAllowed != nil && !appAllowed(route.appAlias) {
		return nil, "", "", "", "", false, false
	}
	operationKey := route.followUpOperationKeyLocked(provider, operationID)
	op := route.operations[operationKey]
	grantedUntil, granted := route.providers[provider]
	if (op == nil || op.provider != "" && op.provider != provider) &&
		(!granted || !now.Before(grantedUntil)) {
		return nil, "", "", "", "", false, false
	}

	retainFor := b.ttl
	if retainFor < sendTimeout {
		retainFor = sendTimeout
	}
	if retainUntil := now.Add(retainFor); route.expiresAt.Before(retainUntil) {
		route.expiresAt = retainUntil
	}
	return route, route.appAlias, route.chatAlias, route.chatID, route.threadReplyTo, route.unconfiguredGroup, true
}

func (b *agentBroker) conversationAppConflictLocked(conversationID, appAlias string) bool {
	route := b.conversations[strings.TrimSpace(conversationID)]
	if route == nil {
		return false
	}
	route.mu.Lock()
	defer route.mu.Unlock()
	return effectiveAppAlias(route.appAlias) != effectiveAppAlias(appAlias)
}

// recordConversationLocked captures the reverse route for a delivered agent
// message and grants each receiving provider a TTL-bounded follow-up scope.
func (b *agentBroker) recordConversationLocked(in CommandInput, providers map[string]struct{}, now time.Time) {
	conversationID := strings.TrimSpace(in.ConversationID)
	if conversationID == "" || len(providers) == 0 {
		return
	}
	route, ok := b.conversations[conversationID]
	if !ok {
		route = &agentConversationRoute{
			appAlias:   in.AppAlias,
			providers:  make(map[string]time.Time, len(providers)),
			operations: make(map[string]*agentFollowUpOperation),
		}
		b.conversations[conversationID] = route
	}
	expiresAt := now.Add(b.ttl)
	route.mu.Lock()
	defer route.mu.Unlock()
	// Opaque conversation handles must reverse-resolve to exactly one app,
	// because the provider request intentionally carries no app field.
	if effectiveAppAlias(route.appAlias) != effectiveAppAlias(in.AppAlias) {
		return
	}
	route.chatAlias = in.ChatAlias
	route.chatID = in.ChatID
	route.unconfiguredGroup = in.UnconfiguredGroup
	route.threadReplyTo = followUpThreadReply(in.Metadata)
	for provider := range providers {
		route.providers[provider] = expiresAt
	}
	if route.expiresAt.Before(expiresAt) {
		route.expiresAt = expiresAt
	}
}

// refreshConversationLocked extends an existing grant when a provider receives a
// later event, such as a card action. It never creates a route, because an
// action carries no ingress chat identity of its own.
func (b *agentBroker) refreshConversationLocked(conversationID, provider string, now time.Time) {
	route, ok := b.conversations[strings.TrimSpace(conversationID)]
	if !ok {
		return
	}
	expiresAt := now.Add(b.ttl)
	route.mu.Lock()
	defer route.mu.Unlock()
	if _, granted := route.providers[provider]; !granted {
		return
	}
	route.providers[provider] = expiresAt
	if route.expiresAt.Before(expiresAt) {
		route.expiresAt = expiresAt
	}
}

// followUpThreadReply returns the message a follow-up must reply to in order to
// land in the same Feishu thread. Feishu's thread id is not a message id, so a
// thread-scoped conversation without a root falls back to the prompt itself.
func followUpThreadReply(metadata map[string]string) string {
	threadID := strings.TrimSpace(metadata["thread_id"])
	rootID := strings.TrimSpace(metadata["root_id"])
	if threadID == "" && rootID == "" {
		return ""
	}
	if rootID != "" {
		return rootID
	}
	return strings.TrimSpace(metadata["message_id"])
}

func validateAgentFollowUpIdentity(provider, conversationID, operationID string) (string, string, string, *notify.APIError) {
	provider = strings.TrimSpace(provider)
	conversationID = strings.TrimSpace(conversationID)
	operationID = strings.TrimSpace(operationID)
	if provider == "" {
		return "", "", "", notify.BadRequest("missing_provider", "provider is required")
	}
	if conversationID == "" {
		return "", "", "", notify.BadRequest("missing_conversation_id", "conversation_id is required")
	}
	if operationID == "" {
		return "", "", "", notify.BadRequest("missing_operation_id", "operation_id is required")
	}
	if len(provider) > 64 || len(conversationID) > 160 || len(operationID) > 64 {
		return "", "", "", notify.BadRequest("field_too_large", "one or more fields are too large")
	}
	return provider, conversationID, operationID, nil
}

func unknownConversationError() *notify.APIError {
	return notify.NewAPIError(404, "unknown_conversation", "unknown conversation", false)
}

// answeredMessageFailure reports whether the error carries a decoded Feishu
// response. Everything else — transport failure, cancellation, timeout, an
// unreadable body — leaves the send outcome unknown, so it is treated as both
// retryable and ambiguous. Local encode failures fall in that bucket too; the
// follow-up path validates content and destination first, so they are
// unreachable here.
func answeredMessageFailure(err error) (*feishu.MessageSendError, bool) {
	var sendErr *feishu.MessageSendError
	if !errors.As(err, &sendErr) || sendErr.Class != "api_rejected" {
		return nil, false
	}
	return sendErr, true
}

func isRetryableMessageFailure(err error) bool {
	sendErr, answered := answeredMessageFailure(err)
	if !answered {
		return true
	}
	return sendErr.Retryable
}

func isAmbiguousMessageFailure(err error) bool {
	sendErr, answered := answeredMessageFailure(err)
	if !answered {
		return true
	}
	// A server error can be emitted after Feishu accepted the message. An
	// explicit rate limit or IM code 230020 is a definitive temporary rejection
	// and carries no such ambiguity.
	return sendErr.HTTPStatus >= 500
}

func agentMessageCallError(err error, message string) *notify.APIError {
	if isRetryableMessageFailure(err) {
		return notify.NewAPIError(502, "feishu_unavailable", message, true)
	}
	return notify.NewAPIError(412, "feishu_rejected", message, false)
}
