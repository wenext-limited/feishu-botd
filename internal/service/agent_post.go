package service

import (
	"context"
	"strings"
	"sync"
	"time"

	"feishu-botd/internal/config"
	"feishu-botd/internal/feishu"
	"feishu-botd/internal/messageroute"
	"feishu-botd/internal/notify"
)

// agentPostSource names this path in the outbound message UUID seed. Unlike a
// follow-up, the seed is derived from the provider, operation id, and content
// rather than a random handle, so even a retry that crosses a daemon restart
// reuses the UUID Feishu deduplicates for one hour.
const agentPostSource = "agent_post"

// PostAgentMessageInput is one proactive message: a new root in ChatAlias, or
// a reply to ReplyToMessageRef. Exactly one target is set.
type PostAgentMessageInput struct {
	Provider          string
	OperationID       string
	ChatAlias         string
	ReplyToMessageRef string
	ReplyInThread     bool
	Markdown          string
	Summary           string
	MentionUserID     string
}

// AgentPostReceipt carries only provider-safe refs.
type AgentPostReceipt struct {
	MessageRef string
	ThreadRef  string
	Duplicate  bool
}

// agentMessageRouteStore is the reverse index from a provider-safe ref (or an
// unconfigured group's opaque alias) to its daemon-private Feishu route.
type agentMessageRouteStore interface {
	Put(key string, route messageroute.Route, now time.Time) error
	Lookup(key string, now time.Time) (messageroute.Route, bool, error)
	Delete(key string, now time.Time) error
}

// SetAgentMessageRouteStore installs restart-safe reply routes. Without it the
// service keeps them in memory for the dedupe TTL.
func (s *Service) SetAgentMessageRouteStore(store agentMessageRouteStore) {
	s.agentRoutes = store
}

// postTarget is a resolved, daemon-private destination.
type postTarget struct {
	appAlias          string
	chatAlias         string
	unconfiguredGroup bool
	chatID            string
	replyToMessageID  string
}

type agentPostOperation struct {
	fingerprint string
	pending     bool
	complete    bool
	ambiguousAt time.Time
	target      postTarget
	posted      feishu.PostedMessage
	// cardID is the CardKit entity a PostAgentCard attempt created. A retry
	// must send that same entity: sending a second one under the first
	// attempt's message UUID would leave the message pointing at an entity
	// botd no longer tracks.
	cardID    string
	expiresAt time.Time
}

type agentPostKey struct {
	provider    string
	operationID string
}

// agentPostLedger is the idempotency record for PostAgentMessage. It is
// process-local and TTL-bounded like every other operation receipt.
type agentPostLedger struct {
	mu         sync.Mutex
	ttl        time.Duration
	operations map[agentPostKey]*agentPostOperation
}

func newAgentPostLedger(ttl time.Duration) *agentPostLedger {
	if ttl <= 0 {
		ttl = time.Hour
	}
	return &agentPostLedger{ttl: ttl, operations: make(map[agentPostKey]*agentPostOperation)}
}

// PostAgentMessage posts one message no inbound delivery triggered. The
// transport has already proven the provider identity and the
// allow_proactive_messages grant; this method checks the target chat against
// proactive_chat_aliases and, for a reply, that the provider received or
// authored the target. Every posted part becomes agent-owned by the provider,
// so an unmentioned reply to it routes back like a reply to a response card.
func (s *Service) PostAgentMessage(ctx context.Context, in PostAgentMessageInput) (AgentPostReceipt, *notify.APIError) {
	provider := strings.TrimSpace(in.Provider)
	operationID := strings.TrimSpace(in.OperationID)
	chatAlias := config.NormalizeChannelAlias(in.ChatAlias)
	replyRef := strings.TrimSpace(in.ReplyToMessageRef)
	markdown := strings.TrimSpace(in.Markdown)
	summary := strings.TrimSpace(in.Summary)
	mentionUserID := strings.TrimSpace(in.MentionUserID)
	switch {
	case provider == "":
		return AgentPostReceipt{}, notify.BadRequest("missing_provider", "provider is required")
	case operationID == "":
		return AgentPostReceipt{}, notify.BadRequest("missing_operation_id", "operation_id is required")
	case chatAlias == "" && replyRef == "":
		return AgentPostReceipt{}, notify.BadRequest("missing_target", "chat_alias or reply_to_message_ref is required")
	case chatAlias != "" && replyRef != "":
		return AgentPostReceipt{}, notify.BadRequest("invalid_target", "set chat_alias or reply_to_message_ref, not both")
	case markdown == "":
		return AgentPostReceipt{}, notify.BadRequest("missing_markdown", "markdown is required")
	}
	if len(provider) > 64 || len(operationID) > 64 || len(chatAlias) > 160 || len(replyRef) > 160 ||
		len(markdown) > maxAgentCardBytes || len(summary) > maxAgentTitleBytes || len(mentionUserID) > 160 {
		return AgentPostReceipt{}, notify.BadRequest("field_too_large", "one or more fields are too large")
	}
	if !s.cfg.ProviderAllowsProactiveMessages(provider) {
		return AgentPostReceipt{}, notify.NewAPIError(403, "provider_scope_denied", "requested provider capability is not allowed", false)
	}
	inThread := in.ReplyInThread && replyRef != ""
	fingerprint := hashJSON(struct {
		ChatAlias     string
		ReplyTo       string
		InThread      bool
		Markdown      string
		Summary       string
		MentionUserID string
	}{chatAlias, replyRef, inThread, markdown, summary, mentionUserID})

	now := time.Now()
	replay, apiErr := s.agentPosts.begin(provider, operationID, fingerprint, now, s.cfg.SendTimeout)
	if apiErr != nil {
		return AgentPostReceipt{}, apiErr
	}
	if replay != nil {
		// A replay re-runs the idempotent ownership writes in case the first
		// attempt posted but failed to record them.
		if apiErr := s.persistPostedMessage(provider, replay.target, replay.posted, ""); apiErr != nil {
			return AgentPostReceipt{}, apiErr
		}
		return postReceipt(replay.target, replay.posted, true), nil
	}

	target, apiErr := s.resolvePostTarget(provider, chatAlias, replyRef, now)
	if apiErr != nil {
		s.agentPosts.release(provider, operationID)
		return AgentPostReceipt{}, apiErr
	}
	backend, ok := s.backendForApp(target.appAlias)
	if !ok || backend.poster == nil {
		s.agentPosts.release(provider, operationID)
		return AgentPostReceipt{}, notify.NotImplemented("agent_post_unavailable", "proactive messages are unavailable for this sender")
	}

	sendCtx, cancel := context.WithTimeout(ctx, s.cfg.SendTimeout)
	defer cancel()
	posted, err := backend.poster.PostMessage(sendCtx, feishu.PostMessageRequest{
		ChatID:           target.chatID,
		ReplyToMessageID: target.replyToMessageID,
		ReplyInThread:    inThread,
		Content: notify.Request{
			Source:        agentPostSource,
			DedupeKey:     hashJSON([3]string{provider, operationID, fingerprint}),
			Title:         summary,
			Markdown:      markdown,
			MentionUserID: mentionUserID,
		},
	})
	if err != nil {
		s.logFeishuFailure("agent post", "post", hashJSON([2]string{provider, operationID}), err)
		if len(posted.MessageIDs) > 0 {
			// Feishu accepted the first parts of a long message. They are real
			// messages: own them now, and pin the operation to its content so
			// only an identical retry — which reuses every part's UUID and is
			// deduplicated by Feishu — can finish the rest.
			_ = s.persistPostedMessage(provider, target, posted, "")
			s.agentPosts.abort(provider, operationID, nil, time.Now())
			if isRetryableMessageFailure(err) {
				return AgentPostReceipt{}, notify.NewAPIError(502, "feishu_unavailable", "Feishu accepted only part of the proactive message", true)
			}
			return AgentPostReceipt{}, notify.NewAPIError(412, "send_state_unknown", "Feishu accepted only part of the proactive message", false)
		}
		s.agentPosts.abort(provider, operationID, err, time.Now())
		return AgentPostReceipt{}, agentMessageCallError(err, "Feishu proactive message send failed")
	}
	if len(posted.MessageIDs) == 0 {
		s.agentPosts.abort(provider, operationID, nil, time.Now())
		return AgentPostReceipt{}, notify.NewAPIError(502, "feishu_unavailable", "Feishu proactive message send returned no message", true)
	}
	s.agentPosts.commit(provider, operationID, target, posted)
	if apiErr := s.persistPostedMessage(provider, target, posted, ""); apiErr != nil {
		return AgentPostReceipt{}, apiErr
	}
	return postReceipt(target, posted, false), nil
}

func postReceipt(target postTarget, posted feishu.PostedMessage, duplicate bool) AgentPostReceipt {
	receipt := AgentPostReceipt{Duplicate: duplicate, ThreadRef: feishu.ThreadRefForApp(target.appAlias, posted.ThreadID)}
	if len(posted.MessageIDs) > 0 {
		receipt.MessageRef = feishu.MessageRefForApp(target.appAlias, posted.MessageIDs[0])
	}
	return receipt
}

// resolvePostTarget maps the provider's target to a private route. A chat the
// provider may not speak in and a chat botd cannot resolve both return
// unknown_channel; an unknown, expired, unreceived, or out-of-scope reply
// target returns unknown_message. Neither error says which check failed.
func (s *Service) resolvePostTarget(provider, chatAlias, replyRef string, now time.Time) (postTarget, *notify.APIError) {
	if chatAlias != "" {
		if !s.cfg.ProviderAllowsProactiveChat(provider, chatAlias) {
			return postTarget{}, unknownChannelError()
		}
		if config.IsUnconfiguredGroupAlias(chatAlias) {
			// An opaque alias is a one-way digest: its chat is known only
			// because ingress saw a message there and retained the route.
			route, ok := s.lookupMessageRoute(chatAlias, now)
			if !ok || !route.UnconfiguredGroup || route.ChatID == "" ||
				!s.appAllowed(provider, route.AppAlias) || !s.cfg.AllowsUnconfiguredGroupChats(route.AppAlias) {
				return postTarget{}, unknownChannelError()
			}
			return postTarget{appAlias: route.AppAlias, chatAlias: chatAlias, unconfiguredGroup: true, chatID: route.ChatID}, nil
		}
		appAlias, chatID, ok := s.cfg.ResolveChannel(chatAlias)
		if !ok || !s.appAllowed(provider, appAlias) {
			return postTarget{}, unknownChannelError()
		}
		return postTarget{appAlias: appAlias, chatAlias: chatAlias, chatID: chatID}, nil
	}

	route, ok := s.lookupMessageRoute(replyRef, now)
	if !ok || route.MessageID == "" || !route.Allows(provider) ||
		!s.appAllowed(provider, route.AppAlias) || !s.cfg.ProviderAllowsProactiveChat(provider, route.ChatAlias) {
		return postTarget{}, unknownMessageError()
	}
	// The reply lands where the target lives, and that chat must still be the
	// one the route was learned in: a configured alias remapped to another
	// chat since then leaves the old message unaddressable.
	if route.UnconfiguredGroup {
		if !s.cfg.AllowsUnconfiguredGroupChats(route.AppAlias) {
			return postTarget{}, unknownMessageError()
		}
	} else {
		appAlias, chatID, ok := s.cfg.ResolveChannel(route.ChatAlias)
		if !ok || appAlias != route.AppAlias || route.ChatID != "" && chatID != route.ChatID {
			return postTarget{}, unknownMessageError()
		}
	}
	return postTarget{
		appAlias:          route.AppAlias,
		chatAlias:         route.ChatAlias,
		unconfiguredGroup: route.UnconfiguredGroup,
		chatID:            route.ChatID,
		replyToMessageID:  route.MessageID,
	}, nil
}

func (s *Service) lookupMessageRoute(key string, now time.Time) (messageroute.Route, bool) {
	if s.agentRoutes == nil {
		return messageroute.Route{}, false
	}
	route, ok, err := s.agentRoutes.Lookup(key, now)
	if err != nil {
		return messageroute.Route{}, false
	}
	return route, ok
}

// persistPostedMessage makes every posted part agent-owned by the provider and
// a later reply target for it. cardID, set for a PostAgentCard card, is kept
// beside the first part's route so UpdateAgentCard can find the entity. Both
// writes are idempotent.
func (s *Service) persistPostedMessage(provider string, target postTarget, posted feishu.PostedMessage, cardID string) *notify.APIError {
	now := time.Now()
	for index, messageID := range posted.MessageIDs {
		messageRef := feishu.MessageRefForApp(target.appAlias, messageID)
		if messageRef == "" {
			continue
		}
		if s.agentOwners != nil {
			if err := s.agentOwners.Put(messageRef, provider, now); err != nil {
				return notify.NewAPIError(500, "ownership_store_unavailable", "could not persist message ownership", true)
			}
		}
		threadID, partCardID := "", ""
		if index == 0 {
			threadID, partCardID = posted.ThreadID, cardID
		}
		if s.agentRoutes != nil {
			if err := s.agentRoutes.Put(messageRef, messageroute.Route{
				AppAlias:          target.appAlias,
				ChatAlias:         target.chatAlias,
				UnconfiguredGroup: target.unconfiguredGroup,
				Providers:         []string{provider},
				ChatID:            target.chatID,
				MessageID:         messageID,
				ThreadID:          threadID,
				CardID:            partCardID,
				Authored:          true,
			}, now); err != nil {
				return notify.NewAPIError(500, "message_route_store_unavailable", "could not persist message route", true)
			}
		}
	}
	return nil
}

// authoredMessage is a response card or follow-up a provider just sent.
type authoredMessage struct {
	provider          string
	appAlias          string
	chatAlias         string
	unconfiguredGroup bool
	chatID            string
	messageID         string
}

// rememberAuthoredMessageSoftly retains a reply route for a message the
// provider authored outside PostAgentMessage, when that provider may speak
// proactively in its chat. The message is already sent, so a failure here is
// logged rather than turned into a failed RPC; the only loss is that the ref
// cannot be a later reply target.
func (s *Service) rememberAuthoredMessageSoftly(message authoredMessage, kind, correlation string) {
	if s.agentRoutes == nil || message.chatAlias == "" || message.chatAlias == "direct" ||
		!s.cfg.ProviderAllowsProactiveChat(message.provider, message.chatAlias) {
		return
	}
	messageRef := feishu.MessageRefForApp(message.appAlias, message.messageID)
	if messageRef == "" {
		return
	}
	if err := s.agentRoutes.Put(messageRef, messageroute.Route{
		AppAlias:          effectiveAppAlias(message.appAlias),
		ChatAlias:         message.chatAlias,
		UnconfiguredGroup: message.unconfiguredGroup,
		Providers:         []string{message.provider},
		ChatID:            message.chatID,
		MessageID:         message.messageID,
		Authored:          true,
	}, time.Now()); err != nil {
		s.logger.Warn("agent message route retention failed",
			"operation", kind,
			"correlation", opaqueLogCorrelationID(kind, correlation),
		)
	}
}

// retainInboundMessageRoute retains, before dispatch, the reply route of a
// group message for every proactive provider that could receive it: any of
// them when the message may be addressed, only the ambient-granted ones when
// it can only be observed. It returns those grantees so the route can be
// narrowed once dispatch shows who actually received it. A deployment without
// proactive grants writes nothing. An unconfigured group's chat is retained
// too, because its opaque alias is the only way a root post can name it.
// Failures are logged and never block ingress; they only leave that ref
// unaddressable.
func (s *Service) retainInboundMessageRoute(in CommandInput, ambientOnly bool) []string {
	if s.agentRoutes == nil || !isGroupChatType(in.Metadata["chat_type"]) {
		return nil
	}
	proactive := s.cfg.ProactiveProvidersForChat(in.AppAlias, in.ChatAlias)
	if len(proactive) == 0 {
		return nil
	}
	now := time.Now()
	failed := false
	if in.UnconfiguredGroup && in.ChatID != "" {
		if err := s.agentRoutes.Put(in.ChatAlias, messageroute.Route{
			AppAlias:          in.AppAlias,
			ChatAlias:         in.ChatAlias,
			UnconfiguredGroup: true,
			Providers:         proactive,
			ChatID:            in.ChatID,
		}, now); err != nil {
			failed = true
		}
	}
	grantees := proactive
	if ambientOnly {
		grantees = make([]string, 0, len(proactive))
		for _, provider := range proactive {
			if s.cfg.ProviderAllowsAmbientChat(provider, in.ChatAlias) {
				grantees = append(grantees, provider)
			}
		}
	}
	if len(grantees) > 0 && feishu.MessageRefForApp(in.AppAlias, in.Metadata["message_id"]) != "" {
		if err := s.agentRoutes.Put(feishu.MessageRefForApp(in.AppAlias, in.Metadata["message_id"]), inboundRoute(in, grantees), now); err != nil {
			failed = true
			grantees = nil
		}
	} else {
		grantees = nil
	}
	if failed {
		s.logRouteRetentionFailure("ingress", in.DeliveryID)
	}
	return grantees
}

// narrowInboundMessageRoute reduces a retained route to the grantees that
// actually received the message, by any selector, so a reply target is a
// message the provider received rather than one it merely could have. The
// recipient set is cumulative per message, so a redelivery cannot strip a
// grant an earlier delivery earned.
func (s *Service) narrowInboundMessageRoute(in CommandInput, grantees []string) {
	if len(grantees) == 0 {
		return
	}
	recipients := s.agentBroker.messageRecipients(in)
	received := make([]string, 0, len(grantees))
	for _, provider := range grantees {
		if _, ok := recipients[provider]; ok {
			received = append(received, provider)
		}
	}
	if len(received) == len(grantees) {
		return
	}
	messageRef := feishu.MessageRefForApp(in.AppAlias, in.Metadata["message_id"])
	var err error
	if len(received) == 0 {
		err = s.agentRoutes.Delete(messageRef, time.Now())
	} else {
		err = s.agentRoutes.Put(messageRef, inboundRoute(in, received), time.Now())
	}
	if err != nil {
		s.logRouteRetentionFailure("ingress", in.DeliveryID)
	}
}

func inboundRoute(in CommandInput, providers []string) messageroute.Route {
	return messageroute.Route{
		AppAlias:          in.AppAlias,
		ChatAlias:         in.ChatAlias,
		UnconfiguredGroup: in.UnconfiguredGroup,
		Providers:         providers,
		ChatID:            in.ChatID,
		MessageID:         in.Metadata["message_id"],
		ThreadID:          in.Metadata["thread_id"],
	}
}

func (s *Service) logRouteRetentionFailure(kind, correlation string) {
	s.logger.Warn("agent message route retention failed",
		"operation", kind,
		"correlation", opaqueLogCorrelationID("command", correlation),
	)
}

func unknownChannelError() *notify.APIError {
	return notify.NewAPIError(404, "unknown_channel", "unknown channel", false)
}

func unknownMessageError() *notify.APIError {
	return notify.NewAPIError(404, "unknown_message", "unknown message", false)
}

// begin claims (provider, operationID). A completed operation with the same
// content returns its record for replay; one with different content is an
// operation_conflict, and an attempt still in flight is operation_in_flight.
func (l *agentPostLedger) begin(provider, operationID, fingerprint string, now time.Time, sendTimeout time.Duration) (*agentPostOperation, *notify.APIError) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for key, op := range l.operations {
		if !op.pending && now.After(op.expiresAt) {
			delete(l.operations, key)
		}
	}
	key := agentPostKey{provider: provider, operationID: operationID}
	op, exists := l.operations[key]
	if exists {
		if op.fingerprint != fingerprint {
			return nil, notify.NewAPIError(409, "operation_conflict", "operation id reused with different content", false)
		}
		if op.complete {
			replay := *op
			replay.posted.MessageIDs = append([]string(nil), op.posted.MessageIDs...)
			return &replay, nil
		}
		if op.pending {
			return nil, notify.NewAPIError(409, "operation_in_flight", "another attempt for this message is in flight", true)
		}
		if messageRetryWindowExhausted(now, sendTimeout, op.ambiguousAt) {
			return nil, closedAgentSendError("send_retry_expired")
		}
	} else {
		op = &agentPostOperation{fingerprint: fingerprint}
		l.operations[key] = op
	}
	op.pending = true
	op.expiresAt = now.Add(l.ttl)
	return nil, nil
}

// setCard records the entity an attempt created, for the attempts after it.
func (l *agentPostLedger) setCard(provider, operationID, cardID string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if op, ok := l.operations[agentPostKey{provider: provider, operationID: operationID}]; ok {
		op.cardID = cardID
	}
}

// card returns the entity an earlier attempt created, if any.
func (l *agentPostLedger) card(provider, operationID string) string {
	l.mu.Lock()
	defer l.mu.Unlock()
	if op, ok := l.operations[agentPostKey{provider: provider, operationID: operationID}]; ok {
		return op.cardID
	}
	return ""
}

func (l *agentPostLedger) commit(provider, operationID string, target postTarget, posted feishu.PostedMessage) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if op, ok := l.operations[agentPostKey{provider: provider, operationID: operationID}]; ok {
		op.pending = false
		op.complete = true
		op.target = target
		op.posted = feishu.PostedMessage{MessageIDs: append([]string(nil), posted.MessageIDs...), ThreadID: posted.ThreadID}
		op.expiresAt = time.Now().Add(l.ttl)
	}
}

// release drops a claim that never reached Feishu, so a corrected request may
// reuse the id. A claim with an ambiguous earlier attempt is kept pinned.
func (l *agentPostLedger) release(provider, operationID string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	key := agentPostKey{provider: provider, operationID: operationID}
	if op, ok := l.operations[key]; ok {
		if op.ambiguousAt.IsZero() {
			delete(l.operations, key)
			return
		}
		op.pending = false
	}
}

// abort applies the follow-up retry rules: an ambiguous failure pins the
// operation to its content and starts the UUID-window clock, a definitive
// temporary rejection keeps it for a same-UUID retry, and a definitive
// rejection with nothing in doubt releases the id.
func (l *agentPostLedger) abort(provider, operationID string, err error, now time.Time) {
	l.abortClassified(provider, operationID, err == nil || isAmbiguousMessageFailure(err), err != nil && isRetryableMessageFailure(err), now)
}

// abortClassified is abort for a caller that classifies its own failure: a
// card send fails with CardKit errors, not message send errors.
func (l *agentPostLedger) abortClassified(provider, operationID string, ambiguous, retryable bool, now time.Time) {
	l.mu.Lock()
	defer l.mu.Unlock()
	key := agentPostKey{provider: provider, operationID: operationID}
	op, ok := l.operations[key]
	if !ok {
		return
	}
	op.pending = false
	switch {
	case ambiguous:
		if op.ambiguousAt.IsZero() {
			op.ambiguousAt = now
		}
	case retryable:
	case op.ambiguousAt.IsZero():
		delete(l.operations, key)
	}
}
