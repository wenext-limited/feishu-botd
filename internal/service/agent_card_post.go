package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/url"
	"strings"
	"sync"
	"time"

	"feishu-botd/internal/config"
	"feishu-botd/internal/feishu"
	"feishu-botd/internal/messageroute"
	"feishu-botd/internal/notify"
)

// AgentCardState is the lifecycle tag in a posted card's header.
type AgentCardState int

const (
	AgentCardStateUnspecified AgentCardState = iota
	AgentCardStateProcessing
	AgentCardStateActive
	AgentCardStateSuspended
	AgentCardStateArchived
)

const (
	maxAgentCardSteps      = 50
	maxAgentCardLinks      = 10
	maxAgentCardStepBytes  = 200
	maxAgentCardLabelBytes = 100
	maxAgentCardURLBytes   = 2048

	agentCardBodyElementID  = "agent_card_body"
	agentCardStepsElementID = "agent_card_steps"
	agentCardLinksElementID = "agent_card_links"
)

// agentCardStateStyle is the wording and colour botd owns for each state:
// the header template and the matching text_tag colour.
var agentCardStateStyle = map[AgentCardState]struct{ label, template, tagColor string }{
	AgentCardStateProcessing: {"进行中", "blue", "blue"},
	AgentCardStateActive:     {"已回答", "green", "green"},
	AgentCardStateSuspended:  {"等人处理", "orange", "orange"},
	AgentCardStateArchived:   {"已归档", "grey", "neutral"},
}

// AgentCardStep is one checklist line.
type AgentCardStep struct {
	Label string
	Done  bool
}

// AgentCardLink is one https link button.
type AgentCardLink struct {
	Label string
	URL   string
}

// AgentCardContent is a posted card's complete content. An update replaces
// all of it, so an empty field removes that part of the card.
type AgentCardContent struct {
	Title    string
	State    AgentCardState
	Markdown string
	Steps    []AgentCardStep
	Links    []AgentCardLink
	Summary  string
}

// PostAgentCardInput is PostAgentMessageInput with typed card content.
type PostAgentCardInput struct {
	Provider          string
	OperationID       string
	ChatAlias         string
	ReplyToMessageRef string
	ReplyInThread     bool
	Content           AgentCardContent
}

// UpdateAgentCardInput replaces the content of a card the provider posted.
type UpdateAgentCardInput struct {
	Provider    string
	MessageRef  string
	OperationID string
	Content     AgentCardContent
}

// AgentCardUpdateReceipt reports how an update settled. Superseded means a
// newer update to the same card was applied in its place.
type AgentCardUpdateReceipt struct {
	Duplicate  bool
	Superseded bool
}

// PinAgentMessageInput names one message the provider authored.
type PinAgentMessageInput struct {
	Provider   string
	MessageRef string
}

// agentCardPostSource seeds a posted card's message UUID, like agentPostSource.
const agentCardPostSource = "agent_card_post"

// buildAgentTaskCard renders typed card content as a Card JSON 2.0 document.
// Every provider string lands in a plain_text field except markdown, which is
// the body the provider means to format, so a step or link label can never
// render as markup.
func buildAgentTaskCard(content AgentCardContent) (string, *notify.APIError) {
	title := strings.TrimSpace(content.Title)
	markdown := strings.TrimSpace(content.Markdown)
	summary := strings.TrimSpace(content.Summary)
	if title == "" {
		return "", notify.BadRequest("missing_title", "title is required")
	}
	if len(title) > maxAgentTitleBytes || len(summary) > maxAgentTitleBytes || len(markdown) > maxAgentCardBytes ||
		len(content.Steps) > maxAgentCardSteps || len(content.Links) > maxAgentCardLinks {
		return "", notify.BadRequest("field_too_large", "one or more fields are too large")
	}
	style, styled := agentCardStateStyle[content.State]
	if !styled && content.State != AgentCardStateUnspecified {
		return "", notify.BadRequest("invalid_card_state", "card state is not supported")
	}
	if summary == "" {
		summary = title
	}

	elements := make([]any, 0, 3)
	if markdown != "" {
		elements = append(elements, map[string]any{"tag": "markdown", "element_id": agentCardBodyElementID, "content": markdown})
	}
	if len(content.Steps) > 0 {
		lines := make([]string, 0, len(content.Steps))
		for _, step := range content.Steps {
			label := strings.TrimSpace(step.Label)
			if label == "" || strings.ContainsAny(label, "\r\n") {
				return "", notify.BadRequest("invalid_step", "each step needs a single-line label")
			}
			if len(label) > maxAgentCardStepBytes {
				return "", notify.BadRequest("field_too_large", "one or more fields are too large")
			}
			mark := "⬜"
			if step.Done {
				mark = "✅"
			}
			lines = append(lines, mark+" "+label)
		}
		elements = append(elements, map[string]any{
			"tag":        "div",
			"element_id": agentCardStepsElementID,
			"text":       map[string]any{"tag": "plain_text", "content": strings.Join(lines, "\n")},
		})
	}
	if len(content.Links) > 0 {
		columns := make([]any, 0, len(content.Links))
		for index, link := range content.Links {
			label := strings.TrimSpace(link.Label)
			target := strings.TrimSpace(link.URL)
			if label == "" || target == "" {
				return "", notify.BadRequest("invalid_link", "each link needs a label and a url")
			}
			if len(label) > maxAgentCardLabelBytes || len(target) > maxAgentCardURLBytes {
				return "", notify.BadRequest("field_too_large", "one or more fields are too large")
			}
			if !isHTTPSURL(target) {
				return "", notify.BadRequest("invalid_link", "link urls must be absolute https urls")
			}
			columns = append(columns, map[string]any{
				"tag":   "column",
				"width": "auto",
				"elements": []any{map[string]any{
					"tag":        "button",
					"element_id": fmt.Sprintf("agent_card_link_%d", index+1),
					"text":       map[string]any{"tag": "plain_text", "content": label},
					"type":       "default",
					"size":       "small",
					"behaviors":  []any{map[string]any{"type": "open_url", "default_url": target}},
				}},
			})
		}
		elements = append(elements, map[string]any{
			"tag":                "column_set",
			"element_id":         agentCardLinksElementID,
			"flex_mode":          "flow",
			"horizontal_spacing": "8px",
			"columns":            columns,
		})
	}

	header := map[string]any{"title": map[string]any{"tag": "plain_text", "content": title}}
	if styled {
		header["template"] = style.template
		header["text_tag_list"] = []any{map[string]any{
			"tag":   "text_tag",
			"text":  map[string]any{"tag": "plain_text", "content": style.label},
			"color": style.tagColor,
		}}
	}
	card := map[string]any{
		"schema": "2.0",
		// update_multi is mandatory for a CardKit entity; streaming_mode stays
		// off because every update replaces the whole card.
		"config": map[string]any{"update_multi": true, "summary": map[string]any{"content": summary}},
		"header": header,
		"body":   map[string]any{"elements": elements},
	}
	data, err := json.Marshal(card)
	if err != nil {
		return "", notify.NewAPIError(500, "internal", "could not encode agent card", false)
	}
	if len(data) > maxAgentCardBytes {
		return "", notify.BadRequest("field_too_large", "agent card exceeds the daemon size limit")
	}
	return string(data), nil
}

func isHTTPSURL(raw string) bool {
	parsed, err := url.Parse(raw)
	return err == nil && parsed.Scheme == "https" && parsed.Host != "" && parsed.User == nil
}

// PostAgentCard posts one CardKit card no inbound delivery triggered. Target
// resolution, grants, idempotency, and ownership are PostAgentMessage's; the
// card's CardKit id is retained beside its reply route so UpdateAgentCard can
// replace it after a restart.
func (s *Service) PostAgentCard(ctx context.Context, in PostAgentCardInput) (AgentPostReceipt, *notify.APIError) {
	provider := strings.TrimSpace(in.Provider)
	operationID := strings.TrimSpace(in.OperationID)
	chatAlias := config.NormalizeChannelAlias(in.ChatAlias)
	replyRef := strings.TrimSpace(in.ReplyToMessageRef)
	switch {
	case provider == "":
		return AgentPostReceipt{}, notify.BadRequest("missing_provider", "provider is required")
	case operationID == "":
		return AgentPostReceipt{}, notify.BadRequest("missing_operation_id", "operation_id is required")
	case chatAlias == "" && replyRef == "":
		return AgentPostReceipt{}, notify.BadRequest("missing_target", "chat_alias or reply_to_message_ref is required")
	case chatAlias != "" && replyRef != "":
		return AgentPostReceipt{}, notify.BadRequest("invalid_target", "set chat_alias or reply_to_message_ref, not both")
	}
	if len(provider) > 64 || len(operationID) > 64 || len(chatAlias) > 160 || len(replyRef) > 160 {
		return AgentPostReceipt{}, notify.BadRequest("field_too_large", "one or more fields are too large")
	}
	cardJSON, apiErr := buildAgentTaskCard(in.Content)
	if apiErr != nil {
		return AgentPostReceipt{}, apiErr
	}
	if !s.cfg.ProviderAllowsProactiveMessages(provider) {
		return AgentPostReceipt{}, notify.NewAPIError(403, "provider_scope_denied", "requested provider capability is not allowed", false)
	}
	inThread := in.ReplyInThread && replyRef != ""
	fingerprint := hashJSON(struct {
		ChatAlias string
		ReplyTo   string
		InThread  bool
		CardJSON  string
	}{chatAlias, replyRef, inThread, cardJSON})

	now := time.Now()
	replay, apiErr := s.agentCardPosts.begin(provider, operationID, fingerprint, now, s.cfg.SendTimeout)
	if apiErr != nil {
		return AgentPostReceipt{}, apiErr
	}
	if replay != nil {
		if apiErr := s.persistPostedMessage(provider, replay.target, replay.posted, replay.cardID); apiErr != nil {
			return AgentPostReceipt{}, apiErr
		}
		return postReceipt(replay.target, replay.posted, true), nil
	}

	target, apiErr := s.resolvePostTarget(provider, chatAlias, replyRef, now)
	if apiErr != nil {
		s.agentCardPosts.release(provider, operationID)
		return AgentPostReceipt{}, apiErr
	}
	backend, ok := s.backendForApp(target.appAlias)
	if !ok || backend.agentCards == nil {
		s.agentCardPosts.release(provider, operationID)
		return AgentPostReceipt{}, notify.NotImplemented("agent_card_unavailable", "proactive cards are unavailable for this sender")
	}

	callCtx, cancel := context.WithTimeout(ctx, s.cfg.SendTimeout)
	defer cancel()
	correlation := hashJSON([2]string{provider, operationID})
	cardID := s.agentCardPosts.card(provider, operationID)
	if cardID == "" {
		created, err := backend.agentCards.CreateCard(callCtx, cardJSON)
		if err != nil {
			// An entity that was never sent is invisible, so a failed create
			// leaves nothing in doubt.
			s.logFeishuFailure("agent card post", "create", correlation, err)
			s.agentCardPosts.release(provider, operationID)
			return AgentPostReceipt{}, agentCardCallError(err, "Feishu card creation failed")
		}
		cardID = created
		s.agentCardPosts.setCard(provider, operationID, cardID)
	}
	sendStartedAt := time.Now()
	posted, err := backend.agentCards.PostCard(callCtx, feishu.CardPostRequest{
		ChatID:           postChatID(target),
		ReplyToMessageID: target.replyToMessageID,
		ReplyInThread:    inThread,
		CardID:           cardID,
		// Seeded by content rather than by a random handle, so even a retry
		// that crosses a restart reuses the UUID Feishu deduplicates.
		UUID: operationUUID(agentCardPostSource, provider, operationID+"\x00"+fingerprint, 50),
	})
	if err != nil {
		s.logFeishuFailure("agent card post", "post", correlation, err)
		rejected := isCardAPIRejection(err) && !isRetryableCardFailure(err)
		s.agentCardPosts.abortClassified(provider, operationID, !rejected && isAmbiguousCardFailure(err), !rejected, sendStartedAt)
		return AgentPostReceipt{}, agentCardCallError(err, "Feishu proactive card send failed")
	}
	if len(posted.MessageIDs) == 0 {
		s.agentCardPosts.abortClassified(provider, operationID, true, true, sendStartedAt)
		return AgentPostReceipt{}, notify.NewAPIError(502, "feishu_unavailable", "Feishu proactive card send returned no message", true)
	}
	s.agentCardPosts.commit(provider, operationID, target, posted)
	if apiErr := s.persistPostedMessage(provider, target, posted, cardID); apiErr != nil {
		return AgentPostReceipt{}, apiErr
	}
	return postReceipt(target, posted, false), nil
}

// postChatID is the chat a root post lands in; a reply names its parent only.
func postChatID(target postTarget) string {
	if target.replyToMessageID != "" {
		return ""
	}
	return target.chatID
}

// lookupAuthoredRoute resolves a message_ref to a message this provider
// authored and may still address. Every miss is the same unknown_message.
func (s *Service) lookupAuthoredRoute(provider, messageRef string, now time.Time) (messageroute.Route, *notify.APIError) {
	route, ok := s.lookupMessageRoute(messageRef, now)
	if !ok || !route.Authored || route.MessageID == "" || !route.Allows(provider) ||
		feishu.MessageRefForApp(route.AppAlias, route.MessageID) != messageRef ||
		!s.appAllowed(provider, route.AppAlias) || !s.cfg.ProviderAllowsProactiveChat(provider, route.ChatAlias) {
		return messageroute.Route{}, unknownMessageError()
	}
	// As for a reply target, the message's chat must still be the one its
	// alias named when botd posted there.
	if route.UnconfiguredGroup {
		if !s.cfg.AllowsUnconfiguredGroupChats(route.AppAlias) {
			return messageroute.Route{}, unknownMessageError()
		}
	} else if appAlias, chatID, ok := s.cfg.ResolveChannel(route.ChatAlias); !ok || appAlias != route.AppAlias || route.ChatID != "" && chatID != route.ChatID {
		return messageroute.Route{}, unknownMessageError()
	}
	return route, nil
}

// UpdateAgentCard replaces the whole content of a card the provider posted.
// Updates to one card run one at a time in arrival order; one that finds a
// newer update already waiting behind it yields to that one. Each attempt
// durably reserves the next CardKit sequence before calling Feishu, so a
// crash after the call can only leave a gap, never a reused sequence.
func (s *Service) UpdateAgentCard(ctx context.Context, in UpdateAgentCardInput) (AgentCardUpdateReceipt, *notify.APIError) {
	provider := strings.TrimSpace(in.Provider)
	messageRef := strings.TrimSpace(in.MessageRef)
	operationID := strings.TrimSpace(in.OperationID)
	switch {
	case provider == "":
		return AgentCardUpdateReceipt{}, notify.BadRequest("missing_provider", "provider is required")
	case messageRef == "":
		return AgentCardUpdateReceipt{}, notify.BadRequest("missing_message_ref", "message_ref is required")
	case operationID == "":
		return AgentCardUpdateReceipt{}, notify.BadRequest("missing_operation_id", "operation_id is required")
	}
	if len(provider) > 64 || len(operationID) > 64 || len(messageRef) > 160 {
		return AgentCardUpdateReceipt{}, notify.BadRequest("field_too_large", "one or more fields are too large")
	}
	cardJSON, apiErr := buildAgentTaskCard(in.Content)
	if apiErr != nil {
		return AgentCardUpdateReceipt{}, apiErr
	}
	if !s.cfg.ProviderAllowsProactiveMessages(provider) {
		return AgentCardUpdateReceipt{}, notify.NewAPIError(403, "provider_scope_denied", "requested provider capability is not allowed", false)
	}
	if route, apiErr := s.lookupAuthoredRoute(provider, messageRef, time.Now()); apiErr != nil || route.CardID == "" {
		return AgentCardUpdateReceipt{}, unknownMessageError()
	}

	fingerprint := hashJSON([2]string{messageRef, cardJSON})
	replay, apiErr := s.agentCardUpdates.begin(provider, operationID, fingerprint, time.Now())
	if apiErr != nil {
		return AgentCardUpdateReceipt{}, apiErr
	}
	if replay != nil {
		return AgentCardUpdateReceipt{Duplicate: true, Superseded: replay.superseded}, nil
	}

	lane, ticket := s.agentCardLanes.enter(messageRef)
	defer s.agentCardLanes.leave(messageRef)
	lane.mu.Lock()
	defer lane.mu.Unlock()
	if s.agentCardLanes.superseded(lane, ticket) {
		s.agentCardUpdates.commit(provider, operationID, true)
		return AgentCardUpdateReceipt{Superseded: true}, nil
	}

	// Re-read under the lane: the sequence is whatever the last update left.
	route, apiErr := s.lookupAuthoredRoute(provider, messageRef, time.Now())
	if apiErr != nil || route.CardID == "" {
		s.agentCardUpdates.release(provider, operationID)
		return AgentCardUpdateReceipt{}, unknownMessageError()
	}
	backend, ok := s.backendForApp(route.AppAlias)
	if !ok || backend.agentCards == nil {
		s.agentCardUpdates.release(provider, operationID)
		return AgentCardUpdateReceipt{}, notify.NotImplemented("agent_card_unavailable", "proactive cards are unavailable for this sender")
	}
	if route.CardSequence >= math.MaxInt32 {
		s.agentCardUpdates.release(provider, operationID)
		return AgentCardUpdateReceipt{}, notify.NewAPIError(412, "card_expired", "the card can no longer be updated; post a new one", false)
	}
	route.CardSequence++
	sequence := route.CardSequence
	if s.agentRoutes == nil || s.agentRoutes.Put(messageRef, route, time.Now()) != nil {
		s.agentCardUpdates.release(provider, operationID)
		return AgentCardUpdateReceipt{}, notify.NewAPIError(500, "message_route_store_unavailable", "could not persist message route", true)
	}

	callCtx, cancel := context.WithTimeout(ctx, s.cfg.SendTimeout)
	defer cancel()
	err := backend.agentCards.ReplaceCard(callCtx, feishu.CardReplaceRequest{
		CardID:   route.CardID,
		CardJSON: cardJSON,
		UUID:     operationUUID("card_update", messageRef, fmt.Sprintf("%d\x00%s", sequence, operationID), 64),
		Sequence: sequence,
	})
	if err != nil {
		s.logFeishuFailure("agent card update", "card", messageRef, err)
		// The update replaces the whole card, so applying it twice is
		// harmless: every failure releases the id for a plain retry.
		s.agentCardUpdates.release(provider, operationID)
		if isCardEntityGone(err) {
			return AgentCardUpdateReceipt{}, notify.NewAPIError(412, "card_expired", "the card can no longer be updated; post a new one", false)
		}
		return AgentCardUpdateReceipt{}, agentCardCallError(err, "Feishu card update failed")
	}
	s.agentCardUpdates.commit(provider, operationID, false)
	return AgentCardUpdateReceipt{}, nil
}

// isCardEntityGone reports CardKit's answer for an entity past its 14-day
// life (200750) or one it no longer has (200740).
func isCardEntityGone(err error) bool {
	var apiErr *feishu.DynamicCardAPIError
	return errors.As(err, &apiErr) && (apiErr.Code == 200750 || apiErr.Code == 200740)
}

// PinAgentMessage pins one message the provider authored. Feishu answers a
// repeated pin with the existing pin, so the call is naturally idempotent.
func (s *Service) PinAgentMessage(ctx context.Context, in PinAgentMessageInput) *notify.APIError {
	provider := strings.TrimSpace(in.Provider)
	messageRef := strings.TrimSpace(in.MessageRef)
	switch {
	case provider == "":
		return notify.BadRequest("missing_provider", "provider is required")
	case messageRef == "":
		return notify.BadRequest("missing_message_ref", "message_ref is required")
	}
	if len(provider) > 64 || len(messageRef) > 160 {
		return notify.BadRequest("field_too_large", "one or more fields are too large")
	}
	if !s.cfg.ProviderAllowsPins(provider) {
		return notify.NewAPIError(403, "provider_scope_denied", "requested provider capability is not allowed", false)
	}
	route, apiErr := s.lookupAuthoredRoute(provider, messageRef, time.Now())
	if apiErr != nil {
		return apiErr
	}
	backend, ok := s.backendForApp(route.AppAlias)
	if !ok || backend.pinner == nil {
		return notify.NotImplemented("agent_pin_unavailable", "pins are unavailable for this sender")
	}
	callCtx, cancel := context.WithTimeout(ctx, s.cfg.SendTimeout)
	defer cancel()
	if err := backend.pinner.PinMessage(callCtx, route.MessageID); err != nil {
		s.logFeishuFailure("agent pin", "message", messageRef, err)
		return pinCallError(err)
	}
	return nil
}

// pinCallError separates the answers a provider can act on. A missing scope
// or a chat that lets only owners and admins pin is pin_permission_denied, so
// the provider can fall back to something other than a pin; a per-message pin
// rate limit is retryable.
func pinCallError(err error) *notify.APIError {
	sendErr, answered := answeredMessageFailure(err)
	if answered {
		switch sendErr.Code {
		case 230027, 230046, 230006, 99991672, 99991679:
			return notify.NewAPIError(412, "pin_permission_denied", "Feishu refused the pin for missing permission", false)
		case 230047:
			return notify.NewAPIError(502, "feishu_unavailable", "Feishu pin rate limit reached", true)
		}
	}
	return agentMessageCallError(err, "Feishu pin failed")
}

// agentCardUpdateLedger is the idempotency record for UpdateAgentCard. A
// failed attempt is always released: an update replaces the whole card, so
// there is no partial outcome to protect.
type agentCardUpdateLedger struct {
	mu         sync.Mutex
	ttl        time.Duration
	operations map[agentPostKey]*agentCardUpdateOperation
}

type agentCardUpdateOperation struct {
	fingerprint string
	pending     bool
	superseded  bool
	expiresAt   time.Time
}

func newAgentCardUpdateLedger(ttl time.Duration) *agentCardUpdateLedger {
	if ttl <= 0 {
		ttl = time.Hour
	}
	return &agentCardUpdateLedger{ttl: ttl, operations: make(map[agentPostKey]*agentCardUpdateOperation)}
}

func (l *agentCardUpdateLedger) begin(provider, operationID, fingerprint string, now time.Time) (*agentCardUpdateOperation, *notify.APIError) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for key, op := range l.operations {
		if !op.pending && now.After(op.expiresAt) {
			delete(l.operations, key)
		}
	}
	key := agentPostKey{provider: provider, operationID: operationID}
	if op, exists := l.operations[key]; exists {
		if op.fingerprint != fingerprint {
			return nil, notify.NewAPIError(409, "operation_conflict", "operation id reused with different content", false)
		}
		if op.pending {
			return nil, notify.NewAPIError(409, "operation_in_flight", "another attempt for this update is in flight", true)
		}
		replay := *op
		return &replay, nil
	}
	l.operations[key] = &agentCardUpdateOperation{fingerprint: fingerprint, pending: true, expiresAt: now.Add(l.ttl)}
	return nil, nil
}

func (l *agentCardUpdateLedger) commit(provider, operationID string, superseded bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if op, ok := l.operations[agentPostKey{provider: provider, operationID: operationID}]; ok {
		op.pending = false
		op.superseded = superseded
		op.expiresAt = time.Now().Add(l.ttl)
	}
}

func (l *agentCardUpdateLedger) release(provider, operationID string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.operations, agentPostKey{provider: provider, operationID: operationID})
}

// agentCardLanes serializes updates per card. Each arrival takes a ticket;
// the holder of the lane mutex yields when a later ticket exists, which
// collapses a backlog to its newest entry.
type agentCardLanes struct {
	mu    sync.Mutex
	lanes map[string]*agentCardLane
}

type agentCardLane struct {
	mu     sync.Mutex
	latest uint64
	refs   int
}

func newAgentCardLanes() *agentCardLanes {
	return &agentCardLanes{lanes: make(map[string]*agentCardLane)}
}

func (l *agentCardLanes) enter(key string) (*agentCardLane, uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	lane := l.lanes[key]
	if lane == nil {
		lane = &agentCardLane{}
		l.lanes[key] = lane
	}
	lane.latest++
	lane.refs++
	return lane, lane.latest
}

func (l *agentCardLanes) superseded(lane *agentCardLane, ticket uint64) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return lane.latest > ticket
}

func (l *agentCardLanes) leave(key string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if lane := l.lanes[key]; lane != nil {
		lane.refs--
		if lane.refs <= 0 {
			delete(l.lanes, key)
		}
	}
}
