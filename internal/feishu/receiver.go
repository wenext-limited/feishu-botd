package feishu

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"sync"

	larkevent "github.com/larksuite/oapi-sdk-go/v3/event"
	"github.com/larksuite/oapi-sdk-go/v3/event/dispatcher"
	larkcallback "github.com/larksuite/oapi-sdk-go/v3/event/dispatcher/callback"
	larkim "github.com/larksuite/oapi-sdk-go/v3/service/im/v1"
	larkws "github.com/larksuite/oapi-sdk-go/v3/ws"

	"feishu-botd/internal/config"
)

type CommandReceiverConfig struct {
	AppAlias  string
	AppID     string
	AppSecret string
	Channels  map[string]string

	AllowUnconfiguredGroupChats bool
	BotOpenID                   string
	BotUserID                   string
	BotUnionID                  string
	BotNames                    []string
	ChatTitleLookup             ChatTitleLookup

	// ConnectionStateChanged receives only a public app alias and a fixed state.
	// Raw SDK errors can contain credentials, connection URLs, or tenant data and
	// must never cross this callback boundary.
	ConnectionStateChanged func(appAlias string, state ConnectionState)
}

type InboundCommand struct {
	AppAlias          string `json:"-"`
	DeliveryID        string
	Command           string
	Text              string
	Prompt            string
	ChatAlias         string
	ConversationID    string
	SenderID          string
	Metadata          map[string]string
	ConversationTitle string

	// ChatID is the raw Feishu reply route. It is daemon-private and must never
	// cross the public command/provider boundary.
	ChatID string `json:"-"`
	// UnconfiguredGroup is daemon-private ingress state. It is true only when a
	// group was admitted as a candidate by the app's explicit wildcard policy;
	// an unmentioned candidate still needs agent-parent ownership below.
	UnconfiguredGroup bool `json:"-"`
	// UnmentionedReply is daemon-private ingress state. It marks a group reply
	// that had a parent message but no matching bot mention. The service must
	// prove that the parent is agent-authored before routing this candidate.
	UnmentionedReply bool `json:"-"`
}

type CommandHandler func(context.Context, InboundCommand) error

// InboundCardAction is the transport-neutral subset of a Feishu interactive
// card callback. Callback credentials and raw chat ids deliberately do not
// appear here. ValueJSON and FormValueJSON preserve JSON types such as booleans,
// arrays, and nested objects for downstream agent providers.
type InboundCardAction struct {
	AppAlias       string `json:"-"`
	DeliveryID     string
	ConversationID string
	MessageID      string `json:"-"`
	SenderID       string
	Tag            string
	Name           string
	Option         string
	Timezone       string
	InputValue     string
	Options        []string
	Checked        bool
	ValueJSON      string
	FormValueJSON  string
	Host           string
	DeliveryType   string
}

type CardActionHandler func(context.Context, InboundCardAction) error

type MessageReactionOperation int

const (
	MessageReactionUnspecified MessageReactionOperation = iota
	MessageReactionAdded
	MessageReactionRemoved
)

// InboundMessageReaction is a provider-safe native reaction. MessageRef and
// DeliveryID are opaque digests; no raw Feishu routing identifier appears.
type InboundMessageReaction struct {
	AppAlias     string
	DeliveryID   string
	MessageRef   string
	SenderID     string
	ReactionType string
	Operation    MessageReactionOperation
}

type MessageReactionHandler func(context.Context, InboundMessageReaction) error

var errCommandReceiverUnavailable = errors.New("feishu command receiver unavailable")

// ConnectionState is the privacy-safe lifecycle state of one app's long
// connection. It deliberately carries no SDK error text.
type ConnectionState string

const (
	ConnectionStateStarting     ConnectionState = "starting"
	ConnectionStateConnected    ConnectionState = "connected"
	ConnectionStateReconnecting ConnectionState = "reconnecting"
	ConnectionStateDisconnected ConnectionState = "disconnected"
	ConnectionStateUnavailable  ConnectionState = "unavailable"
)

// CommandReceiver owns the Feishu long connection used for inbound bot command
// events. The public command contract deals only in configured channel aliases.
type CommandReceiver struct {
	cfg         CommandReceiverConfig
	logger      *slog.Logger
	handler     CommandHandler
	client      *larkws.Client
	chatAliases map[string]string
	botNames    map[string]struct{}
	titleCache  *chatTitleCache

	actionMu      sync.RWMutex
	actionHandler CardActionHandler

	reactionMu      sync.RWMutex
	reactionHandler MessageReactionHandler

	stateMu      sync.RWMutex
	state        ConnectionState
	initialReady chan struct{}
	readyOnce    sync.Once
}

func NewCommandReceiver(cfg CommandReceiverConfig, handler CommandHandler, logger *slog.Logger) *CommandReceiver {
	if logger == nil {
		logger = slog.Default()
	}
	cfg.AppAlias = effectiveAppAlias(cfg.AppAlias)
	r := &CommandReceiver{
		cfg:          cfg,
		logger:       logger,
		handler:      handler,
		chatAliases:  uniqueChatAliases(cfg.Channels),
		botNames:     normalizedNameSet(cfg.BotNames),
		titleCache:   newChatTitleCache(cfg.ChatTitleLookup),
		state:        ConnectionStateDisconnected,
		initialReady: make(chan struct{}),
	}
	if r.titleCache.lookup == nil {
		r.titleCache.lookup = newSDKChatTitleLookup(cfg.AppID, cfg.AppSecret, logger)
	}
	d := dispatcher.NewEventDispatcher("", "").
		OnP2MessageReceiveV1(r.handleMessage).
		OnP2CardActionTrigger(r.handleCardAction).
		OnP2MessageReactionCreatedV1(r.handleReactionCreated).
		OnP2MessageReactionDeletedV1(r.handleReactionDeleted)
	// The dispatcher owns a logger independently from the WebSocket client.
	// Its SDK default logs complete event headers and bodies, so it must use
	// the same argument-discarding logger before it handles any event.
	d.InitConfig(larkevent.WithLogger(safeSDKLogger{logger: logger}))
	r.client = larkws.NewClient(
		cfg.AppID,
		cfg.AppSecret,
		larkws.WithEventHandler(d),
		larkws.WithLogger(safeSDKLogger{logger: logger}),
		larkws.WithOnReady(func() {
			r.setConnectionState(ConnectionStateConnected)
		}),
		larkws.WithOnReconnecting(func() {
			r.setConnectionState(ConnectionStateReconnecting)
		}),
		larkws.WithOnReconnected(func() {
			r.setConnectionState(ConnectionStateConnected)
		}),
		larkws.WithOnDisconnected(func() {
			r.setConnectionState(ConnectionStateDisconnected)
		}),
		larkws.WithOnError(func(error) {
			r.setConnectionState(ConnectionStateUnavailable)
		}),
	)
	return r
}

// SetCardActionHandler installs the daemon-local sink for interactive card
// callbacks. The handler should enqueue work and return quickly; Feishu requires
// callback acknowledgement within three seconds.
func (r *CommandReceiver) SetCardActionHandler(handler CardActionHandler) {
	r.actionMu.Lock()
	defer r.actionMu.Unlock()
	r.actionHandler = handler
}

// SetMessageReactionHandler installs the daemon-local sink for native message
// reactions. Created and deleted events share this one normalized path.
func (r *CommandReceiver) SetMessageReactionHandler(handler MessageReactionHandler) {
	r.reactionMu.Lock()
	defer r.reactionMu.Unlock()
	r.reactionHandler = handler
}

func (r *CommandReceiver) Start(ctx context.Context) error {
	r.setConnectionState(ConnectionStateStarting)
	err := safeWebSocketStartError(r.client.Start(ctx))
	if err != nil {
		r.setConnectionState(ConnectionStateUnavailable)
	}
	return err
}

func (r *CommandReceiver) Close() {
	if r.client != nil {
		r.client.Close()
	}
}

// InitialReady is closed the first time this receiver establishes a long
// connection. It lets process startup wait for every configured app without
// treating later reconnects as a new startup event.
func (r *CommandReceiver) InitialReady() <-chan struct{} {
	return r.initialReady
}

// ConnectionState returns the current fixed-vocabulary connection state.
func (r *CommandReceiver) ConnectionState() ConnectionState {
	r.stateMu.RLock()
	defer r.stateMu.RUnlock()
	return r.state
}

func (r *CommandReceiver) setConnectionState(state ConnectionState) {
	r.stateMu.Lock()
	if r.state == state {
		r.stateMu.Unlock()
		return
	}
	r.state = state
	handler := r.cfg.ConnectionStateChanged
	appAlias := r.cfg.AppAlias
	r.stateMu.Unlock()

	if handler != nil {
		handler(appAlias, state)
	}
	if state == ConnectionStateConnected {
		r.readyOnce.Do(func() {
			close(r.initialReady)
		})
	}
}

func (r *CommandReceiver) handleMessage(ctx context.Context, event *larkim.P2MessageReceiveV1) error {
	cmd, ok := r.CommandFromEvent(event)
	if !ok {
		return nil
	}
	if r.handler == nil {
		return nil
	}
	chatType := cmd.Metadata["chat_type"]
	if chatType == "group" || chatType == "topic_group" {
		title, err := r.titleCache.title(ctx, cmd.ChatID)
		if err != nil {
			r.logger.Warn("chat title lookup failed",
				"operation", "chat_title",
				"error_class", receiverHandlerErrorClass(err),
			)
		} else {
			cmd.ConversationTitle = title
		}
	}
	if err := r.handler(ctx, cmd); err != nil {
		r.logger.Warn("command handler failed",
			"operation", "command",
			"error_class", receiverHandlerErrorClass(err),
		)
	}
	return nil
}

func (r *CommandReceiver) handleReactionCreated(ctx context.Context, event *larkim.P2MessageReactionCreatedV1) error {
	reaction, ok := r.MessageReactionFromCreatedEvent(event)
	if !ok {
		return nil
	}
	return r.dispatchReaction(ctx, reaction)
}

func (r *CommandReceiver) handleReactionDeleted(ctx context.Context, event *larkim.P2MessageReactionDeletedV1) error {
	reaction, ok := r.MessageReactionFromDeletedEvent(event)
	if !ok {
		return nil
	}
	return r.dispatchReaction(ctx, reaction)
}

func (r *CommandReceiver) dispatchReaction(ctx context.Context, reaction InboundMessageReaction) error {
	r.reactionMu.RLock()
	handler := r.reactionHandler
	r.reactionMu.RUnlock()
	if handler == nil {
		return nil
	}
	if err := handler(ctx, reaction); err != nil {
		r.logger.Warn("message reaction handler failed",
			"operation", "message_reaction",
			"error_class", receiverHandlerErrorClass(err),
		)
		return errCommandReceiverUnavailable
	}
	return nil
}

// MessageReactionFromCreatedEvent normalizes a native reaction-created event.
func (r *CommandReceiver) MessageReactionFromCreatedEvent(event *larkim.P2MessageReactionCreatedV1) (InboundMessageReaction, bool) {
	if event == nil || event.Event == nil {
		return InboundMessageReaction{}, false
	}
	return r.messageReaction(event.EventV2Base, event.Event.MessageId, event.Event.ReactionType, event.Event.UserId, MessageReactionAdded)
}

// MessageReactionFromDeletedEvent normalizes a native reaction-deleted event.
func (r *CommandReceiver) MessageReactionFromDeletedEvent(event *larkim.P2MessageReactionDeletedV1) (InboundMessageReaction, bool) {
	if event == nil || event.Event == nil {
		return InboundMessageReaction{}, false
	}
	return r.messageReaction(event.EventV2Base, event.Event.MessageId, event.Event.ReactionType, event.Event.UserId, MessageReactionRemoved)
}

func (r *CommandReceiver) messageReaction(
	base *larkevent.EventV2Base,
	messageID *string,
	emoji *larkim.Emoji,
	userID *larkim.UserId,
	operation MessageReactionOperation,
) (InboundMessageReaction, bool) {
	eventID := ""
	if base != nil && base.Header != nil {
		eventID = strings.TrimSpace(base.Header.EventID)
	}
	reactionType := ""
	if emoji != nil {
		reactionType = strings.TrimSpace(deref(emoji.EmojiType))
	}
	if reactionType != "THUMBSUP" && reactionType != "ThumbsDown" {
		return InboundMessageReaction{}, false
	}
	sender := reactionUserID(userID)
	messageRef := MessageRefForApp(r.cfg.AppAlias, deref(messageID))
	deliveryID := reactionDeliveryIDForApp(r.cfg.AppAlias, eventID)
	if sender == "" || messageRef == "" || deliveryID == "" {
		return InboundMessageReaction{}, false
	}
	return InboundMessageReaction{
		AppAlias:     r.cfg.AppAlias,
		DeliveryID:   deliveryID,
		MessageRef:   messageRef,
		SenderID:     sender,
		ReactionType: reactionType,
		Operation:    operation,
	}, true
}

func reactionUserID(id *larkim.UserId) string {
	if id == nil {
		return ""
	}
	for _, candidate := range []*string{id.OpenId, id.UserId, id.UnionId} {
		if value := strings.TrimSpace(deref(candidate)); value != "" {
			return value
		}
	}
	return ""
}

func reactionDeliveryIDForApp(appAlias, eventID string) string {
	return opaqueDeliveryID(
		"delivery_reaction_",
		"feishu-botd/message-reaction-delivery/v1\x00"+effectiveAppAlias(appAlias)+"\x00",
		eventID,
	)
}

// handleCardAction always acknowledges the Feishu callback. Provider work is
// asynchronous from Feishu's perspective, so handler failures are logged but
// never converted into a callback failure (card callbacks are not retried).
func (r *CommandReceiver) handleCardAction(ctx context.Context, event *larkcallback.CardActionTriggerEvent) (*larkcallback.CardActionTriggerResponse, error) {
	action, ok := r.CardActionFromEvent(event)
	if !ok {
		return nil, nil
	}

	r.actionMu.RLock()
	handler := r.actionHandler
	r.actionMu.RUnlock()
	if handler == nil {
		return cardActionUnavailableResponse(), nil
	}
	if err := handler(ctx, action); err != nil {
		r.logger.Warn("card action handler failed",
			"operation", "card_action",
			"error_class", receiverHandlerErrorClass(err),
		)
		return cardActionUnavailableResponse(), nil
	}
	return nil, nil
}

// receiverHandlerErrorClass intentionally maps arbitrary handler errors to a
// fixed vocabulary. Inbound events contain user-controlled fields and handler
// errors can echo that content, so neither is safe to place in logs.
func receiverHandlerErrorClass(err error) string {
	switch {
	case errors.Is(err, context.Canceled):
		return "context_canceled"
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline_exceeded"
	default:
		return "handler_error"
	}
}

func safeWebSocketStartError(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, context.Canceled):
		return context.Canceled
	case errors.Is(err, context.DeadlineExceeded):
		return context.DeadlineExceeded
	default:
		return errCommandReceiverUnavailable
	}
}

func (r *CommandReceiver) CommandFromEvent(event *larkim.P2MessageReceiveV1) (InboundCommand, bool) {
	if event == nil || event.Event == nil || event.Event.Message == nil {
		return InboundCommand{}, false
	}
	msg := event.Event.Message
	messageType := deref(msg.MessageType)
	if (messageType != "text" && messageType != "post") || msg.Content == nil {
		return InboundCommand{}, false
	}
	chatType := deref(msg.ChatType)
	chatID := deref(msg.ChatId)
	if chatID == "" {
		return InboundCommand{}, false
	}

	chatAlias := ""
	unconfiguredGroup := false
	switch chatType {
	case "p2p":
		chatAlias = "direct"
	case "group", "topic_group":
		var ok bool
		chatAlias, ok = r.chatAliases[chatID]
		if !ok {
			if !r.cfg.AllowUnconfiguredGroupChats {
				return InboundCommand{}, false
			}
			chatAlias = unconfiguredGroupAlias(r.cfg.AppAlias, chatID)
			unconfiguredGroup = true
		}
	default:
		return InboundCommand{}, false
	}

	text := ""
	switch messageType {
	case "text":
		var body struct {
			Text string `json:"text"`
		}
		if err := json.Unmarshal([]byte(deref(msg.Content)), &body); err != nil {
			return InboundCommand{}, false
		}
		text = strings.TrimSpace(body.Text)
	case "post":
		var ok bool
		text, ok = r.postTriggerText(deref(msg.Content), msg.Mentions)
		if !ok {
			return InboundCommand{}, false
		}
	}
	if text == "" {
		return InboundCommand{}, false
	}

	mentionKeys := r.matchingMentionKeys(msg.Mentions)
	unmentionedReply := chatType != "p2p" && len(mentionKeys) == 0 && strings.TrimSpace(deref(msg.ParentId)) != ""
	if chatType != "p2p" && len(mentionKeys) == 0 && !unmentionedReply {
		return InboundCommand{}, false
	}
	for _, key := range mentionKeys {
		if key != "" {
			text = strings.ReplaceAll(text, key, "")
		}
	}
	if len(mentionKeys) > 0 {
		text = r.stripConfiguredBotName(text)
	}
	prompt := strings.TrimSpace(text)
	if prompt == "" {
		return InboundCommand{}, false
	}
	fields := strings.Fields(prompt)
	if len(fields) == 0 {
		return InboundCommand{}, false
	}

	command := strings.TrimLeft(fields[0], "/")
	if command == "" {
		return InboundCommand{}, false
	}
	args := ""
	if len(fields) > 1 {
		args = strings.Join(fields[1:], " ")
	}

	deliveryID := r.deliveryID(event, msg)
	if deliveryID == "" {
		return InboundCommand{}, false
	}
	return InboundCommand{
		AppAlias:          r.cfg.AppAlias,
		DeliveryID:        deliveryID,
		Command:           strings.ToLower(command),
		Text:              args,
		Prompt:            prompt,
		ChatAlias:         chatAlias,
		ConversationID:    conversationIDForApp(r.cfg.AppAlias, chatID, messageConversationKey(msg)),
		ChatID:            chatID,
		UnconfiguredGroup: unconfiguredGroup,
		UnmentionedReply:  unmentionedReply,
		SenderID:          senderID(event.Event.Sender),
		Metadata:          commandMetadataForApp(r.cfg.AppAlias, event, msg, chatType),
	}, true
}

// unconfiguredGroupAlias gives providers a stable correlation value without
// exposing the raw Feishu chat id. It is descriptive only: replies and
// follow-ups use daemon-private routing state captured from ingress.
func unconfiguredGroupAlias(appAlias, chatID string) string {
	chatID = strings.TrimSpace(chatID)
	if chatID == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(
		"feishu-botd/unconfigured-group-alias/v1\x00" +
			effectiveAppAlias(appAlias) + "\x00" + chatID,
	))
	return "unconfigured-group-" + hex.EncodeToString(sum[:])
}

// CardActionFromEvent removes callback-only credentials and raw routing ids
// while retaining the complete typed action/form payload as normalized JSON.
func (r *CommandReceiver) CardActionFromEvent(event *larkcallback.CardActionTriggerEvent) (InboundCardAction, bool) {
	if event == nil || event.Event == nil || event.Event.Action == nil {
		return InboundCardAction{}, false
	}

	deliveryID := ""
	if event.EventV2Base != nil && event.EventV2Base.Header != nil {
		deliveryID = cardActionDeliveryIDForApp(r.cfg.AppAlias, event.EventV2Base.Header.EventID)
	}
	if deliveryID == "" {
		return InboundCardAction{}, false
	}

	valueJSON, ok := normalizedObjectJSON(event.Event.Action.Value)
	if !ok {
		return InboundCardAction{}, false
	}
	formValueJSON, ok := normalizedObjectJSON(event.Event.Action.FormValue)
	if !ok {
		return InboundCardAction{}, false
	}

	messageID := ""
	chatID := ""
	if event.Event.Context != nil {
		messageID = strings.TrimSpace(event.Event.Context.OpenMessageID)
		chatID = strings.TrimSpace(event.Event.Context.OpenChatID)
	}
	action := event.Event.Action
	return InboundCardAction{
		AppAlias:       r.cfg.AppAlias,
		DeliveryID:     deliveryID,
		ConversationID: conversationIDForApp(r.cfg.AppAlias, chatID),
		MessageID:      messageID,
		SenderID:       cardActionSenderID(event.Event.Operator),
		Tag:            strings.TrimSpace(action.Tag),
		Name:           strings.TrimSpace(action.Name),
		Option:         strings.TrimSpace(action.Option),
		Timezone:       strings.TrimSpace(action.Timezone),
		InputValue:     action.InputValue,
		Options:        append([]string(nil), action.Options...),
		Checked:        action.Checked,
		ValueJSON:      valueJSON,
		FormValueJSON:  formValueJSON,
		Host:           strings.TrimSpace(event.Event.Host),
		DeliveryType:   strings.TrimSpace(event.Event.DeliveryType),
	}, true
}

func (r *CommandReceiver) matchingMentionKeys(mentions []*larkim.MentionEvent) []string {
	keys := make([]string, 0, len(mentions))
	for _, mention := range mentions {
		if r.matchesBotMention(mention) {
			keys = append(keys, deref(mention.Key))
		}
	}
	return keys
}

// postTriggerText flattens a rich-text (post) trigger into the prompt. Text
// runs pass through; the bot's own mention is dropped (it addresses, it does
// not ask); other mentions become "@participant" — structured display names
// are provider identities, same rule as the attached-context snapshot; and
// content the daemon cannot carry degrades to an inline placeholder
// ("[image]", "[unsupported video file]") instead of vanishing, so a provider
// knows the message carried something it cannot see and can still answer the
// words around it.
func (r *CommandReceiver) postTriggerText(raw string, mentions []*larkim.MentionEvent) (string, bool) {
	post, ok := localizedAttachedPost(raw)
	if !ok {
		return "", false
	}
	botTokens := r.botMentionTokens(mentions)
	lines := make([]string, 0)
	if title := strings.TrimSpace(post.Title); title != "" {
		lines = append(lines, title)
	}
	for _, row := range post.Content {
		parts := make([]string, 0, len(row))
		for _, element := range row {
			switch strings.TrimSpace(element.Tag) {
			case "img":
				parts = append(parts, placeholderImage)
			case "media":
				parts = append(parts, placeholderVideo)
			case "at":
				if r.isBotPostMention(element, botTokens) {
					continue
				}
				parts = append(parts, "@participant")
			default:
				if element.Text != "" {
					parts = append(parts, element.Text)
				}
			}
		}
		if line := strings.TrimSpace(strings.Join(parts, "")); line != "" {
			lines = append(lines, line)
		}
	}
	return strings.TrimSpace(strings.Join(lines, "\n")), true
}

// botMentionTokens collects every identifier shape a post at-element could
// carry for THIS bot: the event's mention keys and ids for mentions that
// matched the bot, plus the configured strong identities.
func (r *CommandReceiver) botMentionTokens(mentions []*larkim.MentionEvent) map[string]struct{} {
	tokens := make(map[string]struct{})
	add := func(value string) {
		if value = strings.TrimSpace(value); value != "" {
			tokens[value] = struct{}{}
		}
	}
	for _, mention := range mentions {
		if !r.matchesBotMention(mention) {
			continue
		}
		add(deref(mention.Key))
		if mention.Id != nil {
			add(deref(mention.Id.OpenId))
			add(deref(mention.Id.UserId))
			add(deref(mention.Id.UnionId))
		}
	}
	add(r.cfg.BotOpenID)
	add(r.cfg.BotUserID)
	add(r.cfg.BotUnionID)
	return tokens
}

// isBotPostMention says whether one post at-element addresses this bot — by
// any known identity token, or (as a display-only fallback) by configured
// name. A false positive only hides one "@participant" from the prompt; the
// authorization decision stays with matchesBotMention.
func (r *CommandReceiver) isBotPostMention(element postElement, botTokens map[string]struct{}) bool {
	if userID := strings.TrimSpace(element.UserID); userID != "" {
		if _, ok := botTokens[userID]; ok {
			return true
		}
	}
	_, ok := r.botNames[strings.ToLower(strings.TrimSpace(element.UserName))]
	return ok
}

func (r *CommandReceiver) matchesBotMention(mention *larkim.MentionEvent) bool {
	if mention == nil {
		return false
	}
	strongIdentityConfigured := r.cfg.BotOpenID != "" || r.cfg.BotUserID != "" || r.cfg.BotUnionID != ""
	if strongIdentityConfigured {
		if mention.Id == nil {
			return false
		}
		if r.cfg.BotOpenID != "" && deref(mention.Id.OpenId) == r.cfg.BotOpenID {
			return true
		}
		if r.cfg.BotUserID != "" && deref(mention.Id.UserId) == r.cfg.BotUserID {
			return true
		}
		if r.cfg.BotUnionID != "" && deref(mention.Id.UnionId) == r.cfg.BotUnionID {
			return true
		}
		// A configured strong identity is authoritative. Never let a display-name
		// collision override a concrete, mismatching Feishu identity.
		return false
	}
	if len(r.botNames) == 0 {
		return false
	}
	mentionedType := strings.ToLower(strings.TrimSpace(deref(mention.MentionedType)))
	if mentionedType != "bot" {
		return false
	}
	_, ok := r.botNames[strings.ToLower(strings.TrimSpace(deref(mention.Name)))]
	return ok
}

func (r *CommandReceiver) stripConfiguredBotName(text string) string {
	trimmed := strings.TrimSpace(text)
	for name := range r.botNames {
		for _, prefix := range []string{"@" + name, name} {
			if rest, ok := trimCaseInsensitivePrefix(trimmed, prefix); ok {
				return rest
			}
		}
	}
	return trimmed
}

func trimCaseInsensitivePrefix(text, prefix string) (string, bool) {
	textRunes := []rune(text)
	prefixRunes := []rune(prefix)
	if len(textRunes) < len(prefixRunes) {
		return "", false
	}
	if !strings.EqualFold(string(textRunes[:len(prefixRunes)]), prefix) {
		return "", false
	}
	return strings.TrimSpace(string(textRunes[len(prefixRunes):])), true
}

func (r *CommandReceiver) deliveryID(event *larkim.P2MessageReceiveV1, msg *larkim.EventMessage) string {
	if event.EventV2Base != nil && event.EventV2Base.Header != nil {
		if event.EventV2Base.Header.EventID != "" {
			return messageEventDeliveryIDForApp(r.cfg.AppAlias, event.EventV2Base.Header.EventID)
		}
	}
	return messageDeliveryIDForApp(r.cfg.AppAlias, deref(msg.MessageId))
}

// Feishu event ids are retry keys as well as provider-side correlation values.
// Expose stable, domain-separated digests so raw platform identifiers never
// cross the daemon boundary and message/action namespaces cannot collide.
func messageEventDeliveryID(eventID string) string {
	return opaqueDeliveryID("delivery_event_", "feishu-botd/message-event-delivery/v1\x00", eventID)
}

func messageEventDeliveryIDForApp(appAlias, eventID string) string {
	if isDefaultAppAlias(appAlias) {
		return messageEventDeliveryID(eventID)
	}
	return opaqueDeliveryID(
		"delivery_event_",
		"feishu-botd/message-event-delivery/v2\x00"+effectiveAppAlias(appAlias)+"\x00",
		eventID,
	)
}

func cardActionDeliveryID(eventID string) string {
	return opaqueDeliveryID("delivery_action_", "feishu-botd/card-action-delivery/v1\x00", eventID)
}

func cardActionDeliveryIDForApp(appAlias, eventID string) string {
	if isDefaultAppAlias(appAlias) {
		return cardActionDeliveryID(eventID)
	}
	return opaqueDeliveryID(
		"delivery_action_",
		"feishu-botd/card-action-delivery/v2\x00"+effectiveAppAlias(appAlias)+"\x00",
		eventID,
	)
}

// messageDeliveryID provides a stable retry key without exposing Feishu's raw
// message route through the public delivery_id field. The versioned domain
// separator keeps this digest distinct from conversation and future id types.
func messageDeliveryID(messageID string) string {
	return opaqueDeliveryID("delivery_msg_", "feishu-botd/message-delivery/v1\x00", messageID)
}

func messageDeliveryIDForApp(appAlias, messageID string) string {
	if isDefaultAppAlias(appAlias) {
		return messageDeliveryID(messageID)
	}
	return opaqueDeliveryID(
		"delivery_msg_",
		"feishu-botd/message-delivery/v2\x00"+effectiveAppAlias(appAlias)+"\x00",
		messageID,
	)
}

func opaqueDeliveryID(prefix, domain, value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(domain + value))
	return prefix + hex.EncodeToString(sum[:])
}

func commandMetadata(event *larkim.P2MessageReceiveV1, msg *larkim.EventMessage, chatType string) map[string]string {
	metadata := map[string]string{
		"chat_type":    chatType,
		"message_type": deref(msg.MessageType),
	}
	if event.EventV2Base != nil && event.EventV2Base.Header != nil {
		if event.EventV2Base.Header.EventID != "" {
			metadata["event_id"] = event.EventV2Base.Header.EventID
		}
	}
	if messageID := deref(msg.MessageId); messageID != "" {
		metadata["message_id"] = messageID
	}
	if createTime := deref(msg.CreateTime); createTime != "" {
		metadata["create_time"] = createTime
	}
	if threadID := deref(msg.ThreadId); threadID != "" {
		metadata["thread_id"] = threadID
	}
	if rootID := deref(msg.RootId); rootID != "" {
		metadata["root_id"] = rootID
	}
	if parentID := deref(msg.ParentId); parentID != "" {
		metadata["parent_id"] = parentID
	}
	return metadata
}

func commandMetadataForApp(appAlias string, event *larkim.P2MessageReceiveV1, msg *larkim.EventMessage, chatType string) map[string]string {
	metadata := commandMetadata(event, msg, chatType)
	metadata["app_alias"] = effectiveAppAlias(appAlias)
	return metadata
}

func messageConversationKey(msg *larkim.EventMessage) string {
	if msg == nil {
		return ""
	}
	if threadID := strings.TrimSpace(deref(msg.ThreadId)); threadID != "" {
		return threadID
	}
	return strings.TrimSpace(deref(msg.RootId))
}

func conversationID(chatID string, threadKey ...string) string {
	chatID = strings.TrimSpace(chatID)
	if chatID == "" {
		return ""
	}
	thread := ""
	if len(threadKey) > 0 {
		thread = strings.TrimSpace(threadKey[0])
	}
	seed := "feishu-botd/conversation/v1\x00" + chatID
	if thread != "" {
		seed += "\x00" + thread
	}
	sum := sha256.Sum256([]byte(seed))
	return "conv_" + hex.EncodeToString(sum[:])
}

func conversationIDForApp(appAlias, chatID string, threadKey ...string) string {
	if isDefaultAppAlias(appAlias) {
		return conversationID(chatID, threadKey...)
	}
	chatID = strings.TrimSpace(chatID)
	if chatID == "" {
		return ""
	}
	thread := ""
	if len(threadKey) > 0 {
		thread = strings.TrimSpace(threadKey[0])
	}
	seed := "feishu-botd/conversation/v2\x00" + effectiveAppAlias(appAlias) + "\x00" + chatID
	if thread != "" {
		seed += "\x00" + thread
	}
	sum := sha256.Sum256([]byte(seed))
	return "conv_" + hex.EncodeToString(sum[:])
}

func effectiveAppAlias(appAlias string) string {
	appAlias = strings.TrimSpace(appAlias)
	if appAlias == "" {
		return config.DefaultAppAlias
	}
	return appAlias
}

func isDefaultAppAlias(appAlias string) bool {
	return effectiveAppAlias(appAlias) == config.DefaultAppAlias
}

func normalizedObjectJSON(value map[string]interface{}) (string, bool) {
	if value == nil {
		return "{}", true
	}
	body, err := json.Marshal(value)
	if err != nil {
		return "", false
	}
	return string(body), true
}

func cardActionUnavailableResponse() *larkcallback.CardActionTriggerResponse {
	return &larkcallback.CardActionTriggerResponse{
		Toast: &larkcallback.Toast{
			Type:    "error",
			Content: "Agent unavailable. Please retry.",
		},
	}
}

func cardActionSenderID(operator *larkcallback.Operator) string {
	if operator == nil {
		return ""
	}
	if senderID := strings.TrimSpace(operator.OpenID); senderID != "" {
		return senderID
	}
	return deref(operator.UserID)
}

func senderID(sender *larkim.EventSender) string {
	if sender == nil || sender.SenderId == nil {
		return ""
	}
	switch {
	case sender.SenderId.OpenId != nil:
		return *sender.SenderId.OpenId
	case sender.SenderId.UserId != nil:
		return *sender.SenderId.UserId
	case sender.SenderId.UnionId != nil:
		return *sender.SenderId.UnionId
	default:
		return ""
	}
}

func uniqueChatAliases(channels map[string]string) map[string]string {
	aliases := make(map[string]string, len(channels))
	ambiguous := make(map[string]struct{})
	for alias, chatID := range channels {
		alias = strings.TrimSpace(alias)
		chatID = strings.TrimSpace(chatID)
		if alias == "" || chatID == "" {
			continue
		}
		if _, ok := ambiguous[chatID]; ok {
			continue
		}
		if existing, ok := aliases[chatID]; ok && existing != alias {
			delete(aliases, chatID)
			ambiguous[chatID] = struct{}{}
			continue
		}
		aliases[chatID] = alias
	}
	return aliases
}

func normalizedNameSet(names []string) map[string]struct{} {
	out := make(map[string]struct{}, len(names))
	for _, name := range names {
		name = strings.ToLower(strings.TrimSpace(name))
		if name != "" {
			out[name] = struct{}{}
		}
	}
	return out
}

func deref(value *string) string {
	if value == nil {
		return ""
	}
	return strings.TrimSpace(*value)
}
