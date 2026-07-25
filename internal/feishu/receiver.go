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
)

type CommandReceiverConfig struct {
	AppID     string
	AppSecret string
	Channels  map[string]string

	BotOpenID  string
	BotUserID  string
	BotUnionID string
	BotNames   []string
}

type InboundCommand struct {
	DeliveryID     string
	Command        string
	Text           string
	Prompt         string
	ChatAlias      string
	ConversationID string
	SenderID       string
	Metadata       map[string]string

	// ChatID is the raw Feishu reply route. It is daemon-private and must never
	// cross the public command/provider boundary.
	ChatID string `json:"-"`
}

type CommandHandler func(context.Context, InboundCommand) error

// InboundCardAction is the transport-neutral subset of a Feishu interactive
// card callback. Callback credentials and raw chat ids deliberately do not
// appear here. ValueJSON and FormValueJSON preserve JSON types such as booleans,
// arrays, and nested objects for downstream agent providers.
type InboundCardAction struct {
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

var errCommandReceiverUnavailable = errors.New("feishu command receiver unavailable")

// CommandReceiver owns the Feishu long connection used for inbound bot command
// events. The public command contract deals only in configured channel aliases.
type CommandReceiver struct {
	cfg         CommandReceiverConfig
	logger      *slog.Logger
	handler     CommandHandler
	client      *larkws.Client
	chatAliases map[string]string
	botNames    map[string]struct{}

	actionMu      sync.RWMutex
	actionHandler CardActionHandler
}

func NewCommandReceiver(cfg CommandReceiverConfig, handler CommandHandler, logger *slog.Logger) *CommandReceiver {
	if logger == nil {
		logger = slog.Default()
	}
	r := &CommandReceiver{
		cfg:         cfg,
		logger:      logger,
		handler:     handler,
		chatAliases: uniqueChatAliases(cfg.Channels),
		botNames:    normalizedNameSet(cfg.BotNames),
	}
	d := dispatcher.NewEventDispatcher("", "").
		OnP2MessageReceiveV1(r.handleMessage).
		OnP2CardActionTrigger(r.handleCardAction)
	// The dispatcher owns a logger independently from the WebSocket client.
	// Its SDK default logs complete event headers and bodies, so it must use
	// the same argument-discarding logger before it handles any event.
	d.InitConfig(larkevent.WithLogger(safeSDKLogger{logger: logger}))
	r.client = larkws.NewClient(
		cfg.AppID,
		cfg.AppSecret,
		larkws.WithEventHandler(d),
		larkws.WithLogger(safeSDKLogger{logger: logger}),
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

func (r *CommandReceiver) Start(ctx context.Context) error {
	return safeWebSocketStartError(r.client.Start(ctx))
}

func (r *CommandReceiver) Close() {
	if r.client != nil {
		r.client.Close()
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
	if err := r.handler(ctx, cmd); err != nil {
		r.logger.Warn("command handler failed",
			"operation", "command",
			"error_class", receiverHandlerErrorClass(err),
		)
	}
	return nil
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
	if deref(msg.MessageType) != "text" || msg.Content == nil {
		return InboundCommand{}, false
	}
	chatType := deref(msg.ChatType)
	chatID := deref(msg.ChatId)
	if chatID == "" {
		return InboundCommand{}, false
	}

	chatAlias := ""
	switch chatType {
	case "p2p":
		chatAlias = "direct"
	case "group", "topic_group":
		var ok bool
		chatAlias, ok = r.chatAliases[chatID]
		if !ok {
			return InboundCommand{}, false
		}
	default:
		return InboundCommand{}, false
	}

	var body struct {
		Text string `json:"text"`
	}
	if err := json.Unmarshal([]byte(deref(msg.Content)), &body); err != nil {
		return InboundCommand{}, false
	}
	text := strings.TrimSpace(body.Text)
	if text == "" {
		return InboundCommand{}, false
	}

	mentionKeys := r.matchingMentionKeys(msg.Mentions)
	if chatType != "p2p" && len(mentionKeys) == 0 {
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
		DeliveryID:     deliveryID,
		Command:        strings.ToLower(command),
		Text:           args,
		Prompt:         prompt,
		ChatAlias:      chatAlias,
		ConversationID: conversationID(chatID, messageConversationKey(msg)),
		ChatID:         chatID,
		SenderID:       senderID(event.Event.Sender),
		Metadata:       commandMetadata(event, msg, chatType),
	}, true
}

// CardActionFromEvent removes callback-only credentials and raw routing ids
// while retaining the complete typed action/form payload as normalized JSON.
func (r *CommandReceiver) CardActionFromEvent(event *larkcallback.CardActionTriggerEvent) (InboundCardAction, bool) {
	if event == nil || event.Event == nil || event.Event.Action == nil {
		return InboundCardAction{}, false
	}

	deliveryID := ""
	if event.EventV2Base != nil && event.EventV2Base.Header != nil {
		deliveryID = cardActionDeliveryID(event.EventV2Base.Header.EventID)
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
		DeliveryID:     deliveryID,
		ConversationID: conversationID(chatID),
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
			return messageEventDeliveryID(event.EventV2Base.Header.EventID)
		}
	}
	return messageDeliveryID(deref(msg.MessageId))
}

// Feishu event ids are retry keys as well as provider-side correlation values.
// Expose stable, domain-separated digests so raw platform identifiers never
// cross the daemon boundary and message/action namespaces cannot collide.
func messageEventDeliveryID(eventID string) string {
	return opaqueDeliveryID("delivery_event_", "feishu-botd/message-event-delivery/v1\x00", eventID)
}

func cardActionDeliveryID(eventID string) string {
	return opaqueDeliveryID("delivery_action_", "feishu-botd/card-action-delivery/v1\x00", eventID)
}

// messageDeliveryID provides a stable retry key without exposing Feishu's raw
// message route through the public delivery_id field. The versioned domain
// separator keeps this digest distinct from conversation and future id types.
func messageDeliveryID(messageID string) string {
	return opaqueDeliveryID("delivery_msg_", "feishu-botd/message-delivery/v1\x00", messageID)
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
