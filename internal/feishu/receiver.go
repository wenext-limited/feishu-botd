package feishu

import (
	"context"
	"encoding/json"
	"log/slog"
	"strings"

	"github.com/larksuite/oapi-sdk-go/v3/event/dispatcher"
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
	DeliveryID string
	Command    string
	Text       string
	ChatAlias  string
	SenderID   string
	Metadata   map[string]string
}

type CommandHandler func(context.Context, InboundCommand) error

// CommandReceiver owns the Feishu long connection used for inbound bot command
// events. The public command contract deals only in configured channel aliases.
type CommandReceiver struct {
	cfg         CommandReceiverConfig
	logger      *slog.Logger
	handler     CommandHandler
	client      *larkws.Client
	chatAliases map[string]string
	botNames    map[string]struct{}
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
	d := dispatcher.NewEventDispatcher("", "").OnP2MessageReceiveV1(r.handleMessage)
	r.client = larkws.NewClient(cfg.AppID, cfg.AppSecret, larkws.WithEventHandler(d))
	return r
}

func (r *CommandReceiver) Start(ctx context.Context) error {
	return r.client.Start(ctx)
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
			"delivery", cmd.DeliveryID,
			"command", cmd.Command,
			"channel", cmd.ChatAlias,
			"error", err,
		)
	}
	return nil
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
	if chatType != "group" && chatType != "topic_group" {
		return InboundCommand{}, false
	}
	chatAlias, ok := r.chatAliases[deref(msg.ChatId)]
	if !ok {
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
	if len(mentionKeys) == 0 {
		return InboundCommand{}, false
	}
	for _, key := range mentionKeys {
		if key != "" {
			text = strings.ReplaceAll(text, key, "")
		}
	}
	text = strings.TrimSpace(r.stripConfiguredBotName(text))
	fields := strings.Fields(text)
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
		DeliveryID: deliveryID,
		Command:    strings.ToLower(command),
		Text:       args,
		ChatAlias:  chatAlias,
		SenderID:   senderID(event.Event.Sender),
		Metadata:   commandMetadata(event, msg, chatType),
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
	if mention.Id != nil {
		if r.cfg.BotOpenID != "" && deref(mention.Id.OpenId) == r.cfg.BotOpenID {
			return true
		}
		if r.cfg.BotUserID != "" && deref(mention.Id.UserId) == r.cfg.BotUserID {
			return true
		}
		if r.cfg.BotUnionID != "" && deref(mention.Id.UnionId) == r.cfg.BotUnionID {
			return true
		}
	}
	_, ok := r.botNames[strings.ToLower(strings.TrimSpace(deref(mention.Name)))]
	if ok {
		return true
	}
	if r.hasBotIdentityHints() {
		return false
	}
	mentionedType := strings.ToLower(strings.TrimSpace(deref(mention.MentionedType)))
	return mentionedType == "app" || mentionedType == "bot"
}

func (r *CommandReceiver) hasBotIdentityHints() bool {
	return r.cfg.BotOpenID != "" ||
		r.cfg.BotUserID != "" ||
		r.cfg.BotUnionID != "" ||
		len(r.botNames) > 0
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
			return event.EventV2Base.Header.EventID
		}
	}
	return deref(msg.MessageId)
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
