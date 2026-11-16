package service

import (
	"context"
	"strings"
	"time"

	"feishu-botd/internal/chatgrant"
	"feishu-botd/internal/config"
	"feishu-botd/internal/feishu"
	"feishu-botd/internal/messageroute"
	"feishu-botd/internal/notify"
)

// AdoptAgentChatInput names a group by a message delivered to this provider.
type AdoptAgentChatInput struct {
	Provider   string
	MessageRef string
}

// AdoptAgentChatReceipt names the current static alias, or the delivered alias
// when the chat has no static route.
type AdoptAgentChatReceipt struct {
	ChatAlias      string
	AlreadyGranted bool
}

// ReleaseAgentChatInput removes this provider's grant, of either kind.
type ReleaseAgentChatInput struct {
	Provider  string
	ChatAlias string
}

// AdoptAgentChat grants a previously delivered group without exposing raw ids.
// Only group ingress retains unauthored message routes. The ownership store
// describes outbound messages and cannot prove that an inbound ref was received.
func (s *Service) AdoptAgentChat(ctx context.Context, in AdoptAgentChatInput) (AdoptAgentChatReceipt, *notify.APIError) {
	provider, ref := strings.TrimSpace(in.Provider), strings.TrimSpace(in.MessageRef)
	switch {
	case provider == "":
		return AdoptAgentChatReceipt{}, notify.BadRequest("missing_provider", "provider is required")
	case ref == "":
		return AdoptAgentChatReceipt{}, notify.BadRequest("missing_message_ref", "message_ref is required")
	}
	if len(provider) > 64 || len(ref) > 160 {
		return AdoptAgentChatReceipt{}, notify.BadRequest("field_too_large", "one or more fields are too large")
	}
	if !s.cfg().ProviderAllowsAdoptChats(provider) {
		return AdoptAgentChatReceipt{}, providerScopeDenied()
	}

	s.agentChatMu.Lock()
	defer s.agentChatMu.Unlock()
	route, ok := s.deliveredGroupRoute(provider, ref)
	if !ok {
		return AdoptAgentChatReceipt{}, unknownChatError()
	}
	cfg := s.Config()
	// An alias remapped since delivery must never grant the old chat under its
	// new destination. ResolveChannel includes both static and runtime aliases.
	if app, chatID, resolved := cfg.ResolveChannel(route.ChatAlias); resolved && (app != route.AppAlias || chatID != route.ChatID) {
		return AdoptAgentChatReceipt{}, unknownChatError()
	}
	receipt := AdoptAgentChatReceipt{ChatAlias: route.ChatAlias}
	// Static chats are an idempotent no-op; adoption never widens their ACLs.
	static := cfg
	static.RuntimeChats = nil
	if alias := staticAliasForChat(static, route); alias != "" {
		if alias != route.ChatAlias {
			s.logger.Warn("agent chat adoption uses a static alias added since delivery",
				"provider", provider, "chat_alias", alias, "delivered_chat_alias", route.ChatAlias)
		}
		ambient := static.ProviderAllowsAmbientChat(provider, alias)
		proactive := static.ProviderAllowsProactiveChat(provider, alias)
		if !ambient || !proactive {
			s.logger.Warn("agent chat adoption is a static no-op with missing provider permissions",
				"provider", provider, "chat_alias", alias, "ambient_allowed", ambient, "proactive_allowed", proactive)
		}
		receipt.ChatAlias = alias
		receipt.AlreadyGranted = true
		return receipt, nil
	}
	if cfg.RuntimeChats.Granted(provider, route.ChatAlias) {
		receipt.AlreadyGranted = true
		return receipt, nil
	}
	backend, ok := s.backendForApp(route.AppAlias)
	if !ok {
		return AdoptAgentChatReceipt{}, unknownChatError()
	}
	membership, ok := backend.sender.(feishu.ChatMembership)
	if !ok {
		return AdoptAgentChatReceipt{}, notify.NotImplemented("agent_chats_unavailable", "chat membership checks are unavailable for this sender")
	}
	callCtx, cancel := context.WithTimeout(ctx, cfg.SendTimeout)
	defer cancel()
	member, err := membership.IsInChat(callCtx, route.ChatID)
	if err != nil {
		s.logFeishuFailure("agent chat membership", "chat", route.ChatAlias, err)
		return AdoptAgentChatReceipt{}, chatCallError(err, "Feishu chat membership check failed")
	}
	if !member {
		return AdoptAgentChatReceipt{}, unknownChatError()
	}
	if err := cfg.RuntimeChats.Add(chatgrant.Grant{
		Kind: chatgrant.Adopted, Provider: provider, AppAlias: route.AppAlias,
		ChatID: route.ChatID, ChatAlias: route.ChatAlias, CreatedAt: time.Now().UTC(),
	}); err != nil {
		s.logger.Error("agent chat adoption grant was not saved", "error", err)
		return AdoptAgentChatReceipt{}, grantStoreUnavailable()
	}
	s.logger.Info("agent chat adopted", "provider", provider, "chat_alias", route.ChatAlias, "app", route.AppAlias)
	return receipt, nil
}

// staticAliasForChat prefers the delivered alias when it is still static, then
// the first static alias in lexical order for the same app/chat pair. ResolveChannel
// keeps the legacy Channels fallback and canonical ChannelRoutes rules intact.
func staticAliasForChat(static config.Config, route messageroute.Route) string {
	matches := func(alias string) bool {
		app, chatID, ok := static.ResolveChannel(alias)
		return ok && app == route.AppAlias && chatID == route.ChatID
	}
	if matches(route.ChatAlias) {
		return route.ChatAlias
	}
	aliases := make([]string, 0, len(static.ChannelRoutes)+len(static.Channels))
	for alias := range static.ChannelRoutes {
		aliases = append(aliases, alias)
	}
	if static.ChannelRoutes == nil {
		for alias := range static.Channels {
			aliases = append(aliases, alias)
		}
	}
	var found string
	for _, alias := range aliases {
		if matches(alias) && (found == "" || alias < found) {
			found = alias
		}
	}
	return found
}

// deliveredGroupRoute waits for dispatch to finalize its recipient set before
// reading it. This also makes an immediate adopt from a stream consumer safe.
func (s *Service) deliveredGroupRoute(provider, ref string) (messageroute.Route, bool) {
	route, ok := s.lookupMessageRoute(ref, time.Now())
	if !ok || route.Authored || route.ChatID == "" || route.MessageID == "" ||
		!route.Allows(provider) || !s.appAllowed(provider, route.AppAlias) ||
		feishu.MessageRefForApp(route.AppAlias, route.MessageID) != ref {
		return messageroute.Route{}, false
	}
	return route, true
}

// ReleaseAgentChat durably removes only this provider's runtime access. Static
// config, the Feishu chat, its members and messages are untouched.
func (s *Service) ReleaseAgentChat(_ context.Context, in ReleaseAgentChatInput) (bool, *notify.APIError) {
	provider, alias := strings.TrimSpace(in.Provider), strings.TrimSpace(in.ChatAlias)
	switch {
	case provider == "":
		return false, notify.BadRequest("missing_provider", "provider is required")
	case alias == "":
		return false, notify.BadRequest("missing_chat_alias", "chat_alias is required")
	}
	if len(provider) > 64 || len(alias) > 160 {
		return false, notify.BadRequest("field_too_large", "one or more fields are too large")
	}
	if !s.cfg().ProviderAllowsAdoptChats(provider) {
		return false, providerScopeDenied()
	}
	s.agentChatMu.Lock()
	defer s.agentChatMu.Unlock()
	released, err := s.cfg().RuntimeChats.Remove(provider, alias)
	if err != nil {
		s.logger.Error("agent chat release was not saved", "error", err)
		return false, grantStoreUnavailable()
	}
	return released, nil
}

func unknownChatError() *notify.APIError {
	return notify.NewAPIError(404, "unknown_chat", "unknown chat", false)
}
func grantStoreUnavailable() *notify.APIError {
	return notify.NewAPIError(500, "grant_store_unavailable", "could not persist the chat grant", false)
}
