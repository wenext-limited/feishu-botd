package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"sort"
	"strings"
	"time"

	"feishu-botd/internal/chatgrant"
	"feishu-botd/internal/config"
	"feishu-botd/internal/feishu"
	"feishu-botd/internal/notify"
)

const (
	maxAgentChatNameBytes        = 240
	maxAgentChatDescriptionBytes = 1000
	maxAgentChatMembers          = 50
)

// CreateAgentChatInput is one CreateAgentChat call.
type CreateAgentChatInput struct {
	Provider      string
	OperationID   string
	Name          string
	Description   string
	MemberOpenIDs []string
}

// AgentChatReceipt is what CreateAgentChat and InviteAgentChatMembers return.
type AgentChatReceipt struct {
	ChatAlias      string
	Duplicate      bool
	InvalidOpenIDs []string
}

// InviteAgentChatMembersInput adds members to a chat the provider created.
type InviteAgentChatMembersInput struct {
	Provider      string
	ChatAlias     string
	MemberOpenIDs []string
}

// RenameAgentChatInput renames a chat the provider created.
type RenameAgentChatInput struct {
	Provider  string
	ChatAlias string
	Name      string
}

// CreateAgentChat creates a group chat with the bot as owner and grants it to
// the provider at runtime (ADR 0001). The operation id is kept with the grant,
// so a retry, even after a restart, returns the same chat; a retry also adds
// the members again, so one whose first member call failed heals.
func (s *Service) CreateAgentChat(ctx context.Context, in CreateAgentChatInput) (AgentChatReceipt, *notify.APIError) {
	provider := strings.TrimSpace(in.Provider)
	operationID := strings.TrimSpace(in.OperationID)
	name := strings.TrimSpace(in.Name)
	description := strings.TrimSpace(in.Description)
	switch {
	case provider == "":
		return AgentChatReceipt{}, notify.BadRequest("missing_provider", "provider is required")
	case operationID == "":
		return AgentChatReceipt{}, notify.BadRequest("missing_operation_id", "operation_id is required")
	case name == "":
		return AgentChatReceipt{}, notify.BadRequest("missing_name", "name is required")
	}
	members, apiErr := normalizeOpenIDs(in.MemberOpenIDs)
	if apiErr != nil {
		return AgentChatReceipt{}, apiErr
	}
	if len(provider) > 64 || len(operationID) > 128 || len(name) > maxAgentChatNameBytes ||
		len(description) > maxAgentChatDescriptionBytes {
		return AgentChatReceipt{}, notify.BadRequest("field_too_large", "one or more fields are too large")
	}
	if !s.cfg.ProviderAllowsCreateChats(provider) {
		return AgentChatReceipt{}, providerScopeDenied()
	}
	digest := agentChatDigest(name, description, members)

	s.agentChatMu.Lock()
	defer s.agentChatMu.Unlock()
	if grant, ok := s.cfg.RuntimeChats.ByOperation(provider, operationID); ok {
		if grant.RequestDigest != digest {
			return AgentChatReceipt{}, notify.NewAPIError(409, "operation_conflict", "operation id reused with a different request", false)
		}
		backend, ok := s.backendForApp(grant.AppAlias)
		if !ok || backend.chats == nil {
			return AgentChatReceipt{}, chatsUnavailable()
		}
		invalid, apiErr := s.addAgentChatMembers(ctx, backend, grant, members)
		if apiErr != nil {
			return AgentChatReceipt{}, apiErr
		}
		return AgentChatReceipt{ChatAlias: grant.ChatAlias, Duplicate: true, InvalidOpenIDs: invalid}, nil
	}
	appAlias, ok := s.agentChatApp(provider)
	if !ok {
		return AgentChatReceipt{}, chatsUnavailable()
	}
	backend, ok := s.backendForApp(appAlias)
	if !ok || backend.chats == nil {
		return AgentChatReceipt{}, chatsUnavailable()
	}
	callCtx, cancel := context.WithTimeout(ctx, s.cfg.SendTimeout)
	defer cancel()
	chatID, err := backend.chats.CreateChat(callCtx, feishuChatCreateRequest(name, description, agentChatUUID(provider, operationID)))
	if err != nil {
		s.logFeishuFailure("agent chat create", "operation", operationID, err)
		return AgentChatReceipt{}, chatCallError(err, "Feishu chat creation failed")
	}
	grant := chatgrant.Grant{
		Provider:      provider,
		AppAlias:      appAlias,
		ChatID:        chatID,
		ChatAlias:     chatgrant.AliasFor(appAlias, chatID),
		Name:          name,
		CreatedAt:     time.Now().UTC(),
		OperationID:   operationID,
		RequestDigest: digest,
	}
	if err := s.cfg.RuntimeChats.Add(grant); err != nil {
		s.logger.Error("agent chat grant was not saved; the chat exists without a grant", "error", err)
		return AgentChatReceipt{}, notify.NewAPIError(500, "grant_store_unavailable", "could not persist the chat grant", false)
	}
	s.logger.Info("agent chat created", "provider", provider, "chat_alias", grant.ChatAlias, "app", appAlias)
	invalid, apiErr := s.addAgentChatMembers(ctx, backend, grant, members)
	if apiErr != nil {
		return AgentChatReceipt{}, apiErr
	}
	return AgentChatReceipt{ChatAlias: grant.ChatAlias, InvalidOpenIDs: invalid}, nil
}

// InviteAgentChatMembers adds members to a chat the provider created.
func (s *Service) InviteAgentChatMembers(ctx context.Context, in InviteAgentChatMembersInput) (AgentChatReceipt, *notify.APIError) {
	grant, backend, apiErr := s.ownedAgentChat(in.Provider, in.ChatAlias)
	if apiErr != nil {
		return AgentChatReceipt{}, apiErr
	}
	members, apiErr := normalizeOpenIDs(in.MemberOpenIDs)
	if apiErr != nil {
		return AgentChatReceipt{}, apiErr
	}
	invalid, apiErr := s.addAgentChatMembers(ctx, backend, grant, members)
	if apiErr != nil {
		return AgentChatReceipt{}, apiErr
	}
	return AgentChatReceipt{ChatAlias: grant.ChatAlias, InvalidOpenIDs: invalid}, nil
}

// RenameAgentChat renames a chat the provider created.
func (s *Service) RenameAgentChat(ctx context.Context, in RenameAgentChatInput) *notify.APIError {
	name := strings.TrimSpace(in.Name)
	if name == "" {
		return notify.BadRequest("missing_name", "name is required")
	}
	if len(name) > maxAgentChatNameBytes {
		return notify.BadRequest("field_too_large", "one or more fields are too large")
	}
	grant, backend, apiErr := s.ownedAgentChat(in.Provider, in.ChatAlias)
	if apiErr != nil {
		return apiErr
	}
	callCtx, cancel := context.WithTimeout(ctx, s.cfg.SendTimeout)
	defer cancel()
	if err := backend.chats.RenameChat(callCtx, grant.ChatID, name); err != nil {
		s.logFeishuFailure("agent chat rename", "chat", grant.ChatAlias, err)
		return chatCallError(err, "Feishu chat rename failed")
	}
	if err := s.cfg.RuntimeChats.Rename(grant.ChatAlias, name); err != nil {
		s.logger.Warn("agent chat name was not saved", "chat_alias", grant.ChatAlias, "error", err)
	}
	return nil
}

// ownedAgentChat resolves a chat the provider created. Any other alias,
// including another provider's chat, is unknown_chat.
func (s *Service) ownedAgentChat(rawProvider, rawAlias string) (chatgrant.Grant, appBackend, *notify.APIError) {
	provider := strings.TrimSpace(rawProvider)
	alias := strings.TrimSpace(rawAlias)
	switch {
	case provider == "":
		return chatgrant.Grant{}, appBackend{}, notify.BadRequest("missing_provider", "provider is required")
	case alias == "":
		return chatgrant.Grant{}, appBackend{}, notify.BadRequest("missing_chat_alias", "chat_alias is required")
	}
	if !s.cfg.ProviderAllowsCreateChats(provider) {
		return chatgrant.Grant{}, appBackend{}, providerScopeDenied()
	}
	grant, ok := s.cfg.RuntimeChats.ByAlias(alias)
	if !ok || grant.Provider != provider || !s.appAllowed(provider, grant.AppAlias) {
		return chatgrant.Grant{}, appBackend{}, notify.NewAPIError(404, "unknown_chat", "unknown chat", false)
	}
	backend, ok := s.backendForApp(grant.AppAlias)
	if !ok || backend.chats == nil {
		return chatgrant.Grant{}, appBackend{}, chatsUnavailable()
	}
	return grant, backend, nil
}

func (s *Service) addAgentChatMembers(ctx context.Context, backend appBackend, grant chatgrant.Grant, members []string) ([]string, *notify.APIError) {
	if len(members) == 0 {
		return nil, nil
	}
	callCtx, cancel := context.WithTimeout(ctx, s.cfg.SendTimeout)
	defer cancel()
	invalid, err := backend.chats.AddChatMembers(callCtx, grant.ChatID, members)
	if err != nil {
		s.logFeishuFailure("agent chat invite", "chat", grant.ChatAlias, err)
		return nil, chatCallError(err, "Feishu chat member add failed")
	}
	return invalid, nil
}

// agentChatApp picks the app a provider's chats are created in: the only app
// it may access, or the default app when it may access several.
func (s *Service) agentChatApp(provider string) (string, bool) {
	var allowed []string
	for _, alias := range s.cfg.AppAliases() {
		if s.appAllowed(provider, alias) {
			if _, ok := s.backendForApp(alias); ok {
				allowed = append(allowed, alias)
			}
		}
	}
	switch {
	case len(allowed) == 1:
		return allowed[0], true
	case len(allowed) > 1:
		for _, alias := range allowed {
			if alias == config.DefaultAppAlias {
				return alias, true
			}
		}
	}
	return "", false
}

func normalizeOpenIDs(in []string) ([]string, *notify.APIError) {
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, raw := range in {
		id := strings.TrimSpace(raw)
		if id == "" {
			continue
		}
		if !strings.HasPrefix(id, "ou_") || len(id) > 128 {
			return nil, notify.BadRequest("invalid_member", "member_open_ids must be Feishu open_ids (ou_...)")
		}
		if _, dup := seen[id]; dup {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	if len(out) > maxAgentChatMembers {
		return nil, notify.BadRequest("field_too_large", "too many members")
	}
	sort.Strings(out)
	return out, nil
}

func agentChatDigest(name, description string, members []string) string {
	hash := sha256.New()
	_, _ = hash.Write([]byte("feishu-botd/agent-chat-request/v1\x00" + name + "\x00" + description))
	for _, id := range members {
		_, _ = hash.Write([]byte("\x00" + id))
	}
	return hex.EncodeToString(hash.Sum(nil))
}

// agentChatUUID is Feishu's own create idempotency key for the operation, so
// an ambiguous first attempt retried within Feishu's window finds its chat.
func agentChatUUID(provider, operationID string) string {
	sum := sha256.Sum256([]byte("feishu-botd/agent-chat-uuid/v1\x00" + provider + "\x00" + operationID))
	return hex.EncodeToString(sum[:16])
}

func providerScopeDenied() *notify.APIError {
	return notify.NewAPIError(403, "provider_scope_denied", "requested provider capability is not allowed", false)
}

func chatsUnavailable() *notify.APIError {
	return notify.NotImplemented("agent_chats_unavailable", "chat creation is unavailable for this sender")
}

// chatCallError separates a missing Feishu permission (the app lacks the
// im:chat scopes, or bot ability is off), which an operator must fix, from
// ordinary failures.
func chatCallError(err error, message string) *notify.APIError {
	if sendErr, answered := answeredMessageFailure(err); answered {
		switch sendErr.Code {
		case 99991672, 99991679, 232025, 232043:
			return notify.NewAPIError(412, "chat_permission_denied", "Feishu refused for missing permission", false)
		}
	}
	return agentMessageCallError(err, message)
}

func feishuChatCreateRequest(name, description, uuid string) feishu.ChatCreateRequest {
	return feishu.ChatCreateRequest{Name: name, Description: description, UUID: uuid}
}
