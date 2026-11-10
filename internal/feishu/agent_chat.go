package feishu

import (
	"context"
	"strings"

	larkcore "github.com/larksuite/oapi-sdk-go/v3/core"
	larkim "github.com/larksuite/oapi-sdk-go/v3/service/im/v1"
)

// AgentChats creates and manages group chats on a provider's behalf
// (CreateAgentChat). The bot is the owner of every chat it creates.
//
// Scopes: creating needs im:chat or im:chat:create; adding members needs
// im:chat or im:chat.members:write_only; renaming needs im:chat or
// im:chat:update.
type AgentChats interface {
	// CreateChat creates an empty group chat with the bot as owner and
	// returns its chat_id. uuid makes a retry within Feishu's window return
	// the same chat instead of a second one.
	CreateChat(ctx context.Context, req ChatCreateRequest) (string, error)
	// AddChatMembers adds open_ids to chatID, skipping ids Feishu cannot add
	// and reporting them. Already-members are not an error.
	AddChatMembers(ctx context.Context, chatID string, openIDs []string) ([]string, error)
	// RenameChat sets chatID's name.
	RenameChat(ctx context.Context, chatID, name string) error
}

// ChatCreateRequest is one group chat to create.
type ChatCreateRequest struct {
	Name        string
	Description string
	UUID        string
}

type chatAPI interface {
	Create(context.Context, *larkim.CreateChatReq, ...larkcore.RequestOptionFunc) (*larkim.CreateChatResp, error)
	Update(context.Context, *larkim.UpdateChatReq, ...larkcore.RequestOptionFunc) (*larkim.UpdateChatResp, error)
}

type chatMembersAPI interface {
	Create(context.Context, *larkim.CreateChatMembersReq, ...larkcore.RequestOptionFunc) (*larkim.CreateChatMembersResp, error)
}

// CreateChat calls POST /open-apis/im/v1/chats with set_bot_manager=true and
// no members; members are added separately so one id Feishu rejects cannot
// fail the whole creation.
func (s *ChannelSender) CreateChat(ctx context.Context, in ChatCreateRequest) (string, error) {
	if s.chatAPI == nil {
		return "", &MessageSendError{Operation: "chat_create", Class: "not_configured"}
	}
	name := strings.TrimSpace(in.Name)
	if name == "" {
		return "", &MessageSendError{Operation: "chat_create", Class: "invalid_request"}
	}
	body := larkim.NewCreateChatReqBodyBuilder().
		Name(name).
		ChatMode("group").
		ChatType("private")
	if description := strings.TrimSpace(in.Description); description != "" {
		body.Description(description)
	}
	builder := larkim.NewCreateChatReqBuilder().
		UserIdType("open_id").
		SetBotManager(true).
		Body(body.Build())
	if uuid := strings.TrimSpace(in.UUID); uuid != "" {
		builder.Uuid(uuid)
	}
	resp, err := s.chatAPI.Create(ctx, builder.Build())
	if err != nil {
		return "", messageTransportError("chat_create", err)
	}
	if resp == nil {
		return "", &MessageSendError{Operation: "chat_create", Class: "invalid_response"}
	}
	if !resp.Success() {
		return "", messageResponseError("chat_create", resp.ApiResp, resp.Code)
	}
	if resp.Data == nil || strings.TrimSpace(deref(resp.Data.ChatId)) == "" {
		return "", &MessageSendError{Operation: "chat_create", Class: "invalid_response"}
	}
	return strings.TrimSpace(deref(resp.Data.ChatId)), nil
}

// AddChatMembers calls POST /open-apis/im/v1/chats/:chat_id/members with
// member_id_type=open_id and succeed_type=1 (add the valid ids, report the
// rest).
func (s *ChannelSender) AddChatMembers(ctx context.Context, chatID string, openIDs []string) ([]string, error) {
	if s.chatMembersAPI == nil {
		return nil, &MessageSendError{Operation: "chat_members_add", Class: "not_configured"}
	}
	chatID = strings.TrimSpace(chatID)
	if chatID == "" {
		return nil, &MessageSendError{Operation: "chat_members_add", Class: "invalid_destination"}
	}
	ids := make([]string, 0, len(openIDs))
	for _, id := range openIDs {
		if id = strings.TrimSpace(id); id != "" {
			ids = append(ids, id)
		}
	}
	if len(ids) == 0 {
		return nil, nil
	}
	req := larkim.NewCreateChatMembersReqBuilder().
		ChatId(chatID).
		MemberIdType("open_id").
		SucceedType(1).
		Body(larkim.NewCreateChatMembersReqBodyBuilder().IdList(ids).Build()).
		Build()
	resp, err := s.chatMembersAPI.Create(ctx, req)
	if err != nil {
		return nil, messageTransportError("chat_members_add", err)
	}
	if resp == nil {
		return nil, &MessageSendError{Operation: "chat_members_add", Class: "invalid_response"}
	}
	if !resp.Success() {
		return nil, messageResponseError("chat_members_add", resp.ApiResp, resp.Code)
	}
	var invalid []string
	if resp.Data != nil {
		invalid = append(invalid, resp.Data.InvalidIdList...)
		invalid = append(invalid, resp.Data.NotExistedIdList...)
	}
	return invalid, nil
}

// RenameChat calls PUT /open-apis/im/v1/chats/:chat_id with the new name.
func (s *ChannelSender) RenameChat(ctx context.Context, chatID, name string) error {
	if s.chatAPI == nil {
		return &MessageSendError{Operation: "chat_rename", Class: "not_configured"}
	}
	chatID = strings.TrimSpace(chatID)
	name = strings.TrimSpace(name)
	if chatID == "" || name == "" {
		return &MessageSendError{Operation: "chat_rename", Class: "invalid_request"}
	}
	req := larkim.NewUpdateChatReqBuilder().
		ChatId(chatID).
		Body(larkim.NewUpdateChatReqBodyBuilder().Name(name).Build()).
		Build()
	resp, err := s.chatAPI.Update(ctx, req)
	if err != nil {
		return messageTransportError("chat_rename", err)
	}
	if resp == nil {
		return &MessageSendError{Operation: "chat_rename", Class: "invalid_response"}
	}
	if !resp.Success() {
		return messageResponseError("chat_rename", resp.ApiResp, resp.Code)
	}
	return nil
}

var _ AgentChats = (*ChannelSender)(nil)
