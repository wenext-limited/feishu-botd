package feishu

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"slices"
	"testing"

	lark "github.com/larksuite/oapi-sdk-go/v3"
)

func agentChatSender(httpClient *agentCardHTTPClient) *ChannelSender {
	client := lark.NewClient("cli_test", "secret", lark.WithHttpClient(httpClient))
	return &ChannelSender{chatAPI: client.Im.V1.Chat, chatMembersAPI: client.Im.V1.ChatMembers}
}

func TestAgentChatsSDKWireRequests(t *testing.T) {
	httpClient := &agentCardHTTPClient{responses: map[string]string{
		"/open-apis/im/v1/chats":                `{"code":0,"msg":"success","data":{"chat_id":"oc_new"}}`,
		"/open-apis/im/v1/chats/oc_new/members": `{"code":0,"msg":"success","data":{"invalid_id_list":["ou_gone"],"not_existed_id_list":["ou_typo"]}}`,
	}}
	sender := agentChatSender(httpClient)
	ctx := context.Background()

	chatID, err := sender.CreateChat(ctx, ChatCreateRequest{Name: "[Agents] refund-v2", Description: "agents", UUID: "uuid-chat"})
	if err != nil || chatID != "oc_new" {
		t.Fatalf("create = %q err=%v", chatID, err)
	}
	invalid, err := sender.AddChatMembers(ctx, "oc_new", []string{"ou_a", " ", "ou_gone", "ou_typo"})
	if err != nil || !slices.Equal(invalid, []string{"ou_gone", "ou_typo"}) {
		t.Fatalf("add members = %v err=%v", invalid, err)
	}
	if err := sender.RenameChat(ctx, "oc_new", "[Archived] refund-v2"); err != nil {
		t.Fatalf("rename: %v", err)
	}
	if invalid, err := sender.AddChatMembers(ctx, "oc_new", nil); err != nil || invalid != nil {
		t.Fatalf("empty add = %v err=%v", invalid, err)
	}

	requests := nonTokenRequests(httpClient.requests)
	if len(requests) != 3 {
		t.Fatalf("API request count = %d, want 3; requests = %#v", len(requests), requests)
	}
	create := requests[0]
	query, _ := url.ParseQuery(create.RawQuery)
	if create.Method != http.MethodPost || create.Path != "/open-apis/im/v1/chats" ||
		query.Get("set_bot_manager") != "true" || query.Get("user_id_type") != "open_id" || query.Get("uuid") != "uuid-chat" {
		t.Fatalf("create request = %s %s?%s", create.Method, create.Path, create.RawQuery)
	}
	assertWireRequest(t, create, http.MethodPost, "/open-apis/im/v1/chats", create.RawQuery, map[string]any{
		"name":        "[Agents] refund-v2",
		"description": "agents",
		"chat_mode":   "group",
		"chat_type":   "private",
	})
	members := requests[1]
	query, _ = url.ParseQuery(members.RawQuery)
	if members.Method != http.MethodPost || members.Path != "/open-apis/im/v1/chats/oc_new/members" ||
		query.Get("member_id_type") != "open_id" || query.Get("succeed_type") != "1" {
		t.Fatalf("members request = %s %s?%s", members.Method, members.Path, members.RawQuery)
	}
	if string(members.Body) != `{"id_list":["ou_a","ou_gone","ou_typo"]}` {
		t.Fatalf("members body = %s", members.Body)
	}
	assertWireRequest(t, requests[2], http.MethodPut, "/open-apis/im/v1/chats/oc_new", "", map[string]any{
		"name": "[Archived] refund-v2",
	})
}

func TestAgentChatsReportFeishuRejections(t *testing.T) {
	httpClient := &agentCardHTTPClient{responses: map[string]string{
		"/open-apis/im/v1/chats":      `{"code":232025,"msg":"bot ability not enabled"}`,
		"/open-apis/im/v1/chats/oc_x": `{"code":99991672,"msg":"no permission"}`,
	}}
	sender := agentChatSender(httpClient)
	_, err := sender.CreateChat(context.Background(), ChatCreateRequest{Name: "x"})
	var sendErr *MessageSendError
	if !errors.As(err, &sendErr) || sendErr.Class != "api_rejected" || sendErr.Code != 232025 || sendErr.Retryable {
		t.Fatalf("create error = %#v", err)
	}
	err = sender.RenameChat(context.Background(), "oc_x", "y")
	if !errors.As(err, &sendErr) || sendErr.Code != 99991672 {
		t.Fatalf("rename error = %#v", err)
	}
	if _, err := sender.CreateChat(context.Background(), ChatCreateRequest{Name: " "}); err == nil {
		t.Fatal("a nameless chat was created")
	}
}
