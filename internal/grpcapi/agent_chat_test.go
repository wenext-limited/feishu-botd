package grpcapi

import (
	"context"
	"log/slog"
	"net"
	"slices"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "feishu-botd/gen/feishubotd/v1"
	"feishu-botd/internal/chatgrant"
	"feishu-botd/internal/config"
	"feishu-botd/internal/dedupe"
	"feishu-botd/internal/feishu"
	"feishu-botd/internal/service"
)

// fakeChatSender adds group chat creation to the card fixture.
type fakeChatSender struct {
	*fakeCardSender

	chatMu    sync.Mutex
	creates   []feishu.ChatCreateRequest
	adds      map[string][][]string
	renames   map[string]string
	createErr error
	invalid   []string
}

func (f *fakeChatSender) CreateChat(_ context.Context, req feishu.ChatCreateRequest) (string, error) {
	f.chatMu.Lock()
	defer f.chatMu.Unlock()
	if f.createErr != nil {
		return "", f.createErr
	}
	f.creates = append(f.creates, req)
	return "oc_created_" + string(rune('0'+len(f.creates))), nil
}

func (f *fakeChatSender) AddChatMembers(_ context.Context, chatID string, openIDs []string) ([]string, error) {
	f.chatMu.Lock()
	defer f.chatMu.Unlock()
	if f.adds == nil {
		f.adds = map[string][][]string{}
	}
	f.adds[chatID] = append(f.adds[chatID], append([]string(nil), openIDs...))
	return f.invalid, nil
}

func (f *fakeChatSender) RenameChat(_ context.Context, chatID, name string) error {
	f.chatMu.Lock()
	defer f.chatMu.Unlock()
	if f.renames == nil {
		f.renames = map[string]string{}
	}
	f.renames[chatID] = name
	return nil
}

type chatServerOptions struct {
	createChats bool
	registry    *chatgrant.Registry
}

func startChatUnixServer(t *testing.T, opts chatServerOptions) (*grpc.ClientConn, *fakeChatSender, *chatgrant.Registry) {
	t.Helper()
	cfg := testConfig()
	registry := opts.registry
	if registry == nil {
		registry = chatgrant.NewMemory()
	}
	cfg.RuntimeChats = registry
	provider := config.AgentProviderConfig{
		AuthToken:              fixtureAgentToken,
		AllowUnmatchedMessages: true,
		AllowAmbientMessages:   true,
		AllowProactiveMessages: true,
		ProactiveChatAliases:   []string{"ops"},
		AllowCreateChats:       opts.createChats,
	}
	cfg.AgentProviders = map[string]config.AgentProviderConfig{
		"fixture-agent": provider,
		"other-agent":   {AuthToken: "other-agent-token-0123456789abcdef0123456789", AllowAmbientMessages: true, AllowProactiveMessages: true, AllowCreateChats: true},
	}
	sender := &fakeChatSender{fakeCardSender: &fakeCardSender{fakePostingSender: &fakePostingSender{fakeAgentSender: &fakeAgentSender{fakeSender: fakeSender{messageID: "om_fixture"}}}}}
	svc := service.NewService(cfg, sender, dedupe.NewMemoryStore(time.Hour), slog.Default())
	srv := NewServer(cfg, svc, slog.Default())
	ctx, cancel := context.WithCancel(context.Background())
	socketPath := tempSocket(t)
	errCh := make(chan error, 1)
	go func() { errCh <- srv.ListenAndServeUnix(ctx, socketPath) }()
	t.Cleanup(func() {
		cancel()
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), time.Second)
		defer shutdownCancel()
		_ = srv.Shutdown(shutdownCtx)
	})
	conn := dial(t, func(ctx context.Context) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", socketPath)
	}, grpc.WithPerRPCCredentials(testBearerCredentials{token: fixtureAgentToken}))
	waitHealthy(t, conn, errCh)
	return conn, sender, registry
}

func TestGRPCAgentChatsRequireTheirGrant(t *testing.T) {
	conn, sender, _ := startChatUnixServer(t, chatServerOptions{createChats: false})
	client := pb.NewCommandServiceClient(conn)
	ctx := context.Background()
	_, err := client.CreateAgentChat(ctx, &pb.CreateAgentChatRequest{Provider: "fixture-agent", OperationId: "op", Name: "[Agents] x"})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("create without grant = %v", err)
	}
	_, err = client.InviteAgentChatMembers(ctx, &pb.InviteAgentChatMembersRequest{Provider: "fixture-agent", ChatAlias: "agent-chat-x"})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("invite without grant = %v", err)
	}
	_, err = client.RenameAgentChat(ctx, &pb.RenameAgentChatRequest{Provider: "fixture-agent", ChatAlias: "agent-chat-x", Name: "y"})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("rename without grant = %v", err)
	}
	// The bearer token names fixture-agent; claiming another provider fails.
	_, err = client.CreateAgentChat(ctx, &pb.CreateAgentChatRequest{Provider: "other-agent", OperationId: "op", Name: "x"})
	if status.Code(err) == codes.OK {
		t.Fatal("a provider created a chat as another provider")
	}
	if len(sender.creates) != 0 {
		t.Fatalf("creates = %#v", sender.creates)
	}
}

func TestGRPCAgentChatCreateIsIdempotentAndGrantsTheChat(t *testing.T) {
	dir := t.TempDir()
	registry, err := chatgrant.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	conn, sender, _ := startChatUnixServer(t, chatServerOptions{createChats: true, registry: registry})
	client := pb.NewCommandServiceClient(conn)
	ctx := context.Background()
	req := &pb.CreateAgentChatRequest{
		Provider: "fixture-agent", OperationId: "agents-refund-v2", Name: "[Agents] refund-v2",
		MemberOpenIds: []string{"ou_b", "ou_a", "ou_a"},
	}
	sender.invalid = []string{"ou_b"}
	created, err := client.CreateAgentChat(ctx, req)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	alias := created.GetChatAlias()
	if alias != chatgrant.AliasFor(config.DefaultAppAlias, "oc_created_1") || created.GetDuplicate() ||
		!slices.Equal(created.GetInvalidOpenIds(), []string{"ou_b"}) {
		t.Fatalf("created = %v", created)
	}
	if len(sender.creates) != 1 || sender.creates[0].Name != "[Agents] refund-v2" || sender.creates[0].UUID == "" {
		t.Fatalf("creates = %#v", sender.creates)
	}
	if got := sender.adds["oc_created_1"]; len(got) != 1 || !slices.Equal(got[0], []string{"ou_a", "ou_b"}) {
		t.Fatalf("adds = %#v", sender.adds)
	}

	// A retry, even from a fresh registry loaded off disk, is the same chat.
	again, err := client.CreateAgentChat(ctx, req)
	if err != nil || again.GetChatAlias() != alias || !again.GetDuplicate() {
		t.Fatalf("retry = %v err=%v", again, err)
	}
	if len(sender.creates) != 1 {
		t.Fatalf("a retry created a second chat: %#v", sender.creates)
	}
	reloaded, err := chatgrant.Open(dir)
	if err != nil || !reloaded.Granted("fixture-agent", alias) {
		t.Fatalf("grant not persisted: err=%v", err)
	}

	conflict := &pb.CreateAgentChatRequest{Provider: "fixture-agent", OperationId: "agents-refund-v2", Name: "[Agents] other"}
	_, err = client.CreateAgentChat(ctx, conflict)
	if detail := botdDetail(t, err); detail == nil || detail.GetCode() != "operation_conflict" {
		t.Fatalf("conflicting reuse = %v", err)
	}

	// The created chat is a proactive target with no config edit.
	posted, err := client.PostAgentMessage(ctx, &pb.PostAgentMessageRequest{
		Provider: "fixture-agent", OperationId: "hello", Target: &pb.PostAgentMessageRequest_ChatAlias{ChatAlias: alias},
		Markdown: "hi",
	})
	if err != nil || posted.GetMessageRef() == "" {
		t.Fatalf("post to the created chat = %v err=%v", posted, err)
	}

	if _, err := client.RenameAgentChat(ctx, &pb.RenameAgentChatRequest{Provider: "fixture-agent", ChatAlias: alias, Name: "[Archived] refund-v2"}); err != nil {
		t.Fatalf("rename: %v", err)
	}
	if sender.renames["oc_created_1"] != "[Archived] refund-v2" {
		t.Fatalf("renames = %#v", sender.renames)
	}
	if _, err := client.InviteAgentChatMembers(ctx, &pb.InviteAgentChatMembersRequest{Provider: "fixture-agent", ChatAlias: alias, MemberOpenIds: []string{"ou_c"}}); err != nil {
		t.Fatalf("invite: %v", err)
	}
	if _, err := client.RenameAgentChat(ctx, &pb.RenameAgentChatRequest{Provider: "fixture-agent", ChatAlias: "ops", Name: "x"}); status.Code(err) != codes.NotFound {
		t.Fatalf("rename of a configured chat = %v", err)
	}
}

func TestGRPCAgentChatsAreScopedToTheProviderThatCreatedThem(t *testing.T) {
	registry := chatgrant.NewMemory()
	foreign := chatgrant.AliasFor(config.DefaultAppAlias, "oc_foreign")
	if err := registry.Add(chatgrant.Grant{Provider: "other-agent", AppAlias: config.DefaultAppAlias, ChatID: "oc_foreign", ChatAlias: foreign}); err != nil {
		t.Fatal(err)
	}
	conn, sender, _ := startChatUnixServer(t, chatServerOptions{createChats: true, registry: registry})
	client := pb.NewCommandServiceClient(conn)
	ctx := context.Background()
	_, err := client.InviteAgentChatMembers(ctx, &pb.InviteAgentChatMembersRequest{Provider: "fixture-agent", ChatAlias: foreign, MemberOpenIds: []string{"ou_a"}})
	if detail := botdDetail(t, err); status.Code(err) != codes.NotFound || detail == nil || detail.GetCode() != "unknown_chat" {
		t.Fatalf("invite to another provider's chat = %v", err)
	}
	_, err = client.PostAgentMessage(ctx, &pb.PostAgentMessageRequest{
		Provider: "fixture-agent", OperationId: "x", Target: &pb.PostAgentMessageRequest_ChatAlias{ChatAlias: foreign}, Markdown: "hi",
	})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("post to another provider's chat = %v", err)
	}
	if len(sender.adds) != 0 {
		t.Fatalf("adds = %#v", sender.adds)
	}
}

func TestGRPCAgentChatCreateReportsAMissingPermission(t *testing.T) {
	conn, sender, _ := startChatUnixServer(t, chatServerOptions{createChats: true})
	sender.createErr = &feishu.MessageSendError{Operation: "chat_create", Class: "api_rejected", Code: 99991672}
	client := pb.NewCommandServiceClient(conn)
	_, err := client.CreateAgentChat(context.Background(), &pb.CreateAgentChatRequest{Provider: "fixture-agent", OperationId: "op", Name: "x"})
	detail := botdDetail(t, err)
	if detail == nil || detail.GetCode() != "chat_permission_denied" {
		t.Fatalf("err = %v", err)
	}
}
