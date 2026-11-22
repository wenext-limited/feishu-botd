package grpcapi

import (
	"context"
	"log/slog"
	"net"
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

// fakeAnnouncementSender adds a docx announcement to the chat fixture.
type fakeAnnouncementSender struct {
	*fakeChatSender

	annMu   sync.Mutex
	kind    string
	blocks  []feishu.AnnouncementBlock
	writes  int
	nextID  int
	listErr error
}

func (f *fakeAnnouncementSender) GetChatAnnouncement(context.Context, string) (feishu.AnnouncementInfo, error) {
	f.annMu.Lock()
	defer f.annMu.Unlock()
	return feishu.AnnouncementInfo{Type: f.kind, RevisionID: f.writes + 1}, nil
}

func (f *fakeAnnouncementSender) ListChatAnnouncementBlocks(context.Context, string) ([]feishu.AnnouncementBlock, error) {
	f.annMu.Lock()
	defer f.annMu.Unlock()
	if f.listErr != nil {
		return nil, f.listErr
	}
	return append([]feishu.AnnouncementBlock(nil), f.blocks...), nil
}

func (f *fakeAnnouncementSender) CreateChatAnnouncementBlocks(_ context.Context, _ string, index int, blocks []feishu.AnnouncementBlock, _ string) ([]string, error) {
	f.annMu.Lock()
	defer f.annMu.Unlock()
	f.writes++
	if index < 0 {
		index = len(f.blocks)
	}
	ids := make([]string, len(blocks))
	created := make([]feishu.AnnouncementBlock, len(blocks))
	for i, block := range blocks {
		f.nextID++
		block.ID = "blk_" + string(rune('a'+f.nextID))
		ids[i], created[i] = block.ID, block
	}
	f.blocks = append(f.blocks[:index], append(created, f.blocks[index:]...)...)
	return ids, nil
}

func (f *fakeAnnouncementSender) DeleteChatAnnouncementBlocks(_ context.Context, _ string, start, end int, _ string) error {
	f.annMu.Lock()
	defer f.annMu.Unlock()
	f.writes++
	f.blocks = append(f.blocks[:start], f.blocks[end:]...)
	return nil
}

const announcementTestAlias = "agent-chat-0123456789abcdef01234567"

func startAnnouncementServer(t *testing.T, allowAnnouncements bool) (pb.CommandServiceClient, *fakeAnnouncementSender) {
	t.Helper()
	cfg := testConfig()
	registry := chatgrant.NewMemory()
	cfg.RuntimeChats = registry
	cfg.AgentProviders = map[string]config.AgentProviderConfig{
		"fixture-agent": {
			AuthToken: fixtureAgentToken, AllowAmbientMessages: true, AllowProactiveMessages: true,
			ProactiveChatAliases: []string{"ops"}, AllowCreateChats: true, AllowAnnouncements: allowAnnouncements,
		},
		"other-agent": {
			AuthToken: "other-agent-token-0123456789abcdef0123456789", AllowAmbientMessages: true,
			AllowProactiveMessages: true, AllowCreateChats: true, AllowAnnouncements: true,
		},
	}
	for _, grant := range []chatgrant.Grant{
		{Kind: chatgrant.Created, Provider: "fixture-agent", AppAlias: config.DefaultAppAlias, ChatID: "oc_granted", ChatAlias: announcementTestAlias},
		{Kind: chatgrant.Created, Provider: "other-agent", AppAlias: config.DefaultAppAlias, ChatID: "oc_other", ChatAlias: "agent-chat-ffffffffffffffffffffffff"},
	} {
		if err := registry.Add(grant); err != nil {
			t.Fatal(err)
		}
	}
	sender := &fakeAnnouncementSender{kind: feishu.AnnouncementTypeDocx, fakeChatSender: &fakeChatSender{fakeCardSender: &fakeCardSender{
		fakePostingSender: &fakePostingSender{fakeAgentSender: &fakeAgentSender{fakeSender: fakeSender{messageID: "om_fixture"}}}}}}
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
	return pb.NewCommandServiceClient(conn), sender
}

func TestGRPCAnnouncementSectionIsAnAgentMethod(t *testing.T) {
	method := pb.CommandService_SetAgentChatAnnouncementSection_FullMethodName
	if !isAgentMethod(method) || !requiresProviderAuth(method, false) {
		t.Fatal("SetAgentChatAnnouncementSection does not require a provider credential")
	}
}

func TestGRPCAnnouncementSectionWritesAndIsIdempotent(t *testing.T) {
	client, sender := startAnnouncementServer(t, true)
	ctx := context.Background()
	req := &pb.SetAgentChatAnnouncementSectionRequest{
		Provider: "fixture-agent", ChatAlias: announcementTestAlias, OperationId: "op-1",
		Title: "Agent status", Markdown: "- **green**",
	}
	if _, err := client.SetAgentChatAnnouncementSection(ctx, req); err != nil {
		t.Fatalf("set: %v", err)
	}
	if _, err := client.SetAgentChatAnnouncementSection(ctx, req); err != nil {
		t.Fatalf("repeat: %v", err)
	}
	if sender.writes != 1 || len(sender.blocks) != 2 || sender.blocks[0].PlainText() != "Agent status" ||
		sender.blocks[1].Type != feishu.AnnouncementBlockBullet {
		t.Fatalf("writes = %d blocks = %#v", sender.writes, sender.blocks)
	}
	conflict := &pb.SetAgentChatAnnouncementSectionRequest{
		Provider: "fixture-agent", ChatAlias: announcementTestAlias, OperationId: "op-1", Title: "Agent status", Markdown: "red",
	}
	_, err := client.SetAgentChatAnnouncementSection(ctx, conflict)
	if status.Code(err) != codes.AlreadyExists {
		t.Fatalf("conflict = %v, want AlreadyExists", err)
	}
	if detail := botdDetail(t, err); detail == nil || detail.GetCode() != "operation_conflict" {
		t.Fatalf("conflict detail = %v", err)
	}
}

func TestGRPCAnnouncementSectionErrorClasses(t *testing.T) {
	client, sender := startAnnouncementServer(t, true)
	ctx := context.Background()
	req := func(alias string) *pb.SetAgentChatAnnouncementSectionRequest {
		return &pb.SetAgentChatAnnouncementSectionRequest{Provider: "fixture-agent", ChatAlias: alias, OperationId: "op", Title: "T", Markdown: "x"}
	}
	for name, alias := range map[string]string{
		"another provider's chat": "agent-chat-ffffffffffffffffffffffff",
		"a static chat":           "ops",
		"an unknown alias":        "agent-chat-000000000000000000000000",
	} {
		_, err := client.SetAgentChatAnnouncementSection(ctx, req(alias))
		if status.Code(err) != codes.NotFound || botdDetail(t, err).GetCode() != "unknown_chat" {
			t.Fatalf("%s = %v", name, err)
		}
	}

	sender.annMu.Lock()
	sender.kind = "doc"
	sender.annMu.Unlock()
	_, err := client.SetAgentChatAnnouncementSection(ctx, req(announcementTestAlias))
	if status.Code(err) != codes.FailedPrecondition || botdDetail(t, err).GetCode() != "announcement_unsupported" {
		t.Fatalf("legacy announcement = %v", err)
	}

	sender.annMu.Lock()
	sender.kind = feishu.AnnouncementTypeDocx
	sender.listErr = &feishu.MessageSendError{Operation: "announcement_blocks_list", Class: "api_rejected", Code: 99991672, HTTPStatus: 400}
	sender.annMu.Unlock()
	_, err = client.SetAgentChatAnnouncementSection(ctx, req(announcementTestAlias))
	if status.Code(err) != codes.FailedPrecondition || botdDetail(t, err).GetCode() != "announcement_forbidden" {
		t.Fatalf("missing scope = %v", err)
	}

	sender.annMu.Lock()
	sender.listErr = &feishu.MessageSendError{Operation: "announcement_blocks_list", Class: "api_rejected", Code: 99991400, HTTPStatus: 429, Retryable: true}
	sender.annMu.Unlock()
	_, err = client.SetAgentChatAnnouncementSection(ctx, req(announcementTestAlias))
	if status.Code(err) != codes.Unavailable || !botdDetail(t, err).GetRetryable() {
		t.Fatalf("rate limit = %v", err)
	}
	if sender.writes != 0 {
		t.Fatalf("writes = %d", sender.writes)
	}
}

func TestGRPCAnnouncementSectionRequiresTheFlagAndIdentity(t *testing.T) {
	client, sender := startAnnouncementServer(t, false)
	ctx := context.Background()
	_, err := client.SetAgentChatAnnouncementSection(ctx, &pb.SetAgentChatAnnouncementSectionRequest{
		Provider: "fixture-agent", ChatAlias: announcementTestAlias, OperationId: "op", Title: "T",
	})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("without allow_announcements = %v", err)
	}
	// The bearer token names fixture-agent; claiming another provider fails.
	_, err = client.SetAgentChatAnnouncementSection(ctx, &pb.SetAgentChatAnnouncementSectionRequest{
		Provider: "other-agent", ChatAlias: "agent-chat-ffffffffffffffffffffffff", OperationId: "op", Title: "T",
	})
	if status.Code(err) == codes.OK {
		t.Fatal("a provider edited an announcement as another provider")
	}
	if sender.writes != 0 {
		t.Fatalf("writes = %d", sender.writes)
	}
}
