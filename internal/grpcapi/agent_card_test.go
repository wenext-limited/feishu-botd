package grpcapi

import (
	"context"
	"log/slog"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "feishu-botd/gen/feishubotd/v1"
	"feishu-botd/internal/config"
	"feishu-botd/internal/dedupe"
	"feishu-botd/internal/feishu"
	"feishu-botd/internal/service"
)

// fakeCardSender adds the proactive card and pin paths to the post fixture.
type fakeCardSender struct {
	*fakePostingSender

	cardMu   sync.Mutex
	posts    []feishu.CardPostRequest
	replaces []feishu.CardReplaceRequest
	pins     []string
	pinErr   error
}

func (f *fakeCardSender) PostCard(_ context.Context, req feishu.CardPostRequest) (feishu.PostedMessage, error) {
	f.cardMu.Lock()
	defer f.cardMu.Unlock()
	f.posts = append(f.posts, req)
	return feishu.PostedMessage{MessageIDs: []string{"om_card_fixture"}}, nil
}

func (f *fakeCardSender) ReplaceCard(_ context.Context, req feishu.CardReplaceRequest) error {
	f.cardMu.Lock()
	defer f.cardMu.Unlock()
	f.replaces = append(f.replaces, req)
	return nil
}

func (f *fakeCardSender) PinMessage(_ context.Context, messageID string) error {
	f.cardMu.Lock()
	defer f.cardMu.Unlock()
	f.pins = append(f.pins, messageID)
	return f.pinErr
}

func startCardUnixServer(t *testing.T, proactive, pins bool) (*grpc.ClientConn, *fakeCardSender) {
	t.Helper()
	cfg := testConfig()
	provider := config.AgentProviderConfig{AuthToken: fixtureAgentToken, AllowUnmatchedMessages: true, AllowPins: pins}
	if proactive {
		provider.AllowProactiveMessages, provider.ProactiveChatAliases = true, []string{"ops"}
	}
	cfg.AgentProviders = map[string]config.AgentProviderConfig{"fixture-agent": provider}
	sender := &fakeCardSender{fakePostingSender: &fakePostingSender{fakeAgentSender: &fakeAgentSender{fakeSender: fakeSender{messageID: "om_fixture"}}}}
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
	return conn, sender
}

func fixtureCardContent(state pb.AgentCardState) *pb.AgentCardContent {
	return &pb.AgentCardContent{
		Title:    "Deploy blocked",
		State:    state,
		Markdown: "Waiting on **Alice**.",
		Steps:    []*pb.AgentCardStep{{Label: "Triage", Done: true}, {Label: "Escalate"}},
		Links:    []*pb.AgentCardLink{{Label: "Run", Url: "https://ci.example.com/1"}},
	}
}

func TestGRPCAgentCardMethodsRequireAProviderCredential(t *testing.T) {
	for _, method := range []string{
		pb.CommandService_PostAgentCard_FullMethodName,
		pb.CommandService_UpdateAgentCard_FullMethodName,
		pb.CommandService_PinAgentMessage_FullMethodName,
	} {
		if !isAgentMethod(method) || !requiresProviderAuth(method, false) {
			t.Fatalf("%s does not require a provider credential", method)
		}
	}
}

func TestGRPCAgentCardsRequireTheirGrants(t *testing.T) {
	conn, sender := startCardUnixServer(t, false, false)
	client := pb.NewCommandServiceClient(conn)
	for name, call := range map[string]func() error{
		"post": func() error {
			_, err := client.PostAgentCard(context.Background(), &pb.PostAgentCardRequest{
				Provider: "fixture-agent", OperationId: "card-1",
				Target:  &pb.PostAgentCardRequest_ChatAlias{ChatAlias: "ops"},
				Content: fixtureCardContent(pb.AgentCardState_AGENT_CARD_STATE_PROCESSING),
			})
			return err
		},
		"update": func() error {
			_, err := client.UpdateAgentCard(context.Background(), &pb.UpdateAgentCardRequest{
				Provider: "fixture-agent", MessageRef: "msgref_x", OperationId: "u-1",
				Content: fixtureCardContent(pb.AgentCardState_AGENT_CARD_STATE_ACTIVE),
			})
			return err
		},
		"pin": func() error {
			_, err := client.PinAgentMessage(context.Background(), &pb.PinAgentMessageRequest{Provider: "fixture-agent", MessageRef: "msgref_x"})
			return err
		},
	} {
		err := call()
		if status.Code(err) != codes.PermissionDenied {
			t.Fatalf("%s without its grant = %v, want PermissionDenied", name, err)
		}
		if detail := botdDetail(t, err); detail == nil || detail.GetCode() != "provider_scope_denied" {
			t.Fatalf("%s denial detail = %#v", name, detail)
		}
	}
	if len(sender.posts) != 0 || len(sender.pins) != 0 {
		t.Fatal("an ungranted call reached Feishu")
	}
}

func TestGRPCAgentCardPostUpdateAndPin(t *testing.T) {
	conn, sender := startCardUnixServer(t, true, true)
	client := pb.NewCommandServiceClient(conn)

	posted, err := client.PostAgentCard(context.Background(), &pb.PostAgentCardRequest{
		Provider: "fixture-agent", OperationId: "card-1",
		Target:  &pb.PostAgentCardRequest_ChatAlias{ChatAlias: "ops"},
		Content: fixtureCardContent(pb.AgentCardState_AGENT_CARD_STATE_PROCESSING),
	})
	if err != nil {
		t.Fatalf("post card: %v", err)
	}
	if posted.GetMessageRef() != feishu.MessageRefForApp("", "om_card_fixture") || posted.GetDuplicate() {
		t.Fatalf("post response = %#v", posted)
	}
	updated, err := client.UpdateAgentCard(context.Background(), &pb.UpdateAgentCardRequest{
		Provider: "fixture-agent", MessageRef: posted.GetMessageRef(), OperationId: "u-1",
		Content: fixtureCardContent(pb.AgentCardState_AGENT_CARD_STATE_SUSPENDED),
	})
	if err != nil || updated.GetDuplicate() || updated.GetSuperseded() {
		t.Fatalf("update = %#v err=%v", updated, err)
	}
	if _, err := client.PinAgentMessage(context.Background(), &pb.PinAgentMessageRequest{Provider: "fixture-agent", MessageRef: posted.GetMessageRef()}); err != nil {
		t.Fatalf("pin: %v", err)
	}
	sender.cardMu.Lock()
	if len(sender.replaces) != 1 || sender.replaces[0].Sequence != 1 || !strings.Contains(sender.replaces[0].CardJSON, "等人处理") ||
		len(sender.pins) != 1 || sender.pins[0] != "om_card_fixture" {
		t.Fatalf("Feishu calls: replaces=%#v pins=%v", sender.replaces, sender.pins)
	}
	sender.pinErr = &feishu.MessageSendError{Operation: "message_pin", Class: "api_rejected", HTTPStatus: 400, Code: 230027}
	sender.cardMu.Unlock()

	_, err = client.PinAgentMessage(context.Background(), &pb.PinAgentMessageRequest{Provider: "fixture-agent", MessageRef: posted.GetMessageRef()})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("pin without the Feishu permission = %v, want FailedPrecondition", err)
	}
	if detail := botdDetail(t, err); detail == nil || detail.GetCode() != "pin_permission_denied" || detail.GetRetryable() {
		t.Fatalf("pin permission detail = %#v", detail)
	}

	_, err = client.UpdateAgentCard(context.Background(), &pb.UpdateAgentCardRequest{
		Provider: "fixture-agent", MessageRef: "msgref_unknown", OperationId: "u-2",
		Content: fixtureCardContent(pb.AgentCardState_AGENT_CARD_STATE_ACTIVE),
	})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("update of an unknown card = %v, want NotFound", err)
	}
	_, err = client.PostAgentCard(context.Background(), &pb.PostAgentCardRequest{
		Provider: "fixture-agent", OperationId: "card-bad-state",
		Target:  &pb.PostAgentCardRequest_ChatAlias{ChatAlias: "ops"},
		Content: fixtureCardContent(pb.AgentCardState(42)),
	})
	if detail := botdDetail(t, err); status.Code(err) != codes.InvalidArgument || detail == nil || detail.GetCode() != "invalid_card_state" {
		t.Fatalf("unknown wire state = %v detail=%#v", err, detail)
	}
}
