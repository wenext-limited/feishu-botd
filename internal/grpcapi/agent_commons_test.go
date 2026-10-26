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
	"feishu-botd/internal/config"
	"feishu-botd/internal/dedupe"
	"feishu-botd/internal/feishu"
	"feishu-botd/internal/service"
)

// fakePostingSender adds the proactive-post path to the CardKit fixture.
type fakePostingSender struct {
	*fakeAgentSender

	postMu sync.Mutex
	posts  []feishu.PostMessageRequest
}

func (f *fakePostingSender) PostMessage(_ context.Context, req feishu.PostMessageRequest) (feishu.PostedMessage, error) {
	f.postMu.Lock()
	defer f.postMu.Unlock()
	f.posts = append(f.posts, req)
	posted := feishu.PostedMessage{MessageIDs: []string{"om_posted_fixture"}}
	if req.ReplyInThread {
		posted.ThreadID = "omt_fixture"
	}
	return posted, nil
}

// startCommonsUnixServer serves one provider whose ambient and proactive
// grants are the arguments, scoped to the fixture "ops" channel.
func startCommonsUnixServer(t *testing.T, ambient, proactive bool) (*grpc.ClientConn, *service.Service, *fakePostingSender) {
	t.Helper()
	cfg := testConfig()
	provider := config.AgentProviderConfig{AuthToken: fixtureAgentToken, AllowUnmatchedMessages: true}
	if ambient {
		provider.AllowAmbientMessages, provider.AmbientChatAliases = true, []string{"ops"}
	}
	if proactive {
		provider.AllowProactiveMessages, provider.ProactiveChatAliases = true, []string{"ops"}
	}
	cfg.AgentProviders = map[string]config.AgentProviderConfig{"fixture-agent": provider}
	sender := &fakePostingSender{fakeAgentSender: &fakeAgentSender{fakeSender: fakeSender{messageID: "om_fixture"}}}
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
	return conn, svc, sender
}

func ambientFixtureInput() service.CommandInput {
	return service.CommandInput{
		DeliveryID:     "delivery_ambient_fixture",
		ConversationID: "conversation_ambient_fixture",
		Prompt:         "deploy is blocked on @_user_2",
		NamedPrompt:    "deploy is blocked on @Alice",
		Mentions:       []service.AgentMention{{UserID: "ou_alice", Name: "Alice"}},
		ChatAlias:      "ops",
		SenderID:       "sender_fixture",
		AmbientOnly:    true,
		HumanSender:    true,
		Metadata: map[string]string{
			"chat_type": "group", "message_type": "text", "message_id": "om_ambient_fixture",
			"thread_id": "omt_fixture", "root_id": "om_root_fixture", "parent_id": "om_parent_fixture",
		},
	}
}

func TestGRPCAgentMethodsIncludePostAgentMessage(t *testing.T) {
	if !isAgentMethod(pb.CommandService_PostAgentMessage_FullMethodName) || !requiresProviderAuth(pb.CommandService_PostAgentMessage_FullMethodName, false) {
		t.Fatal("PostAgentMessage does not require a provider credential")
	}
}

func TestGRPCAmbientSubscriptionRequiresTheGrant(t *testing.T) {
	conn, _, _ := startCommonsUnixServer(t, false, false)
	client := pb.NewCommandServiceClient(conn)
	stream, err := client.SubscribeAgentEvents(context.Background(), &pb.SubscribeAgentEventsRequest{
		Provider:               "fixture-agent",
		IncludeAmbientMessages: true,
	})
	if err == nil {
		_, err = stream.Recv()
	}
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("ungranted ambient subscription = %v, want PermissionDenied", err)
	}
	if detail := botdDetail(t, err); detail == nil || detail.GetCode() != "provider_scope_denied" {
		t.Fatalf("scope denial detail = %#v", detail)
	}
}

func TestGRPCAmbientEventCarriesRefsAddressingAndMentions(t *testing.T) {
	conn, svc, _ := startCommonsUnixServer(t, true, false)
	client := pb.NewCommandServiceClient(conn)
	streamCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	stream, err := client.SubscribeAgentEvents(streamCtx, &pb.SubscribeAgentEventsRequest{
		Provider:               "fixture-agent",
		IncludeAmbientMessages: true,
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	event := dispatchAndReceiveAgentEvent(t, svc, stream, ambientFixtureInput())
	message := event.GetMessage()
	if message.GetAddressing() != pb.InboundAddressing_INBOUND_ADDRESSING_AMBIENT {
		t.Fatalf("addressing = %v", message.GetAddressing())
	}
	if message.GetText() != "deploy is blocked on @Alice" {
		t.Fatalf("text = %q", message.GetText())
	}
	if len(message.GetMentions()) != 1 || message.GetMentions()[0].GetUserId() != "ou_alice" || message.GetMentions()[0].GetName() != "Alice" {
		t.Fatalf("mentions = %#v", message.GetMentions())
	}
	if message.GetMessageRef() != feishu.MessageRefForApp("", "om_ambient_fixture") ||
		message.GetThreadRef() != feishu.ThreadRefForApp("", "omt_fixture") ||
		message.GetThreadRootMessageRef() != feishu.MessageRefForApp("", "om_root_fixture") {
		t.Fatalf("refs = %#v", message)
	}
	if message.GetReplyToMessageRef() != "" {
		t.Fatalf("an unowned parent leaked as reply_to_message_ref: %q", message.GetReplyToMessageRef())
	}

	_, err = client.StartAgentResponse(context.Background(), &pb.StartAgentResponseRequest{
		Provider: "fixture-agent", DeliveryId: event.GetDeliveryId(), OperationId: "start-1",
		Content: &pb.AgentResponseContent{Markdown: "noted"},
	})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("start on an ambient delivery = %v, want NotFound", err)
	}
	if detail := botdDetail(t, err); detail == nil || detail.GetCode() != "unknown_delivery" {
		t.Fatalf("start detail = %#v", detail)
	}
}

func TestGRPCPostAgentMessageRequiresTheGrant(t *testing.T) {
	conn, _, sender := startCommonsUnixServer(t, false, false)
	client := pb.NewCommandServiceClient(conn)
	_, err := client.PostAgentMessage(context.Background(), &pb.PostAgentMessageRequest{
		Provider: "fixture-agent", OperationId: "post-1",
		Target:   &pb.PostAgentMessageRequest_ChatAlias{ChatAlias: "ops"},
		Markdown: "unprompted",
	})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("ungranted post = %v, want PermissionDenied", err)
	}
	if detail := botdDetail(t, err); detail == nil || detail.GetCode() != "provider_scope_denied" {
		t.Fatalf("scope denial detail = %#v", detail)
	}
	if len(sender.posts) != 0 {
		t.Fatal("an ungranted post reached Feishu")
	}
}

func TestGRPCPostAgentMessageRootReplyAndReplay(t *testing.T) {
	conn, svc, sender := startCommonsUnixServer(t, true, true)
	client := pb.NewCommandServiceClient(conn)

	root, err := client.PostAgentMessage(context.Background(), &pb.PostAgentMessageRequest{
		Provider: "fixture-agent", OperationId: "post-root",
		Target:   &pb.PostAgentMessageRequest_ChatAlias{ChatAlias: "ops"},
		Markdown: "mirrored question", Summary: "Mirror",
	})
	if err != nil {
		t.Fatalf("post root: %v", err)
	}
	if root.GetMessageRef() != feishu.MessageRefForApp("", "om_posted_fixture") || root.GetThreadRef() != "" || root.GetDuplicate() {
		t.Fatalf("root response = %#v", root)
	}
	replay, err := client.PostAgentMessage(context.Background(), &pb.PostAgentMessageRequest{
		Provider: "fixture-agent", OperationId: "post-root",
		Target:   &pb.PostAgentMessageRequest_ChatAlias{ChatAlias: "ops"},
		Markdown: "mirrored question", Summary: "Mirror",
	})
	if err != nil || !replay.GetDuplicate() || replay.GetMessageRef() != root.GetMessageRef() {
		t.Fatalf("replay = %#v err=%v", replay, err)
	}

	streamCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	stream, err := client.SubscribeAgentEvents(streamCtx, &pb.SubscribeAgentEventsRequest{
		Provider: "fixture-agent", IncludeAmbientMessages: true,
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	event := dispatchAndReceiveAgentEvent(t, svc, stream, ambientFixtureInput())
	reply, err := client.PostAgentMessage(context.Background(), &pb.PostAgentMessageRequest{
		Provider: "fixture-agent", OperationId: "post-reply",
		Target:        &pb.PostAgentMessageRequest_ReplyToMessageRef{ReplyToMessageRef: event.GetMessage().GetMessageRef()},
		ReplyInThread: true,
		Markdown:      "answer", MentionUserId: "ou_alice",
	})
	if err != nil {
		t.Fatalf("post reply: %v", err)
	}
	if reply.GetThreadRef() != feishu.ThreadRefForApp("", "omt_fixture") {
		t.Fatalf("reply response = %#v", reply)
	}
	sender.postMu.Lock()
	posts := append([]feishu.PostMessageRequest(nil), sender.posts...)
	sender.postMu.Unlock()
	if len(posts) != 2 || posts[0].ChatID != "oc_test" || posts[1].ReplyToMessageID != "om_ambient_fixture" ||
		!posts[1].ReplyInThread || posts[1].Content.MentionUserID != "ou_alice" {
		t.Fatalf("Feishu posts = %#v", posts)
	}

	_, err = client.PostAgentMessage(context.Background(), &pb.PostAgentMessageRequest{
		Provider: "fixture-agent", OperationId: "post-unknown",
		Target:   &pb.PostAgentMessageRequest_ReplyToMessageRef{ReplyToMessageRef: "msgref_unknown"},
		Markdown: "into the void",
	})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("unknown ref = %v, want NotFound", err)
	}
	if detail := botdDetail(t, err); detail == nil || detail.GetCode() != "unknown_message" || detail.GetRetryable() {
		t.Fatalf("unknown ref detail = %#v", detail)
	}
	_, err = client.PostAgentMessage(context.Background(), &pb.PostAgentMessageRequest{
		Provider: "fixture-agent", OperationId: "post-untargeted", Markdown: "nowhere",
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("untargeted post = %v, want InvalidArgument", err)
	}
}
