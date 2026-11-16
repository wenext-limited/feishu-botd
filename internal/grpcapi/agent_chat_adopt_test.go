package grpcapi

import (
	"context"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "feishu-botd/gen/feishubotd/v1"
	"feishu-botd/internal/config"
	"feishu-botd/internal/service"
)

func TestGRPCAdoptAndReleaseRequireProviderIdentityAndAdoptGrant(t *testing.T) {
	for _, method := range []string{pb.CommandService_AdoptAgentChat_FullMethodName, pb.CommandService_ReleaseAgentChat_FullMethodName} {
		if !isAgentMethod(method) || !requiresProviderAuth(method, false) {
			t.Fatalf("not provider-authenticated: %s", method)
		}
	}
	conn, _, _ := startChatUnixServer(t, chatServerOptions{createChats: true})
	client := pb.NewCommandServiceClient(conn)
	if _, err := client.AdoptAgentChat(context.Background(), &pb.AdoptAgentChatRequest{Provider: "fixture-agent", MessageRef: "ref"}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("disabled adopt: %v", err)
	}
	if _, err := client.ReleaseAgentChat(context.Background(), &pb.ReleaseAgentChatRequest{Provider: "fixture-agent", ChatAlias: "ops"}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("disabled release: %v", err)
	}
	conn, _, _ = startChatUnixServer(t, chatServerOptions{adoptChats: true})
	client = pb.NewCommandServiceClient(conn)
	if _, err := client.AdoptAgentChat(context.Background(), &pb.AdoptAgentChatRequest{Provider: "other-agent", MessageRef: "ref"}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("impersonated adopt: %v", err)
	}
	if _, err := client.ReleaseAgentChat(context.Background(), &pb.ReleaseAgentChatRequest{Provider: "other-agent", ChatAlias: "ops"}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("impersonated release: %v", err)
	}
	if _, err := client.AdoptAgentChat(context.Background(), &pb.AdoptAgentChatRequest{Provider: "fixture-agent", MessageRef: "unknown"}); status.Code(err) != codes.NotFound || botdDetail(t, err).GetCode() != "unknown_chat" {
		t.Fatalf("unknown adopt: %v", err)
	}
}

func TestGRPCAdoptLatestDeliveredMessageAndRelease(t *testing.T) {
	var svc *service.Service
	conn, _, registry := startChatUnixServer(t, chatServerOptions{adoptChats: true, onService: func(s *service.Service) { svc = s }})
	client := pb.NewCommandServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := client.SubscribeAgentEvents(ctx, &pb.SubscribeAgentEventsRequest{Provider: "fixture-agent", IncludeUnmatchedMessages: true, IncludeAmbientMessages: true})
	if err != nil {
		t.Fatal(err)
	}
	input := ambientFixtureInput()
	input.AmbientOnly = false
	event := dispatchAndReceiveAgentEvent(t, svc, stream, input)
	static, err := client.AdoptAgentChat(ctx, &pb.AdoptAgentChatRequest{Provider: "fixture-agent", MessageRef: event.GetMessage().GetMessageRef()})
	if err != nil || !static.GetAlreadyGranted() || static.GetChatAlias() != "ops" {
		t.Fatalf("static = %v %v", static, err)
	}
	if released, err := client.ReleaseAgentChat(ctx, &pb.ReleaseAgentChatRequest{Provider: "fixture-agent", ChatAlias: "ops"}); err != nil || released.GetReleased() {
		t.Fatalf("static release = %v %v", released, err)
	}
	alias := config.UnconfiguredGroupAliasPrefix + strings.Repeat("d", 64)
	input.ChatAlias, input.ChatID, input.UnconfiguredGroup = alias, "oc_adopted", true
	input.DeliveryID, input.ConversationID, input.Metadata["message_id"] = "d_latest", "conv_adopted", "om_latest"
	event = dispatchAndReceiveAgentEvent(t, svc, stream, input)
	adopted, err := client.AdoptAgentChat(ctx, &pb.AdoptAgentChatRequest{Provider: "fixture-agent", MessageRef: event.GetMessage().GetMessageRef()})
	if err != nil || adopted.GetChatAlias() != alias || adopted.GetAlreadyGranted() {
		t.Fatalf("adopted = %v %v", adopted, err)
	}
	if _, err := client.PostAgentMessage(ctx, &pb.PostAgentMessageRequest{Provider: "fixture-agent", OperationId: "post_adopted", Target: &pb.PostAgentMessageRequest_ChatAlias{ChatAlias: alias}, Markdown: "hello"}); err != nil {
		t.Fatal(err)
	}
	input.AmbientOnly, input.UnconfiguredGroup = true, false
	input.DeliveryID, input.Metadata["message_id"] = "d_ambient_adopted", "om_latest_ambient"
	event = dispatchAndReceiveAgentEvent(t, svc, stream, input)
	if event.GetMessage().GetAddressing() != pb.InboundAddressing_INBOUND_ADDRESSING_AMBIENT {
		t.Fatalf("not ambient: %v", event)
	}
	again, err := client.AdoptAgentChat(ctx, &pb.AdoptAgentChatRequest{Provider: "fixture-agent", MessageRef: event.GetMessage().GetMessageRef()})
	if err != nil || !again.GetAlreadyGranted() {
		t.Fatalf("repeat from ambient = %v %v", again, err)
	}
	released, err := client.ReleaseAgentChat(ctx, &pb.ReleaseAgentChatRequest{Provider: "fixture-agent", ChatAlias: alias})
	if err != nil || !released.GetReleased() || registry.Granted("fixture-agent", alias) {
		t.Fatalf("release = %v %v", released, err)
	}
	released, err = client.ReleaseAgentChat(ctx, &pb.ReleaseAgentChatRequest{Provider: "fixture-agent", ChatAlias: alias})
	if err != nil || released.GetReleased() {
		t.Fatalf("repeat release = %v %v", released, err)
	}
}
