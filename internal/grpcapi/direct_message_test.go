package grpcapi

import (
	"context"
	"testing"

	pb "feishu-botd/gen/feishubotd/v1"
	"feishu-botd/internal/config"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestGRPCSendMessageUserEmailTarget(t *testing.T) {
	cfg := testConfig()
	cfg.DirectMessages = config.DirectMessagesConfig{
		AllowedEmailDomains: []string{"example.com"},
		AllowedSources:      []string{"bridge"},
	}
	sender := &fakeSender{messageID: "om_dm"}
	conn, _ := startUnixServerWithService(t, cfg, sender)
	nc := pb.NewNotificationServiceClient(conn)
	emailTarget := &pb.MessageTarget{To: &pb.MessageTarget_UserEmail{UserEmail: "dev@example.com"}}
	markdown := &pb.SendMessageRequest_Markdown{Markdown: &pb.MarkdownContent{Title: "Review", Markdown: "done"}}

	resp, err := nc.SendMessage(context.Background(), &pb.SendMessageRequest{
		Target: emailTarget, Source: "bridge", DedupeKey: "pr-1", Content: markdown,
	})
	if err != nil || resp.GetMessageId() != "om_dm" {
		t.Fatalf("send = %#v, %v", resp, err)
	}
	if sender.chatID != "dev@example.com" || sender.request.Target.UserEmail != "dev@example.com" || sender.request.Target.Channel != "" {
		t.Fatalf("sender got chat=%q target=%#v", sender.chatID, sender.request.Target)
	}

	_, err = nc.SendMessage(context.Background(), &pb.SendMessageRequest{
		Target: emailTarget, Source: "jenkins", Content: markdown,
	})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("other source: code = %v (%v), want PermissionDenied", status.Code(err), err)
	}

	req := validNotifyRequest("ops", "k1")
	req.Target = emailTarget
	req.Source = "bridge"
	sender.calls = 0
	if _, err := nc.SendNotification(context.Background(), req); status.Code(err) != codes.InvalidArgument || sender.calls != 0 {
		t.Fatalf("SendNotification with user_email: code = %v calls=%d, want InvalidArgument and no send", status.Code(err), sender.calls)
	}
}
