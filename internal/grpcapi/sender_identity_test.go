package grpcapi

import (
	"feishu-botd/internal/feishu"
	"feishu-botd/internal/service"
	"testing"
)

func TestSenderIdentityProtoPreservesTypedAuthenticatedFields(t *testing.T) {
	event := service.AgentEvent{Message: &service.AgentMessage{SenderIdentity: &feishu.SenderIdentity{AppID: "cli_bot", TenantKey: "sender-tenant", SenderType: "user", UnionID: "on_person", OpenID: "ou_person", UserID: "employee"}}}
	id := agentEventToProto(event).Event.GetMessage().SenderIdentity
	if id == nil || id.AppId != "cli_bot" || id.TenantKey != "sender-tenant" || id.SenderType != "user" || id.UnionId != "on_person" || id.OpenId != "ou_person" || id.UserId != "employee" {
		t.Fatal("lost typed sender identity")
	}
}
