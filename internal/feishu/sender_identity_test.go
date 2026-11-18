package feishu

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

func TestAuthenticatedSenderIdentityUsesConfiguredAppAndSenderTenant(t *testing.T) {
	r := NewCommandReceiver(CommandReceiverConfig{AppID: "cli_test", AppSecret: "secret", Channels: map[string]string{"ops": "oc_ops"}}, nil, nil)
	event := messageEvent("evt_identity", "om_identity", "oc_ops", "ask")
	event.Event.Message.ChatType = ptr("p2p")
	event.EventV2Base.Header.TenantKey = "header-owner-tenant"
	event.EventV2Base.Header.AppID = "untrusted-header-app"
	event.Event.Sender.TenantKey = ptr("sender-tenant")
	event.Event.Sender.SenderType = ptr("user")
	event.Event.Sender.SenderId.UnionId = ptr("on_sender")
	event.Event.Sender.SenderId.UserId = ptr("employee")
	cmd, ok := r.CommandFromEvent(event)
	if !ok || cmd.SenderIdentity == nil {
		t.Fatal("authenticated identity missing")
	}
	id := cmd.SenderIdentity
	if id.AppID != "cli_test" || id.TenantKey != "sender-tenant" || id.UnionID != "on_sender" || cmd.SenderID != "ou_sender" {
		t.Fatalf("wrong identity origin or changed routing: %s", id)
	}
	wire, _ := json.Marshal(cmd)
	if strings.Contains(string(wire), "on_sender") || strings.Contains(fmt.Sprintf("%+v %#v", id, id), "on_sender") {
		t.Fatal("private identity leaked")
	}
	event.Event.Sender.TenantKey = nil
	cmd, ok = r.CommandFromEvent(event)
	if !ok || cmd.SenderIdentity != nil {
		t.Fatal("missing sender tenant inferred from event header")
	}
	event.Event.Sender.TenantKey = ptr("sender-tenant")
	event.Event.Sender.SenderType = ptr("app")
	cmd, ok = r.CommandFromEvent(event)
	if ok && cmd.SenderIdentity != nil {
		t.Fatal("app sender became a human")
	}
}
