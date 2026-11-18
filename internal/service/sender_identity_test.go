package service

import (
	"feishu-botd/internal/config"
	"feishu-botd/internal/feishu"
	"testing"
)

func TestSenderIdentityRequiresItsOwnGrantAndNeverLooksUpEmail(t *testing.T) {
	backend := &emailAgentBackend{fakeAgentBackend: newFakeAgentBackend()}
	svc := newSenderEmailTestService(backend, nil, map[string]config.AgentProviderConfig{"granted": {AllowSenderIdentity: true}, "plain": {}})
	granted := mustSubscribeAgent(t, svc, AgentSubscribeOptions{Provider: "granted", Commands: []string{"ask"}})
	plain := mustSubscribeAgent(t, svc, AgentSubscribeOptions{Provider: "plain", Commands: []string{"ask"}})
	id := &feishu.SenderIdentity{AppID: "cli_test", TenantKey: "sender-tenant", SenderType: "user", UnionID: "on_sender", OpenID: "ou_sender"}
	dispatch := func(key string, human bool, identity *feishu.SenderIdentity) {
		mustDispatchAgentPrompt(t, svc, CommandInput{DeliveryID: key, Command: "ask", Prompt: "ask", ChatAlias: "ops", SenderID: "ou_sender", HumanSender: human, SenderIdentity: identity,
			Metadata: map[string]string{"message_id": "om_" + key, "sender_identity": "forged", "tenant_key": "forged"}})
	}
	dispatch("one", true, id)
	first := receiveAgentEvent(t, granted).Message
	if first.SenderIdentity == nil || *first.SenderIdentity != *id {
		t.Fatal("granted provider lost authenticated identity")
	}
	if first.SenderEmail != "" || backend.lookups() != 0 {
		t.Fatal("identity grant caused a Contact lookup")
	}
	if receiveAgentEvent(t, plain).Message.SenderIdentity != nil {
		t.Fatal("identity leaked without provider entitlement")
	}
	for _, tc := range []struct {
		key   string
		human bool
		id    *feishu.SenderIdentity
	}{
		{"not-human", false, id},
		{"wrong-app", true, &feishu.SenderIdentity{AppID: "other", TenantKey: "sender-tenant", SenderType: "user", OpenID: "ou_sender"}},
		{"wrong-route", true, &feishu.SenderIdentity{AppID: "cli_test", TenantKey: "sender-tenant", SenderType: "user", OpenID: "ou_other"}},
		{"metadata-only", true, nil},
	} {
		dispatch(tc.key, tc.human, tc.id)
		if receiveAgentEvent(t, granted).Message.SenderIdentity != nil {
			t.Fatalf("accepted invalid identity: %s", tc.key)
		}
		receiveAgentEvent(t, plain)
	}
	// Reload checks the live grant, including on an existing subscription.
	cfg := *svc.cfg()
	cfg.AgentProviders = map[string]config.AgentProviderConfig{"granted": {}, "plain": {}}
	svc.cfgRef.Store(&cfg)
	dispatch("revoked", true, id)
	if receiveAgentEvent(t, granted).Message.SenderIdentity != nil {
		t.Fatal("revoked grant still shared identity")
	}
	receiveAgentEvent(t, plain)
}
