package service

import (
	"context"
	"testing"
	"time"

	"feishu-botd/internal/config"
	"feishu-botd/internal/feishu"
)

func TestApplyConfigResetsOnlyChangedProviders(t *testing.T) {
	providers := map[string]config.AgentProviderConfig{
		"agent": {AuthToken: "agent-token"},
		"other": {AuthToken: "other-token"},
	}
	svc, _, _ := newMultiAppServiceFixture(t, providers)
	kept := mustSubscribeAgent(t, svc, AgentSubscribeOptions{Provider: "agent", Commands: []string{"ask"}})
	reset := mustSubscribeAgent(t, svc, AgentSubscribeOptions{Provider: "other", Commands: []string{"ask"}})
	legacy, apiErr := svc.SubscribeCommandsForApps(context.Background(), CommandSubscribeOptions{Provider: "other", Commands: []string{"ask"}})
	if apiErr != nil {
		t.Fatal(apiErr)
	}
	internal, apiErr := svc.SubscribeInternalCommandsForApps(context.Background(), CommandSubscribeOptions{Provider: "other", Commands: []string{"ask"}})
	if apiErr != nil {
		t.Fatal(apiErr)
	}

	running := svc.Config()
	next := running
	next.AgentProviders = map[string]config.AgentProviderConfig{
		"agent": {AuthToken: "agent-token"},
		"other": {AuthToken: "other-token", AllowCardActions: true},
	}
	plan := config.PlanReload(running, next)
	svc.ApplyConfig(next, plan, nil, nil, time.Unix(100, 0))

	if _, open := <-reset.C; open || !reset.ClosedByReload() {
		t.Fatal("changed provider's agent stream stayed open")
	}
	if _, open := <-legacy.C; open || !legacy.ClosedByReload() {
		t.Fatal("changed provider's legacy stream stayed open")
	}
	if kept.ClosedByReload() {
		t.Fatal("unchanged provider's stream was reset")
	}
	if internal.ClosedByReload() {
		t.Fatal("an in-process consumer was reset as if it were a provider")
	}
	internal.Close()
	dispatchMultiAppPrompt(t, svc, config.DefaultAppAlias, "delivery_after", "conv_after", "ops", "om_after")
	if event := receiveAgentEvent(t, kept); event.DeliveryID == "" {
		t.Fatal("unchanged provider stopped receiving")
	}
	if got := svc.ConfigStatus(); got.Generation != 2 || got.LastReload != "ok" {
		t.Fatalf("config status = %+v", got)
	}
	if !svc.cfg().AgentProviders["other"].AllowCardActions {
		t.Fatal("new grants are not in force")
	}
}

func TestApplyConfigAddsAndRemovesAppBackends(t *testing.T) {
	svc, _, _ := newMultiAppServiceFixture(t, nil)
	running := svc.Config()
	next := running
	next.Apps = map[string]config.AppConfig{
		config.DefaultAppAlias: running.Apps[config.DefaultAppAlias],
		"gamma": {
			AppID: "app-gamma", AppSecret: "secret-gamma",
			Commands: config.CommandConfig{Enabled: true},
			Channels: map[string]string{"lab": "oc_gamma"},
		},
	}
	next.ChannelRoutes = map[string]config.ChannelRoute{
		"ops": {AppAlias: config.DefaultAppAlias, ChatID: "oc_default"},
		"lab": {AppAlias: "gamma", ChatID: "oc_gamma"},
	}
	plan := config.PlanReload(running, next)
	if len(plan.AppsAdded) != 1 || len(plan.AppsRemoved) != 1 {
		t.Fatalf("plan = %+v", plan)
	}
	gamma := newMultiAppTestBackend("gamma")
	svc.ApplyConfig(next, plan, map[string]feishu.Sender{"gamma": gamma}, []string{"state_dir"}, time.Unix(100, 0))

	if _, ok := svc.backendForApp("beta"); ok {
		t.Fatal("removed app kept its backend")
	}
	if _, ok := svc.backendForApp("gamma"); !ok {
		t.Fatal("added app has no backend")
	}
	_, checks := svc.Ready(context.Background())
	if _, tracked := checks["feishu_connection.beta"]; tracked {
		t.Fatalf("removed app still reported: %v", checks)
	}
	if checks["feishu_connection.gamma"] != AppConnectionStarting {
		t.Fatalf("added app connection = %q", checks["feishu_connection.gamma"])
	}
	if got := svc.ConfigStatus().RestartRequired; len(got) != 1 || got[0] != "state_dir" {
		t.Fatalf("restart required = %v", got)
	}
	// The removed app's secret stays redacted.
	if got := svc.Redact("leak secret-beta"); got != "leak <redacted>" {
		t.Fatalf("redact = %q", got)
	}

	svc.RecordRejectedReload(time.Unix(200, 0))
	if got := svc.ConfigStatus(); got.LastReload != "rejected" || got.Generation != 2 || len(got.RestartRequired) != 1 {
		t.Fatalf("after rejection = %+v", got)
	}
}
