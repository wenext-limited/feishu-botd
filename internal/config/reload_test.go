package config

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func writeToken(t *testing.T, dir, name, token string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(token+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestPlanReloadSortsChanges(t *testing.T) {
	dir := t.TempDir()
	nousToken := writeToken(t, dir, "nous.token", strings.Repeat("n", 40))
	otherToken := writeToken(t, dir, "other.token", strings.Repeat("o", 40))
	base := `{
		"listeners":{"grpc_socket":"/tmp/botd.sock"},
		"state_dir":"` + dir + `",
		"apps":{
			"ibot":{"app_id":"cli_a","app_secret":"sec_a","channels":{"ops":"oc_ops"}},
			"nous":{"app_id":"cli_n","app_secret":"sec_n","commands":{"enabled":true}}
		},
		"agent_providers":{
			"nous":{"auth_token_file":"` + nousToken + `","allow_unmatched_messages":true},
			"gone":{"auth_token_file":"` + otherToken + `"}
		}
	}`
	running, err := loadMultiAppFixture(t, base)
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{nousToken, otherToken}; len(running.SourceFiles) != 3 || !reflect.DeepEqual(running.SourceFiles[:2], want) {
		t.Fatalf("source files = %v", running.SourceFiles)
	}

	same, err := loadMultiAppFixture(t, base)
	if err != nil {
		t.Fatal(err)
	}
	if plan := PlanReload(running, same); !plan.Empty() {
		t.Fatalf("identical config planned %+v", plan)
	}

	next, err := loadMultiAppFixture(t, `{
		"listeners":{"grpc_socket":"/tmp/other.sock"},
		"state_dir":"`+dir+`",
		"send_timeout_seconds":20,
		"apps":{
			"ibot":{"app_id":"cli_a","app_secret":"sec_a","channels":{"ops":"oc_ops","dev":"oc_dev"}},
			"nous":{"app_id":"cli_n","app_secret":"sec_n2","commands":{"enabled":true}},
			"commons":{"app_id":"cli_c","app_secret":"sec_c","commands":{"enabled":true}}
		},
		"agent_providers":{
			"nous":{"auth_token_file":"`+nousToken+`","allow_unmatched_messages":false},
			"commons":{"auth_token_file":"`+otherToken+`","allowed_apps":["commons"]}
		}
	}`)
	if err != nil {
		t.Fatal(err)
	}
	plan := PlanReload(running, next)
	want := ReloadPlan{
		AppsAdded:        []string{"commons"},
		AppsRecredited:   []string{"nous"},
		AppsUpdated:      []string{"ibot"},
		ProvidersAdded:   []string{"commons"},
		ProvidersRemoved: []string{"gone"},
		ProvidersChanged: []string{"nous"},
		RestartRequired:  []string{"listeners.grpc_socket"},
		SettingsChanged:  []string{"send_timeout_seconds"},
	}
	if !reflect.DeepEqual(plan, want) {
		t.Fatalf("plan = %+v\nwant %+v", plan, want)
	}
	if !plan.AppConnectionChanged("nous") || plan.AppConnectionChanged("ibot") {
		t.Fatalf("connection changes wrong: %+v", plan)
	}
	if !plan.ProviderReset("gone") || !plan.ProviderReset("nous") || plan.ProviderReset("commons") {
		t.Fatalf("provider resets wrong: %+v", plan)
	}

	applied := KeepRestartOnly(running, next)
	if applied.GRPCSocketPath != "/tmp/botd.sock" || applied.SendTimeout != next.SendTimeout {
		t.Fatalf("restart-only settings not kept: %+v", applied)
	}
}

func TestPlanReloadSeesTokenFileRotation(t *testing.T) {
	dir := t.TempDir()
	token := writeToken(t, dir, "nous.token", strings.Repeat("n", 40))
	body := `{
		"listeners":{"grpc_socket":"/tmp/botd.sock"},
		"apps":{"ibot":{"app_id":"cli_a","app_secret":"sec_a","commands":{"enabled":true}}},
		"agent_providers":{"nous":{"auth_token_file":"` + token + `"}}
	}`
	running, err := loadMultiAppFixture(t, body)
	if err != nil {
		t.Fatal(err)
	}
	writeToken(t, dir, "nous.token", strings.Repeat("m", 40))
	next, err := loadMultiAppFixture(t, body)
	if err != nil {
		t.Fatal(err)
	}
	plan := PlanReload(running, next)
	if !reflect.DeepEqual(plan.ProvidersChanged, []string{"nous"}) || plan.AppConnectionChanged("ibot") {
		t.Fatalf("plan = %+v", plan)
	}
}
