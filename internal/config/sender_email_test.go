package config

import (
	"os"
	"path/filepath"
	"testing"
)

func writeSenderEmailProviderConfig(t *testing.T, entry string) Config {
	t.Helper()
	clearConfigEnv(t)
	dir := t.TempDir()
	tokenPath := filepath.Join(dir, "agent-token")
	const token = "fixture-agent-token-0123456789abcdef0123456789"
	if err := os.WriteFile(tokenPath, []byte(token+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(dir, "feishu-botd.json")
	configJSON := `{
  "feishu": {"app_id":"app_fixture","app_secret":"secret_fixture"},
  "listeners": {"grpc_socket":"/tmp/feishu-botd.fixture.sock"},
  "commands": {"enabled":true},
  "agent_providers": {
    "fixture-agent": {
      "auth_token_file":"` + tokenPath + `",
      "allow_unmatched_messages":true` + entry + `
    }
  }
}`
	if err := os.WriteFile(configPath, []byte(configJSON), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("FEISHU_BOTD_CONFIG", configPath)
	cfg, err := LoadFromEnv()
	if err != nil {
		t.Fatalf("load provider config: %v", err)
	}
	return cfg
}

// A sender's email is personal data: only an explicit grant hands it over.
func TestLoadFromConfigFileAgentProviderSenderEmailGrant(t *testing.T) {
	for _, testCase := range []struct {
		name  string
		entry string
		want  bool
	}{
		{name: "omitted defaults to off", entry: ``, want: false},
		{name: "explicitly disabled", entry: `,"allow_sender_email":false`, want: false},
		{name: "explicitly enabled", entry: `,"allow_sender_email":true`, want: true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			cfg := writeSenderEmailProviderConfig(t, testCase.entry)
			if got := cfg.ProviderAllowsSenderEmail("fixture-agent"); got != testCase.want {
				t.Fatalf("ProviderAllowsSenderEmail = %t, want %t", got, testCase.want)
			}
			if got := cfg.AnyProviderAllowsSenderEmail(DefaultAppAlias); got != testCase.want {
				t.Fatalf("AnyProviderAllowsSenderEmail = %t, want %t", got, testCase.want)
			}
			if cfg.ProviderAllowsSenderEmail("unknown-provider") || cfg.ProviderAllowsSenderEmail("") {
				t.Fatal("an unconfigured provider was granted the sender email")
			}
		})
	}
}

func TestAllowSenderEmailDoesNotWidenItsNeighbours(t *testing.T) {
	cfg := writeSenderEmailProviderConfig(t, `,"allow_sender_email":true`)
	provider := cfg.AgentProviders["fixture-agent"]
	if provider.AllowCardActions || provider.AllowAttachedContext ||
		provider.AllowFollowUpMessages || provider.AllowMessageReactions ||
		provider.AllowLegacyCommands || provider.AllowCoTProgress || provider.AllowImageUpload {
		t.Fatalf("unrequested grants leaked: %#v", provider)
	}
}

// The daemon looks an address up only for an app some granted provider may
// see.
func TestAnyProviderAllowsSenderEmailHonoursAllowedApps(t *testing.T) {
	cfg := Config{AgentProviders: map[string]AgentProviderConfig{
		"granted": {AllowSenderEmail: true, AllowedApps: []string{"support"}, AllowedAppsConfigured: true},
		"other":   {},
	}}
	if !cfg.AnyProviderAllowsSenderEmail("support") {
		t.Fatal("support app should resolve sender emails")
	}
	if cfg.AnyProviderAllowsSenderEmail(DefaultAppAlias) {
		t.Fatal("the default app has no granted provider and should not resolve sender emails")
	}
}
