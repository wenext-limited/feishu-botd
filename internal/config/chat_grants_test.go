package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const (
	chatGrantTestUnconfiguredAlias = UnconfiguredGroupAliasPrefix +
		"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
)

// loadChatGrantConfig builds a two-app config — the legacy default app with
// the refund-backend and ops channels, and a named support app with its own
// channel — plus one provider whose extra JSON is the fragment under test.
func loadChatGrantConfig(t *testing.T, providerEntry, topLevelEntry string) (Config, error) {
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
  "apps": {
    "support": {
      "app_id":"app_support","app_secret":"secret_support",
      "commands": {"enabled":true,"allow_unconfigured_group_chats":true},
      "channels": {"support-room":"oc_support"}
    }
  },
  "listeners": {"grpc_socket":"/tmp/feishu-botd.fixture.sock"},
  "commands": {"enabled":true},
  "channels": {"refund_backend":"oc_refund","ops":"oc_ops"},
  "agent_providers": {
    "commons": {
      "auth_token_file":"` + tokenPath + `"` + providerEntry + `
    }
  }` + topLevelEntry + `
}`
	if err := os.WriteFile(configPath, []byte(configJSON), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("FEISHU_BOTD_CONFIG", configPath)
	return LoadFromEnv()
}

func TestChatGrantsDefaultToOff(t *testing.T) {
	cfg, err := loadChatGrantConfig(t, `,"allow_unmatched_messages":true`, ``)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	provider := cfg.AgentProviders["commons"]
	if provider.AllowAmbientMessages || provider.AllowProactiveMessages ||
		len(provider.AmbientChatAliases) != 0 || len(provider.ProactiveChatAliases) != 0 {
		t.Fatalf("chat grants were not off by default: %#v", provider)
	}
	if cfg.ProviderAllowsAmbientMessages("commons") || cfg.ProviderAllowsProactiveMessages("commons") {
		t.Fatal("an omitted grant was reported as allowed")
	}
	if cfg.ProviderAllowsAmbientChat("commons", "ops") || cfg.ProviderAllowsProactiveChat("commons", "ops") {
		t.Fatal("an omitted grant allowed a chat")
	}
	if cfg.AmbientChatEnabled(DefaultAppAlias, "ops") || cfg.AnyProviderAllowsProactiveMessages() {
		t.Fatal("an omitted grant enabled ambient ingress or route retention")
	}
}

func TestChatGrantsLoadNormalizeAndScopeToTheirAliases(t *testing.T) {
	cfg, err := loadChatGrantConfig(t, `,
      "allow_ambient_messages":true,
      "ambient_chat_aliases":[" Refund_Backend ","refund-backend","`+chatGrantTestUnconfiguredAlias+`"],
      "allow_proactive_messages":true,
      "proactive_chat_aliases":["refund-backend"]`,
		`,"state_dir":"/var/lib/feishu-botd"`)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	provider := cfg.AgentProviders["commons"]
	if got := strings.Join(provider.AmbientChatAliases, ","); got != "refund-backend,"+chatGrantTestUnconfiguredAlias {
		t.Fatalf("ambient aliases = %q", got)
	}
	if !cfg.ProviderAllowsAmbientChat("commons", "REFUND_BACKEND") || cfg.ProviderAllowsAmbientChat("commons", "ops") {
		t.Fatal("ambient grant did not match exactly its normalized aliases")
	}
	if !cfg.ProviderAllowsProactiveChat("commons", "refund-backend") || cfg.ProviderAllowsProactiveChat("commons", chatGrantTestUnconfiguredAlias) {
		t.Fatal("proactive grant leaked into the ambient-only alias")
	}
	if !cfg.AmbientChatEnabled(DefaultAppAlias, "refund-backend") || cfg.AmbientChatEnabled(DefaultAppAlias, "ops") {
		t.Fatal("ambient ingress predicate does not follow the grant")
	}
	if got := cfg.ProactiveProvidersForChat(DefaultAppAlias, "refund-backend"); len(got) != 1 || got[0] != "commons" {
		t.Fatalf("proactive providers = %v", got)
	}
	if !cfg.AnyProviderAllowsProactiveMessages() {
		t.Fatal("route retention was not enabled by a proactive grant")
	}
	if provider.AllowUnmatchedMessages || provider.AllowFollowUpMessages || provider.AllowAttachedContext {
		t.Fatalf("chat grants widened their neighbours: %#v", provider)
	}
}

func TestChatGrantsRejectInconsistentOrUnresolvableShapes(t *testing.T) {
	for _, testCase := range []struct {
		name      string
		entry     string
		topLevel  string
		wantError string
	}{
		{
			name:      "ambient flag without aliases",
			entry:     `,"allow_ambient_messages":true`,
			wantError: "allow_ambient_messages requires at least one ambient_chat_aliases",
		},
		{
			name:      "ambient aliases without flag",
			entry:     `,"ambient_chat_aliases":["ops"]`,
			wantError: "ambient_chat_aliases requires allow_ambient_messages",
		},
		{
			name:      "proactive aliases without flag",
			entry:     `,"proactive_chat_aliases":["ops"]`,
			wantError: "proactive_chat_aliases requires allow_proactive_messages",
		},
		{
			name:      "proactive requires state",
			entry:     `,"allow_proactive_messages":true,"proactive_chat_aliases":["ops"]`,
			wantError: "allow_proactive_messages requires state_dir",
		},
		{
			name:      "pins require the proactive grant",
			entry:     `,"allow_pins":true`,
			wantError: "allow_pins requires allow_proactive_messages",
		},
		{
			name:      "direct is not a group",
			entry:     `,"allow_ambient_messages":true,"ambient_chat_aliases":["direct"]`,
			wantError: "group chats only",
		},
		{
			name:      "unknown alias",
			entry:     `,"allow_ambient_messages":true,"ambient_chat_aliases":["nowhere"]`,
			wantError: `unknown channel alias "nowhere"`,
		},
		{
			name:      "empty alias",
			entry:     `,"allow_ambient_messages":true,"ambient_chat_aliases":[" "]`,
			wantError: "empty chat alias",
		},
		{
			name:      "alias outside allowed apps",
			entry:     `,"allowed_apps":["default"],"allow_proactive_messages":true,"proactive_chat_aliases":["support-room"]`,
			topLevel:  `,"state_dir":"/var/lib/feishu-botd"`,
			wantError: `outside its allowed_apps`,
		},
		{
			name:      "unconfigured alias without an app that admits unconfigured groups",
			entry:     `,"allowed_apps":["default"],"allow_ambient_messages":true,"ambient_chat_aliases":["` + chatGrantTestUnconfiguredAlias + `"]`,
			wantError: "allow_unconfigured_group_chats",
		},
		{
			name:      "malformed unconfigured alias is just unknown",
			entry:     `,"allow_ambient_messages":true,"ambient_chat_aliases":["unconfigured-group-xyz"]`,
			wantError: "unknown channel alias",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			_, err := loadChatGrantConfig(t, testCase.entry, testCase.topLevel)
			if err == nil || !strings.Contains(err.Error(), testCase.wantError) {
				t.Fatalf("error = %v, want substring %q", err, testCase.wantError)
			}
		})
	}
}

func TestChatGrantAccessorsDenyUnconfiguredProviders(t *testing.T) {
	cfg := Config{AgentProviders: map[string]AgentProviderConfig{
		"commons": {
			AllowAmbientMessages: true, AmbientChatAliases: []string{"ops"},
			AllowProactiveMessages: true, ProactiveChatAliases: []string{"ops"},
		},
	}}
	for _, provider := range []string{"someone-else", ""} {
		if cfg.ProviderAllowsAmbientChat(provider, "ops") || cfg.ProviderAllowsProactiveChat(provider, "ops") ||
			cfg.ProviderAllowsAmbientMessages(provider) || cfg.ProviderAllowsProactiveMessages(provider) {
			t.Fatalf("provider %q inherited another provider's chat grants", provider)
		}
	}
	if cfg.ProviderAllowsAmbientChat("commons", "") || cfg.ProviderAllowsProactiveChat("commons", " ") {
		t.Fatal("an empty alias matched a grant")
	}
}

func TestIsUnconfiguredGroupAliasRequiresTheExactDerivedShape(t *testing.T) {
	if !IsUnconfiguredGroupAlias(chatGrantTestUnconfiguredAlias) {
		t.Fatal("derived alias shape was rejected")
	}
	for _, alias := range []string{
		"unconfigured-group-",
		UnconfiguredGroupAliasPrefix + strings.Repeat("A", 64),
		UnconfiguredGroupAliasPrefix + strings.Repeat("0", 63),
		"ops",
	} {
		if IsUnconfiguredGroupAlias(alias) {
			t.Fatalf("alias %q was accepted", alias)
		}
	}
}

func TestPinGrantDefaultsOffAndLoadsBesideTheProactiveGrant(t *testing.T) {
	cfg, err := loadChatGrantConfig(t, `,"allow_proactive_messages":true,"proactive_chat_aliases":["ops"]`,
		`,"state_dir":"/var/lib/feishu-botd"`)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if cfg.AgentProviders["commons"].AllowPins || cfg.ProviderAllowsPins("commons") {
		t.Fatal("allow_pins was not off by default")
	}
	cfg, err = loadChatGrantConfig(t, `,"allow_proactive_messages":true,"proactive_chat_aliases":["ops"],"allow_pins":true`,
		`,"state_dir":"/var/lib/feishu-botd"`)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if !cfg.ProviderAllowsPins("commons") || cfg.ProviderAllowsPins("someone-else") || cfg.ProviderAllowsPins("") {
		t.Fatal("allow_pins did not scope to its provider")
	}
}
