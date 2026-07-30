package config

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func TestLoadChannelsFromEnvForms(t *testing.T) {
	channels := loadChannels([]string{
		"FEISHU_BOTD_CHANNELS_OPS=oc_ops",
		"FEISHU_BOTD_CHANNELS_ON_CALL=oc_oncall",
		"IGNORED=value",
	})
	if got := channels["ops"]; got != "oc_ops" {
		t.Fatalf("ops channel = %q", got)
	}
	if got := channels["on-call"]; got != "oc_oncall" {
		t.Fatalf("on-call channel = %q", got)
	}
}

func TestLoadChannelsFromCommaList(t *testing.T) {
	t.Setenv("FEISHU_BOTD_CHANNELS", "ops=oc_ops, platform = oc_platform, bad-entry")
	channels := loadChannels(nil)
	if got := channels["ops"]; got != "oc_ops" {
		t.Fatalf("ops channel = %q", got)
	}
	if got := channels["platform"]; got != "oc_platform" {
		t.Fatalf("platform channel = %q", got)
	}
	if _, ok := channels["bad-entry"]; ok {
		t.Fatal("malformed channel entry was accepted")
	}
}

func TestValidateLoopbackBind(t *testing.T) {
	for _, addr := range []string{"127.0.0.1:7345", "localhost:7345", "[::1]:7345"} {
		if err := validateLoopbackBind("FEISHU_BOTD_BIND", addr); err != nil {
			t.Fatalf("%s rejected: %v", addr, err)
		}
	}
	for _, addr := range []string{"0.0.0.0:7345", "192.0.2.10:7345", "missing-port"} {
		if err := validateLoopbackBind("FEISHU_BOTD_BIND", addr); err == nil {
			t.Fatalf("%s accepted unexpectedly", addr)
		}
	}
	// The error must name the offending variable, not a hardcoded one.
	if err := validateLoopbackBind("FEISHU_BOTD_GRPC_BIND", "0.0.0.0:7346"); err == nil || !strings.Contains(err.Error(), "FEISHU_BOTD_GRPC_BIND") {
		t.Fatalf("expected error naming FEISHU_BOTD_GRPC_BIND, got %v", err)
	}
}

func TestValidateTCPBindAllowsNonLoopbackWithOptIn(t *testing.T) {
	for _, addr := range []string{"0.0.0.0:7345", ":7345", "192.0.2.10:7345"} {
		if err := validateTCPBind("FEISHU_BOTD_BIND", addr, true); err != nil {
			t.Fatalf("%s rejected with opt-in: %v", addr, err)
		}
	}
}

func TestValidatePlaintextGRPCBindIsAlwaysLoopbackOnly(t *testing.T) {
	for _, addr := range []string{"127.0.0.1:7346", "localhost:7346", "[::1]:7346"} {
		if err := validatePlaintextGRPCBind("FEISHU_BOTD_GRPC_BIND", addr); err != nil {
			t.Fatalf("%s rejected: %v", addr, err)
		}
	}
	for _, addr := range []string{"0.0.0.0:7346", ":7346", "[::]:7346", "192.0.2.10:7346", "missing-port"} {
		if err := validatePlaintextGRPCBind("FEISHU_BOTD_GRPC_BIND", addr); err == nil {
			t.Fatalf("%s accepted unexpectedly", addr)
		}
	}
}

func TestReadTokenFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(path, []byte("abc.DEF_123\nignored"), 0o600); err != nil {
		t.Fatal(err)
	}
	token, err := readTokenFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if token != "abc.DEF_123" {
		t.Fatalf("token = %q", token)
	}
}

func TestReadTokenFileRejectsInvalidToken(t *testing.T) {
	path := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(path, []byte("bad token\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := readTokenFile(path); err == nil {
		t.Fatal("invalid token accepted")
	}
}

// setBaseEnv configures the always-required vars and clears every listener var
// so each test starts from a known state regardless of the host environment.
func setBaseEnv(t *testing.T) {
	t.Helper()
	t.Setenv("FEISHU_BOTD_CONFIG", "")
	t.Setenv("FEISHU_APP_ID", "cli_test")
	t.Setenv("FEISHU_APP_SECRET", "secret")
	t.Setenv("FEISHU_BOTD_CHANNELS_OPS", "oc_test")
	t.Setenv("FEISHU_BOTD_SOCKET", "")
	t.Setenv("FEISHU_BOTD_BIND", "")
	t.Setenv("FEISHU_BOTD_GRPC_SOCKET", "")
	t.Setenv("FEISHU_BOTD_GRPC_BIND", "")
	t.Setenv("FEISHU_BOTD_AUTH_TOKEN_FILE", "")
	t.Setenv("FEISHU_BOTD_ALLOW_NON_LOOPBACK_BIND", "")
	t.Setenv("FEISHU_BOTD_COMMANDS_ENABLED", "")
	t.Setenv("FEISHU_BOTD_BOT_OPEN_ID", "")
	t.Setenv("FEISHU_BOTD_BOT_USER_ID", "")
	t.Setenv("FEISHU_BOTD_BOT_UNION_ID", "")
	t.Setenv("FEISHU_BOTD_BOT_NAMES", "")
	t.Setenv("FEISHU_BOTD_SCRIPTS_ENABLED", "")
	t.Setenv("FEISHU_BOTD_SCRIPTS_COMMAND", "")
	t.Setenv("FEISHU_BOTD_SCRIPTS_DIR", "")
	t.Setenv("FEISHU_BOTD_SCRIPTS_ALLOWED_CHATS", "")
	t.Setenv("FEISHU_BOTD_SCRIPTS_TIMEOUT_SECONDS", "")
}

func clearConfigEnv(t *testing.T) {
	t.Helper()
	for _, name := range []string{
		"FEISHU_BOTD_CONFIG",
		"FEISHU_APP_ID",
		"FEISHU_APP_SECRET",
		"FEISHU_BOTD_SOCKET",
		"FEISHU_BOTD_BIND",
		"FEISHU_BOTD_GRPC_SOCKET",
		"FEISHU_BOTD_GRPC_BIND",
		"FEISHU_BOTD_AUTH_TOKEN_FILE",
		"FEISHU_BOTD_ALLOW_NON_LOOPBACK_BIND",
		"FEISHU_BOTD_COMMANDS_ENABLED",
		"FEISHU_BOTD_BOT_OPEN_ID",
		"FEISHU_BOTD_BOT_USER_ID",
		"FEISHU_BOTD_BOT_UNION_ID",
		"FEISHU_BOTD_BOT_NAMES",
		"FEISHU_BOTD_SCRIPTS_ENABLED",
		"FEISHU_BOTD_SCRIPTS_COMMAND",
		"FEISHU_BOTD_SCRIPTS_DIR",
		"FEISHU_BOTD_SCRIPTS_ALLOWED_CHATS",
		"FEISHU_BOTD_SCRIPTS_TIMEOUT_SECONDS",
		"FEISHU_BOTD_CHANNELS",
		"FEISHU_BOTD_CHANNELS_OPS",
		"FEISHU_BOTD_DEFAULT_CHANNEL",
		"FEISHU_BOTD_DEDUPE_TTL_SECONDS",
		"FEISHU_BOTD_SEND_TIMEOUT_SECONDS",
	} {
		t.Setenv(name, "")
	}
}

func TestLoadFromEnvGRPCSocketSatisfiesListenerRequirement(t *testing.T) {
	setBaseEnv(t)
	t.Setenv("FEISHU_BOTD_GRPC_SOCKET", "/tmp/feishu-botd.grpc.sock")
	cfg, err := LoadFromEnv()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.GRPCSocketPath != "/tmp/feishu-botd.grpc.sock" {
		t.Fatalf("grpc socket = %q", cfg.GRPCSocketPath)
	}
}

func TestLoadFromEnvAllowsDirectMessageOnlyAgentWithoutChannels(t *testing.T) {
	clearConfigEnv(t)
	t.Setenv("FEISHU_APP_ID", "app_test")
	t.Setenv("FEISHU_APP_SECRET", "test-placeholder")
	t.Setenv("FEISHU_BOTD_GRPC_SOCKET", filepath.Join(t.TempDir(), "botd.sock"))
	t.Setenv("FEISHU_BOTD_COMMANDS_ENABLED", "true")

	cfg, err := LoadFromEnv()
	if err != nil {
		t.Fatalf("load direct-message-only agent config: %v", err)
	}
	if !cfg.Commands.Enabled || len(cfg.Channels) != 0 {
		t.Fatalf("unexpected direct-message-only routing: commands=%t channels=%d", cfg.Commands.Enabled, len(cfg.Channels))
	}
}

func TestLoadFromEnvNoListenerFails(t *testing.T) {
	setBaseEnv(t)
	if _, err := LoadFromEnv(); err == nil {
		t.Fatal("expected error when no listener is configured")
	}
}

func TestLoadFromEnvGRPCBindRequiresTokenFile(t *testing.T) {
	setBaseEnv(t)
	t.Setenv("FEISHU_BOTD_GRPC_BIND", "127.0.0.1:7346")
	if _, err := LoadFromEnv(); err == nil {
		t.Fatal("expected error when grpc TCP bind is set without an auth token file")
	}

	tokenPath := filepath.Join(t.TempDir(), "token")
	const token = "0123456789abcdef0123456789abcdef"
	if err := os.WriteFile(tokenPath, []byte(token+"\nignored"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("FEISHU_BOTD_AUTH_TOKEN_FILE", tokenPath)
	cfg, err := LoadFromEnv()
	if err != nil {
		t.Fatalf("load with token: %v", err)
	}
	if cfg.AuthToken != token {
		t.Fatal("general bearer did not use the token file's first line")
	}
}

func TestLoadFromEnvRejectsWeakGeneralBearerWithoutDisclosure(t *testing.T) {
	setBaseEnv(t)
	t.Setenv("FEISHU_BOTD_GRPC_BIND", "127.0.0.1:7346")
	tokenPath := filepath.Join(t.TempDir(), "token")
	const weakToken = "weak-general-bearer-fixture"
	if err := os.WriteFile(tokenPath, []byte(weakToken+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("FEISHU_BOTD_AUTH_TOKEN_FILE", tokenPath)

	_, err := LoadFromEnv()
	if err == nil || !strings.Contains(err.Error(), "general bearer token must be at least 32 bytes") {
		t.Fatalf("expected minimum-length error, got %v", err)
	}
	if strings.Contains(err.Error(), weakToken) {
		t.Fatal("configuration error exposed the general bearer token")
	}
}

func TestLoadFromEnvRejectsNonLoopbackPlaintextGRPCEvenWithLANOptIn(t *testing.T) {
	setBaseEnv(t)
	tokenPath := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenPath, []byte("fixture-general-token-0123456789abcdef01234567\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("FEISHU_BOTD_GRPC_BIND", "0.0.0.0:7346")
	t.Setenv("FEISHU_BOTD_ALLOW_NON_LOOPBACK_BIND", "true")
	t.Setenv("FEISHU_BOTD_AUTH_TOKEN_FILE", tokenPath)

	_, err := LoadFromEnv()
	if err == nil || !strings.Contains(err.Error(), "plaintext") || !strings.Contains(err.Error(), "loopback") {
		t.Fatalf("expected plaintext loopback error, got %v", err)
	}
}

func TestLoadFromConfigFileLoadsScopedAgentProviderToken(t *testing.T) {
	clearConfigEnv(t)
	dir := t.TempDir()
	tokenPath := filepath.Join(dir, "agent-token")
	const token = "fixture-agent-token-0123456789abcdef0123456789"
	const generalToken = "fixture-general-token-0123456789abcdef01234567"
	if err := os.WriteFile(tokenPath, []byte(token+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	generalTokenPath := filepath.Join(dir, "general-token")
	if err := os.WriteFile(generalTokenPath, []byte(generalToken+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(dir, "feishu-botd.json")
	configJSON := `{
  "feishu": {"app_id":"app_fixture","app_secret":"secret_fixture"},
  "listeners": {
    "grpc_socket":"/tmp/feishu-botd.fixture.sock",
    "auth_token_file":"` + generalTokenPath + `"
  },
  "commands": {"enabled":true},
  "agent_providers": {
    "fixture-agent": {
      "auth_token_file":"` + tokenPath + `",
      "allowed_commands":[" /Ask ", "ask"],
      "allow_unmatched_messages":true,
      "allow_card_actions":true
    }
  }
}`
	if err := os.WriteFile(configPath, []byte(configJSON), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("FEISHU_BOTD_CONFIG", configPath)

	cfg, err := LoadFromEnv()
	if err != nil {
		t.Fatalf("load provider token: %v", err)
	}
	if got := cfg.AgentProviders["fixture-agent"].AuthToken; got != token {
		t.Fatal("configured provider token was not loaded")
	}
	if cfg.AuthToken != generalToken {
		t.Fatal("scoped Unix general token was not loaded")
	}
	provider := cfg.AgentProviders["fixture-agent"]
	if strings.Join(provider.AllowedCommands, ",") != "ask" || !provider.AllowUnmatchedMessages || !provider.AllowCardActions || provider.AllowLegacyCommands {
		t.Fatalf("unexpected provider scope: commands=%q unmatched=%t actions=%t legacy=%t",
			provider.AllowedCommands, provider.AllowUnmatchedMessages, provider.AllowCardActions, provider.AllowLegacyCommands)
	}
}

// TestLoadFromConfigFileAgentProviderFollowUpScope pins the explicit-allowlist
// posture: sending a later, unprompted message into a conversation is off until
// the operator turns it on for that provider.
func TestLoadFromConfigFileAgentProviderFollowUpScope(t *testing.T) {
	for _, testCase := range []struct {
		name  string
		entry string
		want  bool
	}{
		{name: "omitted defaults to off", entry: "", want: false},
		{name: "explicitly disabled", entry: `,"allow_follow_up_messages":false`, want: false},
		{name: "explicitly enabled", entry: `,"allow_follow_up_messages":true`, want: true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
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
      "allow_unmatched_messages":true` + testCase.entry + `
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
			if got := cfg.AgentProviders["fixture-agent"].AllowFollowUpMessages; got != testCase.want {
				t.Fatalf("allow_follow_up_messages = %t, want %t", got, testCase.want)
			}
		})
	}
}

func TestLoadFromConfigFileLoadsGeneralTokenForScopedUnixHTTP(t *testing.T) {
	clearConfigEnv(t)
	dir := t.TempDir()
	const (
		providerToken = "fixture-agent-token-0123456789abcdef0123456789"
		generalToken  = "fixture-general-token-0123456789abcdef01234567"
	)
	providerTokenPath := filepath.Join(dir, "agent-token")
	if err := os.WriteFile(providerTokenPath, []byte(providerToken+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	generalTokenPath := filepath.Join(dir, "general-token")
	if err := os.WriteFile(generalTokenPath, []byte(generalToken+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(dir, "feishu-botd.json")
	configJSON := `{
  "feishu":{"app_id":"app_fixture","app_secret":"secret_fixture"},
  "listeners":{
    "http_socket":"/tmp/feishu-botd.fixture.http.sock",
    "auth_token_file":"` + generalTokenPath + `"
  },
  "commands":{"enabled":true},
  "agent_providers":{
    "fixture-agent":{"auth_token_file":"` + providerTokenPath + `"}
  }
}`
	if err := os.WriteFile(configPath, []byte(configJSON), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("FEISHU_BOTD_CONFIG", configPath)

	cfg, err := LoadFromEnv()
	if err != nil {
		t.Fatalf("load scoped Unix HTTP config: %v", err)
	}
	if cfg.SocketPath != "/tmp/feishu-botd.fixture.http.sock" {
		t.Fatalf("HTTP socket = %q", cfg.SocketPath)
	}
	if cfg.AuthToken != generalToken {
		t.Fatal("scoped Unix HTTP general token was not loaded")
	}
}

func TestAgentProviderStateTTLIncludesMessageUUIDRetryWindow(t *testing.T) {
	const token = "fixture-agent-token-0123456789abcdef0123456789"
	for _, test := range []struct {
		name       string
		dedupeSecs int
		wantError  bool
	}{
		{name: "shorter than window and send budget", dedupeSecs: 3600, wantError: true},
		{name: "window plus send budget", dedupeSecs: 3615},
	} {
		t.Run(test.name, func(t *testing.T) {
			clearConfigEnv(t)
			dir := t.TempDir()
			tokenPath := filepath.Join(dir, "agent-token")
			if err := os.WriteFile(tokenPath, []byte(token+"\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			configPath := filepath.Join(dir, "feishu-botd.json")
			configJSON := `{
  "feishu":{"app_id":"app_fixture","app_secret":"secret_fixture"},
  "listeners":{"grpc_socket":"/tmp/feishu-botd.fixture.sock"},
  "commands":{"enabled":true},
  "agent_providers":{"fixture-agent":{"auth_token_file":"` + tokenPath + `"}},
  "dedupe_ttl_seconds":` + strconv.Itoa(test.dedupeSecs) + `,
  "send_timeout_seconds":15
}`
			if err := os.WriteFile(configPath, []byte(configJSON), 0o600); err != nil {
				t.Fatal(err)
			}
			t.Setenv("FEISHU_BOTD_CONFIG", configPath)

			_, err := LoadFromEnv()
			if test.wantError {
				if err == nil || !strings.Contains(err.Error(), "one hour plus send timeout") {
					t.Fatalf("expected UUID retry-window error, got %v", err)
				}
			} else if err != nil {
				t.Fatalf("load boundary TTL: %v", err)
			}
		})
	}
}

func TestLoadFromConfigFileRejectsWeakDuplicateAndGeneralTokenCollisions(t *testing.T) {
	const strongToken = "fixture-agent-token-0123456789abcdef0123456789"
	tests := []struct {
		name        string
		tokens      []string
		grpcTCP     bool
		generalAuth bool
		want        string
	}{
		{name: "weak", tokens: []string{"too-short"}, want: "at least 32 bytes"},
		{name: "duplicate", tokens: []string{strongToken, strongToken}, want: "distinct auth tokens"},
		{name: "TCP general collision", tokens: []string{strongToken}, grpcTCP: true, generalAuth: true, want: "general bearer token"},
		{name: "scoped Unix general collision", tokens: []string{strongToken}, generalAuth: true, want: "general bearer token"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			clearConfigEnv(t)
			dir := t.TempDir()
			firstTokenPath := filepath.Join(dir, "provider-a-token")
			if err := os.WriteFile(firstTokenPath, []byte(test.tokens[0]+"\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			providers := `"provider-a":{"auth_token_file":"` + firstTokenPath + `"}`
			if len(test.tokens) > 1 {
				secondTokenPath := filepath.Join(dir, "provider-b-token")
				if err := os.WriteFile(secondTokenPath, []byte(test.tokens[1]+"\n"), 0o600); err != nil {
					t.Fatal(err)
				}
				providers += `,"provider-b":{"auth_token_file":"` + secondTokenPath + `"}`
			}
			listener := `"grpc_socket":"/tmp/feishu-botd.fixture.sock"`
			if test.grpcTCP {
				listener = `"grpc_bind":"127.0.0.1:7346"`
			}
			if test.generalAuth {
				listener += `,"auth_token_file":"` + firstTokenPath + `"`
			}
			configPath := filepath.Join(dir, "feishu-botd.json")
			configJSON := `{"feishu":{"app_id":"app_fixture","app_secret":"secret_fixture"},"listeners":{` + listener + `},"commands":{"enabled":true},"agent_providers":{` + providers + `}}`
			if err := os.WriteFile(configPath, []byte(configJSON), 0o600); err != nil {
				t.Fatal(err)
			}
			t.Setenv("FEISHU_BOTD_CONFIG", configPath)

			_, err := LoadFromEnv()
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("expected %q error, got %v", test.want, err)
			}
			for _, token := range test.tokens {
				if strings.Contains(err.Error(), token) {
					t.Fatal("configuration error exposed provider token")
				}
			}
		})
	}
}

func TestLoadFromEnvLANBindRequiresOptInAndToken(t *testing.T) {
	setBaseEnv(t)
	t.Setenv("FEISHU_BOTD_BIND", "0.0.0.0:7345")
	if _, err := LoadFromEnv(); err == nil || !strings.Contains(err.Error(), "FEISHU_BOTD_ALLOW_NON_LOOPBACK_BIND") {
		t.Fatalf("expected non-loopback opt-in error, got %v", err)
	}

	t.Setenv("FEISHU_BOTD_ALLOW_NON_LOOPBACK_BIND", "true")
	if _, err := LoadFromEnv(); err == nil || !strings.Contains(err.Error(), "FEISHU_BOTD_AUTH_TOKEN_FILE") {
		t.Fatalf("expected token-file error after opt-in, got %v", err)
	}

	tokenPath := filepath.Join(t.TempDir(), "token")
	const token = "fixture-lan-token-0123456789abcdef0123456789"
	if err := os.WriteFile(tokenPath, []byte(token+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("FEISHU_BOTD_AUTH_TOKEN_FILE", tokenPath)
	cfg, err := LoadFromEnv()
	if err != nil {
		t.Fatalf("load with LAN bind: %v", err)
	}
	if !cfg.AllowLANBind || cfg.BindAddr != "0.0.0.0:7345" || cfg.AuthToken != token {
		t.Fatal("LAN listener did not load the configured strong bearer")
	}
}

func TestLoadFromConfigFile(t *testing.T) {
	clearConfigEnv(t)
	dir := t.TempDir()
	tokenPath := filepath.Join(dir, "token")
	const token = "fixture-file-token-0123456789abcdef012345678"
	if err := os.WriteFile(tokenPath, []byte(token+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(dir, "feishu-botd.json")
	configJSON := `{
  "feishu": {
    "app_id": "cli_file",
    "app_secret": "file-secret"
  },
	  "listeners": {
	    "http_bind": "0.0.0.0:7345",
	    "auth_token_file": "` + tokenPath + `",
	    "allow_non_loopback_bind": true
	  },
	  "commands": {
	    "enabled": true,
	    "bot_open_id": "ou_bot",
	    "bot_names": ["BuildBot", " buildbot "]
	  },
	  "channels": {
	    "ci": "oc_ci",
    "ops": "oc_ops"
  },
  "default_channel": "ops",
  "services": {
    "jenkins": { "default_channel": "ci" }
  },
  "dedupe_ttl_seconds": 60,
  "send_timeout_seconds": 7
}`
	if err := os.WriteFile(configPath, []byte(configJSON), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("FEISHU_BOTD_CONFIG", configPath)

	cfg, err := LoadFromEnv()
	if err != nil {
		t.Fatalf("load config file: %v", err)
	}
	if cfg.AppID != "cli_file" || cfg.AppSecret != "file-secret" {
		t.Fatalf("feishu config = %#v", cfg)
	}
	if cfg.BindAddr != "0.0.0.0:7345" || !cfg.AllowLANBind || cfg.AuthToken != token {
		t.Fatal("listener config did not load the configured strong bearer")
	}
	if cfg.Channels["ci"] != "oc_ci" || cfg.DefaultChannel != "ops" || cfg.Services["jenkins"].DefaultChannel != "ci" {
		t.Fatalf("routing config = %#v", cfg)
	}
	if !cfg.Commands.Enabled || cfg.Commands.BotOpenID != "ou_bot" || len(cfg.Commands.BotNames) != 1 || cfg.Commands.BotNames[0] != "BuildBot" {
		t.Fatalf("command config = %#v", cfg.Commands)
	}
}

func TestEnvOverridesConfigFile(t *testing.T) {
	clearConfigEnv(t)
	dir := t.TempDir()
	configPath := filepath.Join(dir, "feishu-botd.json")
	configJSON := `{
  "feishu": { "app_id": "cli_file", "app_secret": "file-secret" },
  "listeners": { "http_socket": "/tmp/file.sock" },
  "channels": { "ci": "oc_file" },
  "default_channel": "ci"
}`
	if err := os.WriteFile(configPath, []byte(configJSON), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("FEISHU_BOTD_CONFIG", configPath)
	t.Setenv("FEISHU_APP_ID", "cli_env")
	t.Setenv("FEISHU_BOTD_CHANNELS_CI", "oc_env")
	t.Setenv("FEISHU_BOTD_DEFAULT_CHANNEL", "ci")

	cfg, err := LoadFromEnv()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.AppID != "cli_env" {
		t.Fatalf("env app id did not override file: %#v", cfg)
	}
	if cfg.Channels["ci"] != "oc_env" {
		t.Fatalf("env channel did not override file: %#v", cfg.Channels)
	}
}

func TestCommandConfigEnvOverridesFile(t *testing.T) {
	clearConfigEnv(t)
	configPath := filepath.Join(t.TempDir(), "feishu-botd.json")
	configJSON := `{
  "feishu": { "app_id": "cli_file", "app_secret": "file-secret" },
  "listeners": { "http_socket": "/tmp/file.sock" },
  "commands": {
    "enabled": false,
    "bot_open_id": "ou_file",
    "bot_names": ["FileBot"]
  },
  "channels": { "ops": "oc_ops" }
}`
	if err := os.WriteFile(configPath, []byte(configJSON), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("FEISHU_BOTD_CONFIG", configPath)
	t.Setenv("FEISHU_BOTD_COMMANDS_ENABLED", "true")
	t.Setenv("FEISHU_BOTD_BOT_OPEN_ID", "ou_env")
	t.Setenv("FEISHU_BOTD_BOT_NAMES", "EnvBot, envbot , OtherBot")

	cfg, err := LoadFromEnv()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if !cfg.Commands.Enabled || cfg.Commands.BotOpenID != "ou_env" {
		t.Fatalf("command env scalar override failed: %#v", cfg.Commands)
	}
	if got := strings.Join(cfg.Commands.BotNames, ","); got != "EnvBot,OtherBot" {
		t.Fatalf("bot names = %q", got)
	}
}

func TestLoadFromConfigFileRejectsUnknownServiceChannel(t *testing.T) {
	clearConfigEnv(t)
	configPath := filepath.Join(t.TempDir(), "feishu-botd.json")
	configJSON := `{
  "feishu": { "app_id": "cli_file", "app_secret": "file-secret" },
  "listeners": { "http_socket": "/tmp/file.sock" },
  "channels": { "ci": "oc_ci" },
  "services": { "jenkins": { "default_channel": "missing" } }
}`
	if err := os.WriteFile(configPath, []byte(configJSON), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("FEISHU_BOTD_CONFIG", configPath)
	if _, err := LoadFromEnv(); err == nil || !strings.Contains(err.Error(), "jenkins") {
		t.Fatalf("expected service channel validation error, got %v", err)
	}
}

func TestLoadFromConfigFileRejectsUnknownFields(t *testing.T) {
	clearConfigEnv(t)
	configPath := filepath.Join(t.TempDir(), "feishu-botd.json")
	if err := os.WriteFile(configPath, []byte(`{"feishu":{"app_id":"cli","app_secret":"secret"},"typo":true}`), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("FEISHU_BOTD_CONFIG", configPath)
	if _, err := LoadFromEnv(); err == nil || !strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("expected unknown field error, got %v", err)
	}
}

func TestNormalizeScriptExecConfig(t *testing.T) {
	got := normalizeCommandConfig(CommandConfig{
		Enabled: true,
		Scripts: ScriptExecConfig{
			Enabled:      true,
			Command:      " PLS ",
			Dir:          " /opt/scripts ",
			AllowedChats: []string{" CI ", "ci", "Ops"},
		},
	})
	if got.Scripts.Command != "pls" {
		t.Fatalf("command = %q", got.Scripts.Command)
	}
	if got.Scripts.Dir != "/opt/scripts" {
		t.Fatalf("dir = %q", got.Scripts.Dir)
	}
	if strings.Join(got.Scripts.AllowedChats, ",") != "ci,ops" {
		t.Fatalf("allowed chats = %#v", got.Scripts.AllowedChats)
	}
	if got.Scripts.TimeoutSeconds != 120 {
		t.Fatalf("default timeout = %d, want 120", got.Scripts.TimeoutSeconds)
	}
}

func TestNormalizeScriptExecConfigKeepsExplicitTimeout(t *testing.T) {
	got := normalizeCommandConfig(CommandConfig{Scripts: ScriptExecConfig{TimeoutSeconds: 45}})
	if got.Scripts.TimeoutSeconds != 45 {
		t.Fatalf("timeout = %d, want 45", got.Scripts.TimeoutSeconds)
	}
}

func TestScriptsEnvOverridesFile(t *testing.T) {
	clearConfigEnv(t)
	scriptsDir := t.TempDir()
	configPath := filepath.Join(t.TempDir(), "feishu-botd.json")
	configJSON := `{
  "feishu": { "app_id": "cli_file", "app_secret": "file-secret" },
  "listeners": { "http_socket": "/tmp/file.sock" },
  "commands": {
    "enabled": true,
    "scripts": { "enabled": false, "command": "file-cmd", "dir": "` + scriptsDir + `", "allowed_chats": ["ops"] }
  },
  "channels": { "ops": "oc_ops" }
}`
	if err := os.WriteFile(configPath, []byte(configJSON), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("FEISHU_BOTD_CONFIG", configPath)
	t.Setenv("FEISHU_BOTD_SCRIPTS_ENABLED", "true")
	t.Setenv("FEISHU_BOTD_SCRIPTS_COMMAND", "pls")
	t.Setenv("FEISHU_BOTD_SCRIPTS_ALLOWED_CHATS", "ops, ops")
	t.Setenv("FEISHU_BOTD_SCRIPTS_TIMEOUT_SECONDS", "30")

	cfg, err := LoadFromEnv()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	s := cfg.Commands.Scripts
	if !s.Enabled || s.Command != "pls" || s.Dir != scriptsDir {
		t.Fatalf("script config = %#v", s)
	}
	if strings.Join(s.AllowedChats, ",") != "ops" {
		t.Fatalf("allowed chats = %#v", s.AllowedChats)
	}
	if s.TimeoutSeconds != 30 {
		t.Fatalf("timeout = %d, want 30", s.TimeoutSeconds)
	}
}

func TestScriptsEnvDirOverride(t *testing.T) {
	setBaseEnv(t)
	t.Setenv("FEISHU_BOTD_GRPC_SOCKET", "/tmp/feishu-botd.grpc.sock")
	t.Setenv("FEISHU_BOTD_COMMANDS_ENABLED", "true")
	scriptsDir := t.TempDir()
	t.Setenv("FEISHU_BOTD_SCRIPTS_ENABLED", "true")
	t.Setenv("FEISHU_BOTD_SCRIPTS_COMMAND", "pls")
	t.Setenv("FEISHU_BOTD_SCRIPTS_DIR", scriptsDir)
	t.Setenv("FEISHU_BOTD_SCRIPTS_ALLOWED_CHATS", "ops")

	cfg, err := LoadFromEnv()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.Commands.Scripts.Dir != scriptsDir {
		t.Fatalf("dir = %q, want %q", cfg.Commands.Scripts.Dir, scriptsDir)
	}
}

func TestLoadFromEnvScriptsRequiresCommandsEnabled(t *testing.T) {
	setBaseEnv(t)
	t.Setenv("FEISHU_BOTD_GRPC_SOCKET", "/tmp/feishu-botd.grpc.sock")
	t.Setenv("FEISHU_BOTD_COMMANDS_ENABLED", "false")
	t.Setenv("FEISHU_BOTD_SCRIPTS_ENABLED", "true")
	t.Setenv("FEISHU_BOTD_SCRIPTS_COMMAND", "pls")
	t.Setenv("FEISHU_BOTD_SCRIPTS_DIR", t.TempDir())
	t.Setenv("FEISHU_BOTD_SCRIPTS_ALLOWED_CHATS", "ops")
	if _, err := LoadFromEnv(); err == nil || !strings.Contains(err.Error(), "commands.enabled") {
		t.Fatalf("expected commands.enabled error, got %v", err)
	}
}

func TestLoadFromEnvScriptsRequiresCommand(t *testing.T) {
	setBaseEnv(t)
	t.Setenv("FEISHU_BOTD_GRPC_SOCKET", "/tmp/feishu-botd.grpc.sock")
	t.Setenv("FEISHU_BOTD_COMMANDS_ENABLED", "true")
	t.Setenv("FEISHU_BOTD_SCRIPTS_ENABLED", "true")
	t.Setenv("FEISHU_BOTD_SCRIPTS_DIR", t.TempDir())
	t.Setenv("FEISHU_BOTD_SCRIPTS_ALLOWED_CHATS", "ops")
	if _, err := LoadFromEnv(); err == nil || !strings.Contains(err.Error(), "scripts.command") {
		t.Fatalf("expected scripts.command error, got %v", err)
	}
}

func TestLoadFromEnvScriptsRequiresDir(t *testing.T) {
	setBaseEnv(t)
	t.Setenv("FEISHU_BOTD_GRPC_SOCKET", "/tmp/feishu-botd.grpc.sock")
	t.Setenv("FEISHU_BOTD_COMMANDS_ENABLED", "true")
	t.Setenv("FEISHU_BOTD_SCRIPTS_ENABLED", "true")
	t.Setenv("FEISHU_BOTD_SCRIPTS_COMMAND", "pls")
	t.Setenv("FEISHU_BOTD_SCRIPTS_ALLOWED_CHATS", "ops")
	if _, err := LoadFromEnv(); err == nil || !strings.Contains(err.Error(), "scripts.dir") {
		t.Fatalf("expected scripts.dir error, got %v", err)
	}
}

func TestLoadFromEnvScriptsRequiresExistingDir(t *testing.T) {
	setBaseEnv(t)
	t.Setenv("FEISHU_BOTD_GRPC_SOCKET", "/tmp/feishu-botd.grpc.sock")
	t.Setenv("FEISHU_BOTD_COMMANDS_ENABLED", "true")
	t.Setenv("FEISHU_BOTD_SCRIPTS_ENABLED", "true")
	t.Setenv("FEISHU_BOTD_SCRIPTS_COMMAND", "pls")
	t.Setenv("FEISHU_BOTD_SCRIPTS_DIR", filepath.Join(t.TempDir(), "missing"))
	t.Setenv("FEISHU_BOTD_SCRIPTS_ALLOWED_CHATS", "ops")
	if _, err := LoadFromEnv(); err == nil || !strings.Contains(err.Error(), "scripts.dir") {
		t.Fatalf("expected scripts.dir directory error, got %v", err)
	}
}

func TestLoadFromEnvScriptsRequiresAllowedChats(t *testing.T) {
	setBaseEnv(t)
	t.Setenv("FEISHU_BOTD_GRPC_SOCKET", "/tmp/feishu-botd.grpc.sock")
	t.Setenv("FEISHU_BOTD_COMMANDS_ENABLED", "true")
	t.Setenv("FEISHU_BOTD_SCRIPTS_ENABLED", "true")
	t.Setenv("FEISHU_BOTD_SCRIPTS_COMMAND", "pls")
	t.Setenv("FEISHU_BOTD_SCRIPTS_DIR", t.TempDir())
	if _, err := LoadFromEnv(); err == nil || !strings.Contains(err.Error(), "allowed_chats") {
		t.Fatalf("expected allowed_chats error, got %v", err)
	}
}

func TestLoadFromEnvScriptsAllowedChatMustBeConfiguredChannel(t *testing.T) {
	setBaseEnv(t)
	t.Setenv("FEISHU_BOTD_GRPC_SOCKET", "/tmp/feishu-botd.grpc.sock")
	t.Setenv("FEISHU_BOTD_COMMANDS_ENABLED", "true")
	t.Setenv("FEISHU_BOTD_SCRIPTS_ENABLED", "true")
	t.Setenv("FEISHU_BOTD_SCRIPTS_COMMAND", "pls")
	t.Setenv("FEISHU_BOTD_SCRIPTS_DIR", t.TempDir())
	t.Setenv("FEISHU_BOTD_SCRIPTS_ALLOWED_CHATS", "unknown-chat")
	if _, err := LoadFromEnv(); err == nil || !strings.Contains(err.Error(), "unknown-chat") {
		t.Fatalf("expected unknown allowed chat error, got %v", err)
	}
}

func TestLoadFromEnvScriptsValidConfigSucceeds(t *testing.T) {
	setBaseEnv(t)
	t.Setenv("FEISHU_BOTD_GRPC_SOCKET", "/tmp/feishu-botd.grpc.sock")
	t.Setenv("FEISHU_BOTD_COMMANDS_ENABLED", "true")
	t.Setenv("FEISHU_BOTD_SCRIPTS_ENABLED", "true")
	t.Setenv("FEISHU_BOTD_SCRIPTS_COMMAND", "pls")
	t.Setenv("FEISHU_BOTD_SCRIPTS_DIR", t.TempDir())
	t.Setenv("FEISHU_BOTD_SCRIPTS_ALLOWED_CHATS", "ops")
	cfg, err := LoadFromEnv()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.Commands.Scripts.TimeoutSeconds != 120 {
		t.Fatalf("default timeout = %d, want 120", cfg.Commands.Scripts.TimeoutSeconds)
	}
}
