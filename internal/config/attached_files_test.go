package config

import (
	"os"
	"path/filepath"
	"testing"
)

// File carry (ADR-0127) is an explicit sensitive-read grant layered on top
// of allow_attached_context, not an independent one: it names bytes the
// daemon downloads and streams to the provider, so omission always denies,
// and the grant does nothing without its parent also being set.
func TestLoadFromConfigFileAgentProviderAttachedFilesScope(t *testing.T) {
	for _, testCase := range []struct {
		name  string
		entry string
		want  bool
	}{
		{name: "omitted defaults to off", entry: `,"allow_attached_context":true`, want: false},
		{name: "explicitly disabled", entry: `,"allow_attached_context":true,"allow_attached_files":false`, want: false},
		{name: "explicitly enabled", entry: `,"allow_attached_context":true,"allow_attached_files":true`, want: true},
		{
			name:  "files grant without its parent context grant is inert",
			entry: `,"allow_attached_context":false,"allow_attached_files":true`,
			want:  false,
		},
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
			if got := cfg.ProviderAllowsAttachedFiles("fixture-agent"); got != testCase.want {
				t.Fatalf("ProviderAllowsAttachedFiles = %t, want %t", got, testCase.want)
			}
			if cfg.ProviderAllowsAttachedFiles("unknown-provider") {
				t.Fatal("unconfigured provider inherited attached-files access")
			}
		})
	}
}

// The raw field round-trips even when its parent grant is off: the daemon
// keeps the distinction between "files were never asked for" and "files were
// asked for but the parent read grant denies it", for config auditing.
func TestLoadFromConfigFileAgentProviderAttachedFilesFieldRoundTrips(t *testing.T) {
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
      "allow_attached_files":true
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
	if !cfg.AgentProviders["fixture-agent"].AllowAttachedFiles {
		t.Fatal("AllowAttachedFiles field did not round-trip from config")
	}
	if cfg.ProviderAllowsAttachedFiles("fixture-agent") {
		t.Fatal("ProviderAllowsAttachedFiles must still deny without allow_attached_context")
	}
}
