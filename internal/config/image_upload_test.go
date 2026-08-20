package config

import (
	"os"
	"path/filepath"
	"testing"
)

// writeImageUploadProviderConfig builds a one-provider config whose only extra
// entry is the JSON fragment under test.
func writeImageUploadProviderConfig(t *testing.T, entry string) Config {
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

func TestAllowImageUploadIsAnIndependentGrant(t *testing.T) {
	for _, testCase := range []struct {
		name            string
		entry           string
		wantImageUpload bool
	}{
		{
			name:  "neighbouring grants do not imply image upload",
			entry: `,"allow_card_actions":true,"allow_attached_context":true,"allow_follow_up_messages":true`,
		},
		{
			name:            "granted explicitly",
			entry:           `,"allow_image_upload":true`,
			wantImageUpload: true,
		},
		{
			name:  "absent means denied",
			entry: ``,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			cfg := writeImageUploadProviderConfig(t, testCase.entry)
			if got := cfg.AgentProviders["fixture-agent"].AllowImageUpload; got != testCase.wantImageUpload {
				t.Fatalf("allow_image_upload = %t, want %t", got, testCase.wantImageUpload)
			}
			if got := cfg.ProviderAllowsImageUpload("fixture-agent"); got != testCase.wantImageUpload {
				t.Fatalf("ProviderAllowsImageUpload = %t, want %t", got, testCase.wantImageUpload)
			}
		})
	}
}

// Granting image upload must not quietly widen anything else.
func TestAllowImageUploadDoesNotWidenItsNeighbours(t *testing.T) {
	cfg := writeImageUploadProviderConfig(t, `,"allow_image_upload":true`)
	provider := cfg.AgentProviders["fixture-agent"]
	if provider.AllowCardActions || provider.AllowAttachedContext ||
		provider.AllowFollowUpMessages || provider.AllowMessageReactions ||
		provider.AllowLegacyCommands || provider.AllowCoTProgress {
		t.Fatalf("unrequested grants leaked: %#v", provider)
	}
}

func TestProviderAllowsImageUploadDeniesAnUnconfiguredProvider(t *testing.T) {
	cfg := writeImageUploadProviderConfig(t, `,"allow_image_upload":true`)
	if cfg.ProviderAllowsImageUpload("someone-else") {
		t.Fatal("an unconfigured provider was granted image upload")
	}
	if cfg.ProviderAllowsImageUpload("") {
		t.Fatal("an empty provider name was granted image upload")
	}
}
