package config

import (
	"os"
	"path/filepath"
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
	t.Setenv("FEISHU_APP_ID", "cli_test")
	t.Setenv("FEISHU_APP_SECRET", "secret")
	t.Setenv("FEISHU_BOTD_CHANNELS_OPS", "oc_test")
	t.Setenv("FEISHU_BOTD_SOCKET", "")
	t.Setenv("FEISHU_BOTD_BIND", "")
	t.Setenv("FEISHU_BOTD_GRPC_SOCKET", "")
	t.Setenv("FEISHU_BOTD_GRPC_BIND", "")
	t.Setenv("FEISHU_BOTD_AUTH_TOKEN_FILE", "")
	t.Setenv("FEISHU_BOTD_ALLOW_NON_LOOPBACK_BIND", "")
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
	if err := os.WriteFile(tokenPath, []byte("abc123\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("FEISHU_BOTD_AUTH_TOKEN_FILE", tokenPath)
	cfg, err := LoadFromEnv()
	if err != nil {
		t.Fatalf("load with token: %v", err)
	}
	if cfg.AuthToken != "abc123" {
		t.Fatalf("auth token = %q", cfg.AuthToken)
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
	if err := os.WriteFile(tokenPath, []byte("lan-token\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("FEISHU_BOTD_AUTH_TOKEN_FILE", tokenPath)
	cfg, err := LoadFromEnv()
	if err != nil {
		t.Fatalf("load with LAN bind: %v", err)
	}
	if !cfg.AllowLANBind || cfg.BindAddr != "0.0.0.0:7345" || cfg.AuthToken != "lan-token" {
		t.Fatalf("unexpected config: %#v", cfg)
	}
}
