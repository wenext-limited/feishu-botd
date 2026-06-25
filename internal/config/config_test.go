package config

import (
	"os"
	"path/filepath"
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
		if err := validateLoopbackBind(addr); err != nil {
			t.Fatalf("%s rejected: %v", addr, err)
		}
	}
	for _, addr := range []string{"0.0.0.0:7345", "192.0.2.10:7345", "missing-port"} {
		if err := validateLoopbackBind(addr); err == nil {
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
