package service

import (
	"errors"
	"strings"
	"testing"

	"feishu-botd/internal/config"
)

func TestRedactRemovesConfiguredSecrets(t *testing.T) {
	cfg := config.Config{
		AppSecret: "secret-value",
		AuthToken: "token-value",
		Channels:  map[string]string{"ops": "oc_secret"},
	}
	r := newRedactor(cfg)
	msg := r.redact(errors.New("secret-value token-value oc_secret visible"))
	for _, leaked := range []string{"secret-value", "token-value", "oc_secret"} {
		if strings.Contains(msg, leaked) {
			t.Fatalf("redacted message leaked %q: %s", leaked, msg)
		}
	}
	if !strings.Contains(msg, "visible") {
		t.Fatalf("redaction removed non-secret text: %s", msg)
	}
}

func TestRedactTruncatesLongMessages(t *testing.T) {
	r := newRedactor(config.Config{})
	long := strings.Repeat("a", 500)
	msg := r.redact(errors.New(long))
	if len(msg) != 183 { // 180 + "..."
		t.Fatalf("truncated length = %d, want 183", len(msg))
	}
}
