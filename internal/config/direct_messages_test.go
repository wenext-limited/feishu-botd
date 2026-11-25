package config

import (
	"strings"
	"testing"
)

func TestDirectMessagesDefaultOff(t *testing.T) {
	cfg, err := loadChatGrantConfig(t, "", "")
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if cfg.DirectMessages.Enabled() || cfg.DirectMessages.AllowsSource("bridge") {
		t.Fatal("direct messages were enabled without a direct_messages block")
	}
}

func TestDirectMessagesNormalizesAndScopes(t *testing.T) {
	cfg, err := loadChatGrantConfig(t, "", `,"direct_messages":{"app":"support","allowed_email_domains":[" @Example.COM "],"allowed_sources":["bridge"," bridge "]}`)
	if !strings.Contains(errString(err), "is not a bare domain") {
		t.Fatalf("a domain with @ = %v, want rejected", err)
	}
	cfg, err = loadChatGrantConfig(t, "", `,"direct_messages":{"app":"support","allowed_email_domains":[" Example.COM "],"allowed_sources":["bridge"," bridge "]}`)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	dm := cfg.DirectMessages
	if !dm.Enabled() || dm.AppAlias() != "support" || len(dm.AllowedSources) != 1 {
		t.Fatalf("direct_messages = %#v", dm)
	}
	if !dm.AllowsSource("bridge") || dm.AllowsSource("jenkins") || dm.AllowsSource("") {
		t.Fatal("source gate wrong")
	}
	if !dm.AllowsEmail("dev@example.com") || dm.AllowsEmail("dev@sub.example.com") || dm.AllowsEmail("dev@notexample.com") || dm.AllowsEmail("example.com") {
		t.Fatal("domain gate wrong")
	}
}

func TestDirectMessagesRejectsIncompleteOrUnknownApp(t *testing.T) {
	for _, testCase := range []struct{ name, block, want string }{
		{"domains only", `{"allowed_email_domains":["example.com"]}`, "needs both"},
		{"sources only", `{"allowed_sources":["bridge"]}`, "needs both"},
		{"app only", `{"app":"support"}`, "needs both"},
		{"unknown app", `{"app":"nope","allowed_email_domains":["example.com"],"allowed_sources":["bridge"]}`, `direct_messages.app "nope"`},
		{"wildcard domain", `{"allowed_email_domains":["*.example.com"],"allowed_sources":["bridge"]}`, "bare domain"},
		{"unknown field", `{"allowed_domains":["example.com"]}`, "unknown field"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			_, err := loadChatGrantConfig(t, "", `,"direct_messages":`+testCase.block)
			if !strings.Contains(errString(err), testCase.want) {
				t.Fatalf("error = %v, want %q", err, testCase.want)
			}
		})
	}
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
