package config

import (
	"strings"
	"testing"
)

const announcementChatGrants = `,
      "allow_ambient_messages":true,
      "allow_proactive_messages":true,
      "allow_adopt_chats":true`

func TestAnnouncementGrantDefaultsOffAndScopesToItsProvider(t *testing.T) {
	cfg, err := loadChatGrantConfig(t, announcementChatGrants, `,"state_dir":"/var/lib/feishu-botd"`)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if cfg.AgentProviders["commons"].AllowAnnouncements || cfg.ProviderAllowsAnnouncements("commons") {
		t.Fatal("allow_announcements was not off by default")
	}
	cfg, err = loadChatGrantConfig(t, announcementChatGrants+`,"allow_announcements":true`, `,"state_dir":"/var/lib/feishu-botd"`)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if !cfg.ProviderAllowsAnnouncements("commons") || cfg.ProviderAllowsAnnouncements("someone-else") || cfg.ProviderAllowsAnnouncements("") {
		t.Fatal("allow_announcements did not scope to its provider")
	}
}

func TestAnnouncementGrantRequiresRuntimeChatsAndState(t *testing.T) {
	for _, testCase := range []struct{ name, entry, topLevel string }{
		{"without create or adopt", `,"allow_announcements":true`, `,"state_dir":"/var/lib/feishu-botd"`},
		{"with a static proactive grant only", `,"allow_proactive_messages":true,"proactive_chat_aliases":["ops"],"allow_announcements":true`, `,"state_dir":"/var/lib/feishu-botd"`},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			_, err := loadChatGrantConfig(t, testCase.entry, testCase.topLevel)
			if err == nil || !strings.Contains(err.Error(), "allow_announcements requires allow_create_chats or allow_adopt_chats, and state_dir") {
				t.Fatalf("error = %v", err)
			}
		})
	}
}
