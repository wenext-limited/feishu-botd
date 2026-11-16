package main

import (
	"strings"
	"testing"
	"time"

	"feishu-botd/internal/chatgrant"
	"feishu-botd/internal/dedupe"
	"feishu-botd/internal/service"
)

func TestReceiverResolvesAdoptedAliasesUntilLastProviderReleases(t *testing.T) {
	cfg := testRunningConfig()
	svc := service.NewMultiAppService(cfg, nil, dedupe.NewMemoryStore(time.Hour), nil)
	manager := &appManager{svc: svc}
	receiver := manager.receiverConfig("alpha", cfg.Apps["alpha"])
	alias := "unconfigured-group-" + strings.Repeat("e", 64)
	registry := svc.Config().RuntimeChats
	for _, provider := range []string{"nous", "other"} {
		if err := registry.Add(chatgrant.Grant{Kind: chatgrant.Adopted, Provider: provider, AppAlias: "alpha", ChatID: "oc_adopted", ChatAlias: alias}); err != nil {
			t.Fatal(err)
		}
	}
	if got, ok := receiver.RuntimeChatAlias("oc_adopted"); !ok || got != alias {
		t.Fatalf("runtime alias = %q %v", got, ok)
	}
	if _, ok := manager.receiverConfig("beta", cfg.Apps["beta"]).RuntimeChatAlias("oc_adopted"); ok {
		t.Fatal("runtime alias crossed apps")
	}
	if _, err := registry.Remove("nous", alias); err != nil {
		t.Fatal(err)
	}
	if _, ok := receiver.RuntimeChatAlias("oc_adopted"); !ok {
		t.Fatal("one release removed another provider's route")
	}
	if _, err := registry.Remove("other", alias); err != nil {
		t.Fatal(err)
	}
	if _, ok := receiver.RuntimeChatAlias("oc_adopted"); ok {
		t.Fatal("last release left a runtime alias")
	}
}
