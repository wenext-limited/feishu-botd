package chatgrant

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestRegistryPersistsAndReloads(t *testing.T) {
	dir := t.TempDir()
	r, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if r.Granted("commons", "x") {
		t.Fatal("empty registry grants")
	}
	alias := AliasFor("default", "oc_1")
	if !IsAlias(alias) || alias != AliasFor("default", "oc_1") || alias == AliasFor("support", "oc_1") {
		t.Fatalf("alias %q is not stable and app-scoped", alias)
	}
	grant := Grant{Provider: "commons", AppAlias: "default", ChatID: "oc_1", ChatAlias: alias, Name: "[Agents] a",
		CreatedAt: time.Unix(1, 0).UTC(), OperationID: "op-1", RequestDigest: "d"}
	if err := r.Add(grant); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(filepath.Join(dir, FileName))
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("file = %v err=%v", info, err)
	}
	if err := r.Rename(alias, "[Archived] a"); err != nil {
		t.Fatal(err)
	}

	again, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	got, ok := again.ByOperation("commons", "op-1")
	if !ok || got.ChatID != "oc_1" || got.Name != "[Archived] a" {
		t.Fatalf("reloaded = %#v ok=%v", got, ok)
	}
	if !again.Granted("commons", alias) || again.Granted("other", alias) {
		t.Fatal("grant is not provider-scoped")
	}
	if g, ok := again.ByChat("default", "oc_1"); !ok || g.ChatAlias != alias {
		t.Fatalf("by chat = %#v", g)
	}
	if len(again.Grants()) != 1 {
		t.Fatalf("grants = %#v", again.Grants())
	}
}

func TestNilAndCorruptRegistries(t *testing.T) {
	var r *Registry
	if r.Granted("p", "a") || r.Grants() != nil {
		t.Fatal("nil registry grants")
	}
	if _, ok := r.ByChat("default", "oc"); ok {
		t.Fatal("nil registry resolves")
	}
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, FileName), []byte(`{"version":9}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(dir); err == nil {
		t.Fatal("an unknown version loaded")
	}
	if err := os.WriteFile(filepath.Join(dir, FileName), []byte(`{"version":1,"grants":[{"provider":"p","chat_id":"oc","chat_alias":"ops"}]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(dir); err == nil {
		t.Fatal("a grant with a configured-looking alias loaded")
	}
}
