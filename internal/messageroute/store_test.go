package messageroute

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

var testNow = time.Unix(1_800_000_000, 0)

func testKeys() map[string][]byte {
	return map[string][]byte{
		"default": SealKey("default", "default-app-secret"),
		"support": SealKey("support", "support-app-secret"),
	}
}

func inboundRoute(providers ...string) Route {
	return Route{
		AppAlias:  "support",
		ChatAlias: "refund-backend",
		Providers: providers,
		ChatID:    "oc_private_refund",
		MessageID: "om_private_question",
		ThreadID:  "omt_private_thread",
	}
}

func openTestStore(t *testing.T, dir string, keys map[string][]byte, now time.Time) *Store {
	t.Helper()
	store, err := Open(dir, 24*time.Hour, keys, now)
	if err != nil {
		t.Fatalf("open route store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func TestMemoryStoreReplacesProvidersKeepsKnownIDsAndExpires(t *testing.T) {
	store := NewMemory(time.Hour)
	if err := store.Put("msgref_a", inboundRoute("commons", "ibot"), testNow); err != nil {
		t.Fatalf("put: %v", err)
	}
	narrowed := inboundRoute("commons")
	narrowed.ThreadID = ""
	if err := store.Put("msgref_a", narrowed, testNow.Add(time.Minute)); err != nil {
		t.Fatalf("narrowing put: %v", err)
	}
	route, ok, err := store.Lookup("msgref_a", testNow.Add(2*time.Minute))
	if err != nil || !ok {
		t.Fatalf("lookup = %#v ok=%t err=%v", route, ok, err)
	}
	if !route.Allows("commons") || route.Allows("ibot") || route.Allows("") {
		t.Fatalf("providers after narrowing = %v", route.Providers)
	}
	if route.ThreadID != "omt_private_thread" || route.MessageID != "om_private_question" {
		t.Fatalf("an empty field dropped a known private id: %#v", route)
	}
	if err := store.Delete("msgref_a", testNow); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, ok, _ := store.Lookup("msgref_a", testNow); ok {
		t.Fatal("deleted route was still returned")
	}
	if err := store.Put("msgref_b", inboundRoute("commons"), testNow); err != nil {
		t.Fatalf("put: %v", err)
	}
	if _, ok, _ := store.Lookup("msgref_b", testNow.Add(2*time.Hour)); ok {
		t.Fatal("expired route was still returned")
	}
}

func TestStoreRejectsIncompleteRoutes(t *testing.T) {
	store := NewMemory(time.Hour)
	for name, route := range map[string]Route{
		"no provider":   {AppAlias: "support", ChatAlias: "ops"},
		"no app":        {ChatAlias: "ops", Providers: []string{"commons"}},
		"no chat alias": {AppAlias: "support", Providers: []string{"commons"}},
	} {
		if err := store.Put("msgref_x", route, testNow); err == nil {
			t.Fatalf("%s: incomplete route was accepted", name)
		}
	}
	if err := store.Put(" ", inboundRoute("commons"), testNow); err == nil {
		t.Fatal("blank key was accepted")
	}
}

func TestPersistentStoreSurvivesRestartWithoutPlaintextIDs(t *testing.T) {
	dir := t.TempDir()
	store := openTestStore(t, dir, testKeys(), testNow)
	if err := store.Put("msgref_question", inboundRoute("commons"), testNow); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	raw, err := os.ReadFile(filepath.Join(dir, FileName))
	if err != nil {
		t.Fatalf("read journal: %v", err)
	}
	for _, private := range []string{"oc_private_refund", "om_private_question", "omt_private_thread"} {
		if strings.Contains(string(raw), private) {
			t.Fatalf("journal holds plaintext %q: %s", private, raw)
		}
	}
	for _, public := range []string{"msgref_question", "refund-backend", "commons"} {
		if !strings.Contains(string(raw), public) {
			t.Fatalf("journal is missing public field %q: %s", public, raw)
		}
	}
	info, err := os.Stat(filepath.Join(dir, FileName))
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("journal mode = %v err=%v, want 0600", info.Mode().Perm(), err)
	}

	reopened := openTestStore(t, dir, testKeys(), testNow.Add(time.Hour))
	route, ok, err := reopened.Lookup("msgref_question", testNow.Add(time.Hour))
	if err != nil || !ok {
		t.Fatalf("route did not survive restart: ok=%t err=%v", ok, err)
	}
	if route.ChatID != "oc_private_refund" || route.MessageID != "om_private_question" || route.ThreadID != "omt_private_thread" ||
		route.ChatAlias != "refund-backend" || route.AppAlias != "support" || !route.Allows("commons") {
		t.Fatalf("restored route = %#v", route)
	}
}

func TestPersistentStoreFailsClosedForARotatedSecret(t *testing.T) {
	dir := t.TempDir()
	store := openTestStore(t, dir, testKeys(), testNow)
	if err := store.Put("msgref_question", inboundRoute("commons"), testNow); err != nil {
		t.Fatalf("put: %v", err)
	}
	_ = store.Close()

	rotated := testKeys()
	rotated["support"] = SealKey("support", "rotated-support-secret")
	reopened := openTestStore(t, dir, rotated, testNow)
	if _, ok, _ := reopened.Lookup("msgref_question", testNow); ok {
		t.Fatal("a route sealed under the old secret was readable after rotation")
	}
}

func TestPersistentStoreRejectsAppWithoutKey(t *testing.T) {
	store := openTestStore(t, t.TempDir(), map[string][]byte{"default": SealKey("default", "s")}, testNow)
	if err := store.Put("msgref_question", inboundRoute("commons"), testNow); err == nil {
		t.Fatal("a route was persisted for an app with no sealing key")
	}
	if _, ok, _ := store.Lookup("msgref_question", testNow); ok {
		t.Fatal("an unpersisted route became visible")
	}
}

func TestPersistentStoreDropsExpiredTornAndTamperedLines(t *testing.T) {
	dir := t.TempDir()
	store := openTestStore(t, dir, testKeys(), testNow)
	if err := store.Put("msgref_old", inboundRoute("commons"), testNow); err != nil {
		t.Fatalf("put old: %v", err)
	}
	if err := store.Put("msgref_live", inboundRoute("commons"), testNow.Add(20*time.Hour)); err != nil {
		t.Fatalf("put live: %v", err)
	}
	_ = store.Close()

	path := filepath.Join(dir, FileName)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	// A sealed blob moved onto another ref must not authenticate there.
	lines := strings.Split(strings.TrimSpace(string(raw)), "\n")
	tampered := strings.Replace(lines[0], `"msgref_old"`, `"msgref_stolen"`, 1)
	raw = append(raw, []byte(tampered+"\n"+`{"v":1,"key":"msgref_torn","app":"sup`)...)
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}

	reopened := openTestStore(t, dir, testKeys(), testNow.Add(25*time.Hour))
	if _, ok, _ := reopened.Lookup("msgref_old", testNow.Add(25*time.Hour)); ok {
		t.Fatal("expired route survived reopen")
	}
	if _, ok, _ := reopened.Lookup("msgref_stolen", testNow.Add(25*time.Hour)); ok {
		t.Fatal("a sealed route authenticated under a different ref")
	}
	if _, ok, _ := reopened.Lookup("msgref_live", testNow.Add(25*time.Hour)); !ok {
		t.Fatal("live route was lost beside a torn line")
	}
	compacted, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Count(strings.TrimSpace(string(compacted)), "\n") + 1; got != 1 || strings.Contains(string(compacted), "msgref_torn") {
		t.Fatalf("journal was not compacted to its one live route: %s", compacted)
	}
}

func TestPersistentStoreRejectsAnEditedHeader(t *testing.T) {
	dir := t.TempDir()
	store := openTestStore(t, dir, testKeys(), testNow)
	if err := store.Put("msgref_question", inboundRoute("commons"), testNow); err != nil {
		t.Fatalf("put: %v", err)
	}
	_ = store.Close()

	path := filepath.Join(dir, FileName)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for name, edit := range map[string][2]string{
		"widened providers": {`"providers":["commons"]`, `"providers":["commons","mallory"]`},
		"flipped group":     {`"chat_alias":"refund-backend"`, `"chat_alias":"refund-backend","unconfigured_group":true`},
		"extended expiry":   {`"expires_at_unix":`, `"expires_at_unix":9`},
		"moved chat":        {`"chat_alias":"refund-backend"`, `"chat_alias":"ops"`},
	} {
		tampered := strings.Replace(string(raw), edit[0], edit[1], 1)
		if tampered == string(raw) {
			t.Fatalf("%s: fixture edit did not apply", name)
		}
		if err := os.WriteFile(path, []byte(tampered), 0o600); err != nil {
			t.Fatal(err)
		}
		reopened, err := Open(dir, 24*time.Hour, testKeys(), testNow)
		if err != nil {
			t.Fatalf("%s: reopen: %v", name, err)
		}
		if route, ok, _ := reopened.Lookup("msgref_question", testNow); ok {
			t.Fatalf("%s: tampered header authenticated: %#v", name, route)
		}
		_ = reopened.Close()
		if err := os.WriteFile(path, raw, 0o600); err != nil {
			t.Fatal(err)
		}
	}
}

func TestPersistentStoreDeleteSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	store := openTestStore(t, dir, testKeys(), testNow)
	for _, key := range []string{"msgref_kept", "msgref_revoked"} {
		if err := store.Put(key, inboundRoute("commons"), testNow); err != nil {
			t.Fatalf("put %s: %v", key, err)
		}
	}
	if err := store.Delete("msgref_revoked", testNow); err != nil {
		t.Fatalf("delete: %v", err)
	}
	_ = store.Close()
	if err := store.Put("msgref_after_close", inboundRoute("commons"), testNow); err == nil {
		t.Fatal("a closed persistent store accepted a memory-only write")
	}

	reopened := openTestStore(t, dir, testKeys(), testNow)
	if _, ok, _ := reopened.Lookup("msgref_revoked", testNow); ok {
		t.Fatal("a deleted route came back after restart")
	}
	if _, ok, _ := reopened.Lookup("msgref_kept", testNow); !ok {
		t.Fatal("a kept route was lost beside a tombstone")
	}
}

func TestPersistentStoreSkipsRewritingAnUnchangedRoute(t *testing.T) {
	dir := t.TempDir()
	store := openTestStore(t, dir, testKeys(), testNow)
	chat := Route{AppAlias: "support", ChatAlias: "unconfigured-group-x", UnconfiguredGroup: true, Providers: []string{"commons"}, ChatID: "oc_private"}
	for minute := 0; minute < 5; minute++ {
		if err := store.Put("unconfigured-group-x", chat, testNow.Add(time.Duration(minute)*time.Minute)); err != nil {
			t.Fatalf("put %d: %v", minute, err)
		}
	}
	if store.lines != 1 {
		t.Fatalf("journal lines = %d, want one write for an unchanged route", store.lines)
	}
	chat.Providers = []string{"ibot"}
	if err := store.Put("unconfigured-group-x", chat, testNow.Add(10*time.Minute)); err != nil {
		t.Fatalf("widening put: %v", err)
	}
	if store.lines != 2 {
		t.Fatalf("journal lines = %d, want a write for a widened route", store.lines)
	}
}

func TestPersistentStoreKeepsACardAndNeverRewindsItsSequence(t *testing.T) {
	dir := t.TempDir()
	store := openTestStore(t, dir, testKeys(), testNow)
	card := inboundRoute("commons")
	card.CardID = "card_private_entity"
	card.CardSequence = 3
	if err := store.Put("msgref_card", card, testNow); err != nil {
		t.Fatalf("put card: %v", err)
	}
	// A reply-route refresh knows nothing about the card.
	if err := store.Put("msgref_card", inboundRoute("commons"), testNow.Add(time.Minute)); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	stale := card
	stale.CardSequence = 1
	if err := store.Put("msgref_card", stale, testNow.Add(2*time.Minute)); err != nil {
		t.Fatalf("stale put: %v", err)
	}
	advanced := card
	advanced.CardSequence = 4
	if err := store.Put("msgref_card", advanced, testNow.Add(3*time.Minute)); err != nil {
		t.Fatalf("advance: %v", err)
	}
	_ = store.Close()

	raw, err := os.ReadFile(filepath.Join(dir, FileName))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "card_private_entity") {
		t.Fatalf("journal holds the plaintext card id: %s", raw)
	}
	reopened := openTestStore(t, dir, testKeys(), testNow.Add(time.Hour))
	route, ok, err := reopened.Lookup("msgref_card", testNow.Add(time.Hour))
	if err != nil || !ok || route.CardID != "card_private_entity" || route.CardSequence != 4 || route.MessageID != "om_private_question" {
		t.Fatalf("restored card route = %#v ok=%t err=%v", route, ok, err)
	}
	if err := reopened.Put("msgref_bad", Route{AppAlias: "support", ChatAlias: "ops", Providers: []string{"commons"}, CardSequence: -1}, testNow); err == nil {
		t.Fatal("a negative card sequence was accepted")
	}
}
