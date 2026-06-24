package dedupe

import "testing"

func TestMemoryStoreDuplicateAndConflict(t *testing.T) {
	store := NewMemoryStore(1 << 62)
	if got := store.Reserve("xipe", "key", "fp1"); got.Duplicate || got.Conflict || got.InFlight {
		t.Fatalf("first reserve = %#v", got)
	}
	if got := store.Reserve("xipe", "key", "fp1"); !got.InFlight {
		t.Fatalf("second reserve before commit = %#v", got)
	}
	store.Commit("xipe", "key", Result{Provider: "feishu", MessageID: "om"})
	if got := store.Reserve("xipe", "key", "fp1"); !got.Duplicate || got.Result.MessageID != "om" {
		t.Fatalf("duplicate reserve = %#v", got)
	}
	if got := store.Reserve("xipe", "key", "fp2"); !got.Conflict {
		t.Fatalf("conflict reserve = %#v", got)
	}
}
