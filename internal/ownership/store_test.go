package ownership

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestFileStoreSurvivesRestartWithoutRawMessageIDs(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	now := time.Unix(1_700_000_000, 0)
	store, err := Open(dir, time.Hour)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := store.Put("msgref_abc", "nous", now); err != nil {
		t.Fatalf("Put: %v", err)
	}

	reopened, err := Open(dir, time.Hour)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	owner, ok, err := reopened.Lookup("msgref_abc", now.Add(time.Minute))
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	if !ok || owner.Provider != "nous" {
		t.Fatalf("owner = %#v, ok=%v, want nous", owner, ok)
	}

	body, err := os.ReadFile(filepath.Join(dir, FileName))
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if strings.Contains(string(body), "om_raw_message") {
		t.Fatal("ownership snapshot leaked a raw message id")
	}
}

func TestFileStoreExpiresOwnersAndPersistsTheRemoval(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	now := time.Unix(1_700_000_000, 0)
	store, err := Open(dir, time.Minute)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := store.Put("msgref_old", "nous", now); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if _, ok, err := store.Lookup("msgref_old", now.Add(2*time.Minute)); err != nil {
		t.Fatalf("Lookup: %v", err)
	} else if ok {
		t.Fatal("expired owner remained active")
	}

	reopened, err := Open(dir, time.Minute)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if _, ok, err := reopened.Lookup("msgref_old", now.Add(2*time.Minute)); err != nil {
		t.Fatalf("reopened Lookup: %v", err)
	} else if ok {
		t.Fatal("expired owner removal was not durable")
	}
}

func TestFileStoreRejectsMalformedSnapshot(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, FileName), []byte("not-json"), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if _, err := Open(dir, time.Hour); err == nil {
		t.Fatal("Open accepted malformed ownership state")
	}
}

func TestFileStoreRejectsStatePathThatCannotHoldSnapshots(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(path, []byte("file"), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if _, err := Open(path, time.Hour); err == nil {
		t.Fatal("Open accepted a state path that cannot hold snapshots")
	}
}
