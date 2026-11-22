package announcement

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestStorePersistsSectionsAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := store.Section("commons", "ops"); ok {
		t.Fatal("empty store returned a section")
	}
	now := time.Date(2026, 11, 22, 8, 0, 0, 0, time.UTC)
	section := Section{Provider: "commons", ChatAlias: "ops", AppAlias: "default", ChatID: "oc_ops",
		HeadingBlockID: "blk_h", Title: "Status", UpdatedAt: now}
	section = section.WithOperation(Operation{OperationID: "op-1", Digest: "d1", CompletedAt: now})
	if err := store.Put(section); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(filepath.Join(dir, FileName))
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("state file = %v err=%v", info, err)
	}
	reopened, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	got, ok := reopened.Section("commons", "ops")
	if !ok || got.HeadingBlockID != "blk_h" || got.ChatID != "oc_ops" {
		t.Fatalf("reopened section = %#v", got)
	}
	if op, ok := got.Operation("op-1"); !ok || op.Digest != "d1" {
		t.Fatalf("operation = %#v", op)
	}
	if _, ok := reopened.Section("other", "ops"); ok {
		t.Fatal("section leaked to another provider")
	}
}

func TestSectionKeepsTheNewestOperations(t *testing.T) {
	var section Section
	for i := 0; i < MaxOperations+5; i++ {
		section = section.WithOperation(Operation{OperationID: fmt.Sprintf("op-%d", i)})
	}
	if len(section.Operations) != MaxOperations {
		t.Fatalf("operations = %d", len(section.Operations))
	}
	if _, ok := section.Operation("op-0"); ok {
		t.Fatal("oldest operation was kept")
	}
	if _, ok := section.Operation(fmt.Sprintf("op-%d", MaxOperations+4)); !ok {
		t.Fatal("newest operation was dropped")
	}
}

func TestOpenRejectsMalformedState(t *testing.T) {
	for name, content := range map[string]string{
		"garbage":    "{",
		"version":    `{"version":9,"sections":[]}`,
		"incomplete": `{"version":1,"sections":[{"provider":"commons"}]}`,
		"duplicate": `{"version":1,"sections":[{"provider":"p","chat_alias":"a","chat_id":"oc"},
			{"provider":"p","chat_alias":"a","chat_id":"oc"}]}`,
	} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			if err := os.WriteFile(filepath.Join(dir, FileName), []byte(content), 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := Open(dir); err == nil || !strings.Contains(err.Error(), FileName) {
				t.Fatalf("err = %v", err)
			}
		})
	}
}

func TestPutFailureKeepsPreviousState(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(filepath.Join(dir, "state"))
	if err != nil {
		t.Fatal(err)
	}
	first := Section{Provider: "p", ChatAlias: "a", ChatID: "oc", HeadingBlockID: "h1"}
	if err := store.Put(first); err != nil {
		t.Fatal(err)
	}
	// Replace the state dir with a file so the next write fails.
	if err := os.RemoveAll(filepath.Join(dir, "state")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "state"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	second := first
	second.HeadingBlockID = "h2"
	if err := store.Put(second); err == nil {
		t.Fatal("write into a file succeeded")
	}
	if got, _ := store.Section("p", "a"); got.HeadingBlockID != "h1" {
		t.Fatalf("failed put changed memory: %#v", got)
	}
}
