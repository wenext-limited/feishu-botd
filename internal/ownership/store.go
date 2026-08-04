// Package ownership persists the private provider owner of agent-authored
// Feishu messages. It stores provider-safe message references only; raw
// Feishu message identifiers never enter this file.
package ownership

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// FileName is the ownership snapshot name under botd's configured state dir.
const FileName = "agent-message-owners.json"

const snapshotVersion = 1

// Owner is the provider route for one public message reference.
type Owner struct {
	Provider  string `json:"provider"`
	ExpiresAt int64  `json:"expires_at_unix"`
}

type snapshotOwner struct {
	MessageRef string `json:"message_ref"`
	Provider   string `json:"provider"`
	ExpiresAt  int64  `json:"expires_at_unix"`
}

type snapshot struct {
	Version int             `json:"version"`
	Owners  []snapshotOwner `json:"owners"`
}

// Store is a mutex-serialized, atomic JSON snapshot. The traffic is one small
// write per created agent response, so a database would add dependencies and
// operational surface without buying useful concurrency.
type Store struct {
	mu     sync.Mutex
	dir    string
	ttl    time.Duration
	owners map[string]Owner
}

// Open creates stateDir when necessary and loads the current snapshot.
// Malformed or unknown-version snapshots fail closed so a restart cannot
// silently turn owner-only reaction routing into ownerless delivery.
func Open(stateDir string, ttl time.Duration) (*Store, error) {
	stateDir = strings.TrimSpace(stateDir)
	if stateDir == "" {
		return nil, errors.New("ownership state directory is required")
	}
	if ttl <= 0 {
		return nil, errors.New("ownership TTL must be positive")
	}
	if err := os.MkdirAll(stateDir, 0o700); err != nil {
		return nil, fmt.Errorf("prepare ownership state directory: %w", err)
	}
	if err := verifyWritable(stateDir); err != nil {
		return nil, err
	}
	store := &Store{
		dir:    stateDir,
		ttl:    ttl,
		owners: make(map[string]Owner),
	}
	if err := store.load(); err != nil {
		return nil, err
	}
	return store, nil
}

func verifyWritable(stateDir string) error {
	probe, err := os.CreateTemp(stateDir, ".agent-message-owners-write-test-*")
	if err != nil {
		return fmt.Errorf("ownership state directory is not writable: %w", err)
	}
	name := probe.Name()
	if err := probe.Close(); err != nil {
		_ = os.Remove(name)
		return fmt.Errorf("close ownership state write test: %w", err)
	}
	if err := os.Remove(name); err != nil {
		return fmt.Errorf("remove ownership state write test: %w", err)
	}
	return nil
}

// Put records or refreshes the owning provider for messageRef.
func (s *Store) Put(messageRef, provider string, now time.Time) error {
	messageRef = strings.TrimSpace(messageRef)
	provider = strings.TrimSpace(provider)
	if messageRef == "" || provider == "" {
		return errors.New("message reference and provider are required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.owners[messageRef] = Owner{
		Provider:  provider,
		ExpiresAt: now.Add(s.ttl).Unix(),
	}
	return s.persistLocked()
}

// Lookup returns the unexpired owner. Expiry is removed and persisted lazily
// so a quiet daemon does not need a background cleanup goroutine.
func (s *Store) Lookup(messageRef string, now time.Time) (Owner, bool, error) {
	messageRef = strings.TrimSpace(messageRef)
	if messageRef == "" {
		return Owner{}, false, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	owner, ok := s.owners[messageRef]
	if !ok {
		return Owner{}, false, nil
	}
	if now.Unix() < owner.ExpiresAt {
		return owner, true, nil
	}
	delete(s.owners, messageRef)
	if err := s.persistLocked(); err != nil {
		return Owner{}, false, err
	}
	return Owner{}, false, nil
}

func (s *Store) load() error {
	file, err := os.Open(filepath.Join(s.dir, FileName))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("open ownership snapshot: %w", err)
	}
	defer file.Close()
	var value snapshot
	decoder := json.NewDecoder(io.LimitReader(file, 8<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&value); err != nil {
		return fmt.Errorf("decode ownership snapshot: %w", err)
	}
	if value.Version != snapshotVersion {
		return fmt.Errorf("unsupported ownership snapshot version %d", value.Version)
	}
	for _, item := range value.Owners {
		messageRef := strings.TrimSpace(item.MessageRef)
		provider := strings.TrimSpace(item.Provider)
		if messageRef == "" || provider == "" || item.ExpiresAt <= 0 {
			return errors.New("ownership snapshot contains an invalid owner")
		}
		s.owners[messageRef] = Owner{Provider: provider, ExpiresAt: item.ExpiresAt}
	}
	return nil
}

func (s *Store) persistLocked() error {
	owners := make([]snapshotOwner, 0, len(s.owners))
	for messageRef, owner := range s.owners {
		owners = append(owners, snapshotOwner{
			MessageRef: messageRef,
			Provider:   owner.Provider,
			ExpiresAt:  owner.ExpiresAt,
		})
	}
	sort.Slice(owners, func(i, j int) bool { return owners[i].MessageRef < owners[j].MessageRef })
	value := snapshot{Version: snapshotVersion, Owners: owners}

	temp, err := os.CreateTemp(s.dir, ".agent-message-owners-*")
	if err != nil {
		return fmt.Errorf("create ownership snapshot: %w", err)
	}
	tempName := temp.Name()
	committed := false
	defer func() {
		if !committed {
			_ = os.Remove(tempName)
		}
	}()
	if err := temp.Chmod(0o600); err != nil {
		_ = temp.Close()
		return fmt.Errorf("secure ownership snapshot: %w", err)
	}
	encoder := json.NewEncoder(temp)
	if err := encoder.Encode(&value); err != nil {
		_ = temp.Close()
		return fmt.Errorf("encode ownership snapshot: %w", err)
	}
	if err := temp.Sync(); err != nil {
		_ = temp.Close()
		return fmt.Errorf("sync ownership snapshot: %w", err)
	}
	if err := temp.Close(); err != nil {
		return fmt.Errorf("close ownership snapshot: %w", err)
	}
	if err := os.Rename(tempName, filepath.Join(s.dir, FileName)); err != nil {
		return fmt.Errorf("replace ownership snapshot: %w", err)
	}
	directory, err := os.Open(s.dir)
	if err != nil {
		return fmt.Errorf("open ownership state directory for sync: %w", err)
	}
	syncErr := directory.Sync()
	closeErr := directory.Close()
	if syncErr != nil {
		return fmt.Errorf("sync ownership state directory: %w", syncErr)
	}
	if closeErr != nil {
		return fmt.Errorf("close ownership state directory: %w", closeErr)
	}
	committed = true
	return nil
}
