// Package announcement remembers, per provider and chat, which block of the
// chat's announcement (群公告) is the heading of the section the provider
// keeps there, and which SetAgentChatAnnouncementSection operations already
// completed, so a retried operation is a no-op even after a restart.
//
// The state is a small JSON file under botd's state dir, rewritten
// atomically on every change, like agent-chat-grants.json beside it. It
// holds raw chat and block ids, as the grants file already does.
package announcement

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// FileName is the store under botd's configured state dir.
const FileName = "agent-chat-announcements.json"

const fileVersion = 1

// MaxOperations bounds the completed operations kept per section; the
// oldest are forgotten first. A provider retries an operation within
// minutes, not after hundreds of later updates.
const MaxOperations = 128

// Operation is one completed SetAgentChatAnnouncementSection call.
type Operation struct {
	OperationID string    `json:"operation_id"`
	Digest      string    `json:"digest"`
	CompletedAt time.Time `json:"completed_at"`
}

// Section is one provider's section in one chat's announcement.
type Section struct {
	Provider  string `json:"provider"`
	ChatAlias string `json:"chat_alias"`
	AppAlias  string `json:"app_alias"`
	ChatID    string `json:"chat_id"`
	// HeadingBlockID is the heading block botd created for the section; it
	// is how the section is found again.
	HeadingBlockID string      `json:"heading_block_id"`
	Title          string      `json:"title"`
	UpdatedAt      time.Time   `json:"updated_at"`
	Operations     []Operation `json:"operations"`
}

// Operation returns the completed operation with this id.
func (s Section) Operation(operationID string) (Operation, bool) {
	for _, op := range s.Operations {
		if op.OperationID == operationID {
			return op, true
		}
	}
	return Operation{}, false
}

// WithOperation returns s with op recorded, keeping at most MaxOperations.
func (s Section) WithOperation(op Operation) Section {
	ops := make([]Operation, 0, len(s.Operations)+1)
	for _, existing := range s.Operations {
		if existing.OperationID != op.OperationID {
			ops = append(ops, existing)
		}
	}
	ops = append(ops, op)
	if len(ops) > MaxOperations {
		ops = ops[len(ops)-MaxOperations:]
	}
	s.Operations = ops
	return s
}

type fileFormat struct {
	Version  int       `json:"version"`
	Sections []Section `json:"sections"`
}

type key struct{ provider, chatAlias string }

// Store is safe for concurrent use.
type Store struct {
	mu       sync.Mutex
	path     string
	sections map[key]Section
}

// NewMemory returns a store that is never persisted (tests, and botd without
// a state dir).
func NewMemory() *Store { return &Store{sections: make(map[key]Section)} }

// Open loads stateDir/agent-chat-announcements.json. A missing file is an
// empty store; a malformed one fails closed.
func Open(stateDir string) (*Store, error) {
	stateDir = strings.TrimSpace(stateDir)
	if stateDir == "" {
		return nil, errors.New("announcement state directory is required")
	}
	store := &Store{path: filepath.Join(stateDir, FileName), sections: make(map[key]Section)}
	data, err := os.ReadFile(store.path)
	if errors.Is(err, os.ErrNotExist) {
		return store, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", FileName, err)
	}
	var file fileFormat
	if err := json.Unmarshal(data, &file); err != nil {
		return nil, fmt.Errorf("decode %s: %w", FileName, err)
	}
	if file.Version != fileVersion {
		return nil, fmt.Errorf("%s has version %d, want %d", FileName, file.Version, fileVersion)
	}
	for _, section := range file.Sections {
		if section.Provider == "" || section.ChatAlias == "" || section.ChatID == "" {
			return nil, fmt.Errorf("%s holds an incomplete section", FileName)
		}
		k := key{section.Provider, section.ChatAlias}
		if _, dup := store.sections[k]; dup {
			return nil, fmt.Errorf("%s holds provider %q chat alias %q twice", FileName, section.Provider, section.ChatAlias)
		}
		store.sections[k] = section
	}
	return store, nil
}

// Section returns the provider's section in chatAlias.
func (s *Store) Section(provider, chatAlias string) (Section, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	section, ok := s.sections[key{provider, chatAlias}]
	if ok {
		section.Operations = append([]Operation(nil), section.Operations...)
	}
	return section, ok
}

// Put records section and persists the store before returning. On failure
// the previous state is kept.
func (s *Store) Put(section Section) error {
	if section.Provider == "" || section.ChatAlias == "" || section.ChatID == "" {
		return errors.New("incomplete announcement section")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	k := key{section.Provider, section.ChatAlias}
	previous, existed := s.sections[k]
	s.sections[k] = section
	if err := s.persistLocked(); err != nil {
		if existed {
			s.sections[k] = previous
		} else {
			delete(s.sections, k)
		}
		return err
	}
	return nil
}

func (s *Store) persistLocked() error {
	if s.path == "" {
		return nil
	}
	sections := make([]Section, 0, len(s.sections))
	for _, section := range s.sections {
		sections = append(sections, section)
	}
	sort.Slice(sections, func(i, j int) bool {
		if sections[i].ChatAlias != sections[j].ChatAlias {
			return sections[i].ChatAlias < sections[j].ChatAlias
		}
		return sections[i].Provider < sections[j].Provider
	})
	data, err := json.MarshalIndent(fileFormat{Version: fileVersion, Sections: sections}, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(s.path), 0o700); err != nil {
		return fmt.Errorf("write %s: %w", FileName, err)
	}
	tmp, err := os.CreateTemp(filepath.Dir(s.path), ".agent-chat-announcements-*")
	if err != nil {
		return fmt.Errorf("write %s: %w", FileName, err)
	}
	defer os.Remove(tmp.Name())
	if err := tmp.Chmod(0o600); err != nil {
		tmp.Close()
		return err
	}
	if _, err := tmp.Write(append(data, '\n')); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp.Name(), s.path); err != nil {
		return fmt.Errorf("write %s: %w", FileName, err)
	}
	return nil
}
