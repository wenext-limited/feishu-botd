// Package chatgrant keeps the runtime chat grants CreateAgentChat makes: a
// group chat botd created for a provider counts, for that provider, as one of
// its configured channels, in its ambient and proactive allowlists, without a
// config edit (ADR 0001).
//
// The registry is a small JSON file under botd's state dir, rewritten
// atomically on every change. It holds raw chat ids in the clear, as the
// config file's channels map already does; revoking a grant means removing
// its entry while botd is stopped (or removing the bot from the chat).
package chatgrant

import (
	"crypto/sha256"
	"encoding/hex"
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

// FileName is the registry under botd's configured state dir.
const FileName = "agent-chat-grants.json"

// AliasPrefix starts every runtime-granted chat alias. It never collides with
// a configured alias that does not use it, and config loading rejects
// configured aliases that do.
const AliasPrefix = "agent-chat-"

const fileVersion = 1

// Grant is one chat a provider created.
type Grant struct {
	Provider  string    `json:"provider"`
	AppAlias  string    `json:"app_alias"`
	ChatID    string    `json:"chat_id"`
	ChatAlias string    `json:"chat_alias"`
	Name      string    `json:"name"`
	CreatedAt time.Time `json:"created_at"`
	// OperationID and RequestDigest make CreateAgentChat idempotent across
	// restarts: the same key with the same request returns this grant.
	OperationID   string `json:"operation_id"`
	RequestDigest string `json:"request_digest"`
}

type fileFormat struct {
	Version int     `json:"version"`
	Grants  []Grant `json:"grants"`
}

// Registry is safe for concurrent use. A nil *Registry holds no grants, so
// callers need no nil checks.
type Registry struct {
	mu     sync.RWMutex
	path   string
	grants []Grant
}

// NewMemory returns a registry that is never persisted (tests, and botd
// without a state dir).
func NewMemory() *Registry { return &Registry{} }

// Open loads stateDir/agent-chat-grants.json, creating nothing until the
// first grant. A missing file is an empty registry.
func Open(stateDir string) (*Registry, error) {
	path := filepath.Join(stateDir, FileName)
	r := &Registry{path: path}
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return r, nil
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
	seen := make(map[string]struct{}, len(file.Grants))
	for _, grant := range file.Grants {
		if grant.Provider == "" || grant.ChatID == "" || !strings.HasPrefix(grant.ChatAlias, AliasPrefix) {
			return nil, fmt.Errorf("%s has an incomplete grant", FileName)
		}
		if _, dup := seen[grant.ChatAlias]; dup {
			return nil, fmt.Errorf("%s grants chat alias %q twice", FileName, grant.ChatAlias)
		}
		seen[grant.ChatAlias] = struct{}{}
	}
	r.grants = file.Grants
	return r, nil
}

// AliasFor derives the stable alias of a created chat. It is a one-way
// digest of the app and chat id, so it does not reveal the raw id.
func AliasFor(appAlias, chatID string) string {
	sum := sha256.Sum256([]byte("feishu-botd/agent-chat-alias/v1\x00" + appAlias + "\x00" + chatID))
	return AliasPrefix + hex.EncodeToString(sum[:12])
}

// IsAlias reports whether alias has the runtime-grant shape.
func IsAlias(alias string) bool { return strings.HasPrefix(strings.TrimSpace(alias), AliasPrefix) }

// ByAlias returns the grant for chatAlias.
func (r *Registry) ByAlias(chatAlias string) (Grant, bool) {
	if r == nil {
		return Grant{}, false
	}
	chatAlias = strings.TrimSpace(chatAlias)
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, grant := range r.grants {
		if grant.ChatAlias == chatAlias {
			return grant, true
		}
	}
	return Grant{}, false
}

// ByChat returns the grant for a raw chat id of appAlias.
func (r *Registry) ByChat(appAlias, chatID string) (Grant, bool) {
	if r == nil {
		return Grant{}, false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, grant := range r.grants {
		if grant.AppAlias == appAlias && grant.ChatID == chatID {
			return grant, true
		}
	}
	return Grant{}, false
}

// ByOperation returns the grant a provider's CreateAgentChat operation made.
func (r *Registry) ByOperation(provider, operationID string) (Grant, bool) {
	if r == nil {
		return Grant{}, false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, grant := range r.grants {
		if grant.Provider == provider && grant.OperationID == operationID {
			return grant, true
		}
	}
	return Grant{}, false
}

// Granted reports whether provider created chatAlias.
func (r *Registry) Granted(provider, chatAlias string) bool {
	grant, ok := r.ByAlias(chatAlias)
	return ok && grant.Provider == strings.TrimSpace(provider)
}

// Grants returns every grant, ordered by alias.
func (r *Registry) Grants() []Grant {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	out := append([]Grant(nil), r.grants...)
	r.mu.RUnlock()
	sort.Slice(out, func(i, j int) bool { return out[i].ChatAlias < out[j].ChatAlias })
	return out
}

// Add records grant and persists the registry before returning. Adding the
// same alias again replaces its entry.
func (r *Registry) Add(grant Grant) error {
	if r == nil {
		return errors.New("no chat grant registry")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	next := make([]Grant, 0, len(r.grants)+1)
	for _, existing := range r.grants {
		if existing.ChatAlias != grant.ChatAlias {
			next = append(next, existing)
		}
	}
	next = append(next, grant)
	if err := r.persist(next); err != nil {
		return err
	}
	r.grants = next
	return nil
}

// Rename updates a grant's recorded name.
func (r *Registry) Rename(chatAlias, name string) error {
	grant, ok := r.ByAlias(chatAlias)
	if !ok {
		return nil
	}
	grant.Name = name
	return r.Add(grant)
}

func (r *Registry) persist(grants []Grant) error {
	if r.path == "" {
		return nil
	}
	data, err := json.MarshalIndent(fileFormat{Version: fileVersion, Grants: grants}, "", "  ")
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(r.path), ".agent-chat-grants-*")
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
	if err := os.Rename(tmp.Name(), r.path); err != nil {
		return fmt.Errorf("write %s: %w", FileName, err)
	}
	return nil
}
