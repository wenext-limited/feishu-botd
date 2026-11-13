// Package messageroute retains the daemon-private Feishu route behind a
// provider-safe message reference, or behind an unconfigured group's opaque
// alias, so a provider granted proactive messages can later address that
// exact message or chat without ever holding a raw Feishu id.
//
// A message_ref is a one-way digest and cannot be reversed, so this store is
// the reverse index. Under botd's state dir it is an append-only journal whose
// raw chat, message, thread, and CardKit card ids (with a posted card's
// sequence and authorship) are sealed with AES-256-GCM under a key
// derived from the owning app's secret: the file itself holds only refs,
// public aliases, provider names, expiry times, and ciphertext. Rotating an
// app secret therefore makes that app's retained routes unreadable, which is
// treated exactly like expiry.
package messageroute

import (
	"bufio"
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
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

// FileName is the route journal under botd's configured state dir.
const FileName = "agent-message-routes.jsonl"

const (
	journalVersion  = 1
	maxJournalLine  = 16 << 10
	compactionFloor = 1024
	sealDomain      = "feishu-botd/message-route-seal/v1\x00"
	aadDomain       = "feishu-botd/message-route/v1\x00"
)

// Route is where one message (or one opaque chat alias) lives. ChatID,
// MessageID, and ThreadID are daemon-private and are sealed at rest; every
// other field is already provider-safe.
type Route struct {
	AppAlias          string
	ChatAlias         string
	UnconfiguredGroup bool
	// Providers may address this route. Each Put states the complete set, so
	// a caller can narrow a grant as well as widen it.
	Providers []string
	ChatID    string
	MessageID string
	ThreadID  string
	// CardID is the CardKit entity behind a card the provider posted with
	// PostAgentCard, and CardSequence the highest CardKit sequence botd has
	// reserved for it. Both are sealed with the other private ids. A Put that
	// carries no card keeps the one already known, and the sequence never
	// moves backwards, so a later route refresh cannot strand an update.
	CardID       string
	CardSequence int32
	// Authored marks a message the bot sent for the route's provider (a post,
	// card, response, or follow-up) rather than one it received. It is sealed
	// too, and once set a Put cannot clear it.
	Authored  bool
	ExpiresAt time.Time
}

// Allows reports whether provider is one of the route's grantees.
func (r Route) Allows(provider string) bool {
	provider = strings.TrimSpace(provider)
	for _, granted := range r.Providers {
		if granted == provider && provider != "" {
			return true
		}
	}
	return false
}

type entry struct {
	route Route
	line  []byte // the sealed journal line, reused verbatim by compaction
}

type journalLine struct {
	Version           int      `json:"v"`
	Key               string   `json:"key"`
	AppAlias          string   `json:"app,omitempty"`
	ChatAlias         string   `json:"chat_alias,omitempty"`
	UnconfiguredGroup bool     `json:"unconfigured_group,omitempty"`
	Providers         []string `json:"providers,omitempty"`
	ExpiresAt         int64    `json:"expires_at_unix,omitempty"`
	Sealed            string   `json:"sealed,omitempty"`
	// Revoked is a tombstone: the key has no route from this line on. It is
	// unauthenticated on purpose — forging one can only narrow access.
	Revoked bool `json:"revoked,omitempty"`
}

type sealedRoute struct {
	ChatID       string `json:"chat_id,omitempty"`
	MessageID    string `json:"message_id,omitempty"`
	ThreadID     string `json:"thread_id,omitempty"`
	CardID       string `json:"card_id,omitempty"`
	CardSequence int32  `json:"card_sequence,omitempty"`
	Authored     bool   `json:"authored,omitempty"`
}

// Store is safe for concurrent use. A store built by NewMemory never touches
// disk; one built by Open persists every Put before it is visible to Lookup.
type Store struct {
	mu      sync.Mutex
	ttl     time.Duration
	routes  map[string]entry
	dir     string
	file    *os.File
	sealers map[string]cipher.AEAD
	lines   int
	// puts counts writes since the last prune, so routes nobody looks up
	// again still leave memory once they expire.
	puts int
	// failed is set when the journal can no longer be trusted to be durable
	// (a compaction left no appendable file). Every later write returns it
	// rather than silently degrading to a memory-only store.
	failed error
}

// SealKey derives the 32-byte journal key for one app from its secret. The
// alias is part of the derivation, so two apps can never decrypt each other's
// routes even if an operator reused a secret.
func SealKey(appAlias, appSecret string) []byte {
	mac := hmac.New(sha256.New, []byte(appSecret))
	_, _ = mac.Write([]byte(sealDomain + strings.TrimSpace(appAlias)))
	return mac.Sum(nil)
}

// NewMemory returns a process-local store. Routes are lost on restart.
func NewMemory(ttl time.Duration) *Store {
	if ttl <= 0 {
		ttl = time.Hour
	}
	return &Store{ttl: ttl, routes: make(map[string]entry)}
}

// Open loads the journal under stateDir, drops expired and unreadable lines,
// rewrites it compacted, and keeps it open for appends. keys maps each app
// alias to its SealKey. A line that fails to decode or decrypt only narrows
// what can be addressed, so it is skipped rather than failing startup — a
// crash can legitimately leave a torn final line in an append-only file.
func Open(stateDir string, ttl time.Duration, keys map[string][]byte, now time.Time) (*Store, error) {
	stateDir = strings.TrimSpace(stateDir)
	if stateDir == "" {
		return nil, errors.New("message route state directory is required")
	}
	if ttl <= 0 {
		return nil, errors.New("message route TTL must be positive")
	}
	sealers, err := newSealers(keys)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(stateDir, 0o700); err != nil {
		return nil, fmt.Errorf("prepare message route state directory: %w", err)
	}
	store := &Store{
		ttl:     ttl,
		routes:  make(map[string]entry),
		dir:     stateDir,
		sealers: sealers,
	}
	if err := store.load(now); err != nil {
		return nil, err
	}
	if err := store.compactLocked(); err != nil {
		return nil, err
	}
	return store, nil
}

func newSealers(keys map[string][]byte) (map[string]cipher.AEAD, error) {
	sealers := make(map[string]cipher.AEAD, len(keys))
	for appAlias, key := range keys {
		block, err := aes.NewCipher(key)
		if err != nil {
			return nil, fmt.Errorf("message route key for app %q: %w", appAlias, err)
		}
		aead, err := cipher.NewGCM(block)
		if err != nil {
			return nil, fmt.Errorf("message route key for app %q: %w", appAlias, err)
		}
		sealers[strings.TrimSpace(appAlias)] = aead
	}
	return sealers, nil
}

// SetKeys replaces the per-app journal keys after a config reload. Routes
// of an app no longer in keys are dropped; every other route is resealed
// under its app's current key and the journal is rewritten, so a re-credited
// app's routes stay readable after the next restart. A memory store holds
// no keys and only drops routes of removed apps.
func (s *Store) SetKeys(keys map[string][]byte, now time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.dir == "" {
		for key, current := range s.routes {
			if _, kept := keys[current.route.AppAlias]; !kept {
				delete(s.routes, key)
			}
		}
		return nil
	}
	if err := s.writableLocked(); err != nil {
		return err
	}
	sealers, err := newSealers(keys)
	if err != nil {
		return err
	}
	s.sealers = sealers
	s.pruneLocked(now)
	for key, current := range s.routes {
		if _, kept := sealers[current.route.AppAlias]; !kept {
			delete(s.routes, key)
			continue
		}
		line, err := s.encodeLocked(key, current.route)
		if err != nil {
			return err
		}
		s.routes[key] = entry{route: current.route, line: line}
	}
	return s.compactLocked()
}

// Put records or refreshes key's route for one TTL from now. Providers
// replace those held; an empty private field keeps the value already known.
func (s *Store) Put(key string, route Route, now time.Time) error {
	key = strings.TrimSpace(key)
	route.AppAlias = strings.TrimSpace(route.AppAlias)
	route.ChatAlias = strings.TrimSpace(route.ChatAlias)
	route.ChatID = strings.TrimSpace(route.ChatID)
	route.MessageID = strings.TrimSpace(route.MessageID)
	route.ThreadID = strings.TrimSpace(route.ThreadID)
	route.CardID = strings.TrimSpace(route.CardID)
	route.Providers = normalizedProviders(route.Providers)
	if route.CardSequence < 0 {
		return errors.New("message route card sequence must not be negative")
	}
	if key == "" || route.AppAlias == "" || route.ChatAlias == "" || len(route.Providers) == 0 {
		return errors.New("message route key, app, chat alias, and provider are required")
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.writableLocked(); err != nil {
		return err
	}
	if existing, ok := s.routes[key]; ok && now.Before(existing.route.ExpiresAt) && existing.route.AppAlias == route.AppAlias {
		previous := existing.route
		route.UnconfiguredGroup = route.UnconfiguredGroup || previous.UnconfiguredGroup
		route.ChatID = firstNonEmpty(route.ChatID, previous.ChatID)
		route.MessageID = firstNonEmpty(route.MessageID, previous.MessageID)
		route.ThreadID = firstNonEmpty(route.ThreadID, previous.ThreadID)
		route.CardID = firstNonEmpty(route.CardID, previous.CardID)
		route.Authored = route.Authored || previous.Authored
		if previous.CardSequence > route.CardSequence {
			route.CardSequence = previous.CardSequence
		}
		// Ingress re-learns a chat on every message. Skip the write while the
		// held route already says the same thing and has most of its life left.
		if sameRoute(previous, route) && previous.ExpiresAt.After(now.Add(s.ttl/2)) {
			return nil
		}
	}
	route.ExpiresAt = now.Add(s.ttl).Truncate(time.Second)

	var line []byte
	if s.dir != "" {
		encoded, err := s.encodeLocked(key, route)
		if err != nil {
			return err
		}
		if err := s.appendLocked(encoded); err != nil {
			return err
		}
		line = encoded
	}
	s.routes[key] = entry{route: cloneRoute(route), line: line}
	return s.maintainLocked(now)
}

// Delete removes key's route. A persistent store journals a tombstone first,
// so the route stays gone across a restart.
func (s *Store) Delete(key string, now time.Time) error {
	key = strings.TrimSpace(key)
	if key == "" {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.routes[key]; !ok {
		return nil
	}
	if err := s.writableLocked(); err != nil {
		return err
	}
	if s.dir != "" {
		tombstone, err := json.Marshal(journalLine{Version: journalVersion, Key: key, Revoked: true})
		if err != nil {
			return fmt.Errorf("encode message route tombstone: %w", err)
		}
		if err := s.appendLocked(tombstone); err != nil {
			return err
		}
	}
	delete(s.routes, key)
	return s.maintainLocked(now)
}

func (s *Store) writableLocked() error {
	if s.failed != nil {
		return s.failed
	}
	if s.dir != "" && s.file == nil {
		return errors.New("message route journal is closed")
	}
	return nil
}

func (s *Store) appendLocked(line []byte) error {
	if _, err := s.file.Write(append(append([]byte(nil), line...), '\n')); err != nil {
		return fmt.Errorf("append message route: %w", err)
	}
	if err := s.file.Sync(); err != nil {
		return fmt.Errorf("sync message route journal: %w", err)
	}
	s.lines++
	return nil
}

// maintainLocked prunes expired routes now and then and compacts the journal
// once dead lines outnumber live ones.
func (s *Store) maintainLocked(now time.Time) error {
	s.puts++
	if s.puts >= compactionFloor {
		s.pruneLocked(now)
		s.puts = 0
	}
	if s.dir != "" && s.lines > 2*len(s.routes)+compactionFloor {
		s.pruneLocked(now)
		return s.compactLocked()
	}
	return nil
}

// Lookup returns key's unexpired route.
func (s *Store) Lookup(key string, now time.Time) (Route, bool, error) {
	key = strings.TrimSpace(key)
	if key == "" {
		return Route{}, false, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	held, ok := s.routes[key]
	if !ok {
		return Route{}, false, nil
	}
	if !now.Before(held.route.ExpiresAt) {
		// Expired lines leave the journal at the next compaction.
		delete(s.routes, key)
		return Route{}, false, nil
	}
	return cloneRoute(held.route), true, nil
}

// Close releases the journal. A memory store has nothing to release.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.file == nil {
		return nil
	}
	err := s.file.Close()
	s.file = nil
	return err
}

func (s *Store) load(now time.Time) error {
	file, err := os.Open(filepath.Join(s.dir, FileName))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("open message route journal: %w", err)
	}
	defer file.Close()
	reader := bufio.NewReader(file)
	for {
		raw, readErr := reader.ReadBytes('\n')
		if len(raw) > 0 && len(raw) <= maxJournalLine {
			line := bytes.TrimSpace(raw)
			if key, ok := tombstoneKey(line); ok {
				delete(s.routes, key)
			} else if key, route, ok := s.decodeLocked(line); ok && now.Before(route.ExpiresAt) {
				s.routes[key] = entry{route: route, line: append([]byte(nil), line...)}
			} else if ok {
				delete(s.routes, key)
			}
		}
		if errors.Is(readErr, io.EOF) {
			return nil
		}
		if readErr != nil {
			return fmt.Errorf("read message route journal: %w", readErr)
		}
	}
}

func (s *Store) pruneLocked(now time.Time) {
	for key, held := range s.routes {
		if !now.Before(held.route.ExpiresAt) {
			delete(s.routes, key)
		}
	}
}

// compactLocked atomically replaces the journal with one line per held route
// and reopens it for appends.
func (s *Store) compactLocked() error {
	keys := make([]string, 0, len(s.routes))
	for key := range s.routes {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	temp, err := os.CreateTemp(s.dir, ".agent-message-routes-*")
	if err != nil {
		return fmt.Errorf("create message route journal: %w", err)
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
		return fmt.Errorf("secure message route journal: %w", err)
	}
	writer := bufio.NewWriter(temp)
	for _, key := range keys {
		if _, err := writer.Write(append(append([]byte(nil), s.routes[key].line...), '\n')); err != nil {
			_ = temp.Close()
			return fmt.Errorf("write message route journal: %w", err)
		}
	}
	if err := writer.Flush(); err != nil {
		_ = temp.Close()
		return fmt.Errorf("write message route journal: %w", err)
	}
	if err := temp.Sync(); err != nil {
		_ = temp.Close()
		return fmt.Errorf("sync message route journal: %w", err)
	}
	if err := temp.Close(); err != nil {
		return fmt.Errorf("close message route journal: %w", err)
	}
	path := filepath.Join(s.dir, FileName)
	if err := os.Rename(tempName, path); err != nil {
		return fmt.Errorf("replace message route journal: %w", err)
	}
	committed = true
	// From here the old file is unlinked, so appending to it would write
	// into nothing. Reopen before reporting anything else.
	if s.file != nil {
		_ = s.file.Close()
		s.file = nil
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		s.failed = fmt.Errorf("open message route journal for append: %w", err)
		return s.failed
	}
	s.file = file
	s.lines = len(keys)
	return syncDir(s.dir)
}

func syncDir(dir string) error {
	directory, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("open message route state directory for sync: %w", err)
	}
	syncErr := directory.Sync()
	closeErr := directory.Close()
	if syncErr != nil {
		return fmt.Errorf("sync message route state directory: %w", syncErr)
	}
	if closeErr != nil {
		return fmt.Errorf("close message route state directory: %w", closeErr)
	}
	return nil
}

func (s *Store) encodeLocked(key string, route Route) ([]byte, error) {
	aead, ok := s.sealers[route.AppAlias]
	if !ok {
		return nil, fmt.Errorf("no message route key for app %q", route.AppAlias)
	}
	plaintext, err := json.Marshal(sealedRoute{
		ChatID: route.ChatID, MessageID: route.MessageID, ThreadID: route.ThreadID,
		CardID: route.CardID, CardSequence: route.CardSequence, Authored: route.Authored,
	})
	if err != nil {
		return nil, fmt.Errorf("encode message route: %w", err)
	}
	nonce := make([]byte, aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, fmt.Errorf("seal message route: %w", err)
	}
	header := journalLine{
		Version:           journalVersion,
		Key:               key,
		AppAlias:          route.AppAlias,
		ChatAlias:         route.ChatAlias,
		UnconfiguredGroup: route.UnconfiguredGroup,
		Providers:         route.Providers,
		ExpiresAt:         route.ExpiresAt.Unix(),
	}
	aad, err := additionalData(header)
	if err != nil {
		return nil, err
	}
	header.Sealed = base64.RawURLEncoding.EncodeToString(aead.Seal(nonce, nonce, plaintext, aad))
	return json.Marshal(header)
}

// tombstoneKey recognizes a Revoked line.
func tombstoneKey(line []byte) (string, bool) {
	var value journalLine
	if err := json.Unmarshal(line, &value); err != nil || value.Version != journalVersion || !value.Revoked {
		return "", false
	}
	key := strings.TrimSpace(value.Key)
	return key, key != ""
}

func (s *Store) decodeLocked(line []byte) (string, Route, bool) {
	if len(line) == 0 {
		return "", Route{}, false
	}
	var value journalLine
	decoder := json.NewDecoder(bytes.NewReader(line))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&value); err != nil || value.Version != journalVersion {
		return "", Route{}, false
	}
	key := strings.TrimSpace(value.Key)
	providers := normalizedProviders(value.Providers)
	if key == "" || value.AppAlias == "" || value.ChatAlias == "" || len(providers) == 0 || value.ExpiresAt <= 0 {
		return "", Route{}, false
	}
	aead, ok := s.sealers[value.AppAlias]
	if !ok {
		return "", Route{}, false
	}
	sealed, err := base64.RawURLEncoding.DecodeString(value.Sealed)
	if err != nil || len(sealed) < aead.NonceSize() || value.Revoked {
		return "", Route{}, false
	}
	header := value
	header.Key = key
	header.Providers = providers
	header.Sealed = ""
	aad, err := additionalData(header)
	if err != nil {
		return "", Route{}, false
	}
	plaintext, err := aead.Open(nil, sealed[:aead.NonceSize()], sealed[aead.NonceSize():], aad)
	if err != nil {
		return "", Route{}, false
	}
	var private sealedRoute
	if err := json.Unmarshal(plaintext, &private); err != nil {
		return "", Route{}, false
	}
	return key, Route{
		AppAlias:          value.AppAlias,
		ChatAlias:         value.ChatAlias,
		UnconfiguredGroup: value.UnconfiguredGroup,
		Providers:         providers,
		ChatID:            private.ChatID,
		MessageID:         private.MessageID,
		ThreadID:          private.ThreadID,
		CardID:            private.CardID,
		CardSequence:      private.CardSequence,
		Authored:          private.Authored,
		ExpiresAt:         time.Unix(value.ExpiresAt, 0),
	}, true
}

// additionalData authenticates the whole plaintext header with the sealed
// ids: a blob copied onto another ref's line, or a line whose providers,
// alias, group flag, or expiry were edited, fails authentication instead of
// redirecting or widening that route.
func additionalData(header journalLine) ([]byte, error) {
	header.Sealed = ""
	header.Revoked = false
	canonical, err := json.Marshal(header)
	if err != nil {
		return nil, fmt.Errorf("encode message route header: %w", err)
	}
	return append([]byte(aadDomain), canonical...), nil
}

func normalizedProviders(in []string) []string {
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, provider := range in {
		provider = strings.TrimSpace(provider)
		if provider == "" {
			continue
		}
		if _, dup := seen[provider]; dup {
			continue
		}
		seen[provider] = struct{}{}
		out = append(out, provider)
	}
	sort.Strings(out)
	return out
}

func sameRoute(a, b Route) bool {
	if a.AppAlias != b.AppAlias || a.ChatAlias != b.ChatAlias || a.UnconfiguredGroup != b.UnconfiguredGroup ||
		a.ChatID != b.ChatID || a.MessageID != b.MessageID || a.ThreadID != b.ThreadID ||
		a.CardID != b.CardID || a.CardSequence != b.CardSequence || a.Authored != b.Authored ||
		len(a.Providers) != len(b.Providers) {
		return false
	}
	for index := range a.Providers {
		if a.Providers[index] != b.Providers[index] {
			return false
		}
	}
	return true
}

func cloneRoute(route Route) Route {
	route.Providers = append([]string(nil), route.Providers...)
	return route
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}
