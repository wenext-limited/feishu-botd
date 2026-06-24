package dedupe

import (
	"sync"
	"time"
)

type Result struct {
	Provider  string
	MessageID string
}

type ReserveResult struct {
	Duplicate bool
	Conflict  bool
	InFlight  bool
	Result    Result
}

type entry struct {
	fingerprint string
	result      Result
	inFlight    bool
	expiresAt   time.Time
}

type MemoryStore struct {
	mu      sync.Mutex
	ttl     time.Duration
	entries map[string]entry
	now     func() time.Time
}

func NewMemoryStore(ttl time.Duration) *MemoryStore {
	return &MemoryStore{ttl: ttl, entries: make(map[string]entry), now: time.Now}
}

func (s *MemoryStore) Reserve(source, key, fingerprint string) ReserveResult {
	now := s.now()
	fullKey := source + "\x00" + key
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.entries[fullKey]; ok {
		if now.After(existing.expiresAt) {
			delete(s.entries, fullKey)
		} else if existing.fingerprint != fingerprint {
			return ReserveResult{Conflict: true}
		} else if existing.inFlight {
			return ReserveResult{InFlight: true}
		} else {
			return ReserveResult{Duplicate: true, Result: existing.result}
		}
	}
	s.entries[fullKey] = entry{fingerprint: fingerprint, inFlight: true, expiresAt: now.Add(s.ttl)}
	return ReserveResult{}
}

func (s *MemoryStore) Commit(source, key string, result Result) {
	fullKey := source + "\x00" + key
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.entries[fullKey]; ok {
		existing.result = result
		existing.inFlight = false
		existing.expiresAt = s.now().Add(s.ttl)
		s.entries[fullKey] = existing
	}
}

func (s *MemoryStore) Abort(source, key string) {
	fullKey := source + "\x00" + key
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.entries, fullKey)
}

func (s *MemoryStore) Ready() bool { return s != nil }
