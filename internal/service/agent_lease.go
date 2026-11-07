package service

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"feishu-botd/internal/notify"
)

const maxConversationLease = 7 * 24 * time.Hour
const maxConversationLeases = 1024
const maxLeaseOperations = 256

// Leases retain only the exact provider/conversation scope seen at ingress.
// They never refresh the ordinary ingress grant or expose private route ids.
type conversationLeases struct {
	mu      sync.Mutex
	path    string
	entries map[string]*conversationLease
}
type conversationLease struct {
	Provider          string                    `json:"provider"`
	Conversation      string                    `json:"conversation"`
	ID                string                    `json:"id"`
	ExpiresAt         time.Time                 `json:"expires_at"`
	AppAlias          string                    `json:"app_alias"`
	ChatAlias         string                    `json:"chat_alias"`
	ChatID            string                    `json:"chat_id"`
	ThreadReplyTo     string                    `json:"thread_reply_to"`
	UnconfiguredGroup bool                      `json:"unconfigured_group"`
	Revoked           bool                      `json:"revoked"`
	HighestSequence   uint64                    `json:"highest_sequence"`
	Sequences         map[string]uint64         `json:"sequences"`
	Operations        map[string]leaseOperation `json:"operations"`
	route             *agentConversationRoute
}
type leaseOperation struct {
	Provider    string    `json:"provider"`
	Fingerprint string    `json:"fingerprint"`
	FollowUpID  string    `json:"follow_up_id"`
	MessageRef  string    `json:"message_ref"`
	Complete    bool      `json:"complete"`
	AmbiguousAt time.Time `json:"ambiguous_at"`
}

// OpenAgentConversationLeases must run before ingress/servers start. Corrupt
// state fails startup rather than silently forgetting revocations or dedupe.
func (s *Service) OpenAgentConversationLeases(dir string) error {
	if strings.TrimSpace(dir) == "" {
		return errors.New("conversation lease state directory is required")
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	store := &conversationLeases{path: filepath.Join(dir, "agent-conversation-leases.json"), entries: make(map[string]*conversationLease)}
	data, err := os.ReadFile(store.path)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	if err == nil {
		var saved struct {
			Version int                  `json:"version"`
			Entries []*conversationLease `json:"entries"`
		}
		if err = json.Unmarshal(data, &saved); err != nil {
			return err
		}
		if saved.Version != 1 || len(saved.Entries) > maxConversationLeases {
			return errors.New("invalid conversation lease snapshot")
		}
		now := time.Now()
		for _, entry := range saved.Entries {
			if entry == nil || !validLeaseIdentity(entry.Provider, entry.Conversation, entry.ID) || (!entry.Revoked && entry.ChatID == "") || len(entry.Operations) > maxLeaseOperations || len(entry.Sequences) > maxLeaseOperations {
				return errors.New("invalid conversation lease entry")
			}
			if !now.Before(entry.ExpiresAt) {
				continue
			}
			if entry.ExpiresAt.After(now.Add(maxConversationLease)) {
				return errors.New("invalid conversation lease expiry")
			}
			if entry.Sequences == nil {
				entry.Sequences = make(map[string]uint64)
			}
			entry.route = entry.restoreRoute()
			store.entries[leaseKey(entry.Provider, entry.Conversation, entry.ID)] = entry
		}
	}
	s.conversationLeases = store
	return nil
}
func validLeaseIdentity(provider, conversation, id string) bool {
	return provider != "" && len(provider) <= 64 && conversation != "" && len(conversation) <= 160 && id != "" && len(id) <= 160 && strings.TrimSpace(provider) == provider && strings.TrimSpace(conversation) == conversation && strings.TrimSpace(id) == id
}
func leaseKey(provider, conversation, id string) string {
	return hashJSON([3]string{provider, conversation, id})
}
func (l *conversationLease) restoreRoute() *agentConversationRoute {
	r := &agentConversationRoute{appAlias: l.AppAlias, chatAlias: l.ChatAlias, chatID: l.ChatID, threadReplyTo: l.ThreadReplyTo, unconfiguredGroup: l.UnconfiguredGroup, providers: map[string]time.Time{l.Provider: l.ExpiresAt}, operations: make(map[string]*agentFollowUpOperation), uploadedImages: make(map[string]uploadedImage), expiresAt: l.ExpiresAt}
	for key, op := range l.Operations {
		r.operations[key] = &agentFollowUpOperation{provider: op.Provider, fingerprint: op.Fingerprint, followUpID: op.FollowUpID, messageRef: op.MessageRef, complete: op.Complete, ambiguousAt: op.AmbiguousAt}
	}
	return r
}
func (l *conversationLease) snapshotOperations() {
	l.route.mu.Lock()
	defer l.route.mu.Unlock()
	l.Operations = make(map[string]leaseOperation, len(l.route.operations))
	for key, op := range l.route.operations {
		l.Operations[key] = leaseOperation{op.provider, op.fingerprint, op.followUpID, op.messageRef, op.complete, op.ambiguousAt}
	}
}
func (l *conversationLeases) saveLocked() error {
	entries := make([]*conversationLease, 0, len(l.entries))
	for _, entry := range l.entries {
		entries = append(entries, entry)
	}
	data, err := json.Marshal(struct {
		Version int                  `json:"version"`
		Entries []*conversationLease `json:"entries"`
	}{1, entries})
	if err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(l.path), ".conversation-leases-*")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if err = f.Chmod(0600); err == nil {
		_, err = f.Write(data)
	}
	if err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	if err = os.Rename(f.Name(), l.path); err != nil {
		return err
	}
	dir, err := os.Open(filepath.Dir(l.path))
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}
func leaseUnavailable() *notify.APIError {
	return notify.NewAPIError(503, "conversation_lease_unavailable", "durable conversation lease storage is unavailable", true)
}

// RetainAgentConversation acquires from live ingress only, or renews an exact
// unexpired lease. Every renewal is bounded; an expired lease is not authority.
func (s *Service) RetainAgentConversation(_ context.Context, provider, conversation, id string, ttlSeconds int64, acquire bool) (int64, *notify.APIError) {
	if !validLeaseIdentity(provider, conversation, id) || ttlSeconds < 1 || ttlSeconds > int64(maxConversationLease/time.Second) {
		return 0, notify.BadRequest("invalid_conversation_lease", "invalid conversation lease identity or duration")
	}
	store := s.conversationLeases
	if store == nil {
		return 0, leaseUnavailable()
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	now := time.Now()
	key := leaseKey(provider, conversation, id)
	for k, e := range store.entries {
		if !now.Before(e.ExpiresAt) {
			delete(store.entries, k)
		}
	}
	entry := store.entries[key]
	if entry != nil && entry.Revoked {
		return 0, unknownConversationError()
	}
	if entry == nil {
		if !acquire {
			return 0, unknownConversationError()
		}
		if len(store.entries) >= maxConversationLeases {
			return 0, notify.NewAPIError(429, "conversation_lease_limit", "conversation lease limit reached", true)
		}
		route := s.agentBroker.lookupConversation(conversation, now)
		if route == nil {
			return 0, unknownConversationError()
		}
		route.mu.Lock()
		expires, granted := route.providers[provider]
		if !granted || !now.Before(expires) || !s.appAllowed(provider, route.appAlias) {
			route.mu.Unlock()
			return 0, unknownConversationError()
		}
		entry = &conversationLease{Provider: provider, Conversation: conversation, ID: id, AppAlias: route.appAlias, ChatAlias: route.chatAlias, ChatID: route.chatID, ThreadReplyTo: route.threadReplyTo, UnconfiguredGroup: route.unconfiguredGroup, Operations: make(map[string]leaseOperation)}
		route.mu.Unlock()
	} else if !s.appAllowed(provider, entry.AppAlias) {
		return 0, unknownConversationError()
	}
	existing := store.entries[key]
	oldExpiry := entry.ExpiresAt
	entry.ExpiresAt = now.Add(time.Duration(ttlSeconds) * time.Second)
	if entry.route == nil {
		entry.route = entry.restoreRoute()
	} else {
		entry.route.mu.Lock()
		entry.route.providers[provider] = entry.ExpiresAt
		entry.route.expiresAt = entry.ExpiresAt
		entry.route.mu.Unlock()
	}
	store.entries[key] = entry
	if err := store.saveLocked(); err != nil {
		delete(store.entries, key)
		entry.ExpiresAt = oldExpiry
		if existing != nil {
			store.entries[key] = existing
			entry.route.mu.Lock()
			entry.route.providers[provider] = oldExpiry
			entry.route.expiresAt = oldExpiry
			entry.route.mu.Unlock()
		}
		return 0, leaseUnavailable()
	}
	return entry.ExpiresAt.Unix(), nil
}
func (s *Service) ReleaseAgentConversation(_ context.Context, provider, conversation, id string) *notify.APIError {
	if !validLeaseIdentity(provider, conversation, id) {
		return notify.BadRequest("invalid_conversation_lease", "invalid conversation lease identity")
	}
	store := s.conversationLeases
	if store == nil {
		return leaseUnavailable()
	}
	store.mu.Lock()
	defer store.mu.Unlock()

	key := leaseKey(provider, conversation, id)
	now := time.Now()
	for k, e := range store.entries {
		if !now.Before(e.ExpiresAt) {
			delete(store.entries, k)
		}
	}
	existing := store.entries[key]
	if existing != nil && existing.Revoked {
		return nil
	}
	if existing == nil && len(store.entries) >= maxConversationLeases {
		return notify.NewAPIError(429, "conversation_lease_limit", "conversation lease limit reached", true)
	}
	// Keep a route-free tombstone even if acquisition has not arrived yet.
	// A delayed acquire/renew cannot undo a completed cancellation.
	store.entries[key] = &conversationLease{Provider: provider, Conversation: conversation, ID: id, Revoked: true, ExpiresAt: now.Add(maxConversationLease)}
	if err := store.saveLocked(); err != nil {
		delete(store.entries, key)
		if existing != nil {
			store.entries[key] = existing
		}
		return leaseUnavailable()
	}

	return nil
}

// Hold the lease lock through the send: cancellation cannot return while an
// accepted send is still in flight, and no send can begin after release returns.
func (s *Service) sendLeasedFollowUp(ctx context.Context, in SendAgentFollowUpInput) (AgentFollowUpReceipt, *notify.APIError) {
	store := s.conversationLeases
	if store == nil {
		return AgentFollowUpReceipt{}, leaseUnavailable()
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	l := store.entries[leaseKey(in.Provider, in.ConversationID, in.ConversationLeaseID)]
	now := time.Now()
	if l == nil || l.Revoked || !now.Before(l.ExpiresAt) || !s.appAllowed(in.Provider, l.AppAlias) {
		return AgentFollowUpReceipt{}, unknownConversationError()
	}

	if in.ConversationLeaseSequence == 0 {
		return AgentFollowUpReceipt{}, notify.BadRequest("invalid_lease_sequence", "leased follow-ups require a positive operation sequence")
	}
	if l.Sequences == nil {
		l.Sequences = make(map[string]uint64)
	}
	r := l.route
	r.mu.Lock()
	key := r.followUpOperationKeyLocked(in.Provider, in.OperationID)
	_, exists := r.operations[key]
	if exists && l.Sequences[key] != in.ConversationLeaseSequence {
		r.mu.Unlock()
		return AgentFollowUpReceipt{}, notify.NewAPIError(409, "operation_conflict", "operation sequence reused with different identity", false)
	}
	if !exists {
		if in.ConversationLeaseSequence <= l.HighestSequence {
			r.mu.Unlock()
			return AgentFollowUpReceipt{}, notify.NewAPIError(409, "operation_retired", "operation sequence is already retired; it will not be sent again", false)
		}
		for _, op := range r.operations {
			if !op.complete {
				r.mu.Unlock()
				return AgentFollowUpReceipt{}, notify.NewAPIError(409, "operation_in_flight", "previous lease operation has not completed", true)
			}
		}
		// Only acknowledged (complete) operations are retired. The high-water mark
		// prevents a late retry from ever becoming a new send after ledger pruning.
		if len(r.operations) >= maxLeaseOperations {
			var oldest string
			var seq uint64 = ^uint64(0)
			for k, op := range r.operations {
				if op.complete && l.Sequences[k] < seq {
					oldest = k
					seq = l.Sequences[k]
				}
			}
			if oldest != "" {
				delete(r.operations, oldest)
				delete(l.Sequences, oldest)
			}
		}
	}
	r.mu.Unlock()
	return s.sendAgentFollowUpOnRoute(ctx, in, r, func() error {
		r.mu.Lock()
		_, claimed := r.operations[key]
		r.mu.Unlock()
		if claimed {
			l.Sequences[key] = in.ConversationLeaseSequence
			if in.ConversationLeaseSequence > l.HighestSequence {
				l.HighestSequence = in.ConversationLeaseSequence
			}
		}
		if !claimed {
			delete(l.Sequences, key)
		}
		l.snapshotOperations()
		return store.saveLocked()
	}, true)
}
