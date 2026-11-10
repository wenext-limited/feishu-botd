package service

import (
	"context"
	"feishu-botd/internal/feishu"
	"feishu-botd/internal/ownership"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func leasedService(t *testing.T, dir string, backend *fakeFollowUpBackend) *Service {
	t.Helper()
	s := newFollowUpTestService(backend)
	if err := s.OpenAgentConversationLeases(dir); err != nil {
		t.Fatal(err)
	}
	return s
}
func leaseInput() SendAgentFollowUpInput {
	return SendAgentFollowUpInput{Provider: "agent", ConversationID: "conv_follow_up", OperationID: "scheduled-report-1", Markdown: "Report", Summary: "Daily report", ConversationLeaseID: "task-1", ConversationLeaseSequence: 1}
}
func acquireLease(t *testing.T, s *Service) {
	t.Helper()
	sub := seedFollowUpConversation(t, s, "agent", groupFollowUpPrompt())
	defer sub.Close()
	if _, err := s.RetainAgentConversation(context.Background(), "agent", "conv_follow_up", "task-1", 86400, true); err != nil {
		t.Fatal(err)
	}
}
func TestConversationLeaseSurvivesIngressExpiryAndRestart(t *testing.T) {
	dir := t.TempDir()
	backend := newFakeFollowUpBackend()
	s := leasedService(t, dir, backend)
	acquireLease(t, s)
	// Restart has no in-memory ingress conversation map at all.
	s = leasedService(t, dir, backend)
	if _, err := s.SendAgentFollowUp(context.Background(), leaseInput()); err != nil {
		t.Fatal(err)
	}
	if got := backend.snapshot(); len(got) != 1 || got[0].chatID != "oc_test" {
		t.Fatalf("sends = %#v", got)
	}
	// A completed operation and its fingerprint survive a second restart.
	s = leasedService(t, dir, backend)
	receipt, err := s.SendAgentFollowUp(context.Background(), leaseInput())
	if err != nil || !receipt.Duplicate || receipt.MessageRef == "" || len(backend.snapshot()) != 1 {
		t.Fatalf("replay = %#v, %v", receipt, err)
	}
	changed := leaseInput()
	changed.Markdown = "Different report"
	if _, err := s.SendAgentFollowUp(context.Background(), changed); err == nil || err.Code != "operation_conflict" {
		t.Fatalf("changed replay = %v", err)
	}
	info, err2 := os.Stat(filepath.Join(dir, "agent-conversation-leases.json"))
	if err2 != nil {
		t.Fatal(err2)
	}
	if info.Mode().Perm() != 0600 {
		t.Fatalf("snapshot mode = %v", info.Mode())
	}
}
func TestConversationLeaseCannotAcquireFromUnknownOrAnotherProvider(t *testing.T) {
	s := leasedService(t, t.TempDir(), newFakeFollowUpBackend())
	acquireLease(t, s)
	for _, args := range [][3]string{{"other", "conv_follow_up", "task-1"}, {"agent", "unknown", "task-1"}} {
		if _, err := s.RetainAgentConversation(context.Background(), args[0], args[1], args[2], 86400, true); err == nil || err.Code != "unknown_conversation" {
			t.Fatalf("acquire %v = %v", args, err)
		}
	}
	for _, seconds := range []int64{0, -1, 604801} {
		if _, err := s.RetainAgentConversation(context.Background(), "agent", "conv_follow_up", "task-1", seconds, true); err == nil {
			t.Fatalf("accepted ttl %d", seconds)
		}
	}
	input := leaseInput()
	input.Provider = "other"
	if _, err := s.SendAgentFollowUp(context.Background(), input); err == nil {
		t.Fatal("other provider used lease")
	}
	input = leaseInput()
	input.ConversationLeaseID = "unknown"
	if _, err := s.SendAgentFollowUp(context.Background(), input); err == nil {
		t.Fatal("unknown lease fell back to live ingress")
	}
}
func TestConversationLeaseRenewReleaseAndExpiry(t *testing.T) {
	dir := t.TempDir()
	s := leasedService(t, dir, newFakeFollowUpBackend())
	acquireLease(t, s)
	s = leasedService(t, dir, newFakeFollowUpBackend())
	if _, err := s.RetainAgentConversation(context.Background(), "agent", "conv_follow_up", "task-1", 604800, false); err != nil {
		t.Fatalf("renew after restart = %v", err)
	}
	if err := s.ReleaseAgentConversation(context.Background(), "other", "conv_follow_up", "task-1"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.SendAgentFollowUp(context.Background(), leaseInput()); err != nil {
		t.Fatal("another provider released lease", err)
	}
	if err := s.ReleaseAgentConversation(context.Background(), "agent", "conv_follow_up", "task-1"); err != nil {
		t.Fatal(err)
	}
	s = leasedService(t, dir, newFakeFollowUpBackend())
	if _, err := s.SendAgentFollowUp(context.Background(), leaseInput()); err == nil {
		t.Fatal("released lease survived restart")
	}
	if _, err := s.RetainAgentConversation(context.Background(), "agent", "conv_follow_up", "task-1", 86400, true); err == nil {
		t.Fatal("released lease reacquired without ingress")
	}
	s = leasedService(t, t.TempDir(), newFakeFollowUpBackend())
	acquireLease(t, s)
	store := s.conversationLeases
	store.mu.Lock()
	store.entries[leaseKey("agent", "conv_follow_up", "task-1")].ExpiresAt = time.Now().Add(-time.Second)
	store.mu.Unlock()
	if _, err := s.SendAgentFollowUp(context.Background(), leaseInput()); err == nil {
		t.Fatal("expired lease sent")
	}
	// Remove ingress authority too: an expired durable row cannot renew itself.
	s.agentBroker = newAgentBroker(time.Hour)
	if _, err := s.RetainAgentConversation(context.Background(), "agent", "conv_follow_up", "task-1", 86400, true); err == nil {
		t.Fatal("expired lease renewed itself")
	}
}
func TestConversationLeaseAmbiguousSendFailsClosedAfterRestart(t *testing.T) {
	dir := t.TempDir()
	backend := newFakeFollowUpBackend()
	s := leasedService(t, dir, backend)
	acquireLease(t, s)
	store := s.conversationLeases
	store.mu.Lock()
	l := store.entries[leaseKey("agent", "conv_follow_up", "task-1")]
	// Simulate a process crash after persisting the pre-send claim.
	l.Sequences = map[string]uint64{"scheduled-report-1": 1}
	l.HighestSequence = 1
	input := leaseInput()
	_, _, err := l.route.beginFollowUp(input.Provider, input.OperationID, followUpFingerprint(input.ConversationID, input.Markdown, input.Summary, ""), time.Now(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	l.route.mu.Lock()
	l.route.operations[l.route.followUpOperationKeyLocked(input.Provider, input.OperationID)].ambiguousAt = time.Now().Add(-2 * time.Hour)
	l.route.mu.Unlock()
	l.snapshotOperations()
	if err := store.saveLocked(); err != nil {
		t.Fatal(err)
	}
	store.mu.Unlock()
	s = leasedService(t, dir, backend)
	if _, err := s.SendAgentFollowUp(context.Background(), input); err == nil || err.Code != "send_retry_expired" {
		t.Fatalf("ambiguous old attempt = %v", err)
	}
	if len(backend.snapshot()) != 0 {
		t.Fatal("ambiguous old attempt sent again")
	}
}

func TestConversationLeaseRetiresCompletedOperationsWithoutExhaustion(t *testing.T) {
	dir := t.TempDir()
	backend := newFakeFollowUpBackend()
	s := leasedService(t, dir, backend)
	acquireLease(t, s)
	for n := uint64(1); n <= maxLeaseOperations+5; n++ {
		in := leaseInput()
		in.OperationID = fmt.Sprintf("report-%d", n)
		in.ConversationLeaseSequence = n
		if _, err := s.SendAgentFollowUp(context.Background(), in); err != nil {
			t.Fatalf("send %d = %v", n, err)
		}
	}
	s = leasedService(t, dir, backend)
	l := s.conversationLeases.entries[leaseKey("agent", "conv_follow_up", "task-1")]
	if len(l.Operations) > maxLeaseOperations || len(l.Sequences) > maxLeaseOperations {
		t.Fatal("unbounded operation ledger")
	}
	in := leaseInput()
	in.OperationID = "report-1"
	if _, err := s.SendAgentFollowUp(context.Background(), in); err == nil || err.Code != "operation_retired" {
		t.Fatalf("old retired operation = %v", err)
	}
	in.OperationID = fmt.Sprintf("report-%d", maxLeaseOperations+5)
	in.ConversationLeaseSequence = maxLeaseOperations + 5
	if receipt, err := s.SendAgentFollowUp(context.Background(), in); err != nil || !receipt.Duplicate {
		t.Fatalf("latest replay = %v %v", receipt, err)
	}
	if len(backend.snapshot()) != maxLeaseOperations+5 {
		t.Fatal("replay sent again")
	}
}
func TestConversationLeaseRenewalWriteFailureRestoresGrant(t *testing.T) {
	s := leasedService(t, t.TempDir(), newFakeFollowUpBackend())
	acquireLease(t, s)
	store := s.conversationLeases
	key := leaseKey("agent", "conv_follow_up", "task-1")
	entry := store.entries[key]
	expires := entry.ExpiresAt
	original := store.path
	store.path = filepath.Join(t.TempDir(), "missing", "snapshot")
	if _, err := s.RetainAgentConversation(context.Background(), "agent", "conv_follow_up", "task-1", 604800, false); err == nil {
		t.Fatal("renew succeeded without storage")
	}
	if store.entries[key] != entry || !entry.ExpiresAt.Equal(expires) || !entry.route.providers["agent"].Equal(expires) {
		t.Fatal("failed renewal changed existing grant")
	}
	store.path = original
	if _, err := s.SendAgentFollowUp(context.Background(), leaseInput()); err != nil {
		t.Fatal("old lease stopped working", err)
	}
}

func TestFollowUpReceiptRegistersActualAuthoredMessage(t *testing.T) {
	backend := newFakeFollowUpBackend()
	s := leasedService(t, t.TempDir(), backend)
	owners := &fakeOwnerStore{owners: make(map[string]ownership.Owner)}
	s.SetAgentOwnershipStore(owners)
	acquireLease(t, s)
	receipt, err := s.SendAgentFollowUp(context.Background(), leaseInput())
	if err != nil {
		t.Fatal(err)
	}
	expected := feishu.MessageRefForApp("", "om_follow_up")
	if receipt.MessageRef != expected {
		t.Fatalf("message ref %q does not name actual sent message %q", receipt.MessageRef, expected)
	}
	owner, ok, lookupErr := owners.Lookup(receipt.MessageRef, time.Now())
	if lookupErr != nil || !ok || owner.Provider != "agent" {
		t.Fatalf("authored owner = %#v %v %v", owner, ok, lookupErr)
	}
}

func TestConversationLeaseCancellationWinsLiveIngressAndLateAcquire(t *testing.T) {
	for _, acquired := range []bool{false, true} {
		t.Run(fmt.Sprint(acquired), func(t *testing.T) {
			dir := t.TempDir()
			s := leasedService(t, dir, newFakeFollowUpBackend())
			sub := seedFollowUpConversation(t, s, "agent", groupFollowUpPrompt())
			defer sub.Close()
			if acquired {
				if _, err := s.RetainAgentConversation(context.Background(), "agent", "conv_follow_up", "task-1", 86400, true); err != nil {
					t.Fatal(err)
				}
			}
			if err := s.ReleaseAgentConversation(context.Background(), "agent", "conv_follow_up", "task-1"); err != nil {
				t.Fatal(err)
			}
			for _, create := range []bool{false, true} {
				if _, err := s.RetainAgentConversation(context.Background(), "agent", "conv_follow_up", "task-1", 86400, create); err == nil {
					t.Fatal("cancelled lease resurrected while ingress live")
				}
			}
			s = leasedService(t, dir, newFakeFollowUpBackend())
			sub = seedFollowUpConversation(t, s, "agent", groupFollowUpPrompt())
			defer sub.Close()
			if _, err := s.RetainAgentConversation(context.Background(), "agent", "conv_follow_up", "task-1", 86400, true); err == nil {
				t.Fatal("revocation lost after restart and fresh ingress")
			}
			if _, err := s.SendAgentFollowUp(context.Background(), leaseInput()); err == nil {
				t.Fatal("cancelled operation replayed")
			}
			// Once bounded tombstones expire, a stale renewal still cannot create.
			s.conversationLeases.entries[leaseKey("agent", "conv_follow_up", "task-1")].ExpiresAt = time.Now().Add(-time.Second)
			if _, err := s.RetainAgentConversation(context.Background(), "agent", "conv_follow_up", "task-1", 86400, false); err == nil {
				t.Fatal("stale renewal recreated pruned tombstone")
			}
		})
	}
}
func TestConversationLeaseReleaseWriteFailurePreservesLease(t *testing.T) {
	s := leasedService(t, t.TempDir(), newFakeFollowUpBackend())
	acquireLease(t, s)
	store := s.conversationLeases
	key := leaseKey("agent", "conv_follow_up", "task-1")
	entry := store.entries[key]
	original := store.path
	store.path = filepath.Join(t.TempDir(), "missing", "snapshot")
	if err := s.ReleaseAgentConversation(context.Background(), "agent", "conv_follow_up", "task-1"); err == nil {
		t.Fatal("release succeeded without storage")
	}
	if store.entries[key] != entry || entry.Revoked {
		t.Fatal("failed release changed existing grant")
	}
	store.path = original
	if _, err := s.SendAgentFollowUp(context.Background(), leaseInput()); err != nil {
		t.Fatal(err)
	}
}
