package service

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"feishu-botd/internal/config"
	"feishu-botd/internal/dedupe"
)

// emailAgentBackend is the shared agent fake plus the Contact email lookup.
type emailAgentBackend struct {
	*fakeAgentBackend
	mu     sync.Mutex
	emails map[string]string
	err    error
	calls  []string
}

func (e *emailAgentBackend) SenderEmail(_ context.Context, openID string) (string, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.calls = append(e.calls, openID)
	if e.err != nil {
		return "", e.err
	}
	return e.emails[openID], nil
}

func (e *emailAgentBackend) lookups() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.calls)
}

func newSenderEmailTestService(backend *emailAgentBackend, logs *bytes.Buffer, providers map[string]config.AgentProviderConfig) *Service {
	cfg := config.Config{
		AppID:          "cli_test",
		AppSecret:      "secret",
		Channels:       map[string]string{"ops": "oc_test"},
		DedupeTTL:      time.Hour,
		SendTimeout:    time.Second,
		AgentProviders: providers,
	}
	logger := slog.Default()
	if logs != nil {
		logger = slog.New(slog.NewTextHandler(logs, nil))
	}
	return NewService(cfg, backend, dedupe.NewMemoryStore(time.Hour), logger)
}

func dispatchFrom(t *testing.T, svc *Service, deliveryID, senderID string) {
	t.Helper()
	mustDispatchAgentPrompt(t, svc, CommandInput{
		DeliveryID: deliveryID, Command: "ask", Prompt: "ask", ChatAlias: "ops",
		SenderID: senderID,
		Metadata: map[string]string{"message_id": "om_" + deliveryID},
	})
}

// Only the provider granted allow_sender_email sees the address; another
// provider receiving the very same message does not.
func TestAgentMessageCarriesSenderEmailOnlyToAGrantedProvider(t *testing.T) {
	backend := &emailAgentBackend{fakeAgentBackend: newFakeAgentBackend(), emails: map[string]string{"ou_sender_1": "lisi@zhenxinmail.com"}}
	svc := newSenderEmailTestService(backend, nil, map[string]config.AgentProviderConfig{
		"granted": {AllowSenderEmail: true},
		"plain":   {},
	})
	granted := mustSubscribeAgent(t, svc, AgentSubscribeOptions{Provider: "granted", Commands: []string{"ask"}})
	plain := mustSubscribeAgent(t, svc, AgentSubscribeOptions{Provider: "plain", Commands: []string{"ask"}})

	dispatchFrom(t, svc, "evt_email_1", "ou_sender_1")

	if got := receiveAgentEvent(t, granted).Message.SenderEmail; got != "lisi@zhenxinmail.com" {
		t.Fatalf("granted provider sender email = %q", got)
	}
	if got := receiveAgentEvent(t, plain).Message.SenderEmail; got != "" {
		t.Fatalf("ungranted provider sender email = %q, want empty", got)
	}
}

// No grant anywhere means no Contact call at all, and a caller cannot smuggle
// an address in through CommandInput.
func TestAgentMessageWithoutAGrantNeverLooksUpTheSender(t *testing.T) {
	backend := &emailAgentBackend{fakeAgentBackend: newFakeAgentBackend(), emails: map[string]string{"ou_sender_1": "lisi@zhenxinmail.com"}}
	svc := newSenderEmailTestService(backend, nil, map[string]config.AgentProviderConfig{"plain": {}})
	sub := mustSubscribeAgent(t, svc, AgentSubscribeOptions{Provider: "plain", Commands: []string{"ask"}})

	mustDispatchAgentPrompt(t, svc, CommandInput{
		DeliveryID: "evt_email_2", Command: "ask", Prompt: "ask", ChatAlias: "ops",
		SenderID: "ou_sender_1", SenderEmail: "forged@zhenxinmail.com",
		Metadata: map[string]string{"message_id": "om_evt_email_2"},
	})

	if got := receiveAgentEvent(t, sub).Message.SenderEmail; got != "" {
		t.Fatalf("sender email = %q, want empty", got)
	}
	if n := backend.lookups(); n != 0 {
		t.Fatalf("contact lookups = %d, want 0", n)
	}
}

func TestAgentMessageSenderEmailIsCachedPerSender(t *testing.T) {
	backend := &emailAgentBackend{fakeAgentBackend: newFakeAgentBackend(), emails: map[string]string{
		"ou_sender_1": "lisi@zhenxinmail.com",
		"ou_sender_2": "zhangsan@zhenxinmail.com",
	}}
	svc := newSenderEmailTestService(backend, nil, map[string]config.AgentProviderConfig{"granted": {AllowSenderEmail: true}})
	sub := mustSubscribeAgent(t, svc, AgentSubscribeOptions{Provider: "granted", Commands: []string{"ask"}})

	dispatchFrom(t, svc, "evt_cache_1", "ou_sender_1")
	dispatchFrom(t, svc, "evt_cache_2", "ou_sender_2")
	dispatchFrom(t, svc, "evt_cache_3", "ou_sender_1")

	want := []string{"lisi@zhenxinmail.com", "zhangsan@zhenxinmail.com", "lisi@zhenxinmail.com"}
	for i, email := range want {
		if got := receiveAgentEvent(t, sub).Message.SenderEmail; got != email {
			t.Fatalf("message %d sender email = %q, want %q", i, got, email)
		}
	}
	if n := backend.lookups(); n != 2 {
		t.Fatalf("contact lookups = %d, want 2 (one per sender)", n)
	}
}

// A failed lookup costs the message its address, never its delivery, and is
// logged once, with neither the address nor the sender's id.
func TestAgentMessageSenderEmailFailureIsSoftAndLoggedOnce(t *testing.T) {
	backend := &emailAgentBackend{fakeAgentBackend: newFakeAgentBackend(), err: errors.New("missing scope")}
	var logs bytes.Buffer
	svc := newSenderEmailTestService(backend, &logs, map[string]config.AgentProviderConfig{"granted": {AllowSenderEmail: true}})
	sub := mustSubscribeAgent(t, svc, AgentSubscribeOptions{Provider: "granted", Commands: []string{"ask"}})

	dispatchFrom(t, svc, "evt_fail_1", "ou_sender_1")
	dispatchFrom(t, svc, "evt_fail_2", "ou_sender_2")

	for i := 0; i < 2; i++ {
		event := receiveAgentEvent(t, sub)
		if event.Message == nil || event.Message.SenderEmail != "" {
			t.Fatalf("message %d = %#v, want delivered with no sender email", i, event.Message)
		}
	}
	if n := strings.Count(logs.String(), "sender email lookup failed"); n != 1 {
		t.Fatalf("failure logged %d times, want 1:\n%s", n, logs.String())
	}
	if strings.Contains(logs.String(), "ou_sender") || strings.Contains(logs.String(), "@") {
		t.Fatalf("log names the sender:\n%s", logs.String())
	}
}

// A missing scope reads as "no address": also soft, also logged once, and a
// later success re-arms the warning.
func TestAgentMessageSenderEmailMissingAddressIsLoggedOnce(t *testing.T) {
	backend := &emailAgentBackend{fakeAgentBackend: newFakeAgentBackend(), emails: map[string]string{"ou_sender_3": "wangwu@zhenxinmail.com"}}
	var logs bytes.Buffer
	svc := newSenderEmailTestService(backend, &logs, map[string]config.AgentProviderConfig{"granted": {AllowSenderEmail: true}})
	sub := mustSubscribeAgent(t, svc, AgentSubscribeOptions{Provider: "granted", Commands: []string{"ask"}})

	dispatchFrom(t, svc, "evt_none_1", "ou_sender_1")
	dispatchFrom(t, svc, "evt_none_2", "ou_sender_2")
	dispatchFrom(t, svc, "evt_none_3", "ou_sender_3")
	for i := 0; i < 3; i++ {
		receiveAgentEvent(t, sub)
	}

	if n := strings.Count(logs.String(), "sender email lookup returned no address"); n != 1 {
		t.Fatalf("missing address logged %d times, want 1:\n%s", n, logs.String())
	}
	if strings.Contains(logs.String(), "wangwu") {
		t.Fatalf("log names an address:\n%s", logs.String())
	}
}
