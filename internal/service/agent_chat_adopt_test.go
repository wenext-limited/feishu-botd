package service

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"feishu-botd/internal/chatgrant"
	"feishu-botd/internal/config"
	"feishu-botd/internal/dedupe"
	"feishu-botd/internal/feishu"
	"feishu-botd/internal/messageroute"
	"feishu-botd/internal/ownership"
)

type adoptionBackend struct {
	*fakePostBackend
	member          bool
	memberErr       error
	membershipCalls int
}

func (b *adoptionBackend) IsInChat(_ context.Context, _ string) (bool, error) {
	b.membershipCalls++
	return b.member, b.memberErr
}
func (b *adoptionBackend) CreateChat(context.Context, feishu.ChatCreateRequest) (string, error) {
	return "oc_created", nil
}
func (b *adoptionBackend) AddChatMembers(context.Context, string, []string) ([]string, error) {
	return nil, nil
}
func (b *adoptionBackend) RenameChat(context.Context, string, string) error { return nil }

func adoptionService(t *testing.T) (*Service, *adoptionBackend, *fakeOwnerStore) {
	t.Helper()
	cfg := commonsConfig()
	p := cfg.AgentProviders["commons"]
	p.AllowAdoptChats, p.AllowCreateChats = true, true
	cfg.AgentProviders = map[string]config.AgentProviderConfig{"commons": p, "other": p}
	backend := &adoptionBackend{fakePostBackend: newFakePostBackend(), member: true}
	svc := NewMultiAppService(cfg, map[string]feishu.Sender{"support": backend}, dedupe.NewMemoryStore(time.Hour), nil)
	owners := &fakeOwnerStore{owners: make(map[string]ownership.Owner)}
	svc.SetAgentOwnershipStore(owners)
	return svc, backend, owners
}

func adoptMessage(t *testing.T, svc *Service, provider, ref string) AdoptAgentChatReceipt {
	t.Helper()
	receipt, apiErr := svc.AdoptAgentChat(context.Background(), AdoptAgentChatInput{Provider: provider, MessageRef: ref})
	if apiErr != nil {
		t.Fatalf("adopt %s: %+v", ref, apiErr)
	}
	return receipt
}

func TestAdoptLatestReplyThenAmbientSurvivesRestartAndRelease(t *testing.T) {
	svc, backend, owners := adoptionService(t)
	dir := t.TempDir()
	grants, err := chatgrant.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	svc.cfg().RuntimeChats = grants
	keys := map[string][]byte{"support": messageroute.SealKey("support", "secret")}
	routes, err := messageroute.Open(dir, time.Hour, keys, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	svc.SetAgentMessageRouteStore(routes)
	sub := mustSubscribeAgent(t, svc, AgentSubscribeOptions{Provider: "commons", IncludeUnmatchedMessages: true, IncludeAmbientMessages: true})
	alias := config.UnconfiguredGroupAliasPrefix + strings.Repeat("a", 64)
	first := humanGroupMessage(alias, "oc_people", "d_bind", "om_bind")
	first.UnconfiguredGroup = true
	mustDispatchAgentPrompt(t, svc, first)
	receiveAgentEvent(t, sub)
	owners.owners[feishu.MessageRefForApp("support", "om_answer")] = ownership.Owner{Provider: "commons", ExpiresAt: time.Now().Add(time.Hour).Unix()}
	latest := humanGroupMessage(alias, "oc_people", "d_yes", "om_yes")
	latest.Prompt, latest.NamedPrompt, latest.Command, latest.Text = "对", "", "", ""
	latest.UnconfiguredGroup, latest.UnmentionedReply = true, true
	latest.Metadata["parent_id"] = "om_answer"
	mustDispatchAgentPrompt(t, svc, latest)
	event := receiveAgentEvent(t, sub)
	if event.Message.Addressing != AgentAddressingReply {
		t.Fatalf("latest addressing = %v", event.Message.Addressing)
	}
	// Adopt the latest reply, never the original /bind ref.
	receipt := adoptMessage(t, svc, "commons", event.Message.MessageRef)
	if receipt.ChatAlias != alias || receipt.AlreadyGranted {
		t.Fatalf("adopt = %+v", receipt)
	}
	if !svc.Config().ProviderAllowsAmbientChat("commons", alias) || !svc.Config().ProviderAllowsProactiveChat("commons", alias) {
		t.Fatal("grant missing")
	}

	// The grant must resolve even after the short-lived route journal expires,
	// and after app-wide unconfigured ingress is disabled.
	cfg := svc.Config()
	apps := cfg.EffectiveApps()
	app := apps["support"]
	app.Commands.AllowUnconfiguredGroupChats = false
	apps["support"] = app
	cfg.Apps = apps
	svc.ApplyConfig(cfg, config.ReloadPlan{}, nil, nil, time.Now())
	if svc.Config().RuntimeChats != grants {
		t.Fatal("reload lost registry")
	}
	// No opaque alias route is needed once the durable grant exists.
	svc.SetAgentMessageRouteStore(messageroute.NewMemory(time.Hour))
	if _, apiErr := svc.PostAgentMessage(context.Background(), PostAgentMessageInput{Provider: "commons", OperationID: "post_adopted", ChatAlias: alias, Markdown: "hello"}); apiErr != nil {
		t.Fatalf("post: %+v", apiErr)
	}
	svc.SetAgentMessageRouteStore(routes)
	if _, apiErr := svc.PostAgentMessage(context.Background(), PostAgentMessageInput{Provider: "commons", OperationID: "reply_adopted", ReplyToMessageRef: event.Message.MessageRef, Markdown: "reply"}); apiErr != nil {
		t.Fatalf("reply: %+v", apiErr)
	}
	ambient := humanGroupMessage(alias, "oc_people", "d_ambient", "om_latest_ambient")
	ambient.AmbientOnly = true
	mustDispatchAgentPrompt(t, svc, ambient)
	observed := receiveAgentEvent(t, sub)
	if observed.Message.Addressing != AgentAddressingAmbient {
		t.Fatal("not ambient")
	}
	if got := adoptMessage(t, svc, "commons", observed.Message.MessageRef); !got.AlreadyGranted || got.ChatAlias != alias {
		t.Fatalf("ambient re-adopt = %+v", got)
	}
	if err := routes.Close(); err != nil {
		t.Fatal(err)
	}

	// Fresh service + reopened encrypted journal + grants: the latest ambient
	// message is enough to re-adopt once in the new provider process.
	cfg = svc.Config()
	cfg.RuntimeChats, err = chatgrant.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	restarted := NewMultiAppService(cfg, map[string]feishu.Sender{"support": backend}, dedupe.NewMemoryStore(time.Hour), nil)
	routes, err = messageroute.Open(dir, time.Hour, keys, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = routes.Close() })
	restarted.SetAgentMessageRouteStore(routes)
	if got := adoptMessage(t, restarted, "commons", observed.Message.MessageRef); !got.AlreadyGranted || got.ChatAlias != alias {
		t.Fatalf("restart = %+v", got)
	}
	if released, apiErr := restarted.ReleaseAgentChat(context.Background(), ReleaseAgentChatInput{Provider: "commons", ChatAlias: alias}); apiErr != nil || !released {
		t.Fatalf("release = %v %+v", released, apiErr)
	}
	if restarted.Config().ProviderAllowsAmbientChat("commons", alias) || restarted.Config().ProviderAllowsProactiveChat("commons", alias) {
		t.Fatal("released grant is active")
	}
	if _, apiErr := restarted.PostAgentMessage(context.Background(), PostAgentMessageInput{Provider: "commons", OperationID: "after_release", ChatAlias: alias, Markdown: "no"}); apiErr == nil {
		t.Fatal("post after release succeeded")
	}
	reloaded, err := chatgrant.Open(dir)
	if err != nil || reloaded.Granted("commons", alias) {
		t.Fatalf("release did not persist: %v", err)
	}
}

func TestAdoptStaticAndCreatedAreAlreadyGrantedAndCreatedReleaseLosesManagement(t *testing.T) {
	svc, _, _ := adoptionService(t)
	sub := mustSubscribeAgent(t, svc, AgentSubscribeOptions{Provider: "commons", IncludeUnmatchedMessages: true})
	mustDispatchAgentPrompt(t, svc, humanGroupMessage("ops", "oc_ops", "d_static", "om_static"))
	event := receiveAgentEvent(t, sub)
	if got := adoptMessage(t, svc, "commons", event.Message.MessageRef); !got.AlreadyGranted || got.ChatAlias != "ops" {
		t.Fatalf("static = %+v", got)
	}
	if len(svc.Config().RuntimeChats.Grants()) != 0 {
		t.Fatal("static adoption persisted a grant")
	}
	if released, apiErr := svc.ReleaseAgentChat(context.Background(), ReleaseAgentChatInput{Provider: "commons", ChatAlias: "ops"}); apiErr != nil || released {
		t.Fatalf("static release = %v %+v", released, apiErr)
	}
	if _, chat, ok := svc.Config().ResolveChannel("ops"); !ok || chat != "oc_ops" {
		t.Fatal("static route changed")
	}
	created, apiErr := svc.CreateAgentChat(context.Background(), CreateAgentChatInput{Provider: "commons", OperationID: "create", Name: "Agents"})
	if apiErr != nil {
		t.Fatal(apiErr)
	}
	mustDispatchAgentPrompt(t, svc, humanGroupMessage(created.ChatAlias, "oc_created", "d_created", "om_created_inbound"))
	event = receiveAgentEvent(t, sub)
	if got := adoptMessage(t, svc, "commons", event.Message.MessageRef); !got.AlreadyGranted {
		t.Fatalf("created = %+v", got)
	}
	grant, _ := svc.Config().RuntimeChats.ByProviderAlias("commons", created.ChatAlias)
	if grant.Kind != chatgrant.Created {
		t.Fatal("adoption downgraded created rights")
	}
	if released, apiErr := svc.ReleaseAgentChat(context.Background(), ReleaseAgentChatInput{Provider: "commons", ChatAlias: created.ChatAlias}); apiErr != nil || !released {
		t.Fatalf("release created = %v %+v", released, apiErr)
	}
	if apiErr := svc.RenameAgentChat(context.Background(), RenameAgentChatInput{Provider: "commons", ChatAlias: created.ChatAlias, Name: "later"}); apiErr == nil || apiErr.Code != "unknown_chat" {
		t.Fatalf("rename released: %+v", apiErr)
	}
	if _, apiErr := svc.InviteAgentChatMembers(context.Background(), InviteAgentChatMembersInput{Provider: "commons", ChatAlias: created.ChatAlias, MemberOpenIDs: []string{"ou_a"}}); apiErr == nil || apiErr.Code != "unknown_chat" {
		t.Fatalf("invite released: %+v", apiErr)
	}
}

func TestAdoptRejectsUnknownForeignDirectAuthoredExpiredAndRemovedBot(t *testing.T) {
	svc, backend, _ := adoptionService(t)
	// Both are eligible, but only commons receives this exact command.
	p := svc.cfg().AgentProviders["commons"]
	p.AllowedCommands = []string{"refund"}
	svc.cfg().AgentProviders["commons"] = p
	sub := mustSubscribeAgent(t, svc, AgentSubscribeOptions{Provider: "commons", Commands: []string{"refund"}})
	mustSubscribeAgent(t, svc, AgentSubscribeOptions{Provider: "other", IncludeUnmatchedMessages: true})
	mustDispatchAgentPrompt(t, svc, humanGroupMessage("ops", "oc_ops", "d_private", "om_private"))
	event := receiveAgentEvent(t, sub)
	for _, tc := range []struct{ provider, ref string }{{"commons", "unknown"}, {"other", event.Message.MessageRef}} {
		if _, apiErr := svc.AdoptAgentChat(context.Background(), AdoptAgentChatInput{Provider: tc.provider, MessageRef: tc.ref}); apiErr == nil || apiErr.Code != "unknown_chat" {
			t.Fatalf("foreign/unknown = %+v", apiErr)
		}
	}
	if backend.membershipCalls != 0 {
		t.Fatal("unknown ref caused a membership call")
	}
	direct := humanGroupMessage("direct", "oc_dm", "d_direct", "om_direct")
	direct.Metadata["chat_type"] = "p2p"
	mustDispatchAgentPrompt(t, svc, direct)
	directEvent := receiveAgentEvent(t, sub)
	if _, apiErr := svc.AdoptAgentChat(context.Background(), AdoptAgentChatInput{Provider: "commons", MessageRef: directEvent.Message.MessageRef}); apiErr == nil || apiErr.Code != "unknown_chat" {
		t.Fatalf("direct = %+v", apiErr)
	}
	posted, apiErr := svc.PostAgentMessage(context.Background(), PostAgentMessageInput{Provider: "commons", OperationID: "authored", ChatAlias: "refund-backend", Markdown: "hi"})
	if apiErr != nil {
		t.Fatal(apiErr)
	}
	if _, apiErr := svc.AdoptAgentChat(context.Background(), AdoptAgentChatInput{Provider: "commons", MessageRef: posted.MessageRef}); apiErr == nil || apiErr.Code != "unknown_chat" {
		t.Fatalf("authored = %+v", apiErr)
	}
	backend.member = false
	if _, apiErr := svc.AdoptAgentChat(context.Background(), AdoptAgentChatInput{Provider: "commons", MessageRef: event.Message.MessageRef}); apiErr == nil || apiErr.Code != "unknown_chat" {
		t.Fatalf("non-member = %+v", apiErr)
	}
	backend.member = true
	backend.memberErr = &feishu.MessageSendError{Operation: "chat_membership", Class: "api_rejected", Code: 99991672}
	if _, apiErr := svc.AdoptAgentChat(context.Background(), AdoptAgentChatInput{Provider: "commons", MessageRef: event.Message.MessageRef}); apiErr == nil || apiErr.Code != "chat_permission_denied" {
		t.Fatalf("membership permission = %+v", apiErr)
	}
	backend.memberErr = nil
	svc.SetAgentMessageRouteStore(messageroute.NewMemory(time.Hour))
	if err := svc.agentRoutes.Put(event.Message.MessageRef, inboundRoute(humanGroupMessage("ops", "oc_ops", "old", "om_private"), []string{"commons"}), time.Now().Add(-2*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if _, apiErr := svc.AdoptAgentChat(context.Background(), AdoptAgentChatInput{Provider: "commons", MessageRef: event.Message.MessageRef}); apiErr == nil || apiErr.Code != "unknown_chat" {
		t.Fatalf("expired = %+v", apiErr)
	}
}

func TestAdoptConcurrentProviderGrantsAndReleaseIsolation(t *testing.T) {
	svc, _, _ := adoptionService(t)
	sub := mustSubscribeAgent(t, svc, AgentSubscribeOptions{Provider: "commons", IncludeUnmatchedMessages: true, IncludeAmbientMessages: true})
	other := mustSubscribeAgent(t, svc, AgentSubscribeOptions{Provider: "other", IncludeUnmatchedMessages: true, IncludeAmbientMessages: true})
	alias := config.UnconfiguredGroupAliasPrefix + strings.Repeat("b", 64)
	in := humanGroupMessage(alias, "oc_shared", "d_shared", "om_shared")
	in.UnconfiguredGroup = true
	mustDispatchAgentPrompt(t, svc, in)
	event := receiveAgentEvent(t, sub)
	receiveAgentEvent(t, other)
	var wg sync.WaitGroup
	outcomes := make(chan bool, 16)
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			got, apiErr := svc.AdoptAgentChat(context.Background(), AdoptAgentChatInput{Provider: "commons", MessageRef: event.Message.MessageRef})
			if apiErr != nil {
				t.Error(apiErr)
				return
			}
			outcomes <- got.AlreadyGranted
		}()
	}
	wg.Wait()
	close(outcomes)
	first := 0
	for already := range outcomes {
		if !already {
			first++
		}
	}
	if first != 1 {
		t.Fatalf("new grants = %d", first)
	}
	adoptMessage(t, svc, "other", event.Message.MessageRef)
	if _, apiErr := svc.InviteAgentChatMembers(context.Background(), InviteAgentChatMembersInput{Provider: "commons", ChatAlias: alias}); apiErr == nil {
		t.Fatal("adopted grant allowed invite")
	}
	if apiErr := svc.RenameAgentChat(context.Background(), RenameAgentChatInput{Provider: "commons", ChatAlias: alias, Name: "x"}); apiErr == nil {
		t.Fatal("adopted grant allowed rename")
	}
	if _, apiErr := svc.ReleaseAgentChat(context.Background(), ReleaseAgentChatInput{Provider: "commons", ChatAlias: alias}); apiErr != nil {
		t.Fatal(apiErr)
	}
	if svc.Config().RuntimeChats.Granted("commons", alias) || !svc.Config().RuntimeChats.Granted("other", alias) {
		t.Fatal("release crossed provider boundary")
	}
	in = humanGroupMessage(alias, "oc_shared", "d_after_release", "om_after_release")
	in.AmbientOnly = true
	mustDispatchAgentPrompt(t, svc, in)
	receiveAgentEvent(t, other)
	assertNoAgentEvent(t, sub)
}

func TestAdoptDeniesDisabledAndDisallowedApp(t *testing.T) {
	svc, _, _ := adoptionService(t)
	p := svc.cfg().AgentProviders["commons"]
	p.AllowAdoptChats = false
	svc.cfg().AgentProviders["commons"] = p
	if _, apiErr := svc.AdoptAgentChat(context.Background(), AdoptAgentChatInput{Provider: "commons", MessageRef: "ref"}); apiErr == nil || apiErr.Code != "provider_scope_denied" {
		t.Fatalf("disabled adopt: %+v", apiErr)
	}
	if _, apiErr := svc.ReleaseAgentChat(context.Background(), ReleaseAgentChatInput{Provider: "commons", ChatAlias: "ops"}); apiErr == nil || apiErr.Code != "provider_scope_denied" {
		t.Fatalf("disabled release: %+v", apiErr)
	}
	p.AllowAdoptChats = true
	p.AllowedApps = []string{"elsewhere"}
	svc.cfg().AgentProviders["commons"] = p
	ref := feishu.MessageRefForApp("support", "om_other_app")
	if err := svc.agentRoutes.Put(ref, messageroute.Route{AppAlias: "support", ChatID: "oc_ops", ChatAlias: "ops", MessageID: "om_other_app", Providers: []string{"commons"}}, time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, apiErr := svc.AdoptAgentChat(context.Background(), AdoptAgentChatInput{Provider: "commons", MessageRef: ref}); apiErr == nil || apiErr.Code != "unknown_chat" {
		t.Fatalf("disallowed app = %+v", apiErr)
	}
}

func TestAdoptWaitsForDeliveredRoutePublication(t *testing.T) {
	svc, _, _ := adoptionService(t)
	sub := mustSubscribeAgent(t, svc, AgentSubscribeOptions{Provider: "commons", IncludeUnmatchedMessages: true})
	store := &blockingAdoptRoutes{Store: messageroute.NewMemory(time.Hour), entered: make(chan struct{}), resume: make(chan struct{})}
	svc.SetAgentMessageRouteStore(store)
	dispatchDone := make(chan struct{})
	go func() {
		defer close(dispatchDone)
		_, _ = svc.DispatchCommand(context.Background(), humanGroupMessage("ops", "oc_ops", "d_race", "om_race"))
	}()
	event := receiveAgentEvent(t, sub)
	<-store.entered
	adopted := make(chan error, 1)
	go func() {
		_, apiErr := svc.AdoptAgentChat(context.Background(), AdoptAgentChatInput{Provider: "commons", MessageRef: event.Message.MessageRef})
		if apiErr != nil {
			adopted <- fmt.Errorf("%+v", apiErr)
		} else {
			adopted <- nil
		}
	}()
	select {
	case err := <-adopted:
		close(store.resume)
		t.Fatalf("adopt bypassed publication: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(store.resume)
	<-dispatchDone
	if err := <-adopted; err != nil {
		t.Fatal(err)
	}
}

type blockingAdoptRoutes struct {
	*messageroute.Store
	entered, resume chan struct{}
}

func (s *blockingAdoptRoutes) Put(key string, route messageroute.Route, now time.Time) error {
	close(s.entered)
	<-s.resume
	return s.Store.Put(key, route, now)
}

func TestAdoptGrantStoreFailure(t *testing.T) {
	svc, _, _ := adoptionService(t)
	registry, err := chatgrant.Open(filepath.Join(t.TempDir(), "missing"))
	if err != nil {
		t.Fatal(err)
	}
	svc.cfg().RuntimeChats = registry
	sub := mustSubscribeAgent(t, svc, AgentSubscribeOptions{Provider: "commons", IncludeUnmatchedMessages: true})
	in := humanGroupMessage(config.UnconfiguredGroupAliasPrefix+strings.Repeat("c", 64), "oc_new", "d_new", "om_new")
	in.UnconfiguredGroup = true
	mustDispatchAgentPrompt(t, svc, in)
	event := receiveAgentEvent(t, sub)
	if _, apiErr := svc.AdoptAgentChat(context.Background(), AdoptAgentChatInput{Provider: "commons", MessageRef: event.Message.MessageRef}); apiErr == nil || apiErr.Code != "grant_store_unavailable" {
		t.Fatalf("persist failure = %+v", apiErr)
	}
	if registry.Granted("commons", in.ChatAlias) {
		t.Fatal("failed adoption granted in memory")
	}
}

func TestAdoptRejectsRemappedAliasAndFailedRouteRetention(t *testing.T) {
	svc, backend, _ := adoptionService(t)
	sub := mustSubscribeAgent(t, svc, AgentSubscribeOptions{Provider: "commons", IncludeUnmatchedMessages: true})
	store := &countingRouteStore{Store: messageroute.NewMemory(time.Hour), failPuts: 1}
	svc.SetAgentMessageRouteStore(store)
	mustDispatchAgentPrompt(t, svc, humanGroupMessage("ops", "oc_ops", "d_failed_route", "om_failed_route"))
	event := receiveAgentEvent(t, sub)
	if _, apiErr := svc.AdoptAgentChat(context.Background(), AdoptAgentChatInput{Provider: "commons", MessageRef: event.Message.MessageRef}); apiErr == nil || apiErr.Code != "unknown_chat" {
		t.Fatalf("failed route: %+v", apiErr)
	}
	if backend.membershipCalls != 0 {
		t.Fatal("failed retention reached Feishu")
	}
	mustDispatchAgentPrompt(t, svc, humanGroupMessage("ops", "oc_ops", "d_before_remap", "om_before_remap"))
	event = receiveAgentEvent(t, sub)
	cfg := commonsConfig()
	cfg.AgentProviders = svc.Config().AgentProviders
	cfg.ChannelRoutes["ops"] = config.ChannelRoute{AppAlias: "support", ChatID: "oc_somebody_else"}
	svc.ApplyConfig(cfg, config.ReloadPlan{}, nil, nil, time.Now())
	if _, apiErr := svc.AdoptAgentChat(context.Background(), AdoptAgentChatInput{Provider: "commons", MessageRef: event.Message.MessageRef}); apiErr == nil || apiErr.Code != "unknown_chat" {
		t.Fatalf("remapped alias: %+v", apiErr)
	}
}
