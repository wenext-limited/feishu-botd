package service

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"feishu-botd/internal/config"
	"feishu-botd/internal/dedupe"
	"feishu-botd/internal/feishu"
	"feishu-botd/internal/messageroute"
	"feishu-botd/internal/notify"
	"feishu-botd/internal/ownership"
)

func postInput(operationID string) PostAgentMessageInput {
	return PostAgentMessageInput{
		Provider:    "commons",
		OperationID: operationID,
		ChatAlias:   "refund-backend",
		Markdown:    "**Q (agent-a → agent-b):** why does the refund retry loop?",
		Summary:     "Mirrored question",
	}
}

func mustPost(t *testing.T, svc *Service, in PostAgentMessageInput) AgentPostReceipt {
	t.Helper()
	receipt, apiErr := svc.PostAgentMessage(context.Background(), in)
	if apiErr != nil {
		t.Fatalf("post agent message: %v", apiErr)
	}
	return receipt
}

func TestPostAgentMessageCreatesAnOwnedRootThatRepliesRouteBackTo(t *testing.T) {
	svc, backend, owners := commonsService(t)
	commons := mustSubscribeAgent(t, svc, AgentSubscribeOptions{Provider: "commons", IncludeUnmatchedMessages: true})

	receipt := mustPost(t, svc, postInput("mirror-root-1"))
	posts := backend.postSnapshot()
	if len(posts) != 1 {
		t.Fatalf("post calls = %d, want 1", len(posts))
	}
	post := posts[0]
	if post.ChatID != "oc_refund" || post.ReplyToMessageID != "" || post.ReplyInThread {
		t.Fatalf("root post route = %#v", post)
	}
	if post.Content.Markdown != postInput("").Markdown || post.Content.Title != "Mirrored question" ||
		post.Content.Source != agentPostSource || post.Content.DedupeKey == "" {
		t.Fatalf("root post content = %#v", post.Content)
	}
	if receipt.MessageRef != feishu.MessageRefForApp("support", "om_post_1") || receipt.ThreadRef != "" || receipt.Duplicate {
		t.Fatalf("receipt = %#v", receipt)
	}
	if owner := owners.owners[receipt.MessageRef]; owner.Provider != "commons" {
		t.Fatalf("posted message owner = %#v", owner)
	}

	// A human answers the post without mentioning the bot.
	reply := humanGroupMessage("refund-backend", "oc_refund", "delivery_answer", "om_answer")
	reply.UnmentionedReply = true
	reply.Metadata["parent_id"] = "om_post_1"
	mustDispatchAgentPrompt(t, svc, reply)
	event := receiveAgentEvent(t, commons)
	if event.Message.Addressing != AgentAddressingReply || event.Message.ReplyToMessageRef != receipt.MessageRef {
		t.Fatalf("reply to the post = %#v", event.Message)
	}
}

func TestPostAgentMessageRepliesInThreadToAReceivedMessage(t *testing.T) {
	svc, backend, _ := commonsService(t)
	commons := mustSubscribeAgent(t, svc, AgentSubscribeOptions{Provider: "commons", IncludeAmbientMessages: true})
	mustDispatchAgentPrompt(t, svc, ambientMessage("delivery_question", "om_question"))
	question := receiveAgentEvent(t, commons)

	in := postInput("answer-1")
	in.ChatAlias = ""
	in.ReplyToMessageRef = question.Message.MessageRef
	in.ReplyInThread = true
	in.MentionUserID = "ou_human"
	receipt := mustPost(t, svc, in)

	post := backend.postSnapshot()[0]
	if post.ReplyToMessageID != "om_question" || !post.ReplyInThread || post.Content.MentionUserID != "ou_human" {
		t.Fatalf("threaded reply = %#v", post)
	}
	if receipt.ThreadRef != feishu.ThreadRefForApp("support", "omt_mirrored") {
		t.Fatalf("thread ref = %q", receipt.ThreadRef)
	}

	// The provider can keep answering inside the thread through its own post.
	follow := postInput("answer-2")
	follow.ChatAlias = ""
	follow.ReplyToMessageRef = receipt.MessageRef
	follow.ReplyInThread = true
	_ = mustPost(t, svc, follow)
	if got := backend.postSnapshot()[1].ReplyToMessageID; got != "om_post_1" {
		t.Fatalf("reply to own post targeted %q", got)
	}
}

func TestPostAgentMessageCanAnswerItsOwnResponseCard(t *testing.T) {
	svc, backend, _ := commonsService(t)
	commons := mustSubscribeAgent(t, svc, AgentSubscribeOptions{Provider: "commons", IncludeUnmatchedMessages: true})
	mustDispatchAgentPrompt(t, svc, humanGroupMessage("refund-backend", "oc_refund", "delivery_card", "om_card_prompt"))
	event := receiveAgentEvent(t, commons)
	card, apiErr := svc.StartAgentResponse(context.Background(), StartAgentResponseInput{
		Provider: "commons", DeliveryID: event.DeliveryID, OperationID: "start-1",
		Content: AgentResponseContent{Markdown: "Looking into it"},
	})
	if apiErr != nil {
		t.Fatalf("start response: %v", apiErr)
	}

	in := postInput("later-1")
	in.ChatAlias = ""
	in.ReplyToMessageRef = card.MessageRef
	_ = mustPost(t, svc, in)
	if got := backend.postSnapshot()[0].ReplyToMessageID; got != backend.messageID {
		t.Fatalf("reply to own card targeted %q, want %q", got, backend.messageID)
	}
}

func TestPostAgentMessageIsIdempotentPerOperation(t *testing.T) {
	svc, backend, _ := commonsService(t)

	first := mustPost(t, svc, postInput("op-1"))
	replay := mustPost(t, svc, postInput("op-1"))
	if !replay.Duplicate || replay.MessageRef != first.MessageRef || replay.ThreadRef != first.ThreadRef {
		t.Fatalf("replay = %#v, want duplicate of %#v", replay, first)
	}
	if calls := len(backend.postSnapshot()); calls != 1 {
		t.Fatalf("post calls = %d, want 1", calls)
	}

	changed := postInput("op-1")
	changed.Markdown = "a different body"
	if _, apiErr := svc.PostAgentMessage(context.Background(), changed); apiErr == nil || apiErr.Code != "operation_conflict" || apiErr.Retryable {
		t.Fatalf("conflicting reuse = %#v, want operation_conflict", apiErr)
	}
	moved := postInput("op-1")
	moved.ChatAlias = ""
	moved.ReplyToMessageRef = first.MessageRef
	if _, apiErr := svc.PostAgentMessage(context.Background(), moved); apiErr == nil || apiErr.Code != "operation_conflict" {
		t.Fatalf("retargeted reuse = %#v, want operation_conflict", apiErr)
	}
	if calls := len(backend.postSnapshot()); calls != 1 {
		t.Fatalf("post calls after conflicts = %d, want 1", calls)
	}
}

func TestPostAgentMessageRetryReusesTheMessageUUIDSeed(t *testing.T) {
	svc, backend, _ := commonsService(t)
	backend.postErrs = []error{&feishu.MessageSendError{Operation: "message_create", Class: "transport", Retryable: true}}

	if _, apiErr := svc.PostAgentMessage(context.Background(), postInput("op-ambiguous")); apiErr == nil || apiErr.Code != "feishu_unavailable" || !apiErr.Retryable {
		t.Fatalf("ambiguous failure = %#v, want retryable feishu_unavailable", apiErr)
	}
	receipt := mustPost(t, svc, postInput("op-ambiguous"))
	posts := backend.postSnapshot()
	if len(posts) != 2 || posts[0].Content.DedupeKey != posts[1].Content.DedupeKey {
		t.Fatalf("retry did not reuse the UUID seed: %#v", posts)
	}
	if receipt.Duplicate || receipt.MessageRef == "" {
		t.Fatalf("retried receipt = %#v", receipt)
	}

	// An ambiguous attempt pins the operation to its content.
	backend.postErrs = []error{&feishu.MessageSendError{Operation: "message_create", Class: "transport", Retryable: true}}
	_, _ = svc.PostAgentMessage(context.Background(), postInput("op-pinned"))
	changed := postInput("op-pinned")
	changed.Markdown = "corrected"
	if _, apiErr := svc.PostAgentMessage(context.Background(), changed); apiErr == nil || apiErr.Code != "operation_conflict" {
		t.Fatalf("content change after an ambiguous attempt = %#v, want operation_conflict", apiErr)
	}
}

func TestPostAgentMessageDefinitiveRejectionReleasesTheOperation(t *testing.T) {
	svc, backend, _ := commonsService(t)
	backend.postErrs = []error{&feishu.MessageSendError{Operation: "message_create", Class: "api_rejected", HTTPStatus: 400, Code: 230002}}

	if _, apiErr := svc.PostAgentMessage(context.Background(), postInput("op-rejected")); apiErr == nil || apiErr.Code != "feishu_rejected" {
		t.Fatalf("rejection = %#v, want feishu_rejected", apiErr)
	}
	corrected := postInput("op-rejected")
	corrected.Markdown = "corrected body"
	if receipt := mustPost(t, svc, corrected); receipt.Duplicate {
		t.Fatalf("corrected post = %#v", receipt)
	}
}

func TestPostAgentMessageEnforcesTheChatAllowlist(t *testing.T) {
	svc, backend, _ := commonsService(t)

	for name, alias := range map[string]string{
		"configured but not granted": "ops",
		"not configured":             "nowhere",
		"direct":                     "direct",
	} {
		in := postInput("op-" + alias)
		in.ChatAlias = alias
		if _, apiErr := svc.PostAgentMessage(context.Background(), in); apiErr == nil || apiErr.Code != "unknown_channel" {
			t.Fatalf("%s: post = %#v, want unknown_channel", name, apiErr)
		}
	}
	// An unnormalized spelling of a granted alias is the same alias.
	in := postInput("op-spelling")
	in.ChatAlias = " Refund_Backend "
	_ = mustPost(t, svc, in)
	if calls := len(backend.postSnapshot()); calls != 1 {
		t.Fatalf("post calls = %d, want only the granted one", calls)
	}
}

func TestPostAgentMessageRequiresTheProactiveGrant(t *testing.T) {
	svc, backend, _ := commonsService(t)
	in := postInput("op-ibot")
	in.Provider = "ibot"
	if _, apiErr := svc.PostAgentMessage(context.Background(), in); apiErr == nil || apiErr.Code != "provider_scope_denied" {
		t.Fatalf("ungranted provider = %#v, want provider_scope_denied", apiErr)
	}
	if len(backend.postSnapshot()) != 0 {
		t.Fatal("an ungranted provider reached Feishu")
	}
}

func TestPostAgentMessageRejectsReplyTargetsTheProviderMayNotUse(t *testing.T) {
	svc, backend, _ := commonsService(t)
	commons := mustSubscribeAgent(t, svc, AgentSubscribeOptions{Provider: "commons", IncludeUnmatchedMessages: true})

	// Received, but in a chat outside proactive_chat_aliases.
	mustDispatchAgentPrompt(t, svc, humanGroupMessage("ops", "oc_ops", "delivery_ops_mention", "om_ops_mention"))
	opsEvent := receiveAgentEvent(t, commons)
	posted := mustPost(t, svc, postInput("op-root"))

	cfg := commonsConfig()
	other := cfg.AgentProviders["commons"]
	cfg.AgentProviders["other"] = other
	otherSvc := NewMultiAppService(cfg, map[string]feishu.Sender{"support": backend}, dedupe.NewMemoryStore(time.Hour), nil)
	otherSvc.SetAgentMessageRouteStore(svc.agentRoutes)

	for name, testCase := range map[string]struct {
		svc      *Service
		provider string
		ref      string
	}{
		"unknown ref":                  {svc, "commons", "msgref_unknown"},
		"thread ref is not a message":  {svc, "commons", feishu.ThreadRefForApp("support", "omt_x")},
		"received outside the grant":   {svc, "commons", opsEvent.Message.MessageRef},
		"authored by another provider": {otherSvc, "other", posted.MessageRef},
	} {
		in := postInput("op-" + strings.ReplaceAll(name, " ", "-"))
		in.Provider = testCase.provider
		in.ChatAlias = ""
		in.ReplyToMessageRef = testCase.ref
		if _, apiErr := testCase.svc.PostAgentMessage(context.Background(), in); apiErr == nil || apiErr.Code != "unknown_message" || apiErr.Retryable {
			t.Fatalf("%s: post = %#v, want unknown_message", name, apiErr)
		}
	}
	if calls := len(backend.postSnapshot()); calls != 1 {
		t.Fatalf("post calls = %d, want only the root", calls)
	}
}

func TestPostAgentMessageRejectsATargetWhoseAliasNowNamesAnotherChat(t *testing.T) {
	svc, backend, _ := commonsService(t)
	posted := mustPost(t, svc, postInput("op-root"))

	cfg := commonsConfig()
	cfg.ChannelRoutes["refund-backend"] = config.ChannelRoute{AppAlias: "support", ChatID: "oc_someone_else"}
	remapped := NewMultiAppService(cfg, map[string]feishu.Sender{"support": backend}, dedupe.NewMemoryStore(time.Hour), nil)
	remapped.SetAgentMessageRouteStore(svc.agentRoutes)

	in := postInput("op-after-remap")
	in.ChatAlias = ""
	in.ReplyToMessageRef = posted.MessageRef
	if _, apiErr := remapped.PostAgentMessage(context.Background(), in); apiErr == nil || apiErr.Code != "unknown_message" {
		t.Fatalf("post after alias remap = %#v, want unknown_message", apiErr)
	}
}

func TestPostAgentMessageValidatesItsRequest(t *testing.T) {
	svc, _, _ := commonsService(t)
	for name, testCase := range map[string]struct {
		mutate func(*PostAgentMessageInput)
		code   string
	}{
		"no target":    {func(in *PostAgentMessageInput) { in.ChatAlias = "" }, "missing_target"},
		"two targets":  {func(in *PostAgentMessageInput) { in.ReplyToMessageRef = "msgref_x" }, "invalid_target"},
		"no markdown":  {func(in *PostAgentMessageInput) { in.Markdown = "  " }, "missing_markdown"},
		"no operation": {func(in *PostAgentMessageInput) { in.OperationID = "" }, "missing_operation_id"},
		"long summary": {func(in *PostAgentMessageInput) { in.Summary = strings.Repeat("s", maxAgentTitleBytes+1) }, "field_too_large"},
	} {
		in := postInput("op-validate")
		testCase.mutate(&in)
		if _, apiErr := svc.PostAgentMessage(context.Background(), in); apiErr == nil || apiErr.Code != testCase.code {
			t.Fatalf("%s: post = %#v, want %s", name, apiErr, testCase.code)
		}
	}
}

func TestPostAgentMessageReachesAnUnconfiguredGroupByItsOpaqueAlias(t *testing.T) {
	cfg := commonsConfig()
	opaque := "unconfigured-group-" + strings.Repeat("ab", 32)
	commons := cfg.AgentProviders["commons"]
	commons.ProactiveChatAliases = append(commons.ProactiveChatAliases, opaque)
	cfg.AgentProviders["commons"] = commons
	backend := newFakePostBackend()
	svc := NewMultiAppService(cfg, map[string]feishu.Sender{"support": backend}, dedupe.NewMemoryStore(time.Hour), nil)
	svc.SetAgentOwnershipStore(&fakeOwnerStore{owners: make(map[string]ownership.Owner)})

	in := postInput("op-opaque")
	in.ChatAlias = opaque
	if _, apiErr := svc.PostAgentMessage(context.Background(), in); apiErr == nil || apiErr.Code != "unknown_channel" {
		t.Fatalf("opaque alias before ingress = %#v, want unknown_channel", apiErr)
	}

	sub := mustSubscribeAgent(t, svc, AgentSubscribeOptions{Provider: "commons", IncludeUnmatchedMessages: true})
	ingress := humanGroupMessage(opaque, "oc_invited_group", "delivery_invited", "om_invited")
	ingress.UnconfiguredGroup = true
	mustDispatchAgentPrompt(t, svc, ingress)
	_ = receiveAgentEvent(t, sub)

	in.OperationID = "op-opaque-2"
	_ = mustPost(t, svc, in)
	if got := backend.postSnapshot(); len(got) != 1 || got[0].ChatID != "oc_invited_group" {
		t.Fatalf("opaque root post = %#v", got)
	}
}

func TestPostAgentMessageRefsSurviveARestartThroughTheDurableRouteStore(t *testing.T) {
	dir := t.TempDir()
	keys := map[string][]byte{"support": messageroute.SealKey("support", "secret")}
	open := func() *messageroute.Store {
		store, err := messageroute.Open(dir, 7*24*time.Hour, keys, time.Now())
		if err != nil {
			t.Fatalf("open route store: %v", err)
		}
		t.Cleanup(func() { _ = store.Close() })
		return store
	}

	svc, _, _ := commonsService(t)
	svc.SetAgentMessageRouteStore(open())
	commons := mustSubscribeAgent(t, svc, AgentSubscribeOptions{Provider: "commons", IncludeAmbientMessages: true})
	mustDispatchAgentPrompt(t, svc, ambientMessage("delivery_before_restart", "om_before_restart"))
	questionRef := receiveAgentEvent(t, commons).Message.MessageRef
	posted := mustPost(t, svc, postInput("op-before-restart"))

	restarted, backend, _ := commonsService(t)
	restarted.SetAgentMessageRouteStore(open())
	for _, ref := range []string{questionRef, posted.MessageRef} {
		in := postInput("op-after-restart-" + ref[len(ref)-6:])
		in.ChatAlias = ""
		in.ReplyToMessageRef = ref
		_ = mustPost(t, restarted, in)
	}
	posts := backend.postSnapshot()
	if len(posts) != 2 || posts[0].ReplyToMessageID != "om_before_restart" || posts[1].ReplyToMessageID != "om_post_1" {
		t.Fatalf("post-restart replies = %#v", posts)
	}
}

func TestProviderWithoutProactiveGrantRetainsNoReplyRoutes(t *testing.T) {
	svc, backend, _ := commonsService(t)
	routes := &countingRouteStore{Store: messageroute.NewMemory(time.Hour)}
	svc.SetAgentMessageRouteStore(routes)
	ibot := mustSubscribeAgent(t, svc, AgentSubscribeOptions{Provider: "ibot", IncludeUnmatchedMessages: true})

	// ops has no proactive provider at all, so neither ingress nor ibot's
	// own card or follow-up writes a route.
	mustDispatchAgentPrompt(t, svc, humanGroupMessage("ops", "oc_ops", "delivery_ibot_ops", "om_ibot_ops"))
	event := receiveAgentEvent(t, ibot)
	if _, apiErr := svc.StartAgentResponse(context.Background(), StartAgentResponseInput{
		Provider: "ibot", DeliveryID: event.DeliveryID, OperationID: "start-1",
		Content: AgentResponseContent{Markdown: "on it"},
	}); apiErr != nil {
		t.Fatalf("start: %v", apiErr)
	}
	if _, apiErr := sendFollowUp(t, svc, "ibot", event.ConversationID, "fup-1", "done"); apiErr != nil {
		t.Fatalf("follow-up: %v", apiErr)
	}
	if routes.puts != 0 {
		t.Fatalf("route writes = %d, want none for a deployment path without proactive grants", routes.puts)
	}
	if len(backend.snapshot()) != 1 {
		t.Fatalf("follow-up sends = %d", len(backend.snapshot()))
	}
}

func TestPostAgentMessagePersistenceFailureIsRetryableAndReplayRepairsIt(t *testing.T) {
	svc, backend, _ := commonsService(t)
	failing := &countingRouteStore{Store: messageroute.NewMemory(time.Hour), failPuts: 1}
	svc.SetAgentMessageRouteStore(failing)

	if _, apiErr := svc.PostAgentMessage(context.Background(), postInput("op-persist")); apiErr == nil || apiErr.Code != "message_route_store_unavailable" || !apiErr.Retryable {
		t.Fatalf("persistence failure = %#v, want retryable message_route_store_unavailable", apiErr)
	}
	replay := mustPost(t, svc, postInput("op-persist"))
	if !replay.Duplicate || len(backend.postSnapshot()) != 1 {
		t.Fatalf("replay = %#v posts=%d", replay, len(backend.postSnapshot()))
	}
	if _, ok, _ := failing.Lookup(replay.MessageRef, time.Now()); !ok {
		t.Fatal("replay did not repair the missing route")
	}
}

type countingRouteStore struct {
	*messageroute.Store
	puts     int
	failPuts int
}

func (s *countingRouteStore) Put(key string, route messageroute.Route, now time.Time) error {
	s.puts++
	if s.failPuts > 0 {
		s.failPuts--
		return errors.New("disk full")
	}
	return s.Store.Put(key, route, now)
}

func TestReplyGrantNarrowsToProvidersThatActuallyReceivedTheMessage(t *testing.T) {
	cfg := commonsConfig()
	// "bystander" may speak in refund-backend but holds no ambient grant.
	cfg.AgentProviders["bystander"] = config.AgentProviderConfig{
		AllowUnmatchedMessages: true, AllowProactiveMessages: true, ProactiveChatAliases: []string{"refund-backend"},
	}
	backend := newFakePostBackend()
	svc := NewMultiAppService(cfg, map[string]feishu.Sender{"support": backend}, dedupe.NewMemoryStore(time.Hour), nil)
	svc.SetAgentOwnershipStore(&fakeOwnerStore{owners: make(map[string]ownership.Owner)})
	legacy, apiErr := svc.SubscribeCommands(context.Background(), "legacy", []string{"refund"})
	if apiErr != nil {
		t.Fatalf("subscribe legacy: %v", apiErr)
	}
	t.Cleanup(legacy.Close)
	commons := mustSubscribeAgent(t, svc, AgentSubscribeOptions{Provider: "commons", IncludeAmbientMessages: true})
	_ = mustSubscribeAgent(t, svc, AgentSubscribeOptions{Provider: "bystander", IncludeUnmatchedMessages: true})

	// A legacy consumer owns the command; commons observes it; bystander,
	// although proactive in the chat, never receives it.
	in := humanGroupMessage("refund-backend", "oc_refund", "delivery_legacy_owned", "om_legacy_owned")
	mustDispatchAgentPrompt(t, svc, in)
	<-legacy.C
	ref := receiveAgentEvent(t, commons).Message.MessageRef

	reply := func(provider, operationID string) *notify.APIError {
		post := postInput(operationID)
		post.Provider = provider
		post.ChatAlias = ""
		post.ReplyToMessageRef = ref
		_, apiErr := svc.PostAgentMessage(context.Background(), post)
		return apiErr
	}
	if apiErr := reply("bystander", "op-bystander"); apiErr == nil || apiErr.Code != "unknown_message" {
		t.Fatalf("non-recipient reply = %#v, want unknown_message", apiErr)
	}
	if apiErr := reply("commons", "op-commons"); apiErr != nil {
		t.Fatalf("recipient reply: %v", apiErr)
	}

	// A redelivery must not strip the grant the first delivery earned.
	in.DeliveryID = "delivery_legacy_owned_again"
	mustDispatchAgentPrompt(t, svc, in)
	if apiErr := reply("commons", "op-commons-again"); apiErr != nil {
		t.Fatalf("recipient reply after redelivery: %v", apiErr)
	}
}

func TestReplyGrantIsRevokedWhenNoGranteeReceivedTheMessage(t *testing.T) {
	svc, _, _ := commonsService(t)
	// commons may receive addressed messages here but has no live stream.
	mustDispatchAgentPrompt(t, svc, humanGroupMessage("refund-backend", "oc_refund", "delivery_unheard", "om_unheard"))
	if _, ok, _ := svc.agentRoutes.Lookup(feishu.MessageRefForApp("support", "om_unheard"), time.Now()); ok {
		t.Fatal("a route survived for a message no grantee received")
	}
}

func TestPartiallyPostedMessageIsOwnedAndPinnedToItsContent(t *testing.T) {
	svc, backend, owners := commonsService(t)
	partial := &partialPostBackend{fakePostBackend: backend, accepted: []string{"om_part_1"}}
	svc.backends["support"] = appBackend{sender: partial, dynamicCards: partial, poster: partial}

	if _, apiErr := svc.PostAgentMessage(context.Background(), postInput("op-partial")); apiErr == nil || apiErr.Code != "feishu_unavailable" || !apiErr.Retryable {
		t.Fatalf("partial post = %#v, want retryable feishu_unavailable", apiErr)
	}
	if owner := owners.owners[feishu.MessageRefForApp("support", "om_part_1")]; owner.Provider != "commons" {
		t.Fatalf("accepted part is unowned: %#v", owner)
	}
	changed := postInput("op-partial")
	changed.Markdown = "something else"
	if _, apiErr := svc.PostAgentMessage(context.Background(), changed); apiErr == nil || apiErr.Code != "operation_conflict" {
		t.Fatalf("content change after a partial post = %#v, want operation_conflict", apiErr)
	}
}

// partialPostBackend accepts the first parts of a post and then fails.
type partialPostBackend struct {
	*fakePostBackend
	accepted []string
}

func (p *partialPostBackend) PostMessage(context.Context, feishu.PostMessageRequest) (feishu.PostedMessage, error) {
	return feishu.PostedMessage{MessageIDs: append([]string(nil), p.accepted...)},
		&feishu.MessageSendError{Operation: "message_create", Class: "transport", Retryable: true}
}
