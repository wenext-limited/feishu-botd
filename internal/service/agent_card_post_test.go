package service

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"feishu-botd/internal/dedupe"
	"feishu-botd/internal/feishu"
	"feishu-botd/internal/messageroute"
	"feishu-botd/internal/ownership"
)

// fakeCardBackend adds the proactive card and pin paths to the post double.
// Each *Errs slice is consumed one entry per call. replaceGate, when set,
// holds every ReplaceCard until it is closed; replaceEntered reports entry.
type fakeCardBackend struct {
	*fakePostBackend

	cardMu         sync.Mutex
	nextCard       int
	created        []string
	createErrs     []error
	cardPosts      []feishu.CardPostRequest
	cardPostErrs   []error
	replaces       []feishu.CardReplaceRequest
	replaceErrs    []error
	replaceGate    chan struct{}
	replaceEntered chan struct{}
	pins           []string
	pinErrs        []error
}

func popErr(errs *[]error) error {
	if len(*errs) == 0 {
		return nil
	}
	err := (*errs)[0]
	*errs = (*errs)[1:]
	return err
}

func (f *fakeCardBackend) CreateCard(_ context.Context, cardJSON string) (string, error) {
	f.cardMu.Lock()
	defer f.cardMu.Unlock()
	f.created = append(f.created, cardJSON)
	if err := popErr(&f.createErrs); err != nil {
		return "", err
	}
	f.nextCard++
	return fmt.Sprintf("card_task_%d", f.nextCard), nil
}

func (f *fakeCardBackend) PostCard(_ context.Context, req feishu.CardPostRequest) (feishu.PostedMessage, error) {
	f.cardMu.Lock()
	defer f.cardMu.Unlock()
	f.cardPosts = append(f.cardPosts, req)
	if err := popErr(&f.cardPostErrs); err != nil {
		return feishu.PostedMessage{}, err
	}
	posted := feishu.PostedMessage{MessageIDs: []string{"om_" + req.CardID}}
	if req.ReplyInThread {
		posted.ThreadID = "omt_task"
	}
	return posted, nil
}

func (f *fakeCardBackend) ReplaceCard(_ context.Context, req feishu.CardReplaceRequest) error {
	f.cardMu.Lock()
	gate, entered := f.replaceGate, f.replaceEntered
	f.cardMu.Unlock()
	if entered != nil {
		entered <- struct{}{}
	}
	if gate != nil {
		<-gate
	}
	f.cardMu.Lock()
	defer f.cardMu.Unlock()
	f.replaces = append(f.replaces, req)
	return popErr(&f.replaceErrs)
}

func (f *fakeCardBackend) PinMessage(_ context.Context, messageID string) error {
	f.cardMu.Lock()
	defer f.cardMu.Unlock()
	f.pins = append(f.pins, messageID)
	return popErr(&f.pinErrs)
}

func (f *fakeCardBackend) cardSnapshot() ([]string, []feishu.CardPostRequest, []feishu.CardReplaceRequest, []string) {
	f.cardMu.Lock()
	defer f.cardMu.Unlock()
	return append([]string(nil), f.created...), append([]feishu.CardPostRequest(nil), f.cardPosts...),
		append([]feishu.CardReplaceRequest(nil), f.replaces...), append([]string(nil), f.pins...)
}

// cardService is commonsService with the card double; commons may also pin.
func cardService(t *testing.T) (*Service, *fakeCardBackend, *fakeOwnerStore) {
	t.Helper()
	cfg := commonsConfig()
	commons := cfg.AgentProviders["commons"]
	commons.AllowPins = true
	cfg.AgentProviders["commons"] = commons
	backend := &fakeCardBackend{fakePostBackend: newFakePostBackend()}
	svc := NewMultiAppService(cfg, map[string]feishu.Sender{"support": backend}, dedupe.NewMemoryStore(time.Hour), nil)
	owners := &fakeOwnerStore{owners: make(map[string]ownership.Owner)}
	svc.SetAgentOwnershipStore(owners)
	return svc, backend, owners
}

func taskCard(state AgentCardState) AgentCardContent {
	return AgentCardContent{
		Title:    "Refund retry loops",
		State:    state,
		Markdown: "**agent-b** is reading the retry policy.",
		Steps:    []AgentCardStep{{Label: "Reproduce", Done: true}, {Label: "Fix *backoff*"}},
		Links:    []AgentCardLink{{Label: "Diff", URL: "https://example.com/pr/1"}},
		Summary:  "Refund task",
	}
}

func cardInput(operationID string) PostAgentCardInput {
	return PostAgentCardInput{Provider: "commons", OperationID: operationID, ChatAlias: "refund-backend", Content: taskCard(AgentCardStateProcessing)}
}

func mustPostCard(t *testing.T, svc *Service, in PostAgentCardInput) AgentPostReceipt {
	t.Helper()
	receipt, apiErr := svc.PostAgentCard(context.Background(), in)
	if apiErr != nil {
		t.Fatalf("post agent card: %v", apiErr)
	}
	return receipt
}

func updateInput(ref, operationID string, state AgentCardState) UpdateAgentCardInput {
	return UpdateAgentCardInput{Provider: "commons", MessageRef: ref, OperationID: operationID, Content: taskCard(state)}
}

func mustUpdateCard(t *testing.T, svc *Service, in UpdateAgentCardInput) AgentCardUpdateReceipt {
	t.Helper()
	receipt, apiErr := svc.UpdateAgentCard(context.Background(), in)
	if apiErr != nil {
		t.Fatalf("update agent card: %v", apiErr)
	}
	return receipt
}

func TestBuildAgentTaskCardRendersTypedContentAsPlainTextParts(t *testing.T) {
	cardJSON, apiErr := buildAgentTaskCard(taskCard(AgentCardStateSuspended))
	if apiErr != nil {
		t.Fatalf("build: %v", apiErr)
	}
	var card struct {
		Schema string `json:"schema"`
		Config struct {
			UpdateMulti   bool           `json:"update_multi"`
			StreamingMode *bool          `json:"streaming_mode"`
			Summary       map[string]any `json:"summary"`
		} `json:"config"`
		Header struct {
			Title    map[string]any   `json:"title"`
			Template string           `json:"template"`
			Tags     []map[string]any `json:"text_tag_list"`
		} `json:"header"`
		Body struct {
			Elements []map[string]any `json:"elements"`
		} `json:"body"`
	}
	if err := json.Unmarshal([]byte(cardJSON), &card); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if card.Schema != "2.0" || !card.Config.UpdateMulti || card.Config.StreamingMode != nil || card.Config.Summary["content"] != "Refund task" {
		t.Fatalf("card config = %#v", card.Config)
	}
	if card.Header.Title["tag"] != "plain_text" || card.Header.Title["content"] != "Refund retry loops" || card.Header.Template != "orange" ||
		len(card.Header.Tags) != 1 || card.Header.Tags[0]["color"] != "orange" ||
		card.Header.Tags[0]["text"].(map[string]any)["content"] != "等人处理" {
		t.Fatalf("card header = %#v", card.Header)
	}
	if len(card.Body.Elements) != 3 {
		t.Fatalf("elements = %#v", card.Body.Elements)
	}
	steps := card.Body.Elements[1]["text"].(map[string]any)
	if steps["tag"] != "plain_text" || steps["content"] != "✅ Reproduce\n⬜ Fix *backoff*" {
		t.Fatalf("checklist = %#v", steps)
	}
	if !strings.Contains(cardJSON, `"default_url":"https://example.com/pr/1"`) {
		t.Fatalf("link button is missing: %s", cardJSON)
	}

	for state, want := range map[AgentCardState]string{
		AgentCardStateProcessing: "进行中", AgentCardStateActive: "已回答", AgentCardStateArchived: "已归档",
	} {
		cardJSON, _ := buildAgentTaskCard(taskCard(state))
		if !strings.Contains(cardJSON, want) {
			t.Fatalf("state %d does not render %q", state, want)
		}
	}
	plain, _ := buildAgentTaskCard(AgentCardContent{Title: "Untagged"})
	if strings.Contains(plain, "text_tag_list") || strings.Contains(plain, "template") || !strings.Contains(plain, `"summary":{"content":"Untagged"}`) {
		t.Fatalf("unspecified state or summary fallback = %s", plain)
	}
}

func TestBuildAgentTaskCardValidatesContent(t *testing.T) {
	for name, testCase := range map[string]struct {
		mutate func(*AgentCardContent)
		code   string
	}{
		"no title":         {func(c *AgentCardContent) { c.Title = " " }, "missing_title"},
		"unknown state":    {func(c *AgentCardContent) { c.State = 9 }, "invalid_card_state"},
		"empty step":       {func(c *AgentCardContent) { c.Steps = []AgentCardStep{{Label: " "}} }, "invalid_step"},
		"multi-line step":  {func(c *AgentCardContent) { c.Steps = []AgentCardStep{{Label: "a\nb"}} }, "invalid_step"},
		"http link":        {func(c *AgentCardContent) { c.Links = []AgentCardLink{{Label: "x", URL: "http://example.com"}} }, "invalid_link"},
		"relative link":    {func(c *AgentCardContent) { c.Links = []AgentCardLink{{Label: "x", URL: "/pr/1"}} }, "invalid_link"},
		"userinfo link":    {func(c *AgentCardContent) { c.Links = []AgentCardLink{{Label: "x", URL: "https://u:p@example.com"}} }, "invalid_link"},
		"unlabelled link":  {func(c *AgentCardContent) { c.Links = []AgentCardLink{{URL: "https://example.com"}} }, "invalid_link"},
		"too many steps":   {func(c *AgentCardContent) { c.Steps = make([]AgentCardStep, maxAgentCardSteps+1) }, "field_too_large"},
		"too many links":   {func(c *AgentCardContent) { c.Links = make([]AgentCardLink, maxAgentCardLinks+1) }, "field_too_large"},
		"long title":       {func(c *AgentCardContent) { c.Title = strings.Repeat("t", maxAgentTitleBytes+1) }, "field_too_large"},
		"card over budget": {func(c *AgentCardContent) { c.Markdown = strings.Repeat("m", maxAgentCardBytes-10) }, "field_too_large"},
	} {
		content := taskCard(AgentCardStateActive)
		testCase.mutate(&content)
		if _, apiErr := buildAgentTaskCard(content); apiErr == nil || apiErr.Code != testCase.code {
			t.Fatalf("%s: build = %#v, want %s", name, apiErr, testCase.code)
		}
	}
}

func TestPostAgentCardCreatesAnOwnedRootThatRepliesRouteBackTo(t *testing.T) {
	svc, backend, owners := cardService(t)
	commons := mustSubscribeAgent(t, svc, AgentSubscribeOptions{Provider: "commons", IncludeUnmatchedMessages: true})

	receipt := mustPostCard(t, svc, cardInput("task-1"))
	created, posts, _, _ := backend.cardSnapshot()
	if len(created) != 1 || !strings.Contains(created[0], "Refund retry loops") || len(posts) != 1 {
		t.Fatalf("created=%d posts=%d", len(created), len(posts))
	}
	if post := posts[0]; post.ChatID != "oc_refund" || post.ReplyToMessageID != "" || post.CardID != "card_task_1" || post.UUID == "" {
		t.Fatalf("root card post = %#v", post)
	}
	if receipt.MessageRef != feishu.MessageRefForApp("support", "om_card_task_1") || receipt.ThreadRef != "" || receipt.Duplicate {
		t.Fatalf("receipt = %#v", receipt)
	}
	if strings.Contains(receipt.MessageRef, "om_") || strings.Contains(receipt.MessageRef, "card_task") {
		t.Fatalf("receipt leaks a Feishu id: %q", receipt.MessageRef)
	}
	if owner := owners.owners[receipt.MessageRef]; owner.Provider != "commons" {
		t.Fatalf("card owner = %#v", owner)
	}

	// A human answers inside the card's thread without mentioning the bot.
	reply := humanGroupMessage("refund-backend", "oc_refund", "delivery_card_answer", "om_card_answer")
	reply.UnmentionedReply = true
	reply.Metadata["parent_id"] = "om_card_task_1"
	reply.Metadata["root_id"] = "om_card_task_1"
	reply.Metadata["thread_id"] = "omt_task"
	mustDispatchAgentPrompt(t, svc, reply)
	event := receiveAgentEvent(t, commons)
	if event.Message.Addressing != AgentAddressingReply || event.Message.ReplyToMessageRef != receipt.MessageRef {
		t.Fatalf("reply under the card = %#v", event.Message)
	}

	// The provider can thread a plain message under its own card.
	follow := postInput("under-card")
	follow.ChatAlias = ""
	follow.ReplyToMessageRef = receipt.MessageRef
	follow.ReplyInThread = true
	_ = mustPost(t, svc, follow)
	if got := backend.postSnapshot()[0].ReplyToMessageID; got != "om_card_task_1" {
		t.Fatalf("reply to own card targeted %q", got)
	}
}

func TestPostAgentCardRepliesInThreadToAReceivedMessage(t *testing.T) {
	svc, backend, _ := cardService(t)
	commons := mustSubscribeAgent(t, svc, AgentSubscribeOptions{Provider: "commons", IncludeAmbientMessages: true})
	mustDispatchAgentPrompt(t, svc, ambientMessage("delivery_task_question", "om_task_question"))
	question := receiveAgentEvent(t, commons)

	in := cardInput("task-threaded")
	in.ChatAlias = ""
	in.ReplyToMessageRef = question.Message.MessageRef
	in.ReplyInThread = true
	receipt := mustPostCard(t, svc, in)
	_, posts, _, _ := backend.cardSnapshot()
	if posts[0].ReplyToMessageID != "om_task_question" || !posts[0].ReplyInThread || posts[0].ChatID != "" {
		t.Fatalf("threaded card = %#v", posts[0])
	}
	if receipt.ThreadRef != feishu.ThreadRefForApp("support", "omt_task") {
		t.Fatalf("thread ref = %q", receipt.ThreadRef)
	}
}

func TestPostAgentCardIsIdempotentAndRetriesReuseTheEntityAndUUID(t *testing.T) {
	svc, backend, _ := cardService(t)
	first := mustPostCard(t, svc, cardInput("task-1"))
	replay := mustPostCard(t, svc, cardInput("task-1"))
	if !replay.Duplicate || replay.MessageRef != first.MessageRef {
		t.Fatalf("replay = %#v", replay)
	}
	changed := cardInput("task-1")
	changed.Content.Title = "Something else"
	if _, apiErr := svc.PostAgentCard(context.Background(), changed); apiErr == nil || apiErr.Code != "operation_conflict" {
		t.Fatalf("conflicting reuse = %#v", apiErr)
	}
	// The key space is the card's own: the same id posts a message freely.
	_ = mustPost(t, svc, postInput("task-1"))

	backend.cardPostErrs = []error{&feishu.DynamicCardAPIError{Operation: "card post", HTTPStatus: 503, Code: 1}}
	if _, apiErr := svc.PostAgentCard(context.Background(), cardInput("task-2")); apiErr == nil || apiErr.Code != "feishu_unavailable" || !apiErr.Retryable {
		t.Fatalf("ambiguous card post = %#v", apiErr)
	}
	_ = mustPostCard(t, svc, cardInput("task-2"))
	created, posts, _, _ := backend.cardSnapshot()
	if len(created) != 2 || len(posts) != 3 || posts[1].CardID != posts[2].CardID || posts[1].UUID != posts[2].UUID {
		t.Fatalf("retry did not reuse the entity and UUID: created=%d posts=%#v", len(created), posts)
	}

	backend.cardPostErrs = []error{&feishu.DynamicCardAPIError{Operation: "card post", HTTPStatus: 400, Code: 230002}}
	if _, apiErr := svc.PostAgentCard(context.Background(), cardInput("task-3")); apiErr == nil || apiErr.Code != "feishu_rejected" || apiErr.Retryable {
		t.Fatalf("definitive rejection = %#v", apiErr)
	}
	corrected := cardInput("task-3")
	corrected.Content.Title = "Corrected"
	if receipt := mustPostCard(t, svc, corrected); receipt.Duplicate {
		t.Fatalf("corrected card = %#v", receipt)
	}
}

func TestPostAgentCardEnforcesGrantsAndTargets(t *testing.T) {
	svc, backend, _ := cardService(t)
	in := cardInput("task-ibot")
	in.Provider = "ibot"
	if _, apiErr := svc.PostAgentCard(context.Background(), in); apiErr == nil || apiErr.Code != "provider_scope_denied" {
		t.Fatalf("ungranted provider = %#v", apiErr)
	}
	in = cardInput("task-ops")
	in.ChatAlias = "ops"
	if _, apiErr := svc.PostAgentCard(context.Background(), in); apiErr == nil || apiErr.Code != "unknown_channel" {
		t.Fatalf("ungranted chat = %#v", apiErr)
	}
	in = cardInput("task-ref")
	in.ChatAlias = ""
	in.ReplyToMessageRef = "msgref_unknown"
	if _, apiErr := svc.PostAgentCard(context.Background(), in); apiErr == nil || apiErr.Code != "unknown_message" {
		t.Fatalf("unknown reply target = %#v", apiErr)
	}
	in = cardInput("task-both")
	in.ReplyToMessageRef = "msgref_x"
	if _, apiErr := svc.PostAgentCard(context.Background(), in); apiErr == nil || apiErr.Code != "invalid_target" {
		t.Fatalf("two targets = %#v", apiErr)
	}
	in = cardInput("task-untitled")
	in.Content.Title = ""
	if _, apiErr := svc.PostAgentCard(context.Background(), in); apiErr == nil || apiErr.Code != "missing_title" {
		t.Fatalf("untitled card = %#v", apiErr)
	}
	if created, posts, _, _ := backend.cardSnapshot(); len(created) != 0 || len(posts) != 0 {
		t.Fatalf("a rejected request reached Feishu: created=%d posts=%d", len(created), len(posts))
	}
}

func TestUpdateAgentCardReplacesTheCardWithIncreasingSequences(t *testing.T) {
	svc, backend, _ := cardService(t)
	posted := mustPostCard(t, svc, cardInput("task-1"))

	if receipt := mustUpdateCard(t, svc, updateInput(posted.MessageRef, "u-1", AgentCardStateSuspended)); receipt.Duplicate || receipt.Superseded {
		t.Fatalf("first update = %#v", receipt)
	}
	_ = mustUpdateCard(t, svc, updateInput(posted.MessageRef, "u-2", AgentCardStateActive))
	if replay := mustUpdateCard(t, svc, updateInput(posted.MessageRef, "u-1", AgentCardStateSuspended)); !replay.Duplicate {
		t.Fatalf("replay = %#v", replay)
	}
	if _, apiErr := svc.UpdateAgentCard(context.Background(), updateInput(posted.MessageRef, "u-1", AgentCardStateArchived)); apiErr == nil || apiErr.Code != "operation_conflict" {
		t.Fatalf("conflicting reuse = %#v", apiErr)
	}
	_, _, replaces, _ := backend.cardSnapshot()
	if len(replaces) != 2 || replaces[0].CardID != "card_task_1" || replaces[0].Sequence != 1 || replaces[1].Sequence != 2 ||
		replaces[0].UUID == replaces[1].UUID || !strings.Contains(replaces[0].CardJSON, "等人处理") || !strings.Contains(replaces[1].CardJSON, "已回答") {
		t.Fatalf("replaces = %#v", replaces)
	}
}

func TestUpdateAgentCardRejectsRefsThatAreNotTheProvidersCard(t *testing.T) {
	svc, backend, _ := cardService(t)
	commons := mustSubscribeAgent(t, svc, AgentSubscribeOptions{Provider: "commons", IncludeAmbientMessages: true})
	mustDispatchAgentPrompt(t, svc, ambientMessage("delivery_received", "om_received"))
	received := receiveAgentEvent(t, commons).Message.MessageRef
	card := mustPostCard(t, svc, cardInput("task-1"))
	message := mustPost(t, svc, postInput("plain-1"))

	cfg := commonsConfig()
	cfg.AgentProviders["other"] = cfg.AgentProviders["commons"]
	other := NewMultiAppService(cfg, map[string]feishu.Sender{"support": backend}, dedupe.NewMemoryStore(time.Hour), nil)
	other.SetAgentMessageRouteStore(svc.agentRoutes)

	for name, testCase := range map[string]struct {
		svc      *Service
		provider string
		ref      string
	}{
		"unknown ref":                 {svc, "commons", "msgref_unknown"},
		"a received message":          {svc, "commons", received},
		"a posted message, not card":  {svc, "commons", message.MessageRef},
		"another provider's card":     {other, "other", card.MessageRef},
		"a thread ref is not a card":  {svc, "commons", feishu.ThreadRefForApp("support", "omt_task")},
		"an opaque chat alias is not": {svc, "commons", "refund-backend"},
	} {
		in := updateInput(testCase.ref, "u-"+strings.ReplaceAll(name, " ", "-"), AgentCardStateActive)
		in.Provider = testCase.provider
		if _, apiErr := testCase.svc.UpdateAgentCard(context.Background(), in); apiErr == nil || apiErr.Code != "unknown_message" || apiErr.Retryable {
			t.Fatalf("%s: update = %#v, want unknown_message", name, apiErr)
		}
	}
	in := updateInput(card.MessageRef, "u-ibot", AgentCardStateActive)
	in.Provider = "ibot"
	if _, apiErr := svc.UpdateAgentCard(context.Background(), in); apiErr == nil || apiErr.Code != "provider_scope_denied" {
		t.Fatalf("ungranted provider = %#v", apiErr)
	}
	if _, _, replaces, _ := backend.cardSnapshot(); len(replaces) != 0 {
		t.Fatalf("a rejected update reached Feishu: %#v", replaces)
	}
}

func TestUpdateAgentCardSurvivesARestartThroughTheDurableRouteStore(t *testing.T) {
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

	svc, _, _ := cardService(t)
	first := open()
	svc.SetAgentMessageRouteStore(first)
	posted := mustPostCard(t, svc, cardInput("task-before-restart"))
	_ = mustUpdateCard(t, svc, updateInput(posted.MessageRef, "u-before", AgentCardStateSuspended))
	_ = first.Close()

	restarted, backend, _ := cardService(t)
	second := open()
	restarted.SetAgentMessageRouteStore(second)
	// A Feishu failure after the reservation still consumes its sequence.
	backend.replaceErrs = []error{&feishu.DynamicCardAPIError{Operation: "card replace", HTTPStatus: 503}}
	if _, apiErr := restarted.UpdateAgentCard(context.Background(), updateInput(posted.MessageRef, "u-after", AgentCardStateActive)); apiErr == nil || apiErr.Code != "feishu_unavailable" || !apiErr.Retryable {
		t.Fatalf("failed update = %#v", apiErr)
	}
	_ = mustUpdateCard(t, restarted, updateInput(posted.MessageRef, "u-after", AgentCardStateActive))
	_ = second.Close()

	again, againBackend, _ := cardService(t)
	again.SetAgentMessageRouteStore(open())
	_ = mustUpdateCard(t, again, updateInput(posted.MessageRef, "u-archive", AgentCardStateArchived))

	_, _, replaces, _ := backend.cardSnapshot()
	_, _, later, _ := againBackend.cardSnapshot()
	if len(replaces) != 2 || replaces[0].CardID != "card_task_1" || replaces[0].Sequence != 2 || replaces[1].Sequence != 3 ||
		len(later) != 1 || later[0].CardID != "card_task_1" || later[0].Sequence != 4 {
		t.Fatalf("post-restart replaces = %#v then %#v", replaces, later)
	}
}

func TestUpdateAgentCardMapsAnExpiredEntity(t *testing.T) {
	svc, backend, _ := cardService(t)
	posted := mustPostCard(t, svc, cardInput("task-old"))
	for _, code := range []int{200750, 200740} {
		backend.replaceErrs = []error{&feishu.DynamicCardAPIError{Operation: "card replace", HTTPStatus: 400, Code: code}}
		if _, apiErr := svc.UpdateAgentCard(context.Background(), updateInput(posted.MessageRef, fmt.Sprintf("u-%d", code), AgentCardStateActive)); apiErr == nil ||
			apiErr.Code != "card_expired" || apiErr.Status != 412 || apiErr.Retryable {
			t.Fatalf("code %d = %#v, want card_expired", code, apiErr)
		}
	}
	backend.replaceErrs = []error{&feishu.DynamicCardAPIError{Operation: "card replace", HTTPStatus: 400, Code: 300311}}
	if _, apiErr := svc.UpdateAgentCard(context.Background(), updateInput(posted.MessageRef, "u-foreign", AgentCardStateActive)); apiErr == nil || apiErr.Code != "feishu_rejected" {
		t.Fatalf("other rejection = %#v", apiErr)
	}
}

func TestUpdateAgentCardSerializesAndCoalescesABacklog(t *testing.T) {
	svc, backend, _ := cardService(t)
	posted := mustPostCard(t, svc, cardInput("task-busy"))
	gate := make(chan struct{})
	entered := make(chan struct{}, 4)
	backend.cardMu.Lock()
	backend.replaceGate, backend.replaceEntered = gate, entered
	backend.cardMu.Unlock()

	type result struct {
		receipt AgentCardUpdateReceipt
		err     error
	}
	run := func(operationID string, state AgentCardState) <-chan result {
		done := make(chan result, 1)
		go func() {
			receipt, apiErr := svc.UpdateAgentCard(context.Background(), updateInput(posted.MessageRef, operationID, state))
			if apiErr != nil {
				done <- result{err: apiErr}
				return
			}
			done <- result{receipt: receipt}
		}()
		return done
	}
	waitQueued := func(want int) {
		t.Helper()
		deadline := time.Now().Add(2 * time.Second)
		for {
			svc.agentCardLanes.mu.Lock()
			lane := svc.agentCardLanes.lanes[posted.MessageRef]
			queued := lane != nil && lane.refs == want
			svc.agentCardLanes.mu.Unlock()
			if queued {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("lane never held %d updates", want)
			}
			time.Sleep(time.Millisecond)
		}
	}
	first := run("u-1", AgentCardStateProcessing)
	<-entered
	second := run("u-2", AgentCardStateSuspended)
	waitQueued(2)
	third := run("u-3", AgentCardStateActive)
	waitQueued(3)
	close(gate)

	if got := <-first; got.err != nil || got.receipt.Superseded {
		t.Fatalf("first = %#v", got)
	}
	if got := <-second; got.err != nil || !got.receipt.Superseded {
		t.Fatalf("queued middle update = %#v, want superseded", got)
	}
	if got := <-third; got.err != nil || got.receipt.Superseded {
		t.Fatalf("newest update = %#v", got)
	}
	_, _, replaces, _ := backend.cardSnapshot()
	if len(replaces) != 2 || !strings.Contains(replaces[1].CardJSON, "已回答") || replaces[1].Sequence != 2 {
		t.Fatalf("replaces = %#v, want the first and the newest", replaces)
	}
	if replay := mustUpdateCard(t, svc, updateInput(posted.MessageRef, "u-2", AgentCardStateSuspended)); !replay.Duplicate || !replay.Superseded {
		t.Fatalf("superseded replay = %#v", replay)
	}
	if svc.agentCardLanes.lanes[posted.MessageRef] != nil {
		t.Fatal("an idle lane was retained")
	}
}

func TestPinAgentMessageRequiresTheGrantAndAnAuthoredMessage(t *testing.T) {
	svc, backend, _ := cardService(t)
	commons := mustSubscribeAgent(t, svc, AgentSubscribeOptions{Provider: "commons", IncludeAmbientMessages: true})
	mustDispatchAgentPrompt(t, svc, ambientMessage("delivery_pin_question", "om_pin_question"))
	received := receiveAgentEvent(t, commons).Message.MessageRef
	card := mustPostCard(t, svc, cardInput("task-pin"))
	message := mustPost(t, svc, postInput("plain-pin"))

	for _, ref := range []string{card.MessageRef, message.MessageRef, card.MessageRef} {
		if apiErr := svc.PinAgentMessage(context.Background(), PinAgentMessageInput{Provider: "commons", MessageRef: ref}); apiErr != nil {
			t.Fatalf("pin %q: %v", ref, apiErr)
		}
	}
	if apiErr := svc.PinAgentMessage(context.Background(), PinAgentMessageInput{Provider: "commons", MessageRef: received}); apiErr == nil || apiErr.Code != "unknown_message" {
		t.Fatalf("pinning a received message = %#v, want unknown_message", apiErr)
	}
	if apiErr := svc.PinAgentMessage(context.Background(), PinAgentMessageInput{Provider: "commons", MessageRef: "msgref_unknown"}); apiErr == nil || apiErr.Code != "unknown_message" {
		t.Fatalf("pinning an unknown ref = %#v, want unknown_message", apiErr)
	}

	// The same deployment without allow_pins.
	cfg := commonsConfig()
	unpinned := NewMultiAppService(cfg, map[string]feishu.Sender{"support": backend}, dedupe.NewMemoryStore(time.Hour), nil)
	unpinned.SetAgentMessageRouteStore(svc.agentRoutes)
	if apiErr := unpinned.PinAgentMessage(context.Background(), PinAgentMessageInput{Provider: "commons", MessageRef: card.MessageRef}); apiErr == nil || apiErr.Code != "provider_scope_denied" {
		t.Fatalf("pin without the grant = %#v, want provider_scope_denied", apiErr)
	}
	_, _, _, pins := backend.cardSnapshot()
	if strings.Join(pins, ",") != "om_card_task_1,om_post_1,om_card_task_1" {
		t.Fatalf("pins = %v", pins)
	}
}

func TestPinAgentMessageMapsFeishuRefusals(t *testing.T) {
	svc, backend, _ := cardService(t)
	card := mustPostCard(t, svc, cardInput("task-pin"))
	pin := func() (int, string, bool) {
		apiErr := svc.PinAgentMessage(context.Background(), PinAgentMessageInput{Provider: "commons", MessageRef: card.MessageRef})
		if apiErr == nil {
			return 0, "", false
		}
		return apiErr.Status, apiErr.Code, apiErr.Retryable
	}
	for _, code := range []int{230027, 230046, 99991672} {
		backend.pinErrs = []error{&feishu.MessageSendError{Operation: "message_pin", Class: "api_rejected", HTTPStatus: 400, Code: code}}
		if status, apiCode, retryable := pin(); status != 412 || apiCode != "pin_permission_denied" || retryable {
			t.Fatalf("code %d = %d %s retryable=%t, want 412 pin_permission_denied", code, status, apiCode, retryable)
		}
	}
	backend.pinErrs = []error{&feishu.MessageSendError{Operation: "message_pin", Class: "api_rejected", HTTPStatus: 400, Code: 230047}}
	if _, apiCode, retryable := pin(); apiCode != "feishu_unavailable" || !retryable {
		t.Fatalf("pin rate limit = %s retryable=%t", apiCode, retryable)
	}
	backend.pinErrs = []error{&feishu.MessageSendError{Operation: "message_pin", Class: "api_rejected", HTTPStatus: 400, Code: 230011}}
	if _, apiCode, retryable := pin(); apiCode != "feishu_rejected" || retryable {
		t.Fatalf("recalled message = %s retryable=%t", apiCode, retryable)
	}
	backend.pinErrs = []error{&feishu.MessageSendError{Operation: "message_pin", Class: "transport", Retryable: true}}
	if _, apiCode, retryable := pin(); apiCode != "feishu_unavailable" || !retryable {
		t.Fatalf("transport failure = %s retryable=%t", apiCode, retryable)
	}
}
