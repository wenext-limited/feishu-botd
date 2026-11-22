package service

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"feishu-botd/internal/announcement"
	"feishu-botd/internal/chatgrant"
	"feishu-botd/internal/config"
	"feishu-botd/internal/dedupe"
	"feishu-botd/internal/feishu"
	"feishu-botd/internal/notify"
)

// fakeAnnouncementDoc is one chat's docx announcement: the page's top-level
// blocks, edited with Feishu's index semantics.
type fakeAnnouncementDoc struct {
	*fakePostBackend
	mu        sync.Mutex
	kind      string
	revision  int
	blocks    []feishu.AnnouncementBlock
	nextID    int
	writes    int
	tokens    []string
	getErr    error
	createErr []error
	inFlight  int
	maxFlight int
}

func newFakeAnnouncementDoc(blocks ...feishu.AnnouncementBlock) *fakeAnnouncementDoc {
	return &fakeAnnouncementDoc{fakePostBackend: newFakePostBackend(), kind: "docx", revision: 1, blocks: blocks}
}

func (d *fakeAnnouncementDoc) enter() {
	d.mu.Lock()
	d.inFlight++
	if d.inFlight > d.maxFlight {
		d.maxFlight = d.inFlight
	}
	d.mu.Unlock()
	time.Sleep(time.Millisecond)
}

func (d *fakeAnnouncementDoc) leave() {
	d.mu.Lock()
	d.inFlight--
	d.mu.Unlock()
}

func (d *fakeAnnouncementDoc) GetChatAnnouncement(context.Context, string) (feishu.AnnouncementInfo, error) {
	d.enter()
	defer d.leave()
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.getErr != nil {
		return feishu.AnnouncementInfo{}, d.getErr
	}
	return feishu.AnnouncementInfo{Type: d.kind, RevisionID: d.revision}, nil
}

func (d *fakeAnnouncementDoc) ListChatAnnouncementBlocks(context.Context, string) ([]feishu.AnnouncementBlock, error) {
	d.enter()
	defer d.leave()
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]feishu.AnnouncementBlock(nil), d.blocks...), nil
}

func (d *fakeAnnouncementDoc) CreateChatAnnouncementBlocks(_ context.Context, _ string, index int, blocks []feishu.AnnouncementBlock, token string) ([]string, error) {
	d.enter()
	defer d.leave()
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.createErr) > 0 {
		err := d.createErr[0]
		d.createErr = d.createErr[1:]
		if err != nil {
			return nil, err
		}
	}
	d.writes++
	d.revision++
	d.tokens = append(d.tokens, token)
	if index < 0 || index > len(d.blocks) {
		index = len(d.blocks)
	}
	created := make([]feishu.AnnouncementBlock, len(blocks))
	ids := make([]string, len(blocks))
	for i, block := range blocks {
		d.nextID++
		block.ID = fmt.Sprintf("blk_%d", d.nextID)
		created[i], ids[i] = block, block.ID
	}
	d.blocks = append(d.blocks[:index], append(created, d.blocks[index:]...)...)
	return ids, nil
}

func (d *fakeAnnouncementDoc) DeleteChatAnnouncementBlocks(_ context.Context, _ string, start, end int, token string) error {
	d.enter()
	defer d.leave()
	d.mu.Lock()
	defer d.mu.Unlock()
	d.writes++
	d.revision++
	d.tokens = append(d.tokens, token)
	d.blocks = append(d.blocks[:start], d.blocks[end:]...)
	return nil
}

func (d *fakeAnnouncementDoc) render() string {
	d.mu.Lock()
	defer d.mu.Unlock()
	var lines []string
	for _, block := range d.blocks {
		prefix := map[int]string{feishu.AnnouncementBlockHeading3: "### ", feishu.AnnouncementBlockBullet: "- ", 3: "# ", 4: "## "}[block.Type]
		lines = append(lines, prefix+block.PlainText())
	}
	return strings.Join(lines, "\n")
}

func (d *fakeAnnouncementDoc) writeCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.writes
}

func peopleBlock(blockType int, text string) feishu.AnnouncementBlock {
	return feishu.AnnouncementBlock{ID: "people_" + strings.ReplaceAll(text, " ", "_"), Type: blockType,
		Runs: []feishu.AnnouncementTextRun{{Content: text}}}
}

const announcementAlias = "agent-chat-announcements"

func announcementService(t *testing.T, doc *fakeAnnouncementDoc) *Service {
	t.Helper()
	cfg := commonsConfig()
	p := cfg.AgentProviders["commons"]
	p.AllowAdoptChats, p.AllowAnnouncements = true, true
	cfg.AgentProviders["commons"] = p
	cfg.AgentProviders["other"] = p
	svc := NewMultiAppService(cfg, map[string]feishu.Sender{"support": doc}, dedupe.NewMemoryStore(time.Hour), nil)
	if err := svc.cfg().RuntimeChats.Add(chatgrant.Grant{Kind: chatgrant.Adopted, Provider: "commons", AppAlias: "support",
		ChatID: "oc_people", ChatAlias: announcementAlias, CreatedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	return svc
}

func setSection(svc *Service, op, title, markdown string) *notify.APIError {
	return svc.SetAgentChatAnnouncementSection(context.Background(), SetAgentChatAnnouncementSectionInput{
		Provider: "commons", ChatAlias: announcementAlias, OperationID: op, Title: title, Markdown: markdown,
	})
}

func mustSetSection(t *testing.T, svc *Service, op, title, markdown string) {
	t.Helper()
	if apiErr := setSection(svc, op, title, markdown); apiErr != nil {
		t.Fatalf("set %s: %+v", op, apiErr)
	}
}

func assertDoc(t *testing.T, doc *fakeAnnouncementDoc, want string) {
	t.Helper()
	if got := doc.render(); got != want {
		t.Fatalf("announcement =\n%s\nwant\n%s", got, want)
	}
}

func TestAnnouncementSectionAppendsThenReplacesOnlyItsContent(t *testing.T) {
	doc := newFakeAnnouncementDoc(
		peopleBlock(3, "Welcome"),
		peopleBlock(feishu.AnnouncementBlockText, "house rules"),
	)
	svc := announcementService(t, doc)

	mustSetSection(t, svc, "op-1", "Agent status", "Build is **green**\n\n- one\n- two")
	assertDoc(t, doc, "# Welcome\nhouse rules\n### Agent status\nBuild is green\n- one\n- two")
	if doc.writeCount() != 1 {
		t.Fatalf("first call writes = %d, want one append", doc.writeCount())
	}
	headingID := doc.blocks[2].ID

	// People add text after the section, under their own heading.
	doc.mu.Lock()
	doc.blocks = append(doc.blocks, peopleBlock(5, "Links"), peopleBlock(feishu.AnnouncementBlockText, "wiki"))
	doc.mu.Unlock()

	mustSetSection(t, svc, "op-2", "Agent status", "Build is red")
	assertDoc(t, doc, "# Welcome\nhouse rules\n### Agent status\nBuild is red\n### Links\nwiki")
	if doc.blocks[2].ID != headingID || doc.blocks[0].ID != "people_Welcome" || doc.blocks[5].ID != "people_wiki" {
		t.Fatalf("blocks outside the section changed: %#v", doc.blocks)
	}
	section, ok := svc.agentAnnouncements.Section("commons", announcementAlias)
	if !ok || section.HeadingBlockID != headingID || section.ChatID != "oc_people" {
		t.Fatalf("section state = %#v", section)
	}

	// Empty markdown leaves the heading alone.
	mustSetSection(t, svc, "op-3", "Agent status", "")
	assertDoc(t, doc, "# Welcome\nhouse rules\n### Agent status\n### Links\nwiki")
}

func TestAnnouncementSectionTitleChangeReplacesTheHeadingInPlace(t *testing.T) {
	doc := newFakeAnnouncementDoc(peopleBlock(feishu.AnnouncementBlockText, "intro"))
	svc := announcementService(t, doc)
	mustSetSection(t, svc, "op-1", "Status", "old")
	doc.mu.Lock()
	doc.blocks = append(doc.blocks, peopleBlock(5, "After"))
	doc.mu.Unlock()

	mustSetSection(t, svc, "op-2", "Status (v2)", "new\n\n- x")
	assertDoc(t, doc, "intro\n### Status (v2)\nnew\n- x\n### After")
	section, _ := svc.agentAnnouncements.Section("commons", announcementAlias)
	if section.HeadingBlockID != doc.blocks[1].ID || section.Title != "Status (v2)" {
		t.Fatalf("section state = %#v", section)
	}
	// Found again by the new heading id, not the old title.
	mustSetSection(t, svc, "op-3", "Status (v2)", "newer")
	assertDoc(t, doc, "intro\n### Status (v2)\nnewer\n### After")
}

func TestAnnouncementSectionDeletedByAPersonIsFoundByTitleOrAppended(t *testing.T) {
	doc := newFakeAnnouncementDoc(peopleBlock(feishu.AnnouncementBlockText, "intro"))
	svc := announcementService(t, doc)
	mustSetSection(t, svc, "op-1", "Status", "one")

	// A person deletes the whole section.
	doc.mu.Lock()
	doc.blocks = doc.blocks[:1]
	doc.blocks = append(doc.blocks, peopleBlock(feishu.AnnouncementBlockText, "later"))
	doc.mu.Unlock()
	mustSetSection(t, svc, "op-2", "Status", "two")
	assertDoc(t, doc, "intro\nlater\n### Status\ntwo")

	// A person deletes only our heading but writes one with the same title.
	doc.mu.Lock()
	doc.blocks = []feishu.AnnouncementBlock{peopleBlock(5, "Status"), peopleBlock(feishu.AnnouncementBlockText, "stale"), peopleBlock(4, "Other")}
	doc.mu.Unlock()
	mustSetSection(t, svc, "op-3", "Status", "three")
	assertDoc(t, doc, "### Status\nthree\n## Other")
	if section, _ := svc.agentAnnouncements.Section("commons", announcementAlias); section.HeadingBlockID != "people_Status" {
		t.Fatalf("heading id = %q", section.HeadingBlockID)
	}
}

func TestAnnouncementSectionOperationIdempotency(t *testing.T) {
	doc := newFakeAnnouncementDoc()
	svc := announcementService(t, doc)
	mustSetSection(t, svc, "op-1", "Status", "same")
	mustSetSection(t, svc, "op-1", "Status", "same")
	if doc.writeCount() != 1 {
		t.Fatalf("writes = %d, want one Feishu write for a repeated operation", doc.writeCount())
	}
	apiErr := setSection(svc, "op-1", "Status", "different")
	if apiErr == nil || apiErr.Code != "operation_conflict" || apiErr.Status != 409 {
		t.Fatalf("conflicting reuse = %+v", apiErr)
	}
	if doc.writeCount() != 1 {
		t.Fatal("a conflicting operation wrote to Feishu")
	}
}

func TestAnnouncementSectionIdempotencySurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	doc := newFakeAnnouncementDoc()
	svc := announcementService(t, doc)
	store, err := announcement.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	svc.SetAgentAnnouncementStore(store)
	mustSetSection(t, svc, "op-1", "Status", "x")

	restarted := announcementService(t, doc)
	reopened, err := announcement.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	restarted.SetAgentAnnouncementStore(reopened)
	mustSetSection(t, restarted, "op-1", "Status", "x")
	if doc.writeCount() != 1 {
		t.Fatalf("writes = %d after restart", doc.writeCount())
	}
	mustSetSection(t, restarted, "op-2", "Status", "y")
	assertDoc(t, doc, "### Status\ny")
}

func TestAnnouncementSectionRejectsLegacyAnnouncement(t *testing.T) {
	doc := newFakeAnnouncementDoc()
	doc.kind = "doc"
	svc := announcementService(t, doc)
	apiErr := setSection(svc, "op-1", "Status", "x")
	if apiErr == nil || apiErr.Code != "announcement_unsupported" || apiErr.Status != 412 {
		t.Fatalf("legacy announcement = %+v", apiErr)
	}
	if doc.writeCount() != 0 {
		t.Fatal("a legacy announcement was written")
	}
}

func TestAnnouncementSectionMapsFeishuErrors(t *testing.T) {
	for _, testCase := range []struct {
		name      string
		err       error
		code      string
		status    int
		retryable bool
	}{
		{"missing scope", &feishu.MessageSendError{Class: "api_rejected", Code: 99991672, HTTPStatus: 400}, "announcement_forbidden", 412, false},
		{"bot not in chat", &feishu.MessageSendError{Class: "api_rejected", Code: 232011, HTTPStatus: 400}, "announcement_forbidden", 412, false},
		{"http forbidden", &feishu.MessageSendError{Class: "api_rejected", Code: 1, HTTPStatus: 403}, "announcement_forbidden", 412, false},
		{"rate limit after retries", &feishu.MessageSendError{Class: "api_rejected", Code: 99991400, HTTPStatus: 429, Retryable: true}, "feishu_unavailable", 503, true},
		{"server error", &feishu.MessageSendError{Class: "api_rejected", Code: 1770000, HTTPStatus: 500, Retryable: true}, "feishu_unavailable", 502, true},
		{"timeout", &feishu.MessageSendError{Class: "deadline_exceeded"}, "feishu_unavailable", 502, true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			doc := newFakeAnnouncementDoc()
			doc.getErr = testCase.err
			apiErr := setSection(announcementService(t, doc), "op-1", "Status", "x")
			if apiErr == nil || apiErr.Code != testCase.code || apiErr.Status != testCase.status || apiErr.Retryable != testCase.retryable {
				t.Fatalf("error = %+v", apiErr)
			}
		})
	}
}

func TestAnnouncementSectionRetryAfterFailedWriteIsNotDeduplicated(t *testing.T) {
	doc := newFakeAnnouncementDoc()
	doc.createErr = []error{&feishu.MessageSendError{Class: "transport", Retryable: true}}
	svc := announcementService(t, doc)
	if apiErr := setSection(svc, "op-1", "Status", "x"); apiErr == nil || !apiErr.Retryable {
		t.Fatalf("failed write = %+v", apiErr)
	}
	mustSetSection(t, svc, "op-1", "Status", "x")
	assertDoc(t, doc, "### Status\nx")
}

func TestAnnouncementSectionClientTokensAreDeterministicPerRevision(t *testing.T) {
	a := announcementClientToken("commons", "op-1", 3, "create@-1")
	if a != announcementClientToken("commons", "op-1", 3, "create@-1") {
		t.Fatal("token is not deterministic")
	}
	for _, other := range []string{
		announcementClientToken("commons", "op-2", 3, "create@-1"),
		announcementClientToken("commons", "op-1", 4, "create@-1"),
		announcementClientToken("commons", "op-1", 3, "create@2"),
		announcementClientToken("other", "op-1", 3, "create@-1"),
	} {
		if other == a {
			t.Fatal("distinct edits share a client token")
		}
	}
	if len(a) != 36 || strings.Count(a, "-") != 4 || a[14] != '4' {
		t.Fatalf("token %q is not UUID-shaped", a)
	}
}

func TestAnnouncementSectionSerializesPerChat(t *testing.T) {
	doc := newFakeAnnouncementDoc()
	svc := announcementService(t, doc)
	var wg sync.WaitGroup
	errs := make(chan *notify.APIError, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs <- setSection(svc, fmt.Sprintf("op-%d", i), "Status", fmt.Sprintf("v%d", i))
		}(i)
	}
	wg.Wait()
	close(errs)
	for apiErr := range errs {
		if apiErr != nil {
			t.Fatalf("concurrent set: %+v", apiErr)
		}
	}
	if doc.maxFlight != 1 {
		t.Fatalf("max concurrent Feishu calls = %d, want 1", doc.maxFlight)
	}
	if got := doc.render(); strings.Count(got, "### Status") != 1 || strings.Count(got, "\n") != 1 {
		t.Fatalf("interleaved updates left:\n%s", got)
	}
}

func TestAnnouncementSectionRequiresGrantAndFlag(t *testing.T) {
	doc := newFakeAnnouncementDoc()
	svc := announcementService(t, doc)

	cfg := *svc.cfg()
	providers := map[string]config.AgentProviderConfig{}
	for name, p := range cfg.AgentProviders {
		providers[name] = p
	}
	p := providers["commons"]
	p.AllowAnnouncements = false
	providers["commons"] = p
	cfg.AgentProviders = providers
	svc.cfgRef.Store(&cfg)
	if apiErr := setSection(svc, "op-1", "Status", "x"); apiErr == nil || apiErr.Code != "provider_scope_denied" || apiErr.Status != 403 {
		t.Fatalf("without allow_announcements = %+v", apiErr)
	}

	svc = announcementService(t, newFakeAnnouncementDoc())
	for name, in := range map[string]SetAgentChatAnnouncementSectionInput{
		"other provider's chat": {Provider: "other", ChatAlias: announcementAlias},
		"static chat":           {Provider: "commons", ChatAlias: "refund-backend"},
		"unknown alias":         {Provider: "commons", ChatAlias: "nowhere"},
	} {
		in.OperationID, in.Title = "op-1", "Status"
		if apiErr := svc.SetAgentChatAnnouncementSection(context.Background(), in); apiErr == nil || apiErr.Code != "unknown_chat" || apiErr.Status != 404 {
			t.Fatalf("%s = %+v", name, apiErr)
		}
	}
	if doc.writeCount() != 0 {
		t.Fatal("a refused call wrote to Feishu")
	}

	for name, in := range map[string]SetAgentChatAnnouncementSectionInput{
		"missing operation": {Provider: "commons", ChatAlias: announcementAlias, Title: "Status"},
		"missing title":     {Provider: "commons", ChatAlias: announcementAlias, OperationID: "op", Title: "  "},
		"huge markdown":     {Provider: "commons", ChatAlias: announcementAlias, OperationID: "op", Title: "S", Markdown: strings.Repeat("x", maxAnnouncementMarkdownBytes+1)},
		"too many blocks":   {Provider: "commons", ChatAlias: announcementAlias, OperationID: "op", Title: "S", Markdown: strings.Repeat("- b\n", maxAnnouncementContentBlocks+1)},
	} {
		if apiErr := svc.SetAgentChatAnnouncementSection(context.Background(), in); apiErr == nil || apiErr.Status != 400 {
			t.Fatalf("%s = %+v", name, apiErr)
		}
	}
}
