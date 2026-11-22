package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"sync"
	"time"

	"feishu-botd/internal/announcement"
	"feishu-botd/internal/chatgrant"
	"feishu-botd/internal/feishu"
	"feishu-botd/internal/notify"
)

const (
	maxAnnouncementTitleBytes    = 200
	maxAnnouncementMarkdownBytes = 16 * 1024
	// Feishu creates at most 50 children per call; the heading is one.
	maxAnnouncementContentBlocks = 49
)

// SetAgentChatAnnouncementSectionInput is one SetAgentChatAnnouncementSection call.
type SetAgentChatAnnouncementSectionInput struct {
	Provider    string
	ChatAlias   string
	OperationID string
	Title       string
	Markdown    string
}

// SetAgentAnnouncementStore installs the restart-safe section store. Process
// composition calls this before any provider stream starts; without it the
// state lasts the process.
func (s *Service) SetAgentAnnouncementStore(store *announcement.Store) {
	if store != nil {
		s.agentAnnouncements = store
	}
}

// SetAgentChatAnnouncementSection keeps the provider's section of a granted
// chat's announcement: a heading3 block with the title, then the blocks
// rendered from the Markdown, up to the next heading of any level. Calls
// for one chat run one at a time, so deletes and inserts never interleave.
func (s *Service) SetAgentChatAnnouncementSection(ctx context.Context, in SetAgentChatAnnouncementSectionInput) *notify.APIError {
	provider := strings.TrimSpace(in.Provider)
	alias := strings.TrimSpace(in.ChatAlias)
	operationID := strings.TrimSpace(in.OperationID)
	title := strings.Join(strings.Fields(in.Title), " ")
	markdown := strings.TrimSpace(strings.ReplaceAll(in.Markdown, "\r\n", "\n"))
	switch {
	case provider == "":
		return notify.BadRequest("missing_provider", "provider is required")
	case alias == "":
		return notify.BadRequest("missing_chat_alias", "chat_alias is required")
	case operationID == "":
		return notify.BadRequest("missing_operation_id", "operation_id is required")
	case title == "":
		return notify.BadRequest("missing_title", "title is required")
	}
	if len(provider) > 64 || len(alias) > 160 || len(operationID) > 128 ||
		len(title) > maxAnnouncementTitleBytes || len(markdown) > maxAnnouncementMarkdownBytes {
		return notify.BadRequest("field_too_large", "one or more fields are too large")
	}
	content := feishu.AnnouncementBlocksFromMarkdown(markdown)
	if len(content) > maxAnnouncementContentBlocks {
		return notify.BadRequest("field_too_large", "markdown renders to too many blocks")
	}
	if !s.cfg().ProviderAllowsAnnouncements(provider) {
		return providerScopeDenied()
	}
	grant, backend, apiErr := s.grantedAgentChat(provider, alias)
	if apiErr != nil {
		return apiErr
	}
	if backend.announcements == nil {
		return notify.NotImplemented("agent_announcements_unavailable", "chat announcements are unavailable for this sender")
	}

	unlock, err := s.agentAnnouncementLocks.lock(ctx, grant.AppAlias+"\x00"+grant.ChatID)
	if err != nil {
		return notify.NewAPIError(502, "feishu_unavailable", "gave up waiting for an earlier announcement update", true)
	}
	defer unlock()

	digest := announcementDigest(title, markdown)
	section, known := s.agentAnnouncements.Section(provider, grant.ChatAlias)
	if known && (section.AppAlias != grant.AppAlias || section.ChatID != grant.ChatID) {
		// The alias names a different chat than when the section was kept.
		section, known = announcement.Section{}, false
	}
	if known {
		if op, done := section.Operation(operationID); done {
			if op.Digest != digest {
				return notify.NewAPIError(412, "operation_conflict", "operation id reused with different content", false)
			}
			return nil
		}
	}

	headingID, apiErr := s.writeAnnouncementSection(ctx, backend.announcements, grant, section.HeadingBlockID, provider, operationID, title, content)
	if apiErr != nil {
		return apiErr
	}
	now := time.Now().UTC()
	next := announcement.Section{
		Provider: provider, ChatAlias: grant.ChatAlias, AppAlias: grant.AppAlias, ChatID: grant.ChatID,
		HeadingBlockID: headingID, Title: title, UpdatedAt: now,
	}
	if known {
		next.Operations = section.Operations
	}
	next = next.WithOperation(announcement.Operation{OperationID: operationID, Digest: digest, CompletedAt: now})
	if err := s.agentAnnouncements.Put(next); err != nil {
		// The announcement is written; a retry rewrites the same content
		// and records it then.
		s.logger.Error("agent announcement state was not saved", "chat_alias", grant.ChatAlias, "error", err)
		return notify.NewAPIError(503, "announcement_store_unavailable", "could not persist the announcement section", true)
	}
	s.logger.Info("agent announcement section updated", "provider", provider, "chat_alias", grant.ChatAlias, "blocks", len(content))
	return nil
}

// writeAnnouncementSection finds the section and replaces it, returning the
// heading's block id. The section is found by the recorded heading id; if a
// person deleted that block, by the first heading3 whose text is the title;
// otherwise it is appended at the end of the page.
func (s *Service) writeAnnouncementSection(
	ctx context.Context,
	api feishu.ChatAnnouncements,
	grant chatgrant.Grant,
	recordedHeadingID, provider, operationID, title string,
	content []feishu.AnnouncementBlock,
) (string, *notify.APIError) {
	timeout := s.cfg().SendTimeout
	call := func() (context.Context, context.CancelFunc) { return context.WithTimeout(ctx, timeout) }

	callCtx, cancel := call()
	info, err := api.GetChatAnnouncement(callCtx, grant.ChatID)
	cancel()
	if err != nil {
		s.logFeishuFailure("agent announcement get", "chat", grant.ChatAlias, err)
		return "", announcementCallError(err, "Feishu announcement read failed")
	}
	if info.Type != feishu.AnnouncementTypeDocx {
		return "", notify.NewAPIError(412, "announcement_unsupported", "the chat's announcement is not a docx announcement", false)
	}
	callCtx, cancel = call()
	blocks, err := api.ListChatAnnouncementBlocks(callCtx, grant.ChatID)
	cancel()
	if err != nil {
		s.logFeishuFailure("agent announcement list", "chat", grant.ChatAlias, err)
		return "", announcementCallError(err, "Feishu announcement read failed")
	}

	heading := feishu.AnnouncementBlock{Type: feishu.AnnouncementBlockHeading3, Runs: []feishu.AnnouncementTextRun{{Content: title}}}
	// Every token is bound to the revision read above: a retry inside this
	// call reuses it, while a later retry of the same operation, which sees
	// a changed document, never replays an earlier insert.
	token := func(step string) string { return announcementClientToken(provider, operationID, info.RevisionID, step) }
	create := func(index int, children []feishu.AnnouncementBlock) ([]string, *notify.APIError) {
		callCtx, cancel := call()
		defer cancel()
		ids, err := api.CreateChatAnnouncementBlocks(callCtx, grant.ChatID, index, children, token(fmt.Sprintf("create@%d", index)))
		if err != nil {
			s.logFeishuFailure("agent announcement create", "chat", grant.ChatAlias, err)
			return nil, announcementCallError(err, "Feishu announcement update failed")
		}
		return ids, nil
	}
	remove := func(start, end int) *notify.APIError {
		callCtx, cancel := call()
		defer cancel()
		if err := api.DeleteChatAnnouncementBlocks(callCtx, grant.ChatID, start, end, token(fmt.Sprintf("delete@%d-%d", start, end))); err != nil {
			s.logFeishuFailure("agent announcement delete", "chat", grant.ChatAlias, err)
			return announcementCallError(err, "Feishu announcement update failed")
		}
		return nil
	}

	start := findAnnouncementSection(blocks, recordedHeadingID, title)
	if start < 0 {
		ids, apiErr := create(-1, append([]feishu.AnnouncementBlock{heading}, content...))
		if apiErr != nil {
			return "", apiErr
		}
		return ids[0], nil
	}
	end := start + 1
	for end < len(blocks) && !feishu.IsAnnouncementHeading(blocks[end].Type) {
		end++
	}
	if strings.TrimSpace(blocks[start].PlainText()) == title {
		if end > start+1 {
			if apiErr := remove(start+1, end); apiErr != nil {
				return "", apiErr
			}
		}
		if len(content) > 0 {
			if _, apiErr := create(start+1, content); apiErr != nil {
				return "", apiErr
			}
		}
		return blocks[start].ID, nil
	}
	// The title changed: replace the heading too, in place.
	if apiErr := remove(start, end); apiErr != nil {
		return "", apiErr
	}
	ids, apiErr := create(start, append([]feishu.AnnouncementBlock{heading}, content...))
	if apiErr != nil {
		return "", apiErr
	}
	return ids[0], nil
}

// findAnnouncementSection returns the index of the section's heading among
// the page's top-level blocks, or -1.
func findAnnouncementSection(blocks []feishu.AnnouncementBlock, recordedHeadingID, title string) int {
	if recordedHeadingID != "" {
		for i, block := range blocks {
			if block.ID == recordedHeadingID && block.Type == feishu.AnnouncementBlockHeading3 {
				return i
			}
		}
	}
	for i, block := range blocks {
		if block.Type == feishu.AnnouncementBlockHeading3 && strings.TrimSpace(block.PlainText()) == title {
			return i
		}
	}
	return -1
}

// grantedAgentChat resolves a chat granted to the provider at runtime,
// created or adopted, with the flag for that kind of grant still on. Any
// other alias, including a static chat or another provider's, is
// unknown_chat.
func (s *Service) grantedAgentChat(provider, alias string) (chatgrant.Grant, appBackend, *notify.APIError) {
	cfg := s.cfg()
	grant, ok := cfg.RuntimeChats.ByProviderAlias(provider, alias)
	if !ok || !cfg.ProviderAllowsRuntimeChat(provider, alias) || !s.appAllowed(provider, grant.AppAlias) {
		return chatgrant.Grant{}, appBackend{}, unknownChatError()
	}
	backend, ok := s.backendForApp(grant.AppAlias)
	if !ok {
		return chatgrant.Grant{}, appBackend{}, unknownChatError()
	}
	return grant, backend, nil
}

func announcementDigest(title, markdown string) string {
	sum := sha256.Sum256([]byte("feishu-botd/agent-announcement-request/v1\x00" + title + "\x00" + markdown))
	return hex.EncodeToString(sum[:])
}

// announcementClientToken is a UUID-shaped idempotency key for one edit.
func announcementClientToken(provider, operationID string, revision int, step string) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("feishu-botd/agent-announcement-token/v1\x00%s\x00%s\x00%d\x00%s", provider, operationID, revision, step)))
	b := sum[:16]
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// announcementForbiddenCodes are Feishu's answers for a missing scope
// (99991672, 99991679), a docx permission denial (1770032), and the chat
// refusing the bot: not in the chat (232011), other tenant (232010), bot
// ability off (232025), or edits limited to owners and admins (232043).
var announcementForbiddenCodes = map[int]bool{
	99991672: true, 99991679: true, 1770032: true,
	232010: true, 232011: true, 232025: true, 232043: true,
}

// announcementCallError separates a permission problem an operator or chat
// owner must fix (announcement_forbidden) from transient failures, which
// include a rate limit that outlasted the retries.
func announcementCallError(err error, message string) *notify.APIError {
	if sendErr, answered := answeredMessageFailure(err); answered {
		if announcementForbiddenCodes[sendErr.Code] || sendErr.HTTPStatus == 403 {
			return notify.NewAPIError(412, "announcement_forbidden", "Feishu refused the announcement edit for missing permission", false)
		}
		if feishu.IsAnnouncementRateLimited(err) {
			return notify.NewAPIError(503, "feishu_unavailable", "Feishu announcement rate limit reached", true)
		}
	}
	return agentMessageCallError(err, message)
}

// chatLocks serializes work per chat. Waiting honours the caller's context.
type chatLocks struct {
	mu    sync.Mutex
	locks map[string]*chatLock
}

type chatLock struct {
	ch   chan struct{}
	refs int
}

func newChatLocks() *chatLocks { return &chatLocks{locks: make(map[string]*chatLock)} }

func (l *chatLocks) lock(ctx context.Context, key string) (func(), error) {
	l.mu.Lock()
	entry := l.locks[key]
	if entry == nil {
		entry = &chatLock{ch: make(chan struct{}, 1)}
		l.locks[key] = entry
	}
	entry.refs++
	l.mu.Unlock()
	release := func() {
		l.mu.Lock()
		entry.refs--
		if entry.refs == 0 {
			delete(l.locks, key)
		}
		l.mu.Unlock()
	}
	select {
	case entry.ch <- struct{}{}:
		return func() { <-entry.ch; release() }, nil
	case <-ctx.Done():
		release()
		return nil, ctx.Err()
	}
}
