package feishu

import (
	"context"
	"errors"
	"net/url"
	"strings"
	"time"

	larkcore "github.com/larksuite/oapi-sdk-go/v3/core"
	larkdocx "github.com/larksuite/oapi-sdk-go/v3/service/docx/v1"
)

// Docx block types an announcement section is made of. Headings of every
// level are 3 (heading1) through 11 (heading9).
const (
	AnnouncementBlockText     = 2
	AnnouncementBlockHeading3 = 5
	AnnouncementBlockBullet   = 12
)

// AnnouncementTypeDocx is the new, block-based group announcement. Only it
// can be edited block by block.
const AnnouncementTypeDocx = "docx"

// IsAnnouncementHeading reports whether a block type is a heading of any
// level, which ends the section before it.
func IsAnnouncementHeading(blockType int) bool { return blockType >= 3 && blockType <= 11 }

// AnnouncementTextRun is one run of styled text.
type AnnouncementTextRun struct {
	Content string
	Bold    bool
	Code    bool
	LinkURL string
}

// AnnouncementBlock is one top-level block of a chat announcement. ID is
// empty for a block still to be created.
type AnnouncementBlock struct {
	ID   string
	Type int
	Runs []AnnouncementTextRun
}

// PlainText is the block's text without styles.
func (b AnnouncementBlock) PlainText() string {
	var out strings.Builder
	for _, run := range b.Runs {
		out.WriteString(run.Content)
	}
	return out.String()
}

// AnnouncementInfo is what the announcement's metadata says.
type AnnouncementInfo struct {
	Type       string
	RevisionID int
}

// ChatAnnouncements edits a group chat's docx announcement (群公告) with the
// tenant token. Only the page's top-level children are addressed: the page
// block's id is the chat id.
//
// Scopes: im:chat.announcement:read and im:chat.announcement:write_only.
type ChatAnnouncements interface {
	GetChatAnnouncement(ctx context.Context, chatID string) (AnnouncementInfo, error)
	// ListChatAnnouncementBlocks returns the page's top-level children in order.
	ListChatAnnouncementBlocks(ctx context.Context, chatID string) ([]AnnouncementBlock, error)
	// CreateChatAnnouncementBlocks inserts blocks as top-level children at
	// index (-1 appends) and returns the new block ids in order. clientToken
	// makes a retry of the same request idempotent on Feishu's side.
	CreateChatAnnouncementBlocks(ctx context.Context, chatID string, index int, blocks []AnnouncementBlock, clientToken string) ([]string, error)
	// DeleteChatAnnouncementBlocks deletes top-level children [start, end).
	DeleteChatAnnouncementBlocks(ctx context.Context, chatID string, start, end int, clientToken string) error
}

type chatAnnouncementAPI interface {
	Get(context.Context, *larkdocx.GetChatAnnouncementReq, ...larkcore.RequestOptionFunc) (*larkdocx.GetChatAnnouncementResp, error)
}

type chatAnnouncementBlockAPI interface {
	List(context.Context, *larkdocx.ListChatAnnouncementBlockReq, ...larkcore.RequestOptionFunc) (*larkdocx.ListChatAnnouncementBlockResp, error)
}

type chatAnnouncementChildrenAPI interface {
	Create(context.Context, *larkdocx.CreateChatAnnouncementBlockChildrenReq, ...larkcore.RequestOptionFunc) (*larkdocx.CreateChatAnnouncementBlockChildrenResp, error)
	BatchDelete(context.Context, *larkdocx.BatchDeleteChatAnnouncementBlockChildrenReq, ...larkcore.RequestOptionFunc) (*larkdocx.BatchDeleteChatAnnouncementBlockChildrenResp, error)
}

const (
	// Feishu allows 5 calls/s per app and 3 edits/s per announcement; four
	// attempts spaced 0.4s, 0.8s, 1.6s outlast a short burst.
	announcementMaxAttempts = 4
	announcementRetryBase   = 400 * time.Millisecond
	announcementPageSize    = 500
	announcementMaxPages    = 20
	// Feishu's frequency-limit code, also sent with HTTP 429.
	feishuRateLimitCode = 99991400
)

// GetChatAnnouncement calls GET /open-apis/docx/v1/chats/:chat_id/announcement.
func (s *ChannelSender) GetChatAnnouncement(ctx context.Context, chatID string) (AnnouncementInfo, error) {
	const op = "announcement_get"
	if s.announcementAPI == nil {
		return AnnouncementInfo{}, &MessageSendError{Operation: op, Class: "not_configured"}
	}
	if chatID = strings.TrimSpace(chatID); chatID == "" {
		return AnnouncementInfo{}, &MessageSendError{Operation: op, Class: "invalid_destination"}
	}
	var info AnnouncementInfo
	err := s.retryAnnouncement(ctx, op, isRetryableAnnouncementCall, func() *MessageSendError {
		resp, err := s.announcementAPI.Get(ctx, larkdocx.NewGetChatAnnouncementReqBuilder().ChatId(chatID).Build())
		if err != nil {
			return messageTransportError(op, err)
		}
		if resp == nil {
			return &MessageSendError{Operation: op, Class: "invalid_response"}
		}
		if !resp.Success() {
			return messageResponseError(op, resp.ApiResp, resp.Code)
		}
		if resp.Data == nil {
			return &MessageSendError{Operation: op, Class: "invalid_response"}
		}
		info = AnnouncementInfo{Type: strings.TrimSpace(deref(resp.Data.AnnouncementType))}
		if resp.Data.RevisionId != nil {
			info.RevisionID = *resp.Data.RevisionId
		}
		return nil
	})
	if err != nil {
		return AnnouncementInfo{}, err
	}
	return info, nil
}

// ListChatAnnouncementBlocks pages GET .../announcement/blocks and orders the
// page block's children.
func (s *ChannelSender) ListChatAnnouncementBlocks(ctx context.Context, chatID string) ([]AnnouncementBlock, error) {
	const op = "announcement_blocks_list"
	if s.announcementBlockAPI == nil {
		return nil, &MessageSendError{Operation: op, Class: "not_configured"}
	}
	if chatID = strings.TrimSpace(chatID); chatID == "" {
		return nil, &MessageSendError{Operation: op, Class: "invalid_destination"}
	}
	byID := make(map[string]*larkdocx.Block)
	var page *larkdocx.Block
	pageToken := ""
	for pages := 0; ; pages++ {
		if pages == announcementMaxPages {
			return nil, &MessageSendError{Operation: op, Class: "invalid_response"}
		}
		var data *larkdocx.ListChatAnnouncementBlockRespData
		err := s.retryAnnouncement(ctx, op, isRetryableAnnouncementCall, func() *MessageSendError {
			builder := larkdocx.NewListChatAnnouncementBlockReqBuilder().ChatId(chatID).PageSize(announcementPageSize)
			if pageToken != "" {
				builder.PageToken(pageToken)
			}
			resp, err := s.announcementBlockAPI.List(ctx, builder.Build())
			if err != nil {
				return messageTransportError(op, err)
			}
			if resp == nil {
				return &MessageSendError{Operation: op, Class: "invalid_response"}
			}
			if !resp.Success() {
				return messageResponseError(op, resp.ApiResp, resp.Code)
			}
			if resp.Data == nil {
				return &MessageSendError{Operation: op, Class: "invalid_response"}
			}
			data = resp.Data
			return nil
		})
		if err != nil {
			return nil, err
		}
		for _, block := range data.Items {
			if block == nil || block.BlockId == nil {
				continue
			}
			byID[*block.BlockId] = block
			if *block.BlockId == chatID {
				page = block
			}
		}
		if data.HasMore == nil || !*data.HasMore || deref(data.PageToken) == "" {
			break
		}
		pageToken = deref(data.PageToken)
	}
	if page == nil {
		return nil, &MessageSendError{Operation: op, Class: "invalid_response"}
	}
	out := make([]AnnouncementBlock, 0, len(page.Children))
	for _, id := range page.Children {
		block := AnnouncementBlock{ID: id}
		if raw, ok := byID[id]; ok {
			block = announcementBlockFromSDK(raw)
		}
		out = append(out, block)
	}
	return out, nil
}

// CreateChatAnnouncementBlocks calls POST
// .../announcement/blocks/:chat_id/children. A rate limit, a server error or
// a transport failure is retried with the same client token, which Feishu
// uses to apply the insert once.
func (s *ChannelSender) CreateChatAnnouncementBlocks(ctx context.Context, chatID string, index int, blocks []AnnouncementBlock, clientToken string) ([]string, error) {
	const op = "announcement_blocks_create"
	if s.announcementChildrenAPI == nil {
		return nil, &MessageSendError{Operation: op, Class: "not_configured"}
	}
	if chatID = strings.TrimSpace(chatID); chatID == "" {
		return nil, &MessageSendError{Operation: op, Class: "invalid_destination"}
	}
	if len(blocks) == 0 {
		return nil, nil
	}
	children := make([]*larkdocx.Block, 0, len(blocks))
	for _, block := range blocks {
		children = append(children, announcementBlockToSDK(block))
	}
	var ids []string
	err := s.retryAnnouncement(ctx, op, isRetryableAnnouncementCall, func() *MessageSendError {
		builder := larkdocx.NewCreateChatAnnouncementBlockChildrenReqBuilder().
			ChatId(chatID).
			BlockId(chatID).
			Body(larkdocx.NewCreateChatAnnouncementBlockChildrenReqBodyBuilder().Children(children).Index(index).Build())
		if clientToken != "" {
			builder.ClientToken(clientToken)
		}
		resp, err := s.announcementChildrenAPI.Create(ctx, builder.Build())
		if err != nil {
			return messageTransportError(op, err)
		}
		if resp == nil {
			return &MessageSendError{Operation: op, Class: "invalid_response"}
		}
		if !resp.Success() {
			return messageResponseError(op, resp.ApiResp, resp.Code)
		}
		if resp.Data == nil || len(resp.Data.Children) != len(blocks) {
			return &MessageSendError{Operation: op, Class: "invalid_response"}
		}
		ids = ids[:0]
		for _, child := range resp.Data.Children {
			id := ""
			if child != nil {
				id = strings.TrimSpace(deref(child.BlockId))
			}
			if id == "" {
				return &MessageSendError{Operation: op, Class: "invalid_response"}
			}
			ids = append(ids, id)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return ids, nil
}

// DeleteChatAnnouncementBlocks calls DELETE
// .../announcement/blocks/:chat_id/children/batch_delete. Only a definitive
// rate-limit rejection is retried: a delete addresses children by index, so
// an ambiguous attempt that Feishu did apply must not run again.
func (s *ChannelSender) DeleteChatAnnouncementBlocks(ctx context.Context, chatID string, start, end int, clientToken string) error {
	const op = "announcement_blocks_delete"
	if s.announcementChildrenAPI == nil {
		return &MessageSendError{Operation: op, Class: "not_configured"}
	}
	if chatID = strings.TrimSpace(chatID); chatID == "" {
		return &MessageSendError{Operation: op, Class: "invalid_destination"}
	}
	if start < 0 || end <= start {
		return &MessageSendError{Operation: op, Class: "invalid_request"}
	}
	return s.retryAnnouncement(ctx, op, isAnnouncementRateLimited, func() *MessageSendError {
		builder := larkdocx.NewBatchDeleteChatAnnouncementBlockChildrenReqBuilder().
			ChatId(chatID).
			BlockId(chatID).
			Body(larkdocx.NewBatchDeleteChatAnnouncementBlockChildrenReqBodyBuilder().StartIndex(start).EndIndex(end).Build())
		if clientToken != "" {
			builder.ClientToken(clientToken)
		}
		resp, err := s.announcementChildrenAPI.BatchDelete(ctx, builder.Build())
		if err != nil {
			return messageTransportError(op, err)
		}
		if resp == nil {
			return &MessageSendError{Operation: op, Class: "invalid_response"}
		}
		if !resp.Success() {
			return messageResponseError(op, resp.ApiResp, resp.Code)
		}
		return nil
	})
}

var _ ChatAnnouncements = (*ChannelSender)(nil)

// IsAnnouncementRateLimited reports Feishu's frequency limit, sent as HTTP
// 429 or code 99991400.
func IsAnnouncementRateLimited(err error) bool {
	var sendErr *MessageSendError
	return errors.As(err, &sendErr) && isAnnouncementRateLimited(sendErr)
}

func isAnnouncementRateLimited(err *MessageSendError) bool {
	return err.Class == "api_rejected" && (err.HTTPStatus == 429 || err.Code == feishuRateLimitCode)
}

// isRetryableAnnouncementCall covers reads and token-protected creates: a
// rate limit, a server error, or a lost connection.
func isRetryableAnnouncementCall(err *MessageSendError) bool {
	return isAnnouncementRateLimited(err) || err.Retryable
}

func (s *ChannelSender) retryAnnouncement(ctx context.Context, operation string, retryable func(*MessageSendError) bool, call func() *MessageSendError) error {
	maxAttempts := s.announcementMaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = announcementMaxAttempts
	}
	delay := s.announcementRetryBase
	if delay <= 0 {
		delay = announcementRetryBase
	}
	for attempt := 1; ; attempt++ {
		err := call()
		if err == nil {
			return nil
		}
		if isAnnouncementRateLimited(err) {
			// A rate limit is always transient for the caller, even once
			// the retries are spent.
			err.Retryable = true
		}
		if attempt >= maxAttempts || !retryable(err) {
			return err
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return messageTransportError(operation, ctx.Err())
		case <-timer.C:
		}
		delay *= 2
	}
}

func announcementBlockFromSDK(raw *larkdocx.Block) AnnouncementBlock {
	block := AnnouncementBlock{ID: deref(raw.BlockId)}
	if raw.BlockType != nil {
		block.Type = *raw.BlockType
	}
	text := announcementBlockText(raw)
	if text == nil {
		return block
	}
	for _, element := range text.Elements {
		if element == nil || element.TextRun == nil {
			continue
		}
		run := AnnouncementTextRun{}
		if element.TextRun.Content != nil {
			run.Content = *element.TextRun.Content
		}
		if style := element.TextRun.TextElementStyle; style != nil {
			run.Bold = style.Bold != nil && *style.Bold
			run.Code = style.InlineCode != nil && *style.InlineCode
			if style.Link != nil {
				if decoded, err := url.QueryUnescape(deref(style.Link.Url)); err == nil {
					run.LinkURL = decoded
				}
			}
		}
		block.Runs = append(block.Runs, run)
	}
	return block
}

func announcementBlockText(raw *larkdocx.Block) *larkdocx.Text {
	if raw.BlockType == nil {
		return nil
	}
	switch *raw.BlockType {
	case AnnouncementBlockText:
		return raw.Text
	case 3:
		return raw.Heading1
	case 4:
		return raw.Heading2
	case AnnouncementBlockHeading3:
		return raw.Heading3
	case 6:
		return raw.Heading4
	case 7:
		return raw.Heading5
	case 8:
		return raw.Heading6
	case 9:
		return raw.Heading7
	case 10:
		return raw.Heading8
	case 11:
		return raw.Heading9
	case AnnouncementBlockBullet:
		return raw.Bullet
	}
	return nil
}

func announcementBlockToSDK(block AnnouncementBlock) *larkdocx.Block {
	elements := make([]*larkdocx.TextElement, 0, len(block.Runs))
	for _, run := range block.Runs {
		textRun := &larkdocx.TextRun{Content: stringPtr(run.Content)}
		if run.Bold || run.Code || run.LinkURL != "" {
			style := &larkdocx.TextElementStyle{}
			if run.Bold {
				style.Bold = boolPtr(true)
			}
			if run.Code {
				style.InlineCode = boolPtr(true)
			}
			if run.LinkURL != "" {
				style.Link = &larkdocx.Link{Url: stringPtr(url.QueryEscape(run.LinkURL))}
			}
			textRun.TextElementStyle = style
		}
		elements = append(elements, &larkdocx.TextElement{TextRun: textRun})
	}
	text := &larkdocx.Text{Elements: elements}
	out := &larkdocx.Block{BlockType: intPtr(block.Type)}
	switch block.Type {
	case AnnouncementBlockHeading3:
		out.Heading3 = text
	case AnnouncementBlockBullet:
		out.Bullet = text
	default:
		out.BlockType = intPtr(AnnouncementBlockText)
		out.Text = text
	}
	return out
}

func stringPtr(v string) *string { return &v }
func boolPtr(v bool) *bool       { return &v }
func intPtr(v int) *int          { return &v }
