package feishu

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"
	"unicode/utf8"

	larkcore "github.com/larksuite/oapi-sdk-go/v3/core"
	larkim "github.com/larksuite/oapi-sdk-go/v3/service/im/v1"
)

const (
	attachedContextPageSize           = 50
	attachedContextMaxBoundaryScan    = 256
	attachedContextMaxMessages        = 64
	attachedContextMaxTextBytes       = 64 * 1024
	attachedContextMaxImages          = 8
	attachedContextMaxImageBytes      = 5 * 1024 * 1024
	attachedContextMaxTotalImageBytes = 16 * 1024 * 1024
)

// Inline placeholders for content botd cannot carry. They ride in message
// text so a provider sees WHERE an unsupported item sat in the conversation
// and can still address the question beside it, instead of refusing over an
// opaque aggregate issue counter.
const (
	placeholderVideo       = "[unsupported video file]"
	placeholderFile        = "[unsupported file]"
	placeholderAudio       = "[unsupported audio message]"
	placeholderSticker     = "[unsupported sticker]"
	placeholderImage       = "[image]"
	placeholderUnsupported = "[unsupported message]"
)

// AttachedContextStatus distinguishes an absent topic from a topic botd could
// not safely read. Found always contains at least one usable text or image.
type AttachedContextStatus int

const (
	AttachedContextStatusUnspecified AttachedContextStatus = iota
	AttachedContextFound
	AttachedContextMissing
	AttachedContextUnreadable
)

// AttachedContextIssueCode is a provider-safe, fixed vocabulary. It never
// carries Feishu response text, message ids, image keys, or tenant data.
type AttachedContextIssueCode string

const (
	AttachedContextIssueNoThread           AttachedContextIssueCode = "no_thread"
	AttachedContextIssueHistoryUnreadable  AttachedContextIssueCode = "history_unreadable"
	AttachedContextIssueBoundaryNotFound   AttachedContextIssueCode = "boundary_not_found"
	AttachedContextIssueBoundaryScanLimit  AttachedContextIssueCode = "boundary_scan_limit"
	AttachedContextIssueMessageLimit       AttachedContextIssueCode = "message_limit"
	AttachedContextIssueTextLimit          AttachedContextIssueCode = "text_limit"
	AttachedContextIssueImageLimit         AttachedContextIssueCode = "image_limit"
	AttachedContextIssueImageTooLarge      AttachedContextIssueCode = "image_too_large"
	AttachedContextIssueTotalImageLimit    AttachedContextIssueCode = "total_image_limit"
	AttachedContextIssueImageUnreadable    AttachedContextIssueCode = "image_unreadable"
	AttachedContextIssueImageType          AttachedContextIssueCode = "image_type_unsupported"
	AttachedContextIssueVideoOmitted       AttachedContextIssueCode = "video_omitted"
	AttachedContextIssueUnsupportedMessage AttachedContextIssueCode = "unsupported_message"
	AttachedContextIssueMalformedMessage   AttachedContextIssueCode = "malformed_message"
)

type AttachedContextIssue struct {
	Code  AttachedContextIssueCode
	Count uint32
}

type AttachedContextImage struct {
	MediaType string
	Data      []byte
}

type AttachedContextMessage struct {
	// AuthorLabel is meaningful only within this snapshot. It is assigned by
	// first appearance after ordering and is not derived from a stable digest.
	AuthorLabel string
	AuthorType  string
	Text        string
	Images      []AttachedContextImage
}

type AttachedContext struct {
	Status    AttachedContextStatus
	Messages  []AttachedContextMessage
	Issues    []AttachedContextIssue
	Truncated bool
}

// AttachedContextRequest contains daemon-private Feishu identities captured on
// the exact inbound delivery. It must never cross the provider boundary.
type AttachedContextRequest struct {
	ThreadID          string
	TriggerMessageID  string
	TriggerCreateTime string
}

// AttachedContextLookup is implemented by an app-bound Feishu backend. The
// service invokes it only after authenticating the provider's exact delivery.
type AttachedContextLookup interface {
	LookupAttachedContext(context.Context, AttachedContextRequest) (AttachedContext, error)
}

type threadMessageAPI interface {
	List(context.Context, *larkim.ListMessageReq, ...larkcore.RequestOptionFunc) (*larkim.ListMessageResp, error)
}

type messageResourceAPI interface {
	Get(context.Context, *larkim.GetMessageResourceReq, ...larkcore.RequestOptionFunc) (*larkim.GetMessageResourceResp, error)
}

type sdkAttachedContextLookup struct {
	history   threadMessageAPI
	resources messageResourceAPI
}

func newSDKAttachedContextLookup(history threadMessageAPI, resources messageResourceAPI) *sdkAttachedContextLookup {
	return &sdkAttachedContextLookup{history: history, resources: resources}
}

type attachedContextCandidate struct {
	message   *larkim.Message
	isTrigger bool
}

func (s *sdkAttachedContextLookup) LookupAttachedContext(ctx context.Context, in AttachedContextRequest) (AttachedContext, error) {
	in.ThreadID = strings.TrimSpace(in.ThreadID)
	in.TriggerMessageID = strings.TrimSpace(in.TriggerMessageID)
	in.TriggerCreateTime = strings.TrimSpace(in.TriggerCreateTime)
	if in.ThreadID == "" {
		return attachedContextWithIssue(AttachedContextMissing, AttachedContextIssueNoThread), nil
	}
	if in.TriggerMessageID == "" || s == nil || s.history == nil {
		return attachedContextWithIssue(AttachedContextUnreadable, AttachedContextIssueBoundaryNotFound), nil
	}

	candidates, issues, truncated, historyUnreadable := s.snapshotCandidates(ctx, in)
	if !containsTrigger(candidates) {
		issues = appendOrIncrementAttachedContextIssue(issues, AttachedContextIssueBoundaryNotFound)
		return AttachedContext{Status: AttachedContextUnreadable, Issues: issues, Truncated: truncated}, nil
	}

	result := AttachedContext{Issues: issues, Truncated: truncated}
	unreadableContent := historyUnreadable
	totalTextBytes := 0
	totalImageBytes := 0
	totalImageCandidates := 0
	participants := make(map[string]string)
	for index := len(candidates) - 1; index >= 0; index-- {
		candidate := candidates[index]
		parsed, parseIssues, malformed := parseAttachedMessage(candidate.message, candidate.isTrigger)
		for _, issue := range parseIssues {
			result.Issues = appendOrIncrementAttachedContextIssue(result.Issues, issue)
		}
		unreadableContent = unreadableContent || malformed

		text := parsed.text
		if text != "" {
			remaining := attachedContextMaxTextBytes - totalTextBytes
			if remaining <= 0 {
				text = ""
				result.Truncated = true
				result.Issues = appendOrIncrementAttachedContextIssue(result.Issues, AttachedContextIssueTextLimit)
			} else if len(text) > remaining {
				text = utf8Prefix(text, remaining)
				totalTextBytes += len(text)
				result.Truncated = true
				result.Issues = appendOrIncrementAttachedContextIssue(result.Issues, AttachedContextIssueTextLimit)
			} else {
				totalTextBytes += len(text)
			}
		}

		images := make([]AttachedContextImage, 0, len(parsed.imageKeys))
		for _, imageKey := range parsed.imageKeys {
			if totalImageCandidates >= attachedContextMaxImages {
				result.Truncated = true
				result.Issues = appendOrIncrementAttachedContextIssue(result.Issues, AttachedContextIssueImageLimit)
				continue
			}
			totalImageCandidates++
			image, issue := s.downloadImage(ctx, deref(candidate.message.MessageId), imageKey)
			if issue != "" {
				result.Issues = appendOrIncrementAttachedContextIssue(result.Issues, issue)
				unreadableContent = unreadableContent || issue == AttachedContextIssueImageUnreadable
				if issue == AttachedContextIssueImageTooLarge {
					result.Truncated = true
				}
				continue
			}
			if totalImageBytes+len(image.Data) > attachedContextMaxTotalImageBytes {
				result.Truncated = true
				result.Issues = appendOrIncrementAttachedContextIssue(result.Issues, AttachedContextIssueTotalImageLimit)
				continue
			}
			totalImageBytes += len(image.Data)
			images = append(images, image)
		}

		if text == "" && len(images) == 0 {
			continue
		}
		authorKey, authorType := attachedContextAuthor(candidate.message)
		authorLabel := ""
		if authorKey != "" {
			var exists bool
			authorLabel, exists = participants[authorKey]
			if !exists {
				authorLabel = "participant-" + strconv.Itoa(len(participants)+1)
				participants[authorKey] = authorLabel
			}
		}
		result.Messages = append(result.Messages, AttachedContextMessage{
			AuthorLabel: authorLabel,
			AuthorType:  authorType,
			Text:        text,
			Images:      images,
		})
	}

	if len(result.Messages) > 0 {
		result.Status = AttachedContextFound
	} else if unreadableContent {
		result.Status = AttachedContextUnreadable
	} else {
		result.Status = AttachedContextMissing
	}
	return result, nil
}

func (s *sdkAttachedContextLookup) snapshotCandidates(
	ctx context.Context,
	in AttachedContextRequest,
) ([]attachedContextCandidate, []AttachedContextIssue, bool, bool) {
	candidates := make([]attachedContextCandidate, 0, attachedContextMaxMessages+1)
	issues := make([]AttachedContextIssue, 0)
	pageToken := ""
	foundTrigger := false
	scanned := 0
	priorMessages := 0

	for scanned < attachedContextMaxBoundaryScan {
		builder := larkim.NewListMessageReqBuilder().
			ContainerIdType("thread").
			ContainerId(in.ThreadID).
			SortType("ByCreateTimeDesc").
			PageSize(attachedContextPageSize)
		if seconds := feishuMillisecondsToSeconds(in.TriggerCreateTime); seconds != "" {
			builder.EndTime(seconds)
		}
		if pageToken != "" {
			builder.PageToken(pageToken)
		}
		resp, err := s.history.List(ctx, builder.Build())
		if err != nil || resp == nil || !resp.Success() || resp.Data == nil {
			issues = appendOrIncrementAttachedContextIssue(issues, AttachedContextIssueHistoryUnreadable)
			return candidates, issues, false, true
		}

		for itemIndex, message := range resp.Data.Items {
			if scanned >= attachedContextMaxBoundaryScan {
				break
			}
			scanned++
			if message == nil {
				continue
			}
			if !foundTrigger {
				if strings.TrimSpace(deref(message.MessageId)) != in.TriggerMessageID {
					continue
				}
				foundTrigger = true
				candidates = append(candidates, attachedContextCandidate{message: message, isTrigger: true})
				continue
			}
			if priorMessages >= attachedContextMaxMessages {
				issues = appendOrIncrementAttachedContextIssue(issues, AttachedContextIssueMessageLimit)
				return candidates, issues, true, false
			}
			candidates = append(candidates, attachedContextCandidate{message: message})
			priorMessages++
			if priorMessages == attachedContextMaxMessages &&
				(itemIndex+1 < len(resp.Data.Items) || derefBool(resp.Data.HasMore)) {
				issues = appendOrIncrementAttachedContextIssue(issues, AttachedContextIssueMessageLimit)
				return candidates, issues, true, false
			}
		}

		if !derefBool(resp.Data.HasMore) {
			return candidates, issues, false, false
		}
		pageToken = strings.TrimSpace(deref(resp.Data.PageToken))
		if pageToken == "" {
			issues = appendOrIncrementAttachedContextIssue(issues, AttachedContextIssueHistoryUnreadable)
			return candidates, issues, false, true
		}
	}
	issues = appendOrIncrementAttachedContextIssue(issues, AttachedContextIssueBoundaryScanLimit)
	return candidates, issues, true, true
}

type parsedAttachedMessage struct {
	text      string
	imageKeys []string
}

func parseAttachedMessage(message *larkim.Message, trigger bool) (parsedAttachedMessage, []AttachedContextIssueCode, bool) {
	parsed, issues, malformed := parseAttachedMessageContent(message)
	if trigger {
		// The trigger's own words already ride the prompt; only its images
		// (and the omission issues above) belong in the snapshot.
		parsed.text = ""
	}
	return parsed, issues, malformed
}

func parseAttachedMessageContent(message *larkim.Message) (parsedAttachedMessage, []AttachedContextIssueCode, bool) {
	if message == nil || message.Body == nil || message.Body.Content == nil {
		return parsedAttachedMessage{}, []AttachedContextIssueCode{AttachedContextIssueMalformedMessage}, true
	}
	content := *message.Body.Content
	switch strings.TrimSpace(deref(message.MsgType)) {
	case "text":
		var body struct {
			Text string `json:"text"`
		}
		if err := json.Unmarshal([]byte(content), &body); err != nil {
			return parsedAttachedMessage{}, []AttachedContextIssueCode{AttachedContextIssueMalformedMessage}, true
		}
		return parsedAttachedMessage{text: strings.TrimSpace(body.Text)}, nil, false
	case "post":
		text, images, videos, ok := parseAttachedPost(content)
		if !ok {
			return parsedAttachedMessage{}, []AttachedContextIssueCode{AttachedContextIssueMalformedMessage}, true
		}
		issues := make([]AttachedContextIssueCode, videos)
		for index := range issues {
			issues[index] = AttachedContextIssueVideoOmitted
		}
		return parsedAttachedMessage{text: text, imageKeys: images}, issues, false
	case "image":
		var body struct {
			ImageKey string `json:"image_key"`
		}
		if err := json.Unmarshal([]byte(content), &body); err != nil || strings.TrimSpace(body.ImageKey) == "" {
			return parsedAttachedMessage{}, []AttachedContextIssueCode{AttachedContextIssueMalformedMessage}, true
		}
		return parsedAttachedMessage{imageKeys: []string{strings.TrimSpace(body.ImageKey)}}, nil, false
	case "media":
		return parsedAttachedMessage{text: placeholderVideo}, []AttachedContextIssueCode{AttachedContextIssueVideoOmitted}, false
	case "file":
		return parsedAttachedMessage{text: filePlaceholder(content)}, []AttachedContextIssueCode{AttachedContextIssueUnsupportedMessage}, false
	case "audio":
		return parsedAttachedMessage{text: placeholderAudio}, []AttachedContextIssueCode{AttachedContextIssueUnsupportedMessage}, false
	case "sticker":
		return parsedAttachedMessage{text: placeholderSticker}, []AttachedContextIssueCode{AttachedContextIssueUnsupportedMessage}, false
	default:
		return parsedAttachedMessage{text: placeholderUnsupported}, []AttachedContextIssueCode{AttachedContextIssueUnsupportedMessage}, false
	}
}

// filePlaceholder names the file when its descriptor parses — the name is
// ordinary user-shared chat content and often IS the referent of a question
// ("看看这个") — and degrades to the bare placeholder when it does not.
func filePlaceholder(content string) string {
	var body struct {
		FileName string `json:"file_name"`
	}
	if err := json.Unmarshal([]byte(content), &body); err != nil {
		return placeholderFile
	}
	name := strings.TrimSpace(body.FileName)
	if name == "" {
		return placeholderFile
	}
	return "[unsupported file: " + name + "]"
}

// postElement is one typed run inside a Feishu rich-text (post) message.
type postElement struct {
	Tag      string `json:"tag"`
	Text     string `json:"text"`
	ImageKey string `json:"image_key"`
	UserID   string `json:"user_id"`
	UserName string `json:"user_name"`
}

type localizedPost struct {
	Title   string          `json:"title"`
	Content [][]postElement `json:"content"`
}

type attachedPostDocument map[string]localizedPost

// localizedAttachedPost picks the one localization this daemon flattens.
func localizedAttachedPost(raw string) (localizedPost, bool) {
	var document attachedPostDocument
	if err := json.Unmarshal([]byte(raw), &document); err != nil || len(document) == 0 {
		return localizedPost{}, false
	}
	for _, candidate := range []string{"zh_cn", "en_us", "ja_jp"} {
		if localized, ok := document[candidate]; ok {
			return localized, true
		}
	}
	return localizedPost{}, false
}

func parseAttachedPost(raw string) (string, []string, int, bool) {
	localized, ok := localizedAttachedPost(raw)
	if !ok {
		return "", nil, 0, false
	}
	lines := make([]string, 0)
	if title := strings.TrimSpace(localized.Title); title != "" {
		lines = append(lines, title)
	}
	images := make([]string, 0)
	videos := 0
	for _, row := range localized.Content {
		parts := make([]string, 0, len(row))
		for _, element := range row {
			switch strings.TrimSpace(element.Tag) {
			case "img":
				if key := strings.TrimSpace(element.ImageKey); key != "" {
					images = append(images, key)
				}
			case "media":
				// The video itself cannot cross this boundary; the inline
				// placeholder keeps its position in the prose so a provider
				// knows what the surrounding words refer to.
				parts = append(parts, placeholderVideo)
				videos++
			case "at":
				// Structured mention display names are provider identities, not
				// ordinary typed text. Keep the conversational shape without
				// exposing a stable/correlatable name.
				parts = append(parts, "@participant")
			default:
				if element.Text != "" {
					parts = append(parts, element.Text)
				}
			}
		}
		if line := strings.TrimSpace(strings.Join(parts, "")); line != "" {
			lines = append(lines, line)
		}
	}
	return strings.Join(lines, "\n"), images, videos, true
}

func (s *sdkAttachedContextLookup) downloadImage(ctx context.Context, messageID, imageKey string) (AttachedContextImage, AttachedContextIssueCode) {
	if s.resources == nil || strings.TrimSpace(messageID) == "" || strings.TrimSpace(imageKey) == "" {
		return AttachedContextImage{}, AttachedContextIssueImageUnreadable
	}
	req := larkim.NewGetMessageResourceReqBuilder().
		MessageId(messageID).
		FileKey(imageKey).
		Type("image").
		Build()
	resp, err := s.resources.Get(ctx, req)
	if err != nil || resp == nil || !resp.Success() || resp.File == nil {
		return AttachedContextImage{}, AttachedContextIssueImageUnreadable
	}
	data, err := io.ReadAll(io.LimitReader(resp.File, attachedContextMaxImageBytes+1))
	if err != nil {
		return AttachedContextImage{}, AttachedContextIssueImageUnreadable
	}
	if len(data) > attachedContextMaxImageBytes {
		return AttachedContextImage{}, AttachedContextIssueImageTooLarge
	}
	mediaType := detectedImageMediaType(data)
	if mediaType == "" {
		return AttachedContextImage{}, AttachedContextIssueImageType
	}
	return AttachedContextImage{MediaType: mediaType, Data: data}, ""
}

func detectedImageMediaType(data []byte) string {
	detected := http.DetectContentType(data)
	switch detected {
	case "image/jpeg", "image/png", "image/gif", "image/webp":
		return detected
	}
	if len(data) >= 12 && string(data[:4]) == "RIFF" && string(data[8:12]) == "WEBP" {
		return "image/webp"
	}
	return ""
}

func attachedContextAuthor(message *larkim.Message) (string, string) {
	if message == nil || message.Sender == nil {
		return "", "unknown"
	}
	authorType := strings.TrimSpace(deref(message.Sender.SenderType))
	switch authorType {
	case "user", "app", "anonymous":
	default:
		authorType = "unknown"
	}
	id := strings.TrimSpace(deref(message.Sender.Id))
	if id == "" {
		return "", authorType
	}
	return authorType + "\x00" + id, authorType
}

func feishuMillisecondsToSeconds(value string) string {
	milliseconds, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
	if err != nil || milliseconds < 0 {
		return ""
	}
	return strconv.FormatInt(milliseconds/1000, 10)
}

func utf8Prefix(value string, maxBytes int) string {
	if maxBytes <= 0 {
		return ""
	}
	if len(value) <= maxBytes {
		return value
	}
	prefix := value[:maxBytes]
	for !utf8.ValidString(prefix) && len(prefix) > 0 {
		prefix = prefix[:len(prefix)-1]
	}
	return prefix
}

func containsTrigger(candidates []attachedContextCandidate) bool {
	for _, candidate := range candidates {
		if candidate.isTrigger {
			return true
		}
	}
	return false
}

func attachedContextWithIssue(status AttachedContextStatus, code AttachedContextIssueCode) AttachedContext {
	return AttachedContext{Status: status, Issues: []AttachedContextIssue{{Code: code, Count: 1}}}
}

func appendOrIncrementAttachedContextIssue(issues []AttachedContextIssue, code AttachedContextIssueCode) []AttachedContextIssue {
	if code == "" {
		return issues
	}
	for index := range issues {
		if issues[index].Code == code {
			issues[index].Count++
			return issues
		}
	}
	return append(issues, AttachedContextIssue{Code: code, Count: 1})
}

func derefBool(value *bool) bool {
	return value != nil && *value
}
