package feishu

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"mime"
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
	// Video carry (ADR-0126). The daemon downloads and bounds video bytes but
	// never decodes them; a provider without allow_attached_video sees the
	// pre-existing placeholder and video_omitted issue instead.
	attachedContextMaxVideos          = 2
	attachedContextMaxVideoBytes      = 64 * 1024 * 1024
	attachedContextMaxTotalVideoBytes = 96 * 1024 * 1024
	attachedContextMaxVideoDurationMs = 600_000
	attachedContextMaxVideoNameBytes  = 256
	// File carry (ADR-0127). Any file type is carried as opaque bytes: the
	// daemon bounds and labels them but never parses them; a provider without
	// allow_attached_files sees the "[unsupported file: name]" placeholder and
	// a file_omitted issue instead. These bounds are separate from the image
	// and video budgets.
	attachedContextMaxFiles          = 4
	attachedContextMaxFileBytes      = 5 * 1024 * 1024
	attachedContextMaxTotalFileBytes = 10 * 1024 * 1024
	attachedContextMaxFileNameBytes  = 256
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
	// placeholderVideoInline marks a granted, carried video's position inside
	// a post's flattened prose. Unlike placeholderVideo (which means "this
	// video was not delivered"), this token means "the video below/above is
	// this one" — the surrounding words still need to point at it.
	placeholderVideoInline = "[video]"
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
	// Video carry (ADR-0126). VideoOmitted above now means "no
	// allow_attached_video grant"; the codes below describe a granted video
	// that could not be delivered. Every one of them keeps the message's
	// placeholder text — a video never fails the snapshot.
	AttachedContextIssueVideoLimit      AttachedContextIssueCode = "video_limit"
	AttachedContextIssueVideoTooLarge   AttachedContextIssueCode = "video_too_large"
	AttachedContextIssueVideoUnreadable AttachedContextIssueCode = "video_unreadable"
	AttachedContextIssueVideoType       AttachedContextIssueCode = "video_type_unsupported"
	AttachedContextIssueVideoTooLong    AttachedContextIssueCode = "video_too_long"
	// File carry (ADR-0127). FileOmitted means "no allow_attached_files
	// grant"; the others describe a granted file that could not be delivered.
	// Every one of them keeps the message's "[unsupported file: name]"
	// placeholder text — a file never fails the snapshot.
	AttachedContextIssueFileOmitted    AttachedContextIssueCode = "file_omitted"
	AttachedContextIssueFileLimit      AttachedContextIssueCode = "file_limit"
	AttachedContextIssueFileTooLarge   AttachedContextIssueCode = "file_too_large"
	AttachedContextIssueTotalFileLimit AttachedContextIssueCode = "total_file_limit"
	AttachedContextIssueFileUnreadable AttachedContextIssueCode = "file_unreadable"
)

type AttachedContextIssue struct {
	Code  AttachedContextIssueCode
	Count uint32
}

type AttachedContextImage struct {
	MediaType string
	Data      []byte
}

// AttachedContextVideo is a video the daemon downloaded, bounded, and
// type-detected but never decoded (ADR-0126). Only accepted videos are ever
// constructed; a declined or failed one never reaches this type — it stays a
// placeholder and an issue.
type AttachedContextVideo struct {
	MediaType  string
	Data       []byte
	DurationMs uint64 // Feishu-declared; 0 when unknown
	FileName   string // Feishu-declared, bounded to attachedContextMaxVideoNameBytes
}

// AttachedContextFile is a Feishu file message the daemon downloaded and
// bounded but never parsed (ADR-0127). Only accepted files are constructed; a
// declined or failed one stays a placeholder and an issue. FileName is user
// content, never a path: nothing in the daemon opens or resolves it.
type AttachedContextFile struct {
	MediaType string
	Data      []byte
	FileName  string // Feishu-declared, bounded to attachedContextMaxFileNameBytes
}

type AttachedContextMessage struct {
	// AuthorLabel is meaningful only within this snapshot. It is assigned by
	// first appearance after ordering and is not derived from a stable digest.
	AuthorLabel string
	AuthorType  string
	Text        string
	Images      []AttachedContextImage
	Videos      []AttachedContextVideo
	Files       []AttachedContextFile
}

type AttachedContext struct {
	// Snapshot-local label only, established by exact triggering sender identity.
	RequesterLabel string
	Status         AttachedContextStatus
	Messages       []AttachedContextMessage
	Issues         []AttachedContextIssue
	Truncated      bool
}

// AttachedContextRequest contains daemon-private Feishu identities captured on
// the exact inbound delivery. It must never cross the provider boundary.
type AttachedContextRequest struct {
	ThreadID          string
	TriggerMessageID  string
	TriggerCreateTime string
	// AllowVideo is the resolved allow_attached_video grant (ADR-0126),
	// already conjoined with allow_attached_context by the caller. False
	// means every video in this snapshot stays a placeholder + video_omitted,
	// byte-for-byte the pre-video-carry behavior.
	AllowVideo bool
	// AllowFiles is the resolved allow_attached_files grant (ADR-0127), also
	// already conjoined with allow_attached_context. False means every file
	// message stays "[unsupported file: name]" + file_omitted.
	AllowFiles bool
}

// attachedCarryGrants is the per-snapshot media carry decision, resolved by
// the service before a single message is parsed.
type attachedCarryGrants struct {
	video bool
	files bool
}

// AttachedContextLookup is implemented by an app-bound Feishu backend. The
// service invokes it only after authenticating the provider's exact delivery.
type AttachedContextLookup interface {
	LookupAttachedContext(context.Context, AttachedContextRequest) (AttachedContext, error)
}

type threadMessageAPI interface {
	List(context.Context, *larkim.ListMessageReq, ...larkcore.RequestOptionFunc) (*larkim.ListMessageResp, error)
	Get(context.Context, *larkim.GetMessageReq, ...larkcore.RequestOptionFunc) (*larkim.GetMessageResp, error)
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
	if in.TriggerMessageID == "" || s == nil || s.history == nil {
		return attachedContextWithIssue(AttachedContextUnreadable, AttachedContextIssueBoundaryNotFound), nil
	}
	grants := attachedCarryGrants{video: in.AllowVideo, files: in.AllowFiles}
	if in.ThreadID == "" {
		return s.lookupTriggerOnly(ctx, in.TriggerMessageID, grants)
	}

	candidates, issues, truncated, historyUnreadable := s.snapshotCandidates(ctx, in)
	if !containsTrigger(candidates) {
		issues = appendOrIncrementAttachedContextIssue(issues, AttachedContextIssueBoundaryNotFound)
		return AttachedContext{Status: AttachedContextUnreadable, Issues: issues, Truncated: truncated}, nil
	}
	return s.assemble(ctx, candidates, issues, truncated, historyUnreadable, grants)
}

// lookupTriggerOnly downloads images on the triggering message when there is
// no topic thread to list. A plain group or DM still flattens those pictures
// to "[image]" in the prompt; this is the only path that can replace that
// placeholder with bytes. It does not walk the room.
func (s *sdkAttachedContextLookup) lookupTriggerOnly(ctx context.Context, triggerMessageID string, grants attachedCarryGrants) (AttachedContext, error) {
	req := larkim.NewGetMessageReqBuilder().MessageId(triggerMessageID).Build()
	resp, err := s.history.Get(ctx, req)
	if err != nil || resp == nil || !resp.Success() || resp.Data == nil {
		return attachedContextWithIssue(AttachedContextUnreadable, AttachedContextIssueHistoryUnreadable), nil
	}
	var trigger *larkim.Message
	for _, item := range resp.Data.Items {
		if item == nil {
			continue
		}
		if strings.TrimSpace(deref(item.MessageId)) == triggerMessageID {
			trigger = item
			break
		}
		if trigger == nil {
			trigger = item
		}
	}
	if trigger == nil {
		return attachedContextWithIssue(AttachedContextUnreadable, AttachedContextIssueBoundaryNotFound), nil
	}
	return s.assemble(ctx, []attachedContextCandidate{{message: trigger, isTrigger: true}}, nil, false, false, grants)
}

func (s *sdkAttachedContextLookup) assemble(
	ctx context.Context,
	candidates []attachedContextCandidate,
	issues []AttachedContextIssue,
	truncated bool,
	historyUnreadable bool,
	grants attachedCarryGrants,
) (AttachedContext, error) {
	result := AttachedContext{Issues: issues, Truncated: truncated}
	unreadableContent := historyUnreadable
	totalTextBytes := 0
	totalImageBytes := 0
	totalImageCandidates := 0
	totalVideoBytes := 0
	totalVideoCandidates := 0
	totalFileBytes := 0
	totalFileCandidates := 0
	participants := make(map[string]string)
	for index := len(candidates) - 1; index >= 0; index-- {
		candidate := candidates[index]
		// A recalled message was withdrawn by its author; carrying even a
		// placeholder would resurface what they chose to take back.
		if derefBool(candidate.message.Deleted) && !candidate.isTrigger {
			continue
		}
		parsed, parseIssues, malformed := parseAttachedMessage(candidate.message, candidate.isTrigger, grants)
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

		videos := make([]AttachedContextVideo, 0, len(parsed.videoKeys))
		videoFailed := false
		for _, key := range parsed.videoKeys {
			if totalVideoCandidates >= attachedContextMaxVideos {
				result.Truncated = true
				result.Issues = appendOrIncrementAttachedContextIssue(result.Issues, AttachedContextIssueVideoLimit)
				videoFailed = true
				continue
			}
			totalVideoCandidates++
			video, issue := s.downloadVideo(ctx, deref(candidate.message.MessageId), key)
			if issue != "" {
				result.Issues = appendOrIncrementAttachedContextIssue(result.Issues, issue)
				if issue == AttachedContextIssueVideoTooLarge || issue == AttachedContextIssueVideoTooLong {
					result.Truncated = true
				}
				videoFailed = true
				continue
			}
			if totalVideoBytes+len(video.Data) > attachedContextMaxTotalVideoBytes {
				result.Truncated = true
				result.Issues = appendOrIncrementAttachedContextIssue(result.Issues, AttachedContextIssueVideoLimit)
				videoFailed = true
				continue
			}
			totalVideoBytes += len(video.Data)
			videos = append(videos, video)
		}
		// A video that failed any bound or the download keeps the message's
		// placeholder text instead of vanishing — the snapshot never fails
		// because of a video. Only the standalone "media" message type needs
		// this: a post already baked its inline [video] token into text at
		// parse time, before download was attempted.
		if videoFailed && text == "" {
			text = placeholderVideo
		}

		files := make([]AttachedContextFile, 0, len(parsed.fileKeys))
		fileFailed := false
		for _, key := range parsed.fileKeys {
			if totalFileCandidates >= attachedContextMaxFiles {
				result.Truncated = true
				result.Issues = appendOrIncrementAttachedContextIssue(result.Issues, AttachedContextIssueFileLimit)
				fileFailed = true
				continue
			}
			totalFileCandidates++
			file, issue := s.downloadFile(ctx, deref(candidate.message.MessageId), key)
			if issue != "" {
				result.Issues = appendOrIncrementAttachedContextIssue(result.Issues, issue)
				if issue == AttachedContextIssueFileTooLarge {
					result.Truncated = true
				}
				fileFailed = true
				continue
			}
			if totalFileBytes+len(file.Data) > attachedContextMaxTotalFileBytes {
				result.Truncated = true
				result.Issues = appendOrIncrementAttachedContextIssue(result.Issues, AttachedContextIssueTotalFileLimit)
				fileFailed = true
				continue
			}
			totalFileBytes += len(file.Data)
			files = append(files, file)
		}
		// Mirrors the video rule: a granted file that was not delivered keeps
		// its "[unsupported file: name]" row, so the provider still sees where
		// it sat. An accepted file's message carries no text, like a video's.
		if fileFailed && text == "" {
			text = parsed.filePlaceholder
		}

		if text == "" && len(images) == 0 && len(videos) == 0 && len(files) == 0 {
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
			Videos:      videos,
			Files:       files,
		})
	}

	// The guide text may be omitted, so identify its sender from candidates,
	// not from the rendered messages or their order.
	for _, candidate := range candidates {
		if candidate.isTrigger {
			key, kind := attachedContextAuthor(candidate.message)
			if key != "" && kind == "user" {
				result.RequesterLabel = participants[key]
			}
			break
		}
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

// attachedVideoKey is a video reference collected during parsing, before any
// download is attempted. FileName and DurationMs are Feishu-declared and
// unvalidated until downloadVideo bounds them.
type attachedVideoKey struct {
	fileKey    string
	fileName   string
	durationMs uint64
}

// attachedFileKey is a file reference collected during parsing, before any
// download is attempted. fileName is Feishu-declared and unvalidated until
// downloadFile bounds it.
type attachedFileKey struct {
	fileKey  string
	fileName string
}

type parsedAttachedMessage struct {
	text      string
	imageKeys []string
	videoKeys []attachedVideoKey
	fileKeys  []attachedFileKey
	// filePlaceholder is the "[unsupported file: name]" text a granted file
	// message falls back to when its download or a bound fails.
	filePlaceholder string
}

func parseAttachedMessage(message *larkim.Message, trigger bool, grants attachedCarryGrants) (parsedAttachedMessage, []AttachedContextIssueCode, bool) {
	parsed, issues, malformed := parseAttachedMessageContent(message, grants)
	if trigger {
		// The trigger's own words already ride the prompt; only its images
		// and videos (and the omission issues above) belong in the snapshot.
		parsed.text = ""
	}
	return parsed, issues, malformed
}

func parseAttachedMessageContent(message *larkim.Message, grants attachedCarryGrants) (parsedAttachedMessage, []AttachedContextIssueCode, bool) {
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
		text, images, videoKeys, omittedVideos, ok := parseAttachedPost(content, grants.video)
		if !ok {
			return parsedAttachedMessage{}, []AttachedContextIssueCode{AttachedContextIssueMalformedMessage}, true
		}
		issues := make([]AttachedContextIssueCode, omittedVideos)
		for index := range issues {
			issues[index] = AttachedContextIssueVideoOmitted
		}
		return parsedAttachedMessage{text: text, imageKeys: images, videoKeys: videoKeys}, issues, false
	case "image":
		var body struct {
			ImageKey string `json:"image_key"`
		}
		if err := json.Unmarshal([]byte(content), &body); err != nil || strings.TrimSpace(body.ImageKey) == "" {
			return parsedAttachedMessage{}, []AttachedContextIssueCode{AttachedContextIssueMalformedMessage}, true
		}
		return parsedAttachedMessage{imageKeys: []string{strings.TrimSpace(body.ImageKey)}}, nil, false
	case "media":
		if grants.video {
			var body struct {
				FileKey  string `json:"file_key"`
				FileName string `json:"file_name"`
				Duration uint64 `json:"duration"`
			}
			if err := json.Unmarshal([]byte(content), &body); err != nil || strings.TrimSpace(body.FileKey) == "" {
				return parsedAttachedMessage{}, []AttachedContextIssueCode{AttachedContextIssueMalformedMessage}, true
			}
			return parsedAttachedMessage{videoKeys: []attachedVideoKey{{
				fileKey:    strings.TrimSpace(body.FileKey),
				fileName:   strings.TrimSpace(body.FileName),
				durationMs: body.Duration,
			}}}, nil, false
		}
		return parsedAttachedMessage{text: placeholderVideo}, []AttachedContextIssueCode{AttachedContextIssueVideoOmitted}, false
	case "file":
		placeholder := filePlaceholder(content)
		if grants.files {
			var body struct {
				FileKey  string `json:"file_key"`
				FileName string `json:"file_name"`
			}
			if err := json.Unmarshal([]byte(content), &body); err != nil || strings.TrimSpace(body.FileKey) == "" {
				return parsedAttachedMessage{}, []AttachedContextIssueCode{AttachedContextIssueMalformedMessage}, true
			}
			return parsedAttachedMessage{
				fileKeys: []attachedFileKey{{
					fileKey:  strings.TrimSpace(body.FileKey),
					fileName: strings.TrimSpace(body.FileName),
				}},
				filePlaceholder: placeholder,
			}, nil, false
		}
		return parsedAttachedMessage{text: placeholder}, []AttachedContextIssueCode{AttachedContextIssueFileOmitted}, false
	case "audio":
		return parsedAttachedMessage{text: placeholderAudio}, []AttachedContextIssueCode{AttachedContextIssueUnsupportedMessage}, false
	case "sticker":
		return parsedAttachedMessage{text: placeholderSticker}, []AttachedContextIssueCode{AttachedContextIssueUnsupportedMessage}, false
	case "system":
		// Conversational chrome ("… joined the topic"), not content: a
		// placeholder row would only add noise between real messages.
		return parsedAttachedMessage{}, nil, false
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
	FileKey  string `json:"file_key"`
	FileName string `json:"file_name"`
	Duration uint64 `json:"duration"`
	UserID   string `json:"user_id"`
	UserName string `json:"user_name"`
}

type localizedPost struct {
	Title   string          `json:"title"`
	Content [][]postElement `json:"content"`
}

type attachedPostDocument map[string]localizedPost

// localizedAttachedPost extracts the one post body this daemon flattens.
//
// RECEIVED post content — message events and the history/list API alike — is
// the flat `{"title":…,"content":[[…]]}` shape; the `{"zh_cn":{…}}` locale
// wrapper exists only in the SEND format. The wrapped shape stays accepted
// for robustness, but the flat shape is what real inbound traffic carries.
func localizedAttachedPost(raw string) (localizedPost, bool) {
	var flat localizedPost
	if err := json.Unmarshal([]byte(raw), &flat); err == nil && postHasBody(flat) {
		return flat, true
	}
	var document attachedPostDocument
	if err := json.Unmarshal([]byte(raw), &document); err != nil || len(document) == 0 {
		return localizedPost{}, false
	}
	for _, candidate := range []string{"zh_cn", "en_us", "ja_jp"} {
		if localized, ok := document[candidate]; ok && postHasBody(localized) {
			return localized, true
		}
	}
	return localizedPost{}, false
}

func postHasBody(post localizedPost) bool {
	return strings.TrimSpace(post.Title) != "" || len(post.Content) > 0
}

func parseAttachedPost(raw string, allowVideo bool) (string, []string, []attachedVideoKey, int, bool) {
	localized, ok := localizedAttachedPost(raw)
	if !ok {
		return "", nil, nil, 0, false
	}
	lines := make([]string, 0)
	if title := strings.TrimSpace(localized.Title); title != "" {
		lines = append(lines, title)
	}
	images := make([]string, 0)
	videoKeys := make([]attachedVideoKey, 0)
	omittedVideos := 0
	for _, row := range localized.Content {
		parts := make([]string, 0, len(row))
		for _, element := range row {
			switch strings.TrimSpace(element.Tag) {
			case "img":
				if key := strings.TrimSpace(element.ImageKey); key != "" {
					images = append(images, key)
				}
			case "media":
				key := strings.TrimSpace(element.FileKey)
				if allowVideo && key != "" {
					// The bytes ride separately as a descriptor + chunks; the
					// inline token keeps the video's position in the prose so
					// the surrounding words still refer to it.
					videoKeys = append(videoKeys, attachedVideoKey{
						fileKey: key, fileName: strings.TrimSpace(element.FileName), durationMs: element.Duration,
					})
					parts = append(parts, placeholderVideoInline)
					continue
				}
				// No grant, or a malformed element with no file_key: the
				// video itself cannot cross this boundary.
				parts = append(parts, placeholderVideo)
				omittedVideos++
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
	return strings.Join(lines, "\n"), images, videoKeys, omittedVideos, true
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

// downloadVideo fetches, bounds, and type-detects one video the daemon was
// already told it may deliver (allowVideo gated the caller upstream). It
// never decodes the video — only magic-byte detection and the bounds table
// in ADR-0126 apply. GetMessageResource is called with Type("file"): the
// Feishu SDK covers files, audio, and video under that one resource type.
func (s *sdkAttachedContextLookup) downloadVideo(ctx context.Context, messageID string, key attachedVideoKey) (AttachedContextVideo, AttachedContextIssueCode) {
	if s.resources == nil || strings.TrimSpace(messageID) == "" || strings.TrimSpace(key.fileKey) == "" {
		return AttachedContextVideo{}, AttachedContextIssueVideoUnreadable
	}
	if key.durationMs > attachedContextMaxVideoDurationMs {
		return AttachedContextVideo{}, AttachedContextIssueVideoTooLong
	}
	req := larkim.NewGetMessageResourceReqBuilder().
		MessageId(messageID).
		FileKey(key.fileKey).
		Type("file").
		Build()
	resp, err := s.resources.Get(ctx, req)
	if err != nil || resp == nil || !resp.Success() || resp.File == nil {
		return AttachedContextVideo{}, AttachedContextIssueVideoUnreadable
	}
	data, err := io.ReadAll(io.LimitReader(resp.File, attachedContextMaxVideoBytes+1))
	if err != nil {
		return AttachedContextVideo{}, AttachedContextIssueVideoUnreadable
	}
	if len(data) > attachedContextMaxVideoBytes {
		return AttachedContextVideo{}, AttachedContextIssueVideoTooLarge
	}
	mediaType := detectedVideoMediaType(data)
	if mediaType == "" {
		return AttachedContextVideo{}, AttachedContextIssueVideoType
	}
	return AttachedContextVideo{
		MediaType:  mediaType,
		Data:       data,
		DurationMs: key.durationMs,
		FileName:   utf8Prefix(key.fileName, attachedContextMaxVideoNameBytes),
	}, ""
}

// detectedVideoMediaType allowlists a video strictly by magic bytes, never by
// the Feishu-declared file_name (ADR-0126 §1.3). ftyp brand codes are
// canonically 4 ASCII bytes, space-padded when shorter.
func detectedVideoMediaType(data []byte) string {
	if len(data) >= 4 && bytes.Equal(data[:4], []byte{0x1A, 0x45, 0xDF, 0xA3}) {
		return "video/webm"
	}
	if len(data) >= 12 && string(data[4:8]) == "ftyp" {
		switch string(data[8:12]) {
		case "qt  ":
			return "video/quicktime"
		case "isom", "iso2", "iso4", "iso5", "iso6", "mp41", "mp42",
			"avc1", "hvc1", "hev1", "M4V ", "M4A ", "dash":
			return "video/mp4"
		}
	}
	return ""
}

// downloadFile fetches and bounds one file the daemon was already told it may
// deliver (the allow_attached_files grant gated the caller upstream). The
// bytes are never parsed or executed; only the size bounds in ADR-0127 and a
// media-type label apply. Like video, the resource type is "file".
func (s *sdkAttachedContextLookup) downloadFile(ctx context.Context, messageID string, key attachedFileKey) (AttachedContextFile, AttachedContextIssueCode) {
	if s.resources == nil || strings.TrimSpace(messageID) == "" || strings.TrimSpace(key.fileKey) == "" {
		return AttachedContextFile{}, AttachedContextIssueFileUnreadable
	}
	req := larkim.NewGetMessageResourceReqBuilder().
		MessageId(messageID).
		FileKey(key.fileKey).
		Type("file").
		Build()
	resp, err := s.resources.Get(ctx, req)
	if err != nil || resp == nil || !resp.Success() || resp.File == nil {
		return AttachedContextFile{}, AttachedContextIssueFileUnreadable
	}
	data, err := io.ReadAll(io.LimitReader(resp.File, attachedContextMaxFileBytes+1))
	if err != nil {
		return AttachedContextFile{}, AttachedContextIssueFileUnreadable
	}
	if len(data) > attachedContextMaxFileBytes {
		return AttachedContextFile{}, AttachedContextIssueFileTooLarge
	}
	fileName := utf8Prefix(key.fileName, attachedContextMaxFileNameBytes)
	return AttachedContextFile{
		MediaType: detectedFileMediaType(data, fileName),
		Data:      data,
		FileName:  fileName,
	}, ""
}

// detectedFileMediaType labels a carried file. Byte sniffing wins when it is
// specific; its two generic answers (text/plain, application/octet-stream)
// defer to the declared name's extension. A text/plain sniff with no known
// extension stays text/plain; everything else falls back to
// application/octet-stream. Parameters such as "; charset=utf-8" are dropped.
// The label is advisory for the receiver and never selects a parser here.
func detectedFileMediaType(data []byte, fileName string) string {
	const octetStream = "application/octet-stream"
	sniffed := bareMediaType(http.DetectContentType(data))
	if sniffed != "" && sniffed != "text/plain" && sniffed != octetStream {
		return sniffed
	}
	if byExtension := bareMediaType(mime.TypeByExtension(fileNameExtension(fileName))); byExtension != "" {
		return byExtension
	}
	if sniffed == "text/plain" && len(data) > 0 {
		return sniffed
	}
	return octetStream
}

func bareMediaType(value string) string {
	if index := strings.IndexByte(value, ';'); index >= 0 {
		value = value[:index]
	}
	return strings.ToLower(strings.TrimSpace(value))
}

// fileNameExtension returns the final ".ext" of a declared file name, or ""
// when there is none. It is plain string work on user content: the name is
// never treated as a path.
func fileNameExtension(fileName string) string {
	index := strings.LastIndexByte(fileName, '.')
	if index < 0 || index == len(fileName)-1 {
		return ""
	}
	extension := fileName[index:]
	if strings.ContainsAny(extension, "/\\ \x00") {
		return ""
	}
	return extension
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
