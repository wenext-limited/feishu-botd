package feishu

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	larkim "github.com/larksuite/oapi-sdk-go/v3/service/im/v1"
)

// mp4Fixture and friends are minimal magic-byte fixtures: real decoders would
// reject them, but detectedVideoMediaType (and this daemon) never decodes —
// it only inspects the header bytes ADR-0126 §1.3 allowlists.
func mp4Fixture(brand string, extra int) []byte {
	body := append([]byte{0x00, 0x00, 0x00, 0x18}, []byte("ftyp")...)
	body = append(body, []byte(brand)...)
	body = append(body, bytes.Repeat([]byte{0}, extra)...)
	return body
}

func webmFixture(extra int) []byte {
	body := []byte{0x1A, 0x45, 0xDF, 0xA3}
	return append(body, bytes.Repeat([]byte{0}, extra)...)
}

func TestDetectedVideoMediaType(t *testing.T) {
	tests := []struct {
		name string
		data []byte
		want string
	}{
		{name: "mp4 isom brand", data: mp4Fixture("isom", 16), want: "video/mp4"},
		{name: "mp4 mp42 brand", data: mp4Fixture("mp42", 16), want: "video/mp4"},
		{name: "mp4 avc1 brand", data: mp4Fixture("avc1", 16), want: "video/mp4"},
		{name: "mp4 hvc1 brand (HEVC)", data: mp4Fixture("hvc1", 16), want: "video/mp4"},
		{name: "mp4 dash brand", data: mp4Fixture("dash", 16), want: "video/mp4"},
		{name: "quicktime brand", data: mp4Fixture("qt  ", 16), want: "video/quicktime"},
		{name: "webm EBML header", data: webmFixture(16), want: "video/webm"},
		{name: "unrelated ftyp brand", data: mp4Fixture("zzzz", 16), want: ""},
		{name: "plain text", data: []byte("not a video, just text padding out"), want: ""},
		{name: "too short for ftyp probe", data: []byte{0, 0, 0}, want: ""},
		{name: "empty", data: nil, want: ""},
		{name: "file name must never influence detection", data: []byte("MZ\x00\x00fake.mp4 header bytes"), want: ""},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := detectedVideoMediaType(test.data); got != test.want {
				t.Fatalf("detectedVideoMediaType(%q) = %q, want %q", test.name, got, test.want)
			}
		})
	}
}

func TestAttachedContextLookupCollectsGrantedVideoFromMediaMessage(t *testing.T) {
	video := mp4Fixture("isom", 4096)
	history := &fakeThreadMessageAPI{responses: []*larkim.ListMessageResp{listMessageResponse(false, "",
		threadMessage("om_guide", "text", `{"text":"@Nous 看看这个"}`, "2000", "ou_guide", "user"),
		threadMessage("om_video", "media", `{"file_key":"file_video","file_name":"repro.mp4","duration":15000}`, "1000", "ou_one", "user"),
	)}}
	resources := &orderedFakeMessageResourceAPI{data: [][]byte{video}}
	lookup := newSDKAttachedContextLookup(history, resources)

	got, err := lookup.LookupAttachedContext(context.Background(), AttachedContextRequest{
		ThreadID: "omt_thread", TriggerMessageID: "om_guide", TriggerCreateTime: "2000", AllowVideo: true,
	})
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if got.Status != AttachedContextFound || len(got.Messages) != 1 {
		t.Fatalf("context = %#v", got)
	}
	message := got.Messages[0]
	if message.Text != "" {
		t.Fatalf("granted video message kept a placeholder: %#v", message)
	}
	if len(message.Videos) != 1 {
		t.Fatalf("videos = %#v, want one collected video", message.Videos)
	}
	video0 := message.Videos[0]
	if video0.MediaType != "video/mp4" || !bytes.Equal(video0.Data, video) ||
		video0.DurationMs != 15000 || video0.FileName != "repro.mp4" {
		t.Fatalf("video = %#v", video0)
	}
	if hasAttachedContextIssue(got.Issues, AttachedContextIssueVideoOmitted) {
		t.Fatalf("a granted, delivered video must not report video_omitted: %#v", got.Issues)
	}
}

func TestAttachedContextLookupWithoutGrantKeepsPlaceholderAndOmitsDownload(t *testing.T) {
	history := &fakeThreadMessageAPI{responses: []*larkim.ListMessageResp{listMessageResponse(false, "",
		threadMessage("om_guide", "text", `{"text":"@Nous 看看这个"}`, "2000", "ou_guide", "user"),
		threadMessage("om_video", "media", `{"file_key":"file_video","duration":15000}`, "1000", "ou_one", "user"),
	)}}
	resources := &orderedFakeMessageResourceAPI{}
	lookup := newSDKAttachedContextLookup(history, resources)

	// AllowVideo left at its zero value (false) — the grant defaults to deny.
	got, err := lookup.LookupAttachedContext(context.Background(), AttachedContextRequest{
		ThreadID: "omt_thread", TriggerMessageID: "om_guide", TriggerCreateTime: "2000",
	})
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if got.Status != AttachedContextFound || len(got.Messages) != 1 {
		t.Fatalf("context = %#v", got)
	}
	if got.Messages[0].Text != placeholderVideo || len(got.Messages[0].Videos) != 0 {
		t.Fatalf("ungranted message = %#v", got.Messages[0])
	}
	if !hasAttachedContextIssue(got.Issues, AttachedContextIssueVideoOmitted) {
		t.Fatalf("issues = %#v, want video_omitted", got.Issues)
	}
	if resources.calls != 0 {
		t.Fatalf("resource downloads = %d, want 0: an ungranted video must never be fetched", resources.calls)
	}
}

func TestAttachedContextLookupPostVideoKeepsInlineTokenAndCollectsBytes(t *testing.T) {
	video := mp4Fixture("mp42", 4096)
	history := &fakeThreadMessageAPI{responses: []*larkim.ListMessageResp{listMessageResponse(false, "",
		threadMessage("om_guide", "text", `{"text":"@Nous 看看这个"}`, "2000", "ou_guide", "user"),
		threadMessage("om_report", "post", `{"title":"","content":[[{"tag":"text","text":"复现视频："},{"tag":"media","file_key":"video_repro","file_name":"repro.mp4","duration":8000},{"tag":"text","text":"，登录后必现"}]]}`, "1000", "ou_two", "user"),
	)}}
	resources := &orderedFakeMessageResourceAPI{data: [][]byte{video}}
	lookup := newSDKAttachedContextLookup(history, resources)

	got, err := lookup.LookupAttachedContext(context.Background(), AttachedContextRequest{
		ThreadID: "omt_thread", TriggerMessageID: "om_guide", TriggerCreateTime: "2000", AllowVideo: true,
	})
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if got.Status != AttachedContextFound || len(got.Messages) != 1 {
		t.Fatalf("context = %#v", got)
	}
	message := got.Messages[0]
	wantText := "复现视频：[video]，登录后必现"
	if message.Text != wantText {
		t.Fatalf("text = %q, want %q", message.Text, wantText)
	}
	if len(message.Videos) != 1 || message.Videos[0].MediaType != "video/mp4" {
		t.Fatalf("videos = %#v", message.Videos)
	}
	if hasAttachedContextIssue(got.Issues, AttachedContextIssueVideoOmitted) {
		t.Fatalf("a granted post video must not report video_omitted: %#v", got.Issues)
	}
}

func TestAttachedContextLookupDownloadsTriggerVideoWithoutAThread(t *testing.T) {
	video := mp4Fixture("isom", 4096)
	history := &fakeThreadMessageAPI{responses: []*larkim.ListMessageResp{listMessageResponse(false, "",
		threadMessage("om_guide", "media", `{"file_key":"file_video","file_name":"clip.mp4","duration":9000}`, "2000", "ou_guide", "user"),
	)}}
	resources := &orderedFakeMessageResourceAPI{data: [][]byte{video}}
	lookup := newSDKAttachedContextLookup(history, resources)

	got, err := lookup.LookupAttachedContext(context.Background(), AttachedContextRequest{
		TriggerMessageID: "om_guide", AllowVideo: true,
	})
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if got.Status != AttachedContextFound || len(got.Messages) != 1 {
		t.Fatalf("context = %#v", got)
	}
	if len(got.Messages[0].Videos) != 1 {
		t.Fatalf("trigger-only video = %#v, want one collected video", got.Messages[0])
	}
	if history.calls != 0 {
		t.Fatalf("thread list was called %d times; a group message has no thread to list", history.calls)
	}
}

func TestAttachedContextLookupVideoBoundsProduceIssuesAndKeepPlaceholder(t *testing.T) {
	oversizedVideo := bytes.Repeat([]byte{0}, attachedContextMaxVideoBytes+1)
	copy(oversizedVideo, mp4Fixture("isom", 0))
	unreadableType := bytes.Repeat([]byte("not a video"), 8)

	tests := []struct {
		name          string
		content       string
		resources     *orderedFakeMessageResourceAPI
		wantIssue     AttachedContextIssueCode
		wantTruncated bool
	}{
		{
			name:      "too large",
			content:   `{"file_key":"file_video","duration":1000}`,
			resources: &orderedFakeMessageResourceAPI{data: [][]byte{oversizedVideo}},
			wantIssue: AttachedContextIssueVideoTooLarge, wantTruncated: true,
		},
		{
			name:      "too long by declared duration",
			content:   `{"file_key":"file_video","duration":600001}`,
			resources: &orderedFakeMessageResourceAPI{},
			wantIssue: AttachedContextIssueVideoTooLong, wantTruncated: true,
		},
		{
			name:      "unreadable download",
			content:   `{"file_key":"file_video","duration":1000}`,
			resources: &orderedFakeMessageResourceAPI{errs: []error{errors.New("download failed")}},
			wantIssue: AttachedContextIssueVideoUnreadable,
		},
		{
			name:      "unsupported type by magic bytes",
			content:   `{"file_key":"file_video","duration":1000}`,
			resources: &orderedFakeMessageResourceAPI{data: [][]byte{unreadableType}},
			wantIssue: AttachedContextIssueVideoType,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			history := &fakeThreadMessageAPI{responses: []*larkim.ListMessageResp{listMessageResponse(false, "",
				threadMessage("om_guide", "text", `{"text":"guide"}`, "2000", "ou_guide", "user"),
				threadMessage("om_video", "media", test.content, "1000", "ou_one", "user"),
			)}}
			lookup := newSDKAttachedContextLookup(history, test.resources)

			got, err := lookup.LookupAttachedContext(context.Background(), AttachedContextRequest{
				ThreadID: "omt_thread", TriggerMessageID: "om_guide", TriggerCreateTime: "2000", AllowVideo: true,
			})
			if err != nil {
				t.Fatalf("lookup: %v", err)
			}
			if got.Status != AttachedContextFound {
				t.Fatalf("status = %v, want found: a bound-failing video must never fail the snapshot", got.Status)
			}
			if len(got.Messages) != 1 || got.Messages[0].Text != placeholderVideo || len(got.Messages[0].Videos) != 0 {
				t.Fatalf("messages = %#v, want one placeholder row and no video", got.Messages)
			}
			if !hasAttachedContextIssue(got.Issues, test.wantIssue) {
				t.Fatalf("issues = %#v, want %q", got.Issues, test.wantIssue)
			}
			if got.Truncated != test.wantTruncated {
				t.Fatalf("truncated = %t, want %t", got.Truncated, test.wantTruncated)
			}
		})
	}
}

func TestAttachedContextLookupEnforcesPerSnapshotVideoLimit(t *testing.T) {
	video := mp4Fixture("isom", 512)
	history := &fakeThreadMessageAPI{responses: []*larkim.ListMessageResp{listMessageResponse(false, "",
		threadMessage("om_guide", "text", `{"text":"guide"}`, "4000", "ou_guide", "user"),
		threadMessage("om_video1", "media", `{"file_key":"file_one","duration":1000}`, "3000", "ou_one", "user"),
		threadMessage("om_video2", "media", `{"file_key":"file_two","duration":1000}`, "2000", "ou_two", "user"),
		threadMessage("om_video3", "media", `{"file_key":"file_three","duration":1000}`, "1000", "ou_three", "user"),
	)}}
	resources := &orderedFakeMessageResourceAPI{data: [][]byte{video, video}}
	lookup := newSDKAttachedContextLookup(history, resources)

	got, err := lookup.LookupAttachedContext(context.Background(), AttachedContextRequest{
		ThreadID: "omt_thread", TriggerMessageID: "om_guide", TriggerCreateTime: "4000", AllowVideo: true,
	})
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if got.Status != AttachedContextFound || len(got.Messages) != 3 {
		t.Fatalf("context = %#v", got)
	}
	collected := 0
	placeholders := 0
	for _, message := range got.Messages {
		collected += len(message.Videos)
		if message.Text == placeholderVideo {
			placeholders++
		}
	}
	if collected != attachedContextMaxVideos {
		t.Fatalf("collected videos = %d, want %d", collected, attachedContextMaxVideos)
	}
	if placeholders != 1 {
		t.Fatalf("placeholder rows = %d, want 1 (the video over the per-snapshot limit)", placeholders)
	}
	if !hasAttachedContextIssue(got.Issues, AttachedContextIssueVideoLimit) {
		t.Fatalf("issues = %#v, want video_limit", got.Issues)
	}
	if !got.Truncated {
		t.Fatalf("truncated = false, want true")
	}
}

func TestAttachedContextLookupEnforcesTotalVideoByteCap(t *testing.T) {
	// Two videos individually under the per-video cap but together over the
	// 96 MiB aggregate cap.
	big := bytes.Repeat([]byte{0}, 50*1024*1024)
	copy(big, mp4Fixture("isom", 0))
	history := &fakeThreadMessageAPI{responses: []*larkim.ListMessageResp{listMessageResponse(false, "",
		threadMessage("om_guide", "text", `{"text":"guide"}`, "3000", "ou_guide", "user"),
		threadMessage("om_video1", "media", `{"file_key":"file_one","duration":1000}`, "2000", "ou_one", "user"),
		threadMessage("om_video2", "media", `{"file_key":"file_two","duration":1000}`, "1000", "ou_two", "user"),
	)}}
	resources := &orderedFakeMessageResourceAPI{data: [][]byte{big, big}}
	lookup := newSDKAttachedContextLookup(history, resources)

	got, err := lookup.LookupAttachedContext(context.Background(), AttachedContextRequest{
		ThreadID: "omt_thread", TriggerMessageID: "om_guide", TriggerCreateTime: "3000", AllowVideo: true,
	})
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if got.Status != AttachedContextFound || !got.Truncated {
		t.Fatalf("context = %#v", got)
	}
	if !hasAttachedContextIssue(got.Issues, AttachedContextIssueVideoLimit) {
		t.Fatalf("issues = %#v, want video_limit for the aggregate cap", got.Issues)
	}
	collected := 0
	for _, message := range got.Messages {
		collected += len(message.Videos)
	}
	if collected != 1 {
		t.Fatalf("collected videos = %d, want 1 (the second blew the aggregate cap)", collected)
	}
}

func TestAttachedContextLookupTruncatesVideoFileNameWithoutAnIssue(t *testing.T) {
	video := mp4Fixture("isom", 512)
	longName := strings.Repeat("界", 400) // far more than attachedContextMaxVideoNameBytes in UTF-8 bytes
	history := &fakeThreadMessageAPI{responses: []*larkim.ListMessageResp{listMessageResponse(false, "",
		threadMessage("om_guide", "text", `{"text":"guide"}`, "2000", "ou_guide", "user"),
		threadMessage("om_video", "media", `{"file_key":"file_video","file_name":"`+longName+`","duration":1000}`, "1000", "ou_one", "user"),
	)}}
	resources := &orderedFakeMessageResourceAPI{data: [][]byte{video}}
	lookup := newSDKAttachedContextLookup(history, resources)

	got, err := lookup.LookupAttachedContext(context.Background(), AttachedContextRequest{
		ThreadID: "omt_thread", TriggerMessageID: "om_guide", TriggerCreateTime: "2000", AllowVideo: true,
	})
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if got.Status != AttachedContextFound || len(got.Messages) != 1 || len(got.Messages[0].Videos) != 1 {
		t.Fatalf("context = %#v", got)
	}
	fileName := got.Messages[0].Videos[0].FileName
	if len(fileName) > attachedContextMaxVideoNameBytes {
		t.Fatalf("file name length = %d, want <= %d", len(fileName), attachedContextMaxVideoNameBytes)
	}
	if !strings.HasPrefix(longName, fileName) {
		t.Fatalf("truncated name is not a prefix of the original: %q", fileName)
	}
	for _, issue := range got.Issues {
		if strings.Contains(string(issue.Code), "video") && issue.Code != "" {
			t.Fatalf("file-name truncation must not raise an issue, got %#v", got.Issues)
		}
	}
}
