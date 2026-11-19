package feishu

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	larkcore "github.com/larksuite/oapi-sdk-go/v3/core"
	larkim "github.com/larksuite/oapi-sdk-go/v3/service/im/v1"
)

// countingReader is an endless body that records how much was read from it.
type countingReader struct{ read int }

func (c *countingReader) Read(p []byte) (int, error) {
	for index := range p {
		p[index] = 0x42
	}
	c.read += len(p)
	return len(p), nil
}

type readerResourceAPI struct{ reader *countingReader }

func (r readerResourceAPI) Get(context.Context, *larkim.GetMessageResourceReq, ...larkcore.RequestOptionFunc) (*larkim.GetMessageResourceResp, error) {
	return &larkim.GetMessageResourceResp{File: r.reader}, nil
}

func fileMessageContent(key, name string) string {
	return `{"file_key":"` + key + `","file_name":"` + name + `"}`
}

func lookupFileFixture(t *testing.T, resources *orderedFakeMessageResourceAPI, allowFiles bool, messages ...*larkim.Message) AttachedContext {
	t.Helper()
	items := append([]*larkim.Message{
		threadMessage("om_guide", "text", `{"text":"@Nous 看看这个"}`, "9000", "ou_guide", "user"),
	}, messages...)
	history := &fakeThreadMessageAPI{responses: []*larkim.ListMessageResp{listMessageResponse(false, "", items...)}}
	lookup := newSDKAttachedContextLookup(history, resources)
	got, err := lookup.LookupAttachedContext(context.Background(), AttachedContextRequest{
		ThreadID: "omt_thread", TriggerMessageID: "om_guide", TriggerCreateTime: "9000", AllowFiles: allowFiles,
	})
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	return got
}

func TestAttachedContextLookupWithoutFileGrantKeepsPlaceholderAndReportsFileOmitted(t *testing.T) {
	resources := &orderedFakeMessageResourceAPI{data: [][]byte{[]byte("%PDF-1.7")}}
	// The video grant must not imply the file grant.
	history := &fakeThreadMessageAPI{responses: []*larkim.ListMessageResp{listMessageResponse(false, "",
		threadMessage("om_guide", "text", `{"text":"@Nous 看看这个"}`, "2000", "ou_guide", "user"),
		threadMessage("om_file", "file", fileMessageContent("file_doc", "crash报告.pdf"), "1000", "ou_one", "user"),
	)}}
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
	if got.Messages[0].Text != "[unsupported file: crash报告.pdf]" || len(got.Messages[0].Files) != 0 {
		t.Fatalf("ungranted message = %#v", got.Messages[0])
	}
	if !hasAttachedContextIssue(got.Issues, AttachedContextIssueFileOmitted) {
		t.Fatalf("issues = %#v, want file_omitted", got.Issues)
	}
	if hasAttachedContextIssue(got.Issues, AttachedContextIssueUnsupportedMessage) {
		t.Fatalf("a file message must report file_omitted, not unsupported_message: %#v", got.Issues)
	}
	if resources.calls != 0 {
		t.Fatalf("resource downloads = %d, want 0: an ungranted file must never be fetched", resources.calls)
	}
}

func TestAttachedContextLookupCollectsGrantedFile(t *testing.T) {
	pdf := append([]byte("%PDF-1.7\n"), bytes.Repeat([]byte{0xAB}, 200*1024)...)
	resources := &orderedFakeMessageResourceAPI{data: [][]byte{pdf}}
	got := lookupFileFixture(t, resources, true,
		threadMessage("om_file", "file", fileMessageContent("file_doc", "  crash报告.pdf "), "1000", "ou_one", "user"),
	)
	if got.Status != AttachedContextFound || len(got.Messages) != 1 {
		t.Fatalf("context = %#v", got)
	}
	message := got.Messages[0]
	if message.Text != "" {
		t.Fatalf("granted file message kept a placeholder: %q", message.Text)
	}
	if len(message.Files) != 1 {
		t.Fatalf("files = %#v, want one collected file", message.Files)
	}
	file := message.Files[0]
	if file.MediaType != "application/pdf" || !bytes.Equal(file.Data, pdf) || file.FileName != "crash报告.pdf" {
		t.Fatalf("file = {%q %d bytes %q}", file.MediaType, len(file.Data), file.FileName)
	}
	if len(got.Issues) != 0 || got.Truncated {
		t.Fatalf("issues = %#v truncated = %t, want none", got.Issues, got.Truncated)
	}
	if resources.calls != 1 {
		t.Fatalf("resource downloads = %d, want 1", resources.calls)
	}
}

func TestAttachedContextLookupDownloadsTriggerFileWithoutAThread(t *testing.T) {
	data := []byte("2026-11-19 ERROR something broke\n")
	history := &fakeThreadMessageAPI{responses: []*larkim.ListMessageResp{listMessageResponse(false, "",
		threadMessage("om_guide", "file", fileMessageContent("file_log", "app.log"), "2000", "ou_guide", "user"),
	)}}
	resources := &orderedFakeMessageResourceAPI{data: [][]byte{data}}
	lookup := newSDKAttachedContextLookup(history, resources)
	got, err := lookup.LookupAttachedContext(context.Background(), AttachedContextRequest{
		TriggerMessageID: "om_guide", AllowFiles: true,
	})
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if got.Status != AttachedContextFound || len(got.Messages) != 1 || len(got.Messages[0].Files) != 1 {
		t.Fatalf("context = %#v", got)
	}
	if got.Messages[0].Files[0].MediaType != "text/plain" {
		t.Fatalf("media type = %q, want text/plain", got.Messages[0].Files[0].MediaType)
	}
}

func TestAttachedContextLookupFileFailuresKeepPlaceholder(t *testing.T) {
	oversized := bytes.Repeat([]byte{0}, attachedContextMaxFileBytes+1)
	tests := []struct {
		name          string
		resources     *orderedFakeMessageResourceAPI
		wantIssue     AttachedContextIssueCode
		wantTruncated bool
	}{
		{
			name:      "too large",
			resources: &orderedFakeMessageResourceAPI{data: [][]byte{oversized}},
			wantIssue: AttachedContextIssueFileTooLarge, wantTruncated: true,
		},
		{
			name:      "unreadable download",
			resources: &orderedFakeMessageResourceAPI{errs: []error{errors.New("download failed")}},
			wantIssue: AttachedContextIssueFileUnreadable,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := lookupFileFixture(t, test.resources, true,
				threadMessage("om_file", "file", fileMessageContent("file_doc", "dump.zip"), "1000", "ou_one", "user"),
			)
			if got.Status != AttachedContextFound {
				t.Fatalf("status = %v, want found: a failing file must never fail the snapshot", got.Status)
			}
			if len(got.Messages) != 1 || got.Messages[0].Text != "[unsupported file: dump.zip]" || len(got.Messages[0].Files) != 0 {
				t.Fatalf("messages = %#v, want one placeholder row and no file", got.Messages)
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

// The per-file bound stops reading at limit+1 bytes; an endless body must not
// be buffered.
func TestDownloadFileStopsReadingAtTheBound(t *testing.T) {
	counter := &countingReader{}
	lookup := newSDKAttachedContextLookup(nil, readerResourceAPI{reader: counter})
	_, issue := lookup.downloadFile(context.Background(), "om_file", attachedFileKey{fileKey: "file_doc"})
	if issue != AttachedContextIssueFileTooLarge {
		t.Fatalf("issue = %q, want file_too_large", issue)
	}
	if counter.read > attachedContextMaxFileBytes+1+32*1024 {
		t.Fatalf("read %d bytes from an endless body; the bound is %d", counter.read, attachedContextMaxFileBytes)
	}
}

func TestAttachedContextLookupEnforcesPerSnapshotFileLimit(t *testing.T) {
	small := []byte("%PDF-1.4 small")
	data := make([][]byte, attachedContextMaxFiles)
	for index := range data {
		data[index] = small
	}
	messages := make([]*larkim.Message, 0, attachedContextMaxFiles+1)
	for index := 0; index <= attachedContextMaxFiles; index++ {
		id := string(rune('a' + index))
		messages = append(messages, threadMessage("om_file_"+id, "file", fileMessageContent("file_"+id, id+".pdf"), "10"+string(rune('0'+index)), "ou_one", "user"))
	}
	resources := &orderedFakeMessageResourceAPI{data: data}
	got := lookupFileFixture(t, resources, true, messages...)

	collected, placeholders := 0, 0
	for _, message := range got.Messages {
		collected += len(message.Files)
		if strings.HasPrefix(message.Text, "[unsupported file: ") {
			placeholders++
		}
	}
	if collected != attachedContextMaxFiles || placeholders != 1 {
		t.Fatalf("collected = %d placeholders = %d, want %d and 1", collected, placeholders, attachedContextMaxFiles)
	}
	if !hasAttachedContextIssue(got.Issues, AttachedContextIssueFileLimit) || !got.Truncated {
		t.Fatalf("issues = %#v truncated = %t, want file_limit", got.Issues, got.Truncated)
	}
	if resources.calls != attachedContextMaxFiles {
		t.Fatalf("downloads = %d, want %d: a file over the count limit is never fetched", resources.calls, attachedContextMaxFiles)
	}
}

func TestAttachedContextLookupEnforcesTotalFileByteCap(t *testing.T) {
	// Three files each under the 5 MiB per-file cap; the third crosses 10 MiB.
	part := bytes.Repeat([]byte{0x01}, 4*1024*1024)
	resources := &orderedFakeMessageResourceAPI{data: [][]byte{part, part, part}}
	got := lookupFileFixture(t, resources, true,
		threadMessage("om_file1", "file", fileMessageContent("file_one", "a.bin"), "3000", "ou_one", "user"),
		threadMessage("om_file2", "file", fileMessageContent("file_two", "b.bin"), "2000", "ou_one", "user"),
		threadMessage("om_file3", "file", fileMessageContent("file_three", "c.bin"), "1000", "ou_one", "user"),
	)
	collected := 0
	for _, message := range got.Messages {
		collected += len(message.Files)
	}
	if collected != 2 {
		t.Fatalf("collected files = %d, want 2 (the third blew the aggregate cap)", collected)
	}
	if !hasAttachedContextIssue(got.Issues, AttachedContextIssueTotalFileLimit) || !got.Truncated {
		t.Fatalf("issues = %#v truncated = %t, want total_file_limit", got.Issues, got.Truncated)
	}
}

func TestAttachedContextLookupBoundsFileNameAtUTF8Boundary(t *testing.T) {
	longName := strings.Repeat("界", 400)
	resources := &orderedFakeMessageResourceAPI{data: [][]byte{[]byte("x")}}
	got := lookupFileFixture(t, resources, true,
		threadMessage("om_file", "file", fileMessageContent("file_doc", longName), "1000", "ou_one", "user"),
	)
	if len(got.Messages) != 1 || len(got.Messages[0].Files) != 1 {
		t.Fatalf("context = %#v", got)
	}
	name := got.Messages[0].Files[0].FileName
	if len(name) > attachedContextMaxFileNameBytes || !strings.HasPrefix(longName, name) || len(name) < attachedContextMaxFileNameBytes-3 {
		t.Fatalf("bounded name = %d bytes, want a UTF-8 prefix <= %d", len(name), attachedContextMaxFileNameBytes)
	}
}

func TestDetectedFileMediaType(t *testing.T) {
	tests := []struct {
		name     string
		data     []byte
		fileName string
		want     string
	}{
		{name: "pdf by bytes", data: []byte("%PDF-1.7 body"), fileName: "notes.txt", want: "application/pdf"},
		{name: "zip by bytes", data: []byte("PK\x03\x04rest"), fileName: "", want: "application/zip"},
		{name: "png by bytes", data: []byte("\x89PNG\r\n\x1a\nrest"), fileName: "x.bin", want: "image/png"},
		{name: "text with json extension", data: []byte(`{"a":1}`), fileName: "dump.json", want: "application/json"},
		{name: "text with unknown extension stays text", data: []byte("plain log line\n"), fileName: "app.zzlog", want: "text/plain"},
		{name: "binary with no name", data: []byte{0x00, 0x01, 0x02, 0xFF}, fileName: "", want: "application/octet-stream"},
		{name: "binary with unknown extension", data: []byte{0x00, 0x01, 0x02, 0xFF}, fileName: "core.zzdump", want: "application/octet-stream"},
		{name: "empty file", data: nil, fileName: "", want: "application/octet-stream"},
		{name: "path-like name is not a path", data: []byte{0x00, 0x01}, fileName: "a.pdf/../b", want: "application/octet-stream"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := detectedFileMediaType(test.data, test.fileName)
			if got != test.want {
				t.Fatalf("detectedFileMediaType = %q, want %q", got, test.want)
			}
			if strings.Contains(got, ";") {
				t.Fatalf("media type kept parameters: %q", got)
			}
		})
	}
}
