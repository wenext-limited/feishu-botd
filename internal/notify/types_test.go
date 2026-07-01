package notify

import (
	"strings"
	"testing"
)

func TestValidateRejectsUnknownChannel(t *testing.T) {
	req := Request{Source: "xipe", SourceEventID: "evt", DedupeKey: "key", Severity: "info", Title: "title", Markdown: "body", Target: Target{Channel: "missing"}}
	if err := req.Validate(map[string]string{"ops": "oc"}); err == nil || err.Status != 404 {
		t.Fatalf("expected unknown channel, got %#v", err)
	}
}

func TestValidateRejectsInvalidSeverity(t *testing.T) {
	req := Request{Source: "xipe", SourceEventID: "evt", DedupeKey: "key", Severity: "bad", Title: "title", Markdown: "body", Target: Target{Channel: "ops"}}
	if err := req.Validate(map[string]string{"ops": "oc"}); err == nil || err.Code != "invalid_severity" {
		t.Fatalf("expected invalid severity, got %#v", err)
	}
}

func TestValidateAcceptsBoundedLinksAndMetadata(t *testing.T) {
	req := Request{
		Source:        "xipe",
		SourceEventID: "evt",
		DedupeKey:     "key",
		Severity:      "info",
		Title:         "title",
		Markdown:      "body",
		Target:        Target{Channel: "ops"},
		Links:         []Link{{Label: "Open Xipe", URL: "https://xipe.example.com/accounts"}},
		Metadata:      map[string]string{"trigger": "reauth_required"},
	}
	if err := req.Validate(map[string]string{"ops": "oc"}); err != nil {
		t.Fatalf("valid request rejected: %#v", err)
	}
}

func TestValidateRejectsOversizedReplyToMessageID(t *testing.T) {
	req := Request{
		Source:           "xipe",
		SourceEventID:    "evt",
		DedupeKey:        "key",
		Severity:         "info",
		Title:            "title",
		Markdown:         "body",
		Target:           Target{Channel: "ops"},
		ReplyToMessageID: strings.Repeat("a", 161),
	}
	if err := req.Validate(map[string]string{"ops": "oc"}); err == nil || err.Code != "field_too_large" {
		t.Fatalf("expected field_too_large, got %#v", err)
	}
}

func TestValidateRejectsInvalidLinkURL(t *testing.T) {
	req := Request{
		Source:        "xipe",
		SourceEventID: "evt",
		DedupeKey:     "key",
		Severity:      "info",
		Title:         "title",
		Markdown:      "body",
		Target:        Target{Channel: "ops"},
		Links:         []Link{{Label: "bad", URL: "file:///tmp/secret"}},
	}
	if err := req.Validate(map[string]string{"ops": "oc"}); err == nil || err.Code != "invalid_link_url" {
		t.Fatalf("expected invalid link url, got %#v", err)
	}
}
