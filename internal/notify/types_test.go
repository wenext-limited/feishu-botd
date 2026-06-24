package notify

import "testing"

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
