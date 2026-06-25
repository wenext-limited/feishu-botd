package grpcapi

import (
	"testing"

	pb "feishu-botd/gen/feishubotd/v1"
)

func TestSeverityRoundTrip(t *testing.T) {
	for _, s := range []string{"info", "warning", "critical"} {
		if got := severityToString(severityFromString(s)); got != s {
			t.Fatalf("round trip %q -> %q", s, got)
		}
	}
	if severityToString(pb.Severity_SEVERITY_UNSPECIFIED) != "" {
		t.Fatal("unspecified severity must map to empty string (fail-closed)")
	}
	if severityFromString("bogus") != pb.Severity_SEVERITY_UNSPECIFIED {
		t.Fatal("unknown severity must map to unspecified")
	}
}

func TestTargetChannel(t *testing.T) {
	if targetChannel(nil) != "" {
		t.Fatal("nil target must yield empty channel")
	}
	target := &pb.MessageTarget{To: &pb.MessageTarget_Channel{Channel: "ops"}}
	if targetChannel(target) != "ops" {
		t.Fatalf("channel = %q", targetChannel(target))
	}
}

func TestNotifyRequestFromProto(t *testing.T) {
	in := &pb.SendNotificationRequest{
		Source:        "xipe",
		SourceEventId: "evt_1",
		DedupeKey:     "k1",
		Severity:      pb.Severity_SEVERITY_CRITICAL,
		Title:         "Title",
		Markdown:      "**body**",
		Target:        &pb.MessageTarget{To: &pb.MessageTarget_Channel{Channel: "ops"}},
		Links:         []*pb.Link{{Label: "run", Url: "https://example.com"}},
		Metadata:      map[string]string{"k": "v"},
	}
	req := notifyRequestFromProto(in)
	if req.Source != "xipe" || req.SourceEventID != "evt_1" || req.DedupeKey != "k1" {
		t.Fatalf("identity fields = %#v", req)
	}
	if req.Severity != "critical" || req.Title != "Title" || req.Markdown != "**body**" {
		t.Fatalf("content fields = %#v", req)
	}
	if req.Target.Channel != "ops" {
		t.Fatalf("channel = %q", req.Target.Channel)
	}
	if len(req.Links) != 1 || req.Links[0].Label != "run" || req.Links[0].URL != "https://example.com" {
		t.Fatalf("links = %#v", req.Links)
	}
	if req.Metadata["k"] != "v" {
		t.Fatalf("metadata = %#v", req.Metadata)
	}
}
