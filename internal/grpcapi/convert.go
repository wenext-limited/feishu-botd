package grpcapi

import (
	pb "feishu-botd/gen/feishubotd/v1"
	"feishu-botd/internal/notify"
)

// severityToString maps the proto Severity enum onto the string vocabulary the
// core service validates. SEVERITY_UNSPECIFIED maps to "" so the existing
// strict validation rejects it (fail-closed).
func severityToString(s pb.Severity) string {
	switch s {
	case pb.Severity_SEVERITY_INFO:
		return "info"
	case pb.Severity_SEVERITY_WARNING:
		return "warning"
	case pb.Severity_SEVERITY_CRITICAL:
		return "critical"
	default:
		return ""
	}
}

// severityFromString is the inverse of severityToString. Unknown values map to
// SEVERITY_UNSPECIFIED. It exists for symmetry and round-trip testing.
func severityFromString(s string) pb.Severity {
	switch s {
	case "info":
		return pb.Severity_SEVERITY_INFO
	case "warning":
		return pb.Severity_SEVERITY_WARNING
	case "critical":
		return pb.Severity_SEVERITY_CRITICAL
	default:
		return pb.Severity_SEVERITY_UNSPECIFIED
	}
}

// targetChannel extracts the channel alias from a MessageTarget. A nil target
// or a non-channel variant yields "", which validation then rejects.
func targetChannel(t *pb.MessageTarget) string {
	if t == nil {
		return ""
	}
	return t.GetChannel()
}

// notifyRequestFromProto converts the proto SendNotification request into the
// shared notify.Request domain type. Validation happens later in the service,
// so this is a pure field copy.
func notifyRequestFromProto(in *pb.SendNotificationRequest) notify.Request {
	req := notify.Request{
		Source:        in.GetSource(),
		SourceEventID: in.GetSourceEventId(),
		DedupeKey:     in.GetDedupeKey(),
		Severity:      severityToString(in.GetSeverity()),
		Title:         in.GetTitle(),
		Markdown:      in.GetMarkdown(),
		Target:        notify.Target{Channel: targetChannel(in.GetTarget())},
		Metadata:      in.GetMetadata(),
	}
	for _, link := range in.GetLinks() {
		req.Links = append(req.Links, notify.Link{Label: link.GetLabel(), URL: link.GetUrl()})
	}
	return req
}

// notifyResponseToProto converts the shared notify.Response into the proto
// SendNotification response.
func notifyResponseToProto(resp notify.Response) *pb.SendNotificationResponse {
	return &pb.SendNotificationResponse{
		Status:    resp.Status,
		Provider:  resp.Provider,
		DedupeKey: resp.DedupeKey,
		MessageId: resp.MessageID,
		Duplicate: resp.Duplicate,
	}
}
