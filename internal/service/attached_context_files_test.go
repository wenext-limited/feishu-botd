package service

import (
	"context"
	"testing"

	"feishu-botd/internal/config"
	"feishu-botd/internal/feishu"
)

// File carry (ADR-0127) is resolved once by the service, like video, and
// handed down as AttachedContextRequest.AllowFiles. allow_attached_files
// without allow_attached_context never reaches the lookup at all (the RPC is
// denied), and the two media grants never imply each other.
func TestGetAgentAttachedContextPassesResolvedFileGrantToLookup(t *testing.T) {
	tests := []struct {
		name      string
		providers map[string]config.AgentProviderConfig
		wantFiles bool
		wantVideo bool
	}{
		{
			name: "both grants set",
			providers: map[string]config.AgentProviderConfig{
				"nous": {AllowAttachedContext: true, AllowAttachedFiles: true},
			},
			wantFiles: true,
		},
		{
			name: "context granted, files omitted defaults to deny",
			providers: map[string]config.AgentProviderConfig{
				"nous": {AllowAttachedContext: true, AllowAttachedVideo: true},
			},
			wantVideo: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			backend := &attachedContextTestBackend{fakeSender: &fakeSender{messageID: "om_answer"}}
			svc := newAttachedVideoTestService(backend, test.providers)
			seedAttachedContextDelivery(t, svc, "nous", "delivery_topic")

			if _, apiErr := svc.GetAgentAttachedContext(context.Background(), AgentAttachedContextInput{
				Provider: "nous", DeliveryID: "delivery_topic",
			}); apiErr != nil {
				t.Fatalf("get attached context: %v", apiErr)
			}
			if len(backend.requests) != 1 {
				t.Fatalf("lookup requests = %#v", backend.requests)
			}
			if got := backend.requests[0].AllowFiles; got != test.wantFiles {
				t.Fatalf("AllowFiles = %t, want %t", got, test.wantFiles)
			}
			if got := backend.requests[0].AllowVideo; got != test.wantVideo {
				t.Fatalf("AllowVideo = %t, want %t", got, test.wantVideo)
			}
		})
	}
}

func TestGetAgentAttachedContextFileGrantWithoutContextGrantIsDenied(t *testing.T) {
	backend := &attachedContextTestBackend{fakeSender: &fakeSender{messageID: "om_answer"}}
	svc := newAttachedVideoTestService(backend, map[string]config.AgentProviderConfig{
		"nous": {AllowAttachedFiles: true},
	})

	_, apiErr := svc.GetAgentAttachedContext(context.Background(), AgentAttachedContextInput{
		Provider: "nous", DeliveryID: "delivery_topic",
	})
	if apiErr == nil || apiErr.Status != 403 {
		t.Fatalf("apiErr = %#v, want 403 provider_scope_denied", apiErr)
	}
	if len(backend.requests) != 0 {
		t.Fatalf("lookup requests = %#v, want none", backend.requests)
	}
}

func TestGetAgentAttachedContextForwardsCollectedFiles(t *testing.T) {
	backend := &attachedContextTestBackend{
		fakeSender: &fakeSender{messageID: "om_answer"},
		result: feishu.AttachedContext{
			Status: feishu.AttachedContextFound,
			Messages: []feishu.AttachedContextMessage{{
				AuthorLabel: "participant-1", AuthorType: "user",
				Files: []feishu.AttachedContextFile{{MediaType: "application/pdf", Data: []byte("%PDF-fixture"), FileName: "report.pdf"}},
			}},
		},
	}
	svc := newAttachedVideoTestService(backend, map[string]config.AgentProviderConfig{
		"nous": {AllowAttachedContext: true, AllowAttachedFiles: true},
	})
	seedAttachedContextDelivery(t, svc, "nous", "delivery_topic")

	got, apiErr := svc.GetAgentAttachedContext(context.Background(), AgentAttachedContextInput{
		Provider: "nous", DeliveryID: "delivery_topic",
	})
	if apiErr != nil {
		t.Fatalf("get attached context: %v", apiErr)
	}
	if len(got.Messages) != 1 || len(got.Messages[0].Files) != 1 ||
		got.Messages[0].Files[0].MediaType != "application/pdf" {
		t.Fatalf("result = %#v, want the backend's file passed through unchanged", got)
	}
}
