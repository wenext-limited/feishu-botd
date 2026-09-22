package service

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"feishu-botd/internal/config"
	"feishu-botd/internal/dedupe"
	"feishu-botd/internal/feishu"
)

func newAttachedVideoTestService(backend *attachedContextTestBackend, providers map[string]config.AgentProviderConfig) *Service {
	cfg := config.Config{
		AppID: "cli_test", AppSecret: "secret",
		Channels:       map[string]string{"ops": "oc_ops"},
		DedupeTTL:      time.Hour,
		SendTimeout:    time.Second,
		AgentProviders: providers,
	}
	return NewService(cfg, backend, dedupe.NewMemoryStore(time.Hour), slog.Default())
}

// The lookup must know whether THIS provider may receive video before it
// parses a single message, so the service resolves the grant once, here, and
// hands it down as AttachedContextRequest.AllowVideo (ADR-0126). It must
// never widen on its own: allow_attached_video without allow_attached_context
// is inert (see config.ProviderAllowsAttachedVideo).
func TestGetAgentAttachedContextPassesResolvedVideoGrantToLookup(t *testing.T) {
	tests := []struct {
		name      string
		providers map[string]config.AgentProviderConfig
		want      bool
	}{
		{
			name: "both grants set",
			providers: map[string]config.AgentProviderConfig{
				"nous": {AllowAttachedContext: true, AllowAttachedVideo: true},
			},
			want: true,
		},
		{
			name: "context granted, video omitted defaults to deny",
			providers: map[string]config.AgentProviderConfig{
				"nous": {AllowAttachedContext: true},
			},
			want: false,
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
			if got := backend.requests[0].AllowVideo; got != test.want {
				t.Fatalf("AllowVideo = %t, want %t", got, test.want)
			}
		})
	}
}

func TestGetAgentAttachedContextForwardsCollectedVideos(t *testing.T) {
	backend := &attachedContextTestBackend{
		fakeSender: &fakeSender{messageID: "om_answer"},
		result: feishu.AttachedContext{
			Status: feishu.AttachedContextFound,
			Messages: []feishu.AttachedContextMessage{{
				AuthorLabel: "participant-1", AuthorType: "user",
				Videos: []feishu.AttachedContextVideo{{MediaType: "video/mp4", Data: []byte("fixture"), DurationMs: 9000}},
			}},
		},
	}
	svc := newAttachedVideoTestService(backend, map[string]config.AgentProviderConfig{
		"nous": {AllowAttachedContext: true, AllowAttachedVideo: true},
	})
	seedAttachedContextDelivery(t, svc, "nous", "delivery_topic")

	got, apiErr := svc.GetAgentAttachedContext(context.Background(), AgentAttachedContextInput{
		Provider: "nous", DeliveryID: "delivery_topic",
	})
	if apiErr != nil {
		t.Fatalf("get attached context: %v", apiErr)
	}
	if len(got.Messages) != 1 || len(got.Messages[0].Videos) != 1 ||
		got.Messages[0].Videos[0].MediaType != "video/mp4" {
		t.Fatalf("result = %#v, want the backend's video passed through unchanged", got)
	}
}
