package service

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"feishu-botd/internal/config"
	"feishu-botd/internal/dedupe"
)

func TestHealthStatic(t *testing.T) {
	svc := newTestService(&fakeSender{})
	info := svc.Health()
	if info.Status != "ok" || info.Service != "feishu-botd" || info.Version != Version {
		t.Fatalf("unexpected health info: %#v", info)
	}
}

func TestReadyAllOK(t *testing.T) {
	svc := newTestService(&fakeSender{})
	ready, checks := svc.Ready(context.Background())
	if !ready {
		t.Fatalf("expected ready, checks=%v", checks)
	}
	for name, state := range checks {
		if state != "ok" {
			t.Fatalf("check %q = %q, want ok", name, state)
		}
	}
}

func TestReadyMissingCredentials(t *testing.T) {
	cfg := config.Config{Channels: map[string]string{"ops": "oc_test"}, SendTimeout: time.Second}
	svc := NewService(cfg, &fakeSender{}, dedupe.NewMemoryStore(time.Hour), slog.Default())
	ready, checks := svc.Ready(context.Background())
	if ready {
		t.Fatal("expected not ready with missing credentials")
	}
	if checks["feishu_auth"] != "missing_credentials" {
		t.Fatalf("feishu_auth = %q", checks["feishu_auth"])
	}
}

func TestReadyAuthFailure(t *testing.T) {
	svc := newTestService(&fakeSender{readyErr: errors.New("token failed")})
	ready, checks := svc.Ready(context.Background())
	if ready {
		t.Fatal("expected not ready when auth check fails")
	}
	if checks["feishu_auth"] != "unavailable" {
		t.Fatalf("feishu_auth = %q", checks["feishu_auth"])
	}
}
