package main

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"feishu-botd/internal/feishu"
	"feishu-botd/internal/notify"
)

type preflightSender struct {
	mu       sync.Mutex
	readyErr error
	calls    int
}

type fakeAppReceiver struct {
	readyOnce sync.Once
	ready     chan struct{}
	started   chan struct{}
	exit      chan error

	mu         sync.Mutex
	closeCalls int
}

func newFakeAppReceiver() *fakeAppReceiver {
	return &fakeAppReceiver{
		ready:   make(chan struct{}),
		started: make(chan struct{}),
		exit:    make(chan error, 1),
	}
}

func (r *fakeAppReceiver) Start(ctx context.Context) error {
	close(r.started)
	select {
	case err := <-r.exit:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *fakeAppReceiver) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closeCalls++
}

func (r *fakeAppReceiver) InitialReady() <-chan struct{} {
	return r.ready
}

func (r *fakeAppReceiver) markReady() {
	r.readyOnce.Do(func() {
		close(r.ready)
	})
}

func (s *preflightSender) Ready(context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	return s.readyErr
}

func (*preflightSender) Send(context.Context, string, notify.Request) (string, error) {
	return "", nil
}

func (s *preflightSender) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

func TestPreflightAppSendersChecksEveryApp(t *testing.T) {
	defaultSender := &preflightSender{}
	blueSender := &preflightSender{}
	senders := map[string]feishu.Sender{
		"default": defaultSender,
		"blue":    blueSender,
	}
	if err := preflightAppSenders(
		context.Background(),
		time.Second,
		[]string{"blue", "default"},
		senders,
	); err != nil {
		t.Fatalf("preflight: %v", err)
	}
	if defaultSender.callCount() != 1 || blueSender.callCount() != 1 {
		t.Fatalf("ready calls: default=%d blue=%d", defaultSender.callCount(), blueSender.callCount())
	}
}

func TestPreflightAppSendersRedactsCredentialFailure(t *testing.T) {
	const privateError = "app_secret=must-never-be-logged"
	senders := map[string]feishu.Sender{
		"alpha": &preflightSender{readyErr: errors.New(privateError)},
		"beta":  &preflightSender{},
	}
	err := preflightAppSenders(
		context.Background(),
		time.Second,
		[]string{"alpha", "beta"},
		senders,
	)
	if err == nil {
		t.Fatal("credential failure passed preflight")
	}
	if !strings.Contains(err.Error(), `Feishu app "alpha" credential preflight failed`) {
		t.Fatalf("preflight error omitted public alias: %v", err)
	}
	if strings.Contains(err.Error(), privateError) || strings.Contains(err.Error(), "must-never-be-logged") {
		t.Fatalf("preflight error leaked credential failure: %v", err)
	}
}

func TestPreflightAppSendersFailsClosedWhenBackendMissing(t *testing.T) {
	err := preflightAppSenders(
		context.Background(),
		time.Second,
		[]string{"blue"},
		map[string]feishu.Sender{},
	)
	if err == nil || !strings.Contains(err.Error(), `Feishu app "blue" credential preflight failed`) {
		t.Fatalf("missing backend error = %v", err)
	}
}

func TestReceiverStartupWaitsForEveryConfiguredApp(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	blue := newFakeAppReceiver()
	defaultReceiver := newFakeAppReceiver()
	receivers := map[string]appReceiver{
		"blue":    blue,
		"default": defaultReceiver,
	}
	componentCh := make(chan componentResult, len(receivers))
	startupCh := startAppReceivers(
		ctx,
		[]string{"blue", "default"},
		receivers,
		componentCh,
	)
	<-blue.started
	<-defaultReceiver.started

	blue.markReady()
	first := <-startupCh
	if first.component != "blue" || first.err != nil {
		t.Fatalf("first startup result = %#v", first)
	}
	done := make(chan error, 1)
	go func() {
		done <- waitForAppReceivers(ctx, time.Second, 1, startupCh)
	}()
	select {
	case err := <-done:
		t.Fatalf("startup completed before default app connected: %v", err)
	default:
	}
	defaultReceiver.markReady()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("startup wait: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("startup did not complete after every app connected")
	}
}

func TestReceiverStartupFailureNamesOnlyPublicAlias(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	const privateError = "wss://example.invalid?ticket=private-ticket"
	blue := newFakeAppReceiver()
	blue.exit <- errors.New(privateError)
	componentCh := make(chan componentResult, 1)
	startupCh := startAppReceivers(
		ctx,
		[]string{"blue"},
		map[string]appReceiver{"blue": blue},
		componentCh,
	)
	err := waitForAppReceivers(ctx, time.Second, 1, startupCh)
	if err == nil || !strings.Contains(err.Error(), `Feishu app "blue" receiver startup failed`) {
		t.Fatalf("startup failure = %v", err)
	}
	if strings.Contains(err.Error(), "private-ticket") || strings.Contains(err.Error(), privateError) {
		t.Fatalf("startup failure leaked SDK error: %v", err)
	}
}

func TestReceiverRuntimeResultsRetainTheirAppAlias(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	blue := newFakeAppReceiver()
	defaultReceiver := newFakeAppReceiver()
	receivers := map[string]appReceiver{
		"blue":    blue,
		"default": defaultReceiver,
	}
	componentCh := make(chan componentResult, len(receivers))
	startupCh := startAppReceivers(
		ctx,
		[]string{"blue", "default"},
		receivers,
		componentCh,
	)
	<-blue.started
	<-defaultReceiver.started
	blue.markReady()
	defaultReceiver.markReady()
	if err := waitForAppReceivers(ctx, time.Second, len(receivers), startupCh); err != nil {
		t.Fatalf("startup wait: %v", err)
	}

	blue.exit <- errors.New("receiver unavailable")
	defaultReceiver.exit <- errors.New("receiver unavailable")
	components := make(map[string]struct{}, len(receivers))
	for range receivers {
		result := <-componentCh
		if !errors.Is(result.err, errReceiverUnavailable) {
			t.Fatalf("runtime receiver error = %v, want fixed unavailable state", result.err)
		}
		components[result.component] = struct{}{}
	}
	for _, want := range []string{`Feishu app "blue" receiver`, `Feishu app "default" receiver`} {
		if _, ok := components[want]; !ok {
			t.Fatalf("runtime components = %#v, missing %q", components, want)
		}
	}
}
