package main

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"feishu-botd/internal/config"
	"feishu-botd/internal/dedupe"
	"feishu-botd/internal/feishu"
	"feishu-botd/internal/grpcapi"
	"feishu-botd/internal/httpapi"
	"feishu-botd/internal/service"
)

// fakeLiveReceiver connects as soon as it starts and records the settings
// and handlers a reload gives it.
type fakeLiveReceiver struct {
	*fakeAppReceiver
	mu       sync.Mutex
	settings feishu.CommandReceiverConfig
	commands bool
}

func (r *fakeLiveReceiver) Start(ctx context.Context) error {
	r.markReady()
	return r.fakeAppReceiver.Start(ctx)
}

func (r *fakeLiveReceiver) UpdateSettings(cfg feishu.CommandReceiverConfig) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.settings = cfg
}

func (r *fakeLiveReceiver) SetCommandHandler(handler feishu.CommandHandler) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.commands = handler != nil
}

func (r *fakeLiveReceiver) SetCardActionHandler(feishu.CardActionHandler)           {}
func (r *fakeLiveReceiver) SetMessageReactionHandler(feishu.MessageReactionHandler) {}

func (r *fakeLiveReceiver) closed() bool {
	r.fakeAppReceiver.mu.Lock()
	defer r.fakeAppReceiver.mu.Unlock()
	return r.closeCalls > 0
}

type reloadFixture struct {
	svc       *service.Service
	manager   *appManager
	reloader  *reloader
	receivers map[string][]*fakeLiveReceiver
	next      config.Config
	loadErr   error
	senderErr error
	logs      *bytes.Buffer
	mu        sync.Mutex
}

func testApp(id string, channels map[string]string) config.AppConfig {
	return config.AppConfig{
		AppID: id, AppSecret: "secret-" + id,
		Commands: config.CommandConfig{Enabled: true},
		Channels: channels,
	}
}

func testRunningConfig() config.Config {
	return config.Config{
		GRPCSocketPath: "/tmp/botd.sock",
		Apps: map[string]config.AppConfig{
			"alpha": testApp("cli_alpha", map[string]string{"ops": "oc_ops"}),
			"beta":  testApp("cli_beta", map[string]string{"dev": "oc_dev"}),
		},
		ChannelRoutes: map[string]config.ChannelRoute{
			"ops": {AppAlias: "alpha", ChatID: "oc_ops"},
			"dev": {AppAlias: "beta", ChatID: "oc_dev"},
		},
		AgentProviders: map[string]config.AgentProviderConfig{
			"nous": {AuthToken: strings.Repeat("n", 40), AllowedCommands: []string{"ask"}},
		},
		DedupeTTL:   2 * time.Hour,
		SendTimeout: time.Second,
	}
}

func newReloadFixture(t *testing.T) *reloadFixture {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	logs := &bytes.Buffer{}
	logger := slog.New(slog.NewTextHandler(&lockedWriter{w: logs}, nil))
	cfg := testRunningConfig()
	senders := map[string]feishu.Sender{"alpha": &preflightSender{}, "beta": &preflightSender{}}
	svc := service.NewMultiAppService(cfg, senders, dedupe.NewMemoryStore(time.Hour), logger)
	componentCh := make(chan componentResult, 16)
	f := &reloadFixture{svc: svc, receivers: map[string][]*fakeLiveReceiver{}, logs: logs}
	f.manager = newAppManager(ctx, svc, logger, componentCh, time.Second)
	f.manager.newReceiver = func(alias string, cfg feishu.CommandReceiverConfig) liveReceiver {
		r := &fakeLiveReceiver{fakeAppReceiver: newFakeAppReceiver(), settings: cfg}
		f.mu.Lock()
		f.receivers[alias] = append(f.receivers[alias], r)
		f.mu.Unlock()
		return r
	}
	initial := map[string]appReceiver{}
	for _, alias := range cfg.AppAliases() {
		initial[alias] = f.manager.build(alias, cfg.Apps[alias])
	}
	startupCh := startAppReceivers(ctx, cfg.AppAliases(), initial, componentCh)
	if err := waitForAppReceivers(ctx, time.Second, len(initial), startupCh); err != nil {
		t.Fatal(err)
	}
	f.reloader = &reloader{
		svc:        svc,
		httpServer: httpapi.NewServer(cfg, svc, logger),
		grpcServer: grpcapi.NewServer(cfg, svc, logger),
		apps:       f.manager,
		logger:     logger,
		timeout:    time.Second,
		load: func() (config.Config, error) {
			f.mu.Lock()
			defer f.mu.Unlock()
			return f.next, f.loadErr
		},
		newSender: func(config.AppConfig) feishu.Sender {
			f.mu.Lock()
			defer f.mu.Unlock()
			return &preflightSender{readyErr: f.senderErr}
		},
	}
	return f
}

func (f *reloadFixture) receiver(alias string, index int) *fakeLiveReceiver {
	f.mu.Lock()
	defer f.mu.Unlock()
	if index >= len(f.receivers[alias]) {
		return nil
	}
	return f.receivers[alias][index]
}

type lockedWriter struct {
	mu sync.Mutex
	w  *bytes.Buffer
}

func (w *lockedWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.w.Write(p)
}

func TestReloadTouchesOnlyChangedApps(t *testing.T) {
	f := newReloadFixture(t)
	alpha, beta := f.receiver("alpha", 0), f.receiver("beta", 0)

	next := testRunningConfig()
	next.GRPCSocketPath = "/tmp/moved.sock"
	next.Apps = map[string]config.AppConfig{
		"alpha": testApp("cli_alpha", map[string]string{"ops": "oc_ops", "lab": "oc_lab"}),
		"gamma": testApp("cli_gamma", map[string]string{"dev": "oc_dev"}),
	}
	next.ChannelRoutes = map[string]config.ChannelRoute{
		"ops": {AppAlias: "alpha", ChatID: "oc_ops"},
		"lab": {AppAlias: "alpha", ChatID: "oc_lab"},
		"dev": {AppAlias: "gamma", ChatID: "oc_dev"},
	}
	f.next = next
	f.reloader.reload(context.Background(), "test")

	if alpha.closed() || f.receiver("alpha", 1) != nil {
		t.Fatal("a channel change reconnected alpha")
	}
	alpha.mu.Lock()
	channels := alpha.settings.Channels
	alpha.mu.Unlock()
	if channels["lab"] != "oc_lab" {
		t.Fatalf("alpha channels not updated: %v", channels)
	}
	if !beta.closed() {
		t.Fatal("removed app beta still connected")
	}
	if gamma := f.receiver("gamma", 0); gamma == nil || gamma.closed() {
		t.Fatal("added app gamma did not connect")
	}
	status := f.svc.ConfigStatus()
	if status.Generation != 2 || status.LastReload != "ok" ||
		len(status.RestartRequired) != 1 || status.RestartRequired[0] != "listeners.grpc_socket" {
		t.Fatalf("status = %+v", status)
	}
	if f.svc.Config().GRPCSocketPath != "/tmp/botd.sock" {
		t.Fatal("a restart-only setting was applied live")
	}
	logs := f.logs.String()
	if !strings.Contains(logs, "config reload applied") || !strings.Contains(logs, "restart_required") {
		t.Fatalf("logs = %s", logs)
	}
	// beta's late exit is expected, not a daemon failure.
	if f.manager.isCurrent(beta) {
		t.Fatal("removed receiver is still current")
	}
}

func TestReloadRecreditReplacesConnectionAndDisablingCommandsKeepsIt(t *testing.T) {
	f := newReloadFixture(t)
	alpha, beta := f.receiver("alpha", 0), f.receiver("beta", 0)
	next := testRunningConfig()
	recredited := next.Apps["alpha"]
	recredited.AppSecret = "rotated"
	next.Apps["alpha"] = recredited
	disabled := next.Apps["beta"]
	disabled.Commands.Enabled = false
	disabled.Commands.AllowUnconfiguredGroupChats = false
	next.Apps["beta"] = disabled
	f.next = next
	f.reloader.reload(context.Background(), "test")

	replacement := f.receiver("alpha", 1)
	if replacement == nil || !alpha.closed() || replacement.closed() {
		t.Fatal("re-credited app was not reconnected")
	}
	if beta.closed() {
		t.Fatal("disabling commands reconnected beta")
	}
	beta.mu.Lock()
	commands := beta.commands
	beta.mu.Unlock()
	if commands {
		t.Fatal("beta still dispatches commands")
	}
}

func TestReloadRejectsInvalidConfigAndFailedCredentials(t *testing.T) {
	f := newReloadFixture(t)
	f.loadErr = errors.New("parse FEISHU_BOTD_CONFIG: unexpected end of JSON input")
	f.reloader.reload(context.Background(), "test")
	if status := f.svc.ConfigStatus(); status.LastReload != "rejected" || status.Generation != 1 {
		t.Fatalf("status = %+v", status)
	}

	next := testRunningConfig()
	next.Apps["gamma"] = testApp("cli_gamma", map[string]string{"lab": "oc_lab"})
	next.ChannelRoutes["lab"] = config.ChannelRoute{AppAlias: "gamma", ChatID: "oc_lab"}
	f.loadErr = nil
	f.senderErr = errors.New("bad secret")
	f.next = next
	f.reloader.reload(context.Background(), "test")
	if status := f.svc.ConfigStatus(); status.LastReload != "rejected" || status.Generation != 1 {
		t.Fatalf("status = %+v", status)
	}
	if f.receiver("gamma", 0) != nil {
		t.Fatal("an app with failing credentials was started")
	}
	if !strings.Contains(f.logs.String(), "config reload rejected") {
		t.Fatalf("logs = %s", f.logs.String())
	}
}

func TestWatcherReloadsWhenFilesSettle(t *testing.T) {
	f := newReloadFixture(t)
	path := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(path, []byte("v1"), 0o600); err != nil {
		t.Fatal(err)
	}
	running := f.svc.Config()
	running.SourceFiles = []string{path}
	f.svc.ApplyConfig(running, config.ReloadPlan{}, nil, nil, time.Now())

	next := running
	next.SendTimeout = 3 * time.Second
	f.next = next
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go f.reloader.serve(ctx, nil, 20*time.Millisecond)
	time.Sleep(60 * time.Millisecond)
	if err := os.WriteFile(path, []byte("v2"), 0o600); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for f.svc.Config().SendTimeout != 3*time.Second {
		if time.Now().After(deadline) {
			t.Fatal("watcher did not reload")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
