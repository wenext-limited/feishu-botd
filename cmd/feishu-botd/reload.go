package main

import (
	"context"
	"crypto/sha256"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"

	"feishu-botd/internal/config"
	"feishu-botd/internal/feishu"
	"feishu-botd/internal/grpcapi"
	"feishu-botd/internal/httpapi"
	"feishu-botd/internal/messageroute"
	"feishu-botd/internal/service"
)

const defaultWatchInterval = 2 * time.Second

// watchIntervalFromEnv reads FEISHU_BOTD_CONFIG_WATCH. Unset or "1"/"true"
// watches every two seconds; "0"/"false"/"off" turns the watcher off, and a
// Go duration ("5s") sets the interval. With no config file there is
// nothing to watch.
func watchIntervalFromEnv(logger *slog.Logger) time.Duration {
	if strings.TrimSpace(os.Getenv("FEISHU_BOTD_CONFIG")) == "" {
		return 0
	}
	raw := strings.ToLower(strings.TrimSpace(os.Getenv("FEISHU_BOTD_CONFIG_WATCH")))
	switch raw {
	case "", "1", "true", "on":
		return defaultWatchInterval
	case "0", "false", "off":
		return 0
	}
	interval, err := time.ParseDuration(raw)
	if err != nil || interval < 100*time.Millisecond {
		logger.Warn("FEISHU_BOTD_CONFIG_WATCH is not a duration of at least 100ms; using the default", "default", defaultWatchInterval.String())
		return defaultWatchInterval
	}
	return interval
}

// reloader re-reads the configuration on request and applies whatever can
// change while botd runs. One reload runs at a time.
type reloader struct {
	svc        *service.Service
	httpServer *httpapi.Server
	grpcServer *grpcapi.Server
	apps       *appManager
	routes     *messageroute.Store
	logger     *slog.Logger
	timeout    time.Duration
	load       func() (config.Config, error)
	// newSender builds a sender for an app the reload adds or re-credits;
	// tests replace it.
	newSender func(app config.AppConfig) feishu.Sender

	mu sync.Mutex
}

// serve reloads on every SIGHUP and, when interval is positive, whenever
// the config file or a token file it names changes on disk.
func (r *reloader) serve(ctx context.Context, hup <-chan os.Signal, interval time.Duration) {
	var tick <-chan time.Time
	if interval > 0 {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		tick = ticker.C
		r.logger.Info("watching config files for changes", "interval", interval.String())
	}
	seen := fingerprint(r.svc.Config().SourceFiles)
	// pending holds a fingerprint that differs from the running one. The
	// reload waits until two polls agree, so a file caught mid-write is
	// not read.
	var pending [sha256.Size]byte
	havePending := false
	for {
		select {
		case <-ctx.Done():
			return
		case <-hup:
			r.reload(ctx, "signal")
			seen = fingerprint(r.svc.Config().SourceFiles)
			havePending = false
		case <-tick:
			now := fingerprint(r.svc.Config().SourceFiles)
			switch {
			case now == seen:
				havePending = false
			case havePending && now == pending:
				r.reload(ctx, "file change")
				// Applied or rejected, wait for the files to change again.
				seen = fingerprint(r.svc.Config().SourceFiles)
				havePending = false
			default:
				pending = now
				havePending = true
			}
		}
	}
}

// fingerprint hashes the named files' contents. A missing or unreadable
// file contributes its name only, so it still registers as a change.
func fingerprint(files []string) [sha256.Size]byte {
	hash := sha256.New()
	for _, path := range files {
		_, _ = io.WriteString(hash, path)
		hash.Write([]byte{0})
		if file, err := os.Open(path); err == nil {
			_, _ = io.Copy(hash, file)
			_ = file.Close()
		}
		hash.Write([]byte{0})
	}
	var sum [sha256.Size]byte
	copy(sum[:], hash.Sum(nil))
	return sum
}

// reload loads, validates, and applies the configuration. An invalid
// config, or an app whose new credentials fail their check, rejects the
// whole reload and botd keeps running on the old one.
func (r *reloader) reload(ctx context.Context, trigger string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := time.Now()

	next, err := r.load()
	if err != nil {
		r.svc.RecordRejectedReload(now)
		r.logger.Error("config reload rejected; still running the previous config",
			"trigger", trigger, "reason", r.svc.Redact(err.Error()),
			"generation", r.svc.ConfigGeneration())
		return
	}
	running := r.svc.Config()
	plan := config.PlanReload(running, next)
	if plan.Empty() {
		r.logger.Info("config reload: no changes", "trigger", trigger, "generation", r.svc.ConfigGeneration())
		return
	}
	applied := config.KeepRestartOnly(running, next)
	restartRequired := append([]string(nil), plan.RestartRequired...)
	if r.routes == nil && running.StateDir != "" && applied.AnyProviderAllowsProactiveMessages() {
		// The reply-route journal is opened at startup only when some
		// provider may post proactively. Until a restart, the new grant
		// keeps reply routes in memory for the dedupe TTL.
		restartRequired = append(restartRequired, "agent_providers.allow_proactive_messages (reply-route journal)")
	}

	apps := applied.EffectiveApps()
	newApps := append(append([]string(nil), plan.AppsAdded...), plan.AppsRecredited...)
	senders := make(map[string]feishu.Sender, len(newApps))
	for _, alias := range newApps {
		senders[alias] = r.sender(apps[alias])
	}
	if len(newApps) > 0 {
		if err := preflightAppSenders(ctx, r.timeout, newApps, senders); err != nil {
			r.svc.RecordRejectedReload(now)
			r.logger.Error("config reload rejected; still running the previous config",
				"trigger", trigger, "reason", err.Error(),
				"generation", r.svc.ConfigGeneration())
			return
		}
	}

	// Swap credentials first, so a provider whose stream the service is
	// about to close reconnects under its new token and grants.
	r.grpcServer.ApplyConfig(applied)
	r.httpServer.ApplyConfig(applied)
	r.svc.ApplyConfig(applied, plan, senders, restartRequired, now)
	if r.routes != nil && (len(plan.AppsAdded) > 0 || len(plan.AppsRemoved) > 0 || len(plan.AppsRecredited) > 0) {
		if err := r.routes.SetKeys(messageRouteKeys(apps), now); err != nil {
			r.logger.Error("config reload: re-keying the reply-route journal failed", "reason", r.svc.Redact(err.Error()))
		}
	}
	changes := r.apps.apply(applied, plan)

	attrs := []any{
		"trigger", trigger,
		"generation", r.svc.ConfigGeneration(),
		"apps_started", changes.started,
		"apps_stopped", changes.stopped,
		"apps_reconnected", changes.replaced,
		"apps_updated", changes.updated,
		"providers_added", plan.ProvidersAdded,
		"providers_reset", append(append([]string(nil), plan.ProvidersChanged...), plan.ProvidersRemoved...),
		"settings_changed", plan.SettingsChanged,
	}
	if len(changes.failed) > 0 {
		attrs = append(attrs, "apps_failed", changes.failed)
	}
	r.logger.Info("config reload applied", attrs...)
	if len(restartRequired) > 0 {
		r.logger.Warn("config reload: some changes take effect only after a restart",
			"restart_required", restartRequired)
	}
}

func (r *reloader) sender(app config.AppConfig) feishu.Sender {
	if r.newSender != nil {
		return r.newSender(app)
	}
	return feishu.NewChannelSender(app.AppID, app.AppSecret, r.logger)
}
