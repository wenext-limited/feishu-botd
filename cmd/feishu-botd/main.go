package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"feishu-botd/internal/chatgrant"
	"feishu-botd/internal/config"
	"feishu-botd/internal/dedupe"
	"feishu-botd/internal/feishu"
	"feishu-botd/internal/grpcapi"
	"feishu-botd/internal/httpapi"
	"feishu-botd/internal/messageroute"
	"feishu-botd/internal/ownership"
	"feishu-botd/internal/scriptexec"
	"feishu-botd/internal/service"
)

const (
	startupTimeoutFallback = 15 * time.Second
	shutdownTimeout        = 10 * time.Second
	agentOwnershipTTL      = 24 * time.Hour
	// agentMessageRouteTTL bounds how long a delivered or authored
	// message_ref stays a PostAgentMessage reply target. It outlives the
	// 24-hour ownership window on purpose: an answer may arrive days after
	// the question, long after a mentionless reply would still route.
	agentMessageRouteTTL = 7 * 24 * time.Hour
)

var errReceiverUnavailable = errors.New("Feishu receiver unavailable")

type componentResult struct {
	component string
	err       error
	// receiver is set for an app receiver's runtime exit, so the run loop
	// can tell a reload's deliberate stop from a failure.
	receiver appReceiver
}

type appReceiver interface {
	Start(context.Context) error
	Close()
	InitialReady() <-chan struct{}
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	cfg, err := config.LoadFromEnv()
	if err != nil {
		logger.Error("configuration failed", "error", err)
		os.Exit(2)
	}

	// Register for SIGHUP before anything slow: its default action would
	// terminate the daemon.
	hup := make(chan os.Signal, 1)
	signal.Notify(hup, syscall.SIGHUP)
	defer signal.Stop(hup)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, cfg, logger, runOptions{reload: hup, watchInterval: watchIntervalFromEnv(logger)}); err != nil {
		logger.Error("daemon stopped", "error", err)
		os.Exit(1)
	}
}

// runOptions carries process-level wiring that is not configuration.
type runOptions struct {
	// reload delivers a request to reload the config (SIGHUP). Nil never
	// does.
	reload <-chan os.Signal
	// watchInterval is how often the config and token files are checked
	// for changes; zero turns the watcher off.
	watchInterval time.Duration
}

func run(ctx context.Context, cfg config.Config, logger *slog.Logger, opts runOptions) error {
	if logger == nil {
		logger = slog.Default()
	}
	apps := cfg.EffectiveApps()
	aliases := cfg.AppAliases()
	if len(aliases) == 0 {
		return errors.New("no Feishu apps are configured")
	}

	senders := make(map[string]feishu.Sender, len(aliases))
	for _, alias := range aliases {
		app := apps[alias]
		senders[alias] = feishu.NewChannelSender(app.AppID, app.AppSecret, logger)
	}
	if err := preflightAppSenders(ctx, startupTimeout(cfg), aliases, senders); err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return err
	}

	// Chats providers created with CreateAgentChat: loaded before anything
	// copies cfg, so the service and every receiver share one registry.
	if cfg.StateDir != "" {
		grants, err := chatgrant.Open(cfg.StateDir)
		if err != nil {
			return fmt.Errorf("open agent chat grants: %w", err)
		}
		cfg.RuntimeChats = grants
	}
	store := dedupe.NewMemoryStore(cfg.DedupeTTL)
	svc := service.NewMultiAppService(cfg, senders, store, logger)
	// routes stays nil unless a provider may post proactively; a reload
	// re-keys it when apps change.
	var routes *messageroute.Store
	if cfg.StateDir != "" {
		owners, err := ownership.Open(cfg.StateDir, agentOwnershipTTL)
		if err != nil {
			return fmt.Errorf("open agent message ownership state: %w", err)
		}
		svc.SetAgentOwnershipStore(owners)
		if cfg.AnyProviderAllowsProactiveMessages() {
			opened, err := messageroute.Open(cfg.StateDir, agentMessageRouteTTL, messageRouteKeys(apps), time.Now())
			if err != nil {
				return fmt.Errorf("open agent message route state: %w", err)
			}
			defer opened.Close()
			routes = opened
			svc.SetAgentMessageRouteStore(routes)
		}
		if err := svc.OpenAgentConversationLeases(cfg.StateDir); err != nil {
			return fmt.Errorf("open conversation leases: %w", err)
		}
	}
	httpServer := httpapi.NewServer(cfg, svc, logger)
	grpcServer := grpcapi.NewServer(cfg, svc, logger)

	runCtx, cancelRun := context.WithCancel(ctx)
	defer cancelRun()

	componentCh := make(chan componentResult, len(aliases)+16)
	manager := newAppManager(runCtx, svc, logger, componentCh, startupTimeout(cfg))
	receivers := make(map[string]appReceiver, len(aliases))
	for _, alias := range aliases {
		receivers[alias] = manager.build(alias, apps[alias])
	}
	for _, alias := range aliases {
		if err := manager.startScripts(alias); err != nil {
			manager.closeAll()
			return err
		}
	}
	defer manager.closeAll()

	// Receiver Start blocks for the lifetime of a successful SDK connection.
	// Each wrapper reports initial readiness separately, then forwards only an
	// unexpected later return to the shared component channel.
	startupCh := startAppReceivers(runCtx, aliases, receivers, componentCh)
	if err := waitForAppReceivers(runCtx, startupTimeout(cfg), len(receivers), startupCh); err != nil {
		cancelRun()
		manager.closeAll()
		if ctx.Err() != nil {
			return nil
		}
		return err
	}

	listenerCount := startListeners(runCtx, cfg, httpServer, grpcServer, componentCh)
	if listenerCount == 0 {
		cancelRun()
		manager.closeAll()
		return errors.New("no listeners are configured")
	}

	reloads := &reloader{
		svc:        svc,
		httpServer: httpServer,
		grpcServer: grpcServer,
		apps:       manager,
		routes:     routes,
		logger:     logger,
		timeout:    startupTimeout(cfg),
		load:       config.LoadFromEnv,
	}
	go reloads.serve(runCtx, opts.reload, opts.watchInterval)

	var runErr error
wait:
	for {
		select {
		case <-ctx.Done():
			logger.Info("shutdown requested")
			break wait
		case result := <-componentCh:
			// A receiver a reload replaced or removed is expected to stop.
			if result.receiver != nil && !manager.isCurrent(result.receiver) {
				continue
			}
			switch {
			case result.err == nil:
				runErr = fmt.Errorf("%s stopped unexpectedly", result.component)
			case errors.Is(result.err, http.ErrServerClosed):
				// Treat the HTTP server's normal shutdown sentinel as clean.
			default:
				runErr = fmt.Errorf("%s stopped: %w", result.component, result.err)
			}
			break wait
		}
	}

	cancelRun()
	manager.closeAll()
	if err := shutdownServers(httpServer, grpcServer); err != nil {
		if runErr != nil {
			return errors.Join(runErr, err)
		}
		return err
	}
	return runErr
}

// messageRouteKeys derives each app's route-journal key from its secret, so
// the journal under state_dir is unreadable without the config that holds
// those secrets.
func messageRouteKeys(apps map[string]config.AppConfig) map[string][]byte {
	keys := make(map[string][]byte, len(apps))
	for alias, app := range apps {
		keys[alias] = messageroute.SealKey(alias, app.AppSecret)
	}
	return keys
}

func agentMentions(in []feishu.Mention) []service.AgentMention {
	if len(in) == 0 {
		return nil
	}
	out := make([]service.AgentMention, 0, len(in))
	for _, mention := range in {
		out = append(out, service.AgentMention{UserID: mention.UserID, Name: mention.Name})
	}
	return out
}

func startupTimeout(cfg config.Config) time.Duration {
	if cfg.SendTimeout > 0 {
		return cfg.SendTimeout
	}
	return startupTimeoutFallback
}

// preflightAppSenders validates every credential set before any listener or
// long connection starts. Checks run concurrently so N apps still consume one
// bounded startup window. Returned errors contain only the public app alias.
func preflightAppSenders(
	ctx context.Context,
	timeout time.Duration,
	aliases []string,
	senders map[string]feishu.Sender,
) error {
	checkCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	type result struct {
		alias string
		err   error
	}
	results := make(chan result, len(aliases))
	for _, alias := range aliases {
		appAlias := alias
		sender := senders[appAlias]
		go func(appAlias string, sender feishu.Sender) {
			if sender == nil {
				results <- result{alias: appAlias, err: errors.New("sender is unavailable")}
				return
			}
			results <- result{alias: appAlias, err: sender.Ready(checkCtx)}
		}(appAlias, sender)
	}

	failures := make(map[string]error)
	for received := 0; received < len(aliases); {
		select {
		case result := <-results:
			received++
			if result.err != nil {
				failures[result.alias] = result.err
			}
		case <-checkCtx.Done():
			for _, alias := range aliases {
				if failures[alias] != nil {
					return fmt.Errorf("Feishu app %q credential preflight failed", alias)
				}
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return errors.New("Feishu credential preflight timed out")
		}
	}
	for _, alias := range aliases {
		if failures[alias] != nil {
			return fmt.Errorf("Feishu app %q credential preflight failed", alias)
		}
	}
	return nil
}

func startAppReceivers(
	ctx context.Context,
	aliases []string,
	receivers map[string]appReceiver,
	componentCh chan<- componentResult,
) <-chan componentResult {
	startupCh := make(chan componentResult, len(aliases))
	for _, alias := range aliases {
		appAlias := alias
		receiver := receivers[appAlias]
		go func(appAlias string, receiver appReceiver) {
			done := make(chan error, 1)
			go func() {
				done <- receiver.Start(ctx)
			}()
			select {
			case <-receiver.InitialReady():
				startupCh <- componentResult{component: appAlias}
				err := <-done
				componentCh <- componentResult{
					component: fmt.Sprintf("Feishu app %q receiver", appAlias),
					err:       safeReceiverError(err),
					receiver:  receiver,
				}
			case err := <-done:
				if err == nil {
					err = errors.New("receiver stopped before connecting")
				}
				startupCh <- componentResult{component: appAlias, err: safeReceiverError(err)}
			case <-ctx.Done():
				startupCh <- componentResult{component: appAlias, err: ctx.Err()}
			}
		}(appAlias, receiver)
	}
	return startupCh
}

func safeReceiverError(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, context.Canceled):
		return context.Canceled
	case errors.Is(err, context.DeadlineExceeded):
		return context.DeadlineExceeded
	default:
		return errReceiverUnavailable
	}
}

func waitForAppReceivers(
	ctx context.Context,
	timeout time.Duration,
	count int,
	startupCh <-chan componentResult,
) error {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for range count {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			return errors.New("Feishu receiver startup timed out")
		case result := <-startupCh:
			if result.err != nil {
				return fmt.Errorf("Feishu app %q receiver startup failed", result.component)
			}
		}
	}
	return nil
}

func startScriptExecutors(
	ctx context.Context,
	apps map[string]config.AppConfig,
	aliases []string,
	svc *service.Service,
	logger *slog.Logger,
) ([]*service.CommandSubscription, error) {
	subs := make([]*service.CommandSubscription, 0)
	for _, alias := range aliases {
		appAlias := alias
		scriptCfg := apps[appAlias].Commands.Scripts
		if !scriptCfg.Enabled {
			continue
		}
		executor := scriptexec.New(scriptCfg, logger)
		sub, apiErr := svc.SubscribeInternalCommandsForApps(ctx, service.CommandSubscribeOptions{
			Provider:              "script-executor",
			Commands:              []string{scriptCfg.Command},
			AllowedApps:           []string{appAlias},
			AllowedAppsConfigured: true,
		})
		if apiErr != nil {
			for _, existing := range subs {
				existing.Close()
			}
			return nil, fmt.Errorf("script executor subscribe for app %q failed", appAlias)
		}
		subs = append(subs, sub)
		go func(appAlias string, executor *scriptexec.Executor, sub *service.CommandSubscription) {
			for cmd := range sub.C {
				// Each delivery has already been app-filtered by the broker.
				// Run against Background so a build already in flight can finish
				// and reply within its own configured timeout during shutdown.
				go func(cmd service.CommandInput) {
					title, markdown := executor.Run(context.Background(), cmd.ChatAlias, cmd.Text)
					if apiErr := svc.RespondInternalCommand(context.Background(), service.CommandResponse{
						Provider:   "script-executor",
						DeliveryID: cmd.DeliveryID,
						Title:      title,
						Markdown:   markdown,
					}); apiErr != nil {
						logger.Error("script command respond failed", "app_alias", appAlias)
					}
				}(cmd)
			}
		}(appAlias, executor, sub)
	}
	return subs, nil
}

func startListeners(
	ctx context.Context,
	cfg config.Config,
	httpServer *httpapi.Server,
	grpcServer *grpcapi.Server,
	componentCh chan<- componentResult,
) int {
	count := 0
	start := func(component string, serve func() error) {
		count++
		go func() {
			componentCh <- componentResult{component: component, err: serve()}
		}()
	}
	if cfg.SocketPath != "" {
		start("HTTP Unix listener", func() error {
			return httpServer.ListenAndServeUnix(ctx, cfg.SocketPath)
		})
	}
	if cfg.BindAddr != "" {
		start("HTTP TCP listener", func() error {
			return httpServer.ListenAndServeTCP(ctx, cfg.BindAddr)
		})
	}
	if cfg.GRPCSocketPath != "" {
		start("gRPC Unix listener", func() error {
			return grpcServer.ListenAndServeUnix(ctx, cfg.GRPCSocketPath)
		})
	}
	if cfg.GRPCBindAddr != "" {
		start("gRPC TCP listener", func() error {
			return grpcServer.ListenAndServeTCP(ctx, cfg.GRPCBindAddr)
		})
	}
	return count
}

func shutdownServers(httpServer *httpapi.Server, grpcServer *grpcapi.Server) error {
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	var wg sync.WaitGroup
	shutdownErrs := make([]error, 2)
	wg.Add(2)
	go func() {
		defer wg.Done()
		shutdownErrs[0] = httpServer.Shutdown(shutdownCtx)
	}()
	go func() {
		defer wg.Done()
		shutdownErrs[1] = grpcServer.Shutdown(shutdownCtx)
	}()
	wg.Wait()
	if err := errors.Join(shutdownErrs...); err != nil {
		return fmt.Errorf("graceful shutdown failed: %w", err)
	}
	return nil
}
