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

	"feishu-botd/internal/config"
	"feishu-botd/internal/dedupe"
	"feishu-botd/internal/feishu"
	"feishu-botd/internal/grpcapi"
	"feishu-botd/internal/httpapi"
	"feishu-botd/internal/ownership"
	"feishu-botd/internal/scriptexec"
	"feishu-botd/internal/service"
)

const (
	startupTimeoutFallback = 15 * time.Second
	shutdownTimeout        = 10 * time.Second
	agentOwnershipTTL      = 24 * time.Hour
)

var errReceiverUnavailable = errors.New("Feishu receiver unavailable")

type componentResult struct {
	component string
	err       error
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

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, cfg, logger); err != nil {
		logger.Error("daemon stopped", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, cfg config.Config, logger *slog.Logger) error {
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

	store := dedupe.NewMemoryStore(cfg.DedupeTTL)
	svc := service.NewMultiAppService(cfg, senders, store, logger)
	if cfg.StateDir != "" {
		owners, err := ownership.Open(cfg.StateDir, agentOwnershipTTL)
		if err != nil {
			return fmt.Errorf("open agent message ownership state: %w", err)
		}
		svc.SetAgentOwnershipStore(owners)
	}
	httpServer := httpapi.NewServer(cfg, svc, logger)
	grpcServer := grpcapi.NewServer(cfg, svc, logger)

	runCtx, cancelRun := context.WithCancel(ctx)
	defer cancelRun()

	scriptSubs, err := startScriptExecutors(runCtx, apps, aliases, svc, logger)
	if err != nil {
		return err
	}
	receivers := make(map[string]appReceiver, len(aliases))
	for _, alias := range aliases {
		appAlias := alias
		app := apps[appAlias]
		var commandHandler feishu.CommandHandler
		if app.Commands.Enabled {
			commandHandler = func(ctx context.Context, cmd feishu.InboundCommand) error {
				_, apiErr := svc.DispatchCommand(ctx, service.CommandInput{
					AppAlias:          cmd.AppAlias,
					DeliveryID:        cmd.DeliveryID,
					Command:           cmd.Command,
					Text:              cmd.Text,
					Prompt:            cmd.Prompt,
					ConversationID:    cmd.ConversationID,
					ChatAlias:         cmd.ChatAlias,
					SenderID:          cmd.SenderID,
					Metadata:          cmd.Metadata,
					ConversationTitle: cmd.ConversationTitle,
					ChatID:            cmd.ChatID,
					UnconfiguredGroup: cmd.UnconfiguredGroup,
				})
				if apiErr != nil {
					return apiErr
				}
				return nil
			}
		}
		receiver := feishu.NewCommandReceiver(feishu.CommandReceiverConfig{
			AppAlias:                    appAlias,
			AppID:                       app.AppID,
			AppSecret:                   app.AppSecret,
			Channels:                    app.Channels,
			AllowUnconfiguredGroupChats: app.Commands.AllowUnconfiguredGroupChats,
			BotOpenID:                   app.Commands.BotOpenID,
			BotUserID:                   app.Commands.BotUserID,
			BotUnionID:                  app.Commands.BotUnionID,
			BotNames:                    app.Commands.BotNames,
			ConnectionStateChanged: func(alias string, state feishu.ConnectionState) {
				svc.SetAppConnectionState(alias, string(state))
			},
		}, commandHandler, logger)
		if app.Commands.Enabled {
			receiver.SetCardActionHandler(func(ctx context.Context, action feishu.InboundCardAction) error {
				if apiErr := svc.DispatchInboundCardAction(ctx, action); apiErr != nil {
					return apiErr
				}
				return nil
			})
		}
		receiver.SetMessageReactionHandler(func(ctx context.Context, reaction feishu.InboundMessageReaction) error {
			operation := service.MessageReactionUnspecified
			switch reaction.Operation {
			case feishu.MessageReactionAdded:
				operation = service.MessageReactionAdded
			case feishu.MessageReactionRemoved:
				operation = service.MessageReactionRemoved
			}
			if apiErr := svc.DispatchAgentMessageReaction(ctx, service.AgentMessageReactionInput{
				AppAlias:     reaction.AppAlias,
				DeliveryID:   reaction.DeliveryID,
				MessageRef:   reaction.MessageRef,
				SenderID:     reaction.SenderID,
				ReactionType: reaction.ReactionType,
				Operation:    operation,
			}); apiErr != nil {
				return apiErr
			}
			return nil
		})
		receivers[appAlias] = receiver
	}

	var cleanupOnce sync.Once
	cleanupApps := func() {
		cleanupOnce.Do(func() {
			for _, alias := range aliases {
				receivers[alias].Close()
			}
			for _, sub := range scriptSubs {
				sub.Close()
			}
		})
	}
	defer cleanupApps()

	// Receiver Start blocks for the lifetime of a successful SDK connection.
	// Each wrapper reports initial readiness separately, then forwards only an
	// unexpected later return to the shared component channel.
	componentCh := make(chan componentResult, len(receivers)+4)
	startupCh := startAppReceivers(runCtx, aliases, receivers, componentCh)
	if err := waitForAppReceivers(runCtx, startupTimeout(cfg), len(receivers), startupCh); err != nil {
		cancelRun()
		cleanupApps()
		if ctx.Err() != nil {
			return nil
		}
		return err
	}

	listenerCount := startListeners(runCtx, cfg, httpServer, grpcServer, componentCh)
	if listenerCount == 0 {
		cancelRun()
		cleanupApps()
		return errors.New("no listeners are configured")
	}

	var runErr error
	select {
	case <-ctx.Done():
		logger.Info("shutdown requested")
	case result := <-componentCh:
		switch {
		case result.err == nil:
			runErr = fmt.Errorf("%s stopped unexpectedly", result.component)
		case errors.Is(result.err, http.ErrServerClosed):
			// Treat the HTTP server's normal shutdown sentinel as clean.
		default:
			runErr = fmt.Errorf("%s stopped: %w", result.component, result.err)
		}
	}

	cancelRun()
	cleanupApps()
	if err := shutdownServers(httpServer, grpcServer); err != nil {
		if runErr != nil {
			return errors.Join(runErr, err)
		}
		return err
	}
	return runErr
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
