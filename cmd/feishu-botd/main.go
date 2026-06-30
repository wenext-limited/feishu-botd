package main

import (
	"context"
	"errors"
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
	"feishu-botd/internal/service"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	cfg, err := config.LoadFromEnv()
	if err != nil {
		logger.Error("configuration failed", "error", err)
		os.Exit(2)
	}

	sender := feishu.NewChannelSender(cfg.AppID, cfg.AppSecret, logger)
	store := dedupe.NewMemoryStore(cfg.DedupeTTL)
	svc := service.NewService(cfg, sender, store, logger)

	httpServer := httpapi.NewServer(cfg, svc, logger)
	grpcServer := grpcapi.NewServer(cfg, svc, logger)
	var commandReceiver *feishu.CommandReceiver
	if cfg.Commands.Enabled {
		commandReceiver = feishu.NewCommandReceiver(feishu.CommandReceiverConfig{
			AppID:      cfg.AppID,
			AppSecret:  cfg.AppSecret,
			Channels:   cfg.Channels,
			BotOpenID:  cfg.Commands.BotOpenID,
			BotUserID:  cfg.Commands.BotUserID,
			BotUnionID: cfg.Commands.BotUnionID,
			BotNames:   cfg.Commands.BotNames,
		}, func(ctx context.Context, cmd feishu.InboundCommand) error {
			_, apiErr := svc.DispatchCommand(ctx, service.CommandInput{
				DeliveryID: cmd.DeliveryID,
				Command:    cmd.Command,
				Text:       cmd.Text,
				ChatAlias:  cmd.ChatAlias,
				SenderID:   cmd.SenderID,
				Metadata:   cmd.Metadata,
			})
			if apiErr != nil {
				return apiErr
			}
			return nil
		}, logger)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	errCh := make(chan error, 5)
	if cfg.SocketPath != "" {
		go func() { errCh <- httpServer.ListenAndServeUnix(ctx, cfg.SocketPath) }()
	}
	if cfg.BindAddr != "" {
		go func() { errCh <- httpServer.ListenAndServeTCP(ctx, cfg.BindAddr) }()
	}
	if cfg.GRPCSocketPath != "" {
		go func() { errCh <- grpcServer.ListenAndServeUnix(ctx, cfg.GRPCSocketPath) }()
	}
	if cfg.GRPCBindAddr != "" {
		go func() { errCh <- grpcServer.ListenAndServeTCP(ctx, cfg.GRPCBindAddr) }()
	}
	if commandReceiver != nil {
		go func() { errCh <- commandReceiver.Start(ctx) }()
	}

	select {
	case <-ctx.Done():
		logger.Info("shutdown requested")
	case err := <-errCh:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("server stopped", "error", err)
			os.Exit(1)
		}
	}
	if commandReceiver != nil {
		commandReceiver.Close()
	}

	// Drain both transports concurrently so a slow HTTP shutdown cannot consume
	// the gRPC server's graceful-stop budget and force a hard stop.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	shutdownErrs := make([]error, 2)
	wg.Add(2)
	go func() { defer wg.Done(); shutdownErrs[0] = httpServer.Shutdown(shutdownCtx) }()
	go func() { defer wg.Done(); shutdownErrs[1] = grpcServer.Shutdown(shutdownCtx) }()
	wg.Wait()
	if err := errors.Join(shutdownErrs...); err != nil {
		logger.Error("graceful shutdown failed", "error", err)
		os.Exit(1)
	}
}
