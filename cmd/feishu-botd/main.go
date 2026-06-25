package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"feishu-botd/internal/config"
	"feishu-botd/internal/dedupe"
	"feishu-botd/internal/feishu"
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

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	errCh := make(chan error, 2)
	if cfg.SocketPath != "" {
		go func() { errCh <- httpServer.ListenAndServeUnix(ctx, cfg.SocketPath) }()
	}
	if cfg.BindAddr != "" {
		go func() { errCh <- httpServer.ListenAndServeTCP(ctx, cfg.BindAddr) }()
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

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		logger.Error("graceful shutdown failed", "error", err)
		os.Exit(1)
	}
}
