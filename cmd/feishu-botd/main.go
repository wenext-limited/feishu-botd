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

	"github.com/oops-rs/feishu-botd/internal/config"
	"github.com/oops-rs/feishu-botd/internal/dedupe"
	"github.com/oops-rs/feishu-botd/internal/feishu"
	"github.com/oops-rs/feishu-botd/internal/httpapi"
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
	server := httpapi.NewServer(cfg, sender, store, logger)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	errCh := make(chan error, 2)
	if cfg.SocketPath != "" {
		go func() { errCh <- server.ListenAndServeUnix(ctx, cfg.SocketPath) }()
	}
	if cfg.BindAddr != "" {
		go func() { errCh <- server.ListenAndServeTCP(ctx, cfg.BindAddr) }()
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
	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Error("graceful shutdown failed", "error", err)
		os.Exit(1)
	}
}
