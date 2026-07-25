package feishu

import (
	"context"
	"log/slog"
)

// safeSDKLogger deliberately discards every SDK-provided argument. The SDK
// includes request URLs, query credentials, routing identifiers, callback
// payloads, and raw errors in those arguments, so only a fixed source/level
// classification is forwarded to the daemon logger.
type safeSDKLogger struct {
	logger *slog.Logger
}

func (l safeSDKLogger) Debug(ctx context.Context, _ ...interface{}) {
	l.log(ctx, slog.LevelDebug, "sdk_debug")
}

func (l safeSDKLogger) Info(ctx context.Context, _ ...interface{}) {
	l.log(ctx, slog.LevelInfo, "sdk_info")
}

func (l safeSDKLogger) Warn(ctx context.Context, _ ...interface{}) {
	l.log(ctx, slog.LevelWarn, "sdk_warn")
}

func (l safeSDKLogger) Error(ctx context.Context, _ ...interface{}) {
	l.log(ctx, slog.LevelError, "sdk_error")
}

func (l safeSDKLogger) log(ctx context.Context, level slog.Level, eventClass string) {
	if l.logger == nil {
		return
	}
	l.logger.Log(ctx, level, "feishu sdk event", "event_class", eventClass)
}
