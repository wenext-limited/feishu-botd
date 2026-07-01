// Package scriptexec resolves an inbound command's action word to a local
// script by naming convention and executes it, capturing output for a chat
// reply. It is deliberately independent of the service/notify packages —
// callers translate its plain string results into whatever transport-level
// response type they need.
package scriptexec

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"
	"time"

	"feishu-botd/internal/config"
)

// outputCap bounds captured stdout+stderr so a runaway script can't produce a
// reply too large for Feishu (matching the 8000-char Text field cap already
// enforced in internal/service/command.go).
const outputCap = 4000

var actionPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]*$`)

// Executor runs "<Dir>/<Command>-<action>.sh" scripts for inbound commands
// whose chat is in AllowedChats.
type Executor struct {
	dir          string
	command      string
	allowedChats map[string]struct{}
	timeout      time.Duration
	logger       *slog.Logger
}

// New builds an Executor from an already-normalized ScriptExecConfig (see
// internal/config.LoadFromEnv, which defaults TimeoutSeconds and normalizes
// AllowedChats before this is called in production).
func New(cfg config.ScriptExecConfig, logger *slog.Logger) *Executor {
	if logger == nil {
		logger = slog.Default()
	}
	allowed := make(map[string]struct{}, len(cfg.AllowedChats))
	for _, chat := range cfg.AllowedChats {
		allowed[chat] = struct{}{}
	}
	return &Executor{
		dir:          cfg.Dir,
		command:      cfg.Command,
		allowedChats: allowed,
		timeout:      time.Duration(cfg.TimeoutSeconds) * time.Second,
		logger:       logger,
	}
}

// Run resolves and executes the script for a single inbound command's Text
// field (e.g. "build ludo develop") and returns a chat-ready title/markdown
// reply. It never returns an error: every failure mode is reported as a
// user-facing markdown explanation instead, and logged.
func (e *Executor) Run(ctx context.Context, chatAlias, text string) (title, markdown string) {
	if _, ok := e.allowedChats[chatAlias]; !ok {
		e.logger.Warn("script command rejected: chat not allowed", "chat", chatAlias)
		return e.command, fmt.Sprintf("chat %q is not allowed to run `%s` commands.", chatAlias, e.command)
	}

	fields := strings.Fields(text)
	if len(fields) == 0 {
		return e.command, fmt.Sprintf("usage: `%s <action> [args...]`", e.command)
	}
	action := strings.ToLower(fields[0])
	args := fields[1:]
	title = e.command + " " + action

	if !actionPattern.MatchString(action) {
		e.logger.Warn("script command rejected: invalid action", "chat", chatAlias, "action", action)
		return title, fmt.Sprintf("invalid action %q: must match `%s`.", action, actionPattern.String())
	}

	scriptPath, err := e.resolveScript(action)
	if err != nil {
		e.logger.Warn("script command rejected", "chat", chatAlias, "action", action, "error", err)
		return title, fmt.Sprintf("no script configured for action %q.", action)
	}

	return title, e.execute(ctx, scriptPath, title, args)
}

// resolveScript maps action to "<dir>/<command>-<action>.sh" and verifies the
// result stays inside dir, exists, is a regular file, and is executable.
// Lstat (not Stat) is used deliberately: a symlink dropped into dir must be
// rejected rather than trusted via its target's permissions/type, since dir
// is the whole security boundary for what this executor may run.
func (e *Executor) resolveScript(action string) (string, error) {
	scriptPath := filepath.Clean(filepath.Join(e.dir, e.command+"-"+action+".sh"))
	if filepath.Dir(scriptPath) != filepath.Clean(e.dir) {
		return "", errors.New("resolved path escapes scripts directory")
	}
	info, err := os.Lstat(scriptPath)
	if err != nil {
		return "", err
	}
	if !info.Mode().IsRegular() {
		return "", errors.New("not a regular file")
	}
	if info.Mode()&0o111 == 0 {
		return "", errors.New("not executable")
	}
	return scriptPath, nil
}

func (e *Executor) execute(ctx context.Context, scriptPath, title string, args []string) string {
	runCtx, cancel := context.WithTimeout(ctx, e.timeout)
	defer cancel()

	out := &limitWriter{limit: outputCap}
	cmd := exec.Command(scriptPath, args...)
	cmd.Stdout = out
	cmd.Stderr = out
	// Run the script as its own process group leader so a timeout can kill
	// any subprocesses it spawns (fastlane, xcodebuild, curl, ...). Killing
	// only the direct child leaves such grandchildren holding the stdout/
	// stderr pipes open, and cmd.Wait() would block on them regardless of
	// the deadline.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	if err := cmd.Start(); err != nil {
		e.logger.Error("script failed to start", "script", scriptPath, "error", err)
		return fmt.Sprintf("**%s** failed to start: %s", title, err)
	}

	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()

	select {
	case <-runCtx.Done():
		// The process may have exited at almost exactly the deadline instant
		// (select doesn't order this against runCtx.Done() becoming ready).
		// Prefer a real result over sending a signal at a pid that may have
		// already been reaped and reused for an unrelated process.
		select {
		case runErr := <-done:
			return e.formatResult(title, scriptPath, runErr, out)
		default:
		}
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		<-done
		e.logger.Error("script timed out", "script", scriptPath, "timeout", e.timeout)
		return fmt.Sprintf("**%s** timed out after %s.\n\n```\n%s\n```", title, e.timeout, out.String())
	case runErr := <-done:
		return e.formatResult(title, scriptPath, runErr, out)
	}
}

func (e *Executor) formatResult(title, scriptPath string, runErr error, out *limitWriter) string {
	exitCode := 0
	if runErr != nil {
		var exitErr *exec.ExitError
		if errors.As(runErr, &exitErr) {
			exitCode = exitErr.ExitCode()
		} else {
			e.logger.Error("script failed to run", "script", scriptPath, "error", runErr)
			return fmt.Sprintf("**%s** failed to run: %s", title, runErr)
		}
	}
	return fmt.Sprintf("**%s**\n\nexit code: %d\n\n```\n%s\n```", title, exitCode, out.String())
}

// limitWriter caps total bytes retained and appends a truncation marker once
// the cap is reached, without ever returning a write error (an error here
// would abort exec.Cmd's output copy and corrupt exit-code handling).
type limitWriter struct {
	buf       bytes.Buffer
	limit     int
	truncated bool
}

func (w *limitWriter) Write(p []byte) (int, error) {
	if !w.truncated {
		remaining := w.limit - w.buf.Len()
		if remaining <= 0 {
			w.truncated = true
		} else if len(p) > remaining {
			w.buf.Write(p[:remaining])
			w.truncated = true
		} else {
			w.buf.Write(p)
		}
	}
	return len(p), nil
}

func (w *limitWriter) String() string {
	if w.truncated {
		return w.buf.String() + "\n(output truncated)"
	}
	return w.buf.String()
}
