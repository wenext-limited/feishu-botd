package scriptexec

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"feishu-botd/internal/config"
)

func writeScript(t *testing.T, dir, name, body string, executable bool) string {
	t.Helper()
	path := filepath.Join(dir, name)
	mode := os.FileMode(0o644)
	if executable {
		mode = 0o755
	}
	if err := os.WriteFile(path, []byte(body), mode); err != nil {
		t.Fatal(err)
	}
	return path
}

func newTestExecutor(t *testing.T, dir string) *Executor {
	t.Helper()
	return New(config.ScriptExecConfig{
		Command:        "pls",
		Dir:            dir,
		AllowedChats:   []string{"ops"},
		TimeoutSeconds: 5,
	}, nil)
}

func TestRunExecutesResolvedScriptWithArgs(t *testing.T) {
	dir := t.TempDir()
	writeScript(t, dir, "pls-build.sh", "#!/bin/sh\nfor a in \"$@\"; do echo \"arg:$a\"; done\n", true)
	e := newTestExecutor(t, dir)

	_, markdown := e.Run(context.Background(), "ops", "build ludo develop")
	if !strings.Contains(markdown, "arg:ludo") || !strings.Contains(markdown, "arg:develop") {
		t.Fatalf("markdown missing expected args: %s", markdown)
	}
	if !strings.Contains(markdown, "exit code: 0") {
		t.Fatalf("markdown missing exit code: %s", markdown)
	}
}

func TestRunRejectsDisallowedChat(t *testing.T) {
	dir := t.TempDir()
	writeScript(t, dir, "pls-build.sh", "#!/bin/sh\necho ran\n", true)
	e := newTestExecutor(t, dir)

	_, markdown := e.Run(context.Background(), "unknown-chat", "build ludo develop")
	if !strings.Contains(markdown, "not allowed") {
		t.Fatalf("expected not-allowed message, got: %s", markdown)
	}
}

func TestRunRejectsInvalidActionName(t *testing.T) {
	dir := t.TempDir()
	e := newTestExecutor(t, dir)

	for _, text := range []string{"../etc/passwd arg", "build/ludo arg", "build; rm -rf / arg", "/absolute arg"} {
		_, markdown := e.Run(context.Background(), "ops", text)
		if !strings.Contains(markdown, "invalid action") {
			t.Fatalf("text %q: expected invalid action message, got: %s", text, markdown)
		}
	}
}

func TestRunRejectsMissingText(t *testing.T) {
	dir := t.TempDir()
	e := newTestExecutor(t, dir)

	_, markdown := e.Run(context.Background(), "ops", "")
	if !strings.Contains(markdown, "usage") {
		t.Fatalf("expected usage message, got: %s", markdown)
	}
}

func TestRunRejectsUnknownAction(t *testing.T) {
	dir := t.TempDir()
	e := newTestExecutor(t, dir)

	_, markdown := e.Run(context.Background(), "ops", "deploy ludo")
	if !strings.Contains(markdown, "no script configured") {
		t.Fatalf("expected no-script message, got: %s", markdown)
	}
}

func TestRunRejectsNonExecutableScript(t *testing.T) {
	dir := t.TempDir()
	writeScript(t, dir, "pls-build.sh", "#!/bin/sh\necho ran\n", false)
	e := newTestExecutor(t, dir)

	_, markdown := e.Run(context.Background(), "ops", "build ludo")
	if !strings.Contains(markdown, "no script configured") {
		t.Fatalf("expected no-script message for non-executable script, got: %s", markdown)
	}
}

func TestRunActionIsCaseInsensitiveArgsPreserveCase(t *testing.T) {
	dir := t.TempDir()
	writeScript(t, dir, "pls-build.sh", "#!/bin/sh\nfor a in \"$@\"; do echo \"arg:$a\"; done\n", true)
	e := newTestExecutor(t, dir)

	_, markdown := e.Run(context.Background(), "ops", "BUILD Develop")
	if !strings.Contains(markdown, "arg:Develop") {
		t.Fatalf("expected case-preserved arg, got: %s", markdown)
	}
}

func TestRunDoesNotInterpretArgsAsShell(t *testing.T) {
	dir := t.TempDir()
	writeScript(t, dir, "pls-build.sh", "#!/bin/sh\nfor a in \"$@\"; do echo \"arg:$a\"; done\n", true)
	e := newTestExecutor(t, dir)

	_, markdown := e.Run(context.Background(), "ops", "build $(whoami) `id`")
	if !strings.Contains(markdown, "arg:$(whoami)") || !strings.Contains(markdown, "arg:`id`") {
		t.Fatalf("expected literal unevaluated args, got: %s", markdown)
	}
}

func TestRunEnforcesTimeout(t *testing.T) {
	dir := t.TempDir()
	writeScript(t, dir, "pls-build.sh", "#!/bin/sh\nsleep 3\necho done\n", true)
	e := New(config.ScriptExecConfig{
		Command:        "pls",
		Dir:            dir,
		AllowedChats:   []string{"ops"},
		TimeoutSeconds: 1,
	}, nil)

	start := time.Now()
	_, markdown := e.Run(context.Background(), "ops", "build")
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("timeout not enforced, took %s", elapsed)
	}
	if !strings.Contains(markdown, "timed out") {
		t.Fatalf("expected timeout message, got: %s", markdown)
	}
}

func TestRunTruncatesLargeOutput(t *testing.T) {
	dir := t.TempDir()
	writeScript(t, dir, "pls-build.sh", "#!/bin/sh\nyes x | head -c 20000\n", true)
	e := newTestExecutor(t, dir)

	_, markdown := e.Run(context.Background(), "ops", "build")
	if !strings.Contains(markdown, "truncated") {
		t.Fatalf("expected truncation marker, got length %d", len(markdown))
	}
	if len(markdown) > 6000 {
		t.Fatalf("markdown not capped, length %d", len(markdown))
	}
}

func TestRunRejectsSymlinkedScript(t *testing.T) {
	dir := t.TempDir()
	outsideDir := t.TempDir()
	target := writeScript(t, outsideDir, "real.sh", "#!/bin/sh\necho SHOULD_NOT_RUN\n", true)
	if err := os.Symlink(target, filepath.Join(dir, "pls-build.sh")); err != nil {
		t.Fatal(err)
	}
	e := newTestExecutor(t, dir)

	_, markdown := e.Run(context.Background(), "ops", "build")
	if !strings.Contains(markdown, "no script configured") {
		t.Fatalf("expected no-script message for symlinked script, got: %s", markdown)
	}
	if strings.Contains(markdown, "SHOULD_NOT_RUN") {
		t.Fatalf("symlinked script outside dir was executed: %s", markdown)
	}
}

func TestRunNonZeroExitCode(t *testing.T) {
	dir := t.TempDir()
	writeScript(t, dir, "pls-build.sh", "#!/bin/sh\necho failing\nexit 3\n", true)
	e := newTestExecutor(t, dir)

	_, markdown := e.Run(context.Background(), "ops", "build")
	if !strings.Contains(markdown, "exit code: 3") {
		t.Fatalf("expected exit code 3, got: %s", markdown)
	}
}
