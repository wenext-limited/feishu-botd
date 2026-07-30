package config

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func loadMultiAppFixture(t *testing.T, body string) (Config, error) {
	t.Helper()
	clearConfigEnv(t)
	path := filepath.Join(t.TempDir(), "feishu-botd.json")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("FEISHU_BOTD_CONFIG", path)
	return LoadFromEnv()
}

func TestLoadFromConfigFileMultiAppShapes(t *testing.T) {
	tests := []struct {
		name    string
		body    string
		wantErr string
		check   func(*testing.T, Config)
	}{
		{
			name: "legacy only preserves exact default projection",
			body: `{
				"feishu":{"app_id":"legacy_app","app_secret":"legacy_secret"},
				"listeners":{"grpc_socket":"/tmp/legacy.sock"},
				"commands":{"enabled":true,"bot_open_id":"ou_legacy","bot_names":["Legacy Bot"]},
				"channels":{"Legacy_Ops":"oc_legacy"}
			}`,
			check: func(t *testing.T, cfg Config) {
				t.Helper()
				if cfg.AppID != "legacy_app" || cfg.AppSecret != "legacy_secret" {
					t.Fatalf("legacy credentials projection changed: %#v", cfg)
				}
				if !cfg.Commands.Enabled || cfg.Commands.BotOpenID != "ou_legacy" ||
					!reflect.DeepEqual(cfg.Commands.BotNames, []string{"Legacy Bot"}) {
					t.Fatalf("legacy command projection changed: %#v", cfg.Commands)
				}
				if !reflect.DeepEqual(cfg.Channels, map[string]string{"legacy-ops": "oc_legacy"}) {
					t.Fatalf("legacy channel projection changed: %#v", cfg.Channels)
				}
				apps := cfg.EffectiveApps()
				if len(apps) != 1 {
					t.Fatalf("effective apps = %#v", apps)
				}
				app := apps[DefaultAppAlias]
				if app.AppID != cfg.AppID || app.AppSecret != cfg.AppSecret ||
					!reflect.DeepEqual(app.Commands, cfg.Commands) ||
					!reflect.DeepEqual(app.Channels, cfg.Channels) {
					t.Fatalf("implicit default does not match legacy config: %#v", app)
				}
				if alias, chatID, ok := cfg.ResolveChannel("LEGACY_OPS"); !ok || alias != DefaultAppAlias || chatID != "oc_legacy" {
					t.Fatalf("legacy route = (%q, %q, %t)", alias, chatID, ok)
				}
			},
		},
		{
			name: "two named apps",
			body: `{
				"listeners":{"grpc_socket":"/tmp/two-app.sock"},
				"apps":{
					"alpha":{
						"app_id":"app_alpha",
						"app_secret":"secret_alpha",
						"commands":{"enabled":true,"bot_names":["Alpha Bot"]},
						"channels":{}
					},
					"beta":{
						"app_id":"app_beta",
						"app_secret":"secret_beta",
						"commands":{"enabled":false},
						"channels":{"Ops":"oc_beta"}
					}
				}
			}`,
			check: func(t *testing.T, cfg Config) {
				t.Helper()
				if cfg.AppID != "" || cfg.AppSecret != "" {
					t.Fatalf("apps-only config synthesized legacy credentials: %#v", cfg)
				}
				if got := cfg.AppAliases(); !reflect.DeepEqual(got, []string{"alpha", "beta"}) {
					t.Fatalf("app aliases = %q", got)
				}
				apps := cfg.EffectiveApps()
				if !apps["alpha"].Commands.Enabled || apps["beta"].Commands.Enabled {
					t.Fatalf("per-app commands were not retained: %#v", apps)
				}
				if !reflect.DeepEqual(cfg.Channels, map[string]string{"ops": "oc_beta"}) {
					t.Fatalf("global channel projection = %#v", cfg.Channels)
				}
				if alias, chatID, ok := cfg.ResolveChannel("ops"); !ok || alias != "beta" || chatID != "oc_beta" {
					t.Fatalf("named app route = (%q, %q, %t)", alias, chatID, ok)
				}
			},
		},
		{
			name: "legacy and named apps coexist",
			body: `{
				"feishu":{"app_id":"app_default","app_secret":"secret_default"},
				"listeners":{"grpc_socket":"/tmp/coexist.sock"},
				"commands":{"enabled":true,"bot_names":["Default Bot"]},
				"channels":{"legacy":"oc_default"},
				"apps":{
					"support":{
						"app_id":"app_support",
						"app_secret":"secret_support",
						"commands":{"enabled":true,"bot_names":["Support Bot"]},
						"channels":{"support":"oc_support"}
					}
				}
			}`,
			check: func(t *testing.T, cfg Config) {
				t.Helper()
				if got := cfg.AppAliases(); !reflect.DeepEqual(got, []string{DefaultAppAlias, "support"}) {
					t.Fatalf("app aliases = %q", got)
				}
				if alias, _, ok := cfg.ResolveChannel("legacy"); !ok || alias != DefaultAppAlias {
					t.Fatalf("legacy channel owner = %q, found=%t", alias, ok)
				}
				if alias, _, ok := cfg.ResolveChannel("support"); !ok || alias != "support" {
					t.Fatalf("named channel owner = %q, found=%t", alias, ok)
				}
			},
		},
		{
			name: "reserved explicit default alias",
			body: `{
				"listeners":{"grpc_socket":"/tmp/reserved.sock"},
				"apps":{"default":{
					"app_id":"app_default",
					"app_secret":"secret_default",
					"commands":{"enabled":true}
				}}
			}`,
			wantErr: "reserved",
		},
		{
			name: "app aliases reject control bytes",
			body: `{
				"listeners":{"grpc_socket":"/tmp/control-alias.sock"},
				"apps":{"a\u0000b":{
					"app_id":"app_control",
					"app_secret":"secret_control",
					"commands":{"enabled":true}
				}}
			}`,
			wantErr: "must match",
		},
		{
			name: "duplicate app id across named apps",
			body: `{
				"listeners":{"grpc_socket":"/tmp/duplicate-id.sock"},
				"apps":{
					"alpha":{"app_id":"same_app","app_secret":"secret_alpha","commands":{"enabled":true}},
					"beta":{"app_id":"same_app","app_secret":"secret_beta","commands":{"enabled":true}}
				}
			}`,
			wantErr: "distinct app_id",
		},
		{
			name: "duplicate app id across legacy and named app",
			body: `{
				"feishu":{"app_id":"same_app","app_secret":"secret_default"},
				"listeners":{"grpc_socket":"/tmp/duplicate-default-id.sock"},
				"commands":{"enabled":true},
				"apps":{
					"alpha":{"app_id":"same_app","app_secret":"secret_alpha","commands":{"enabled":true}}
				}
			}`,
			wantErr: "distinct app_id",
		},
		{
			name: "global channel alias uniqueness after normalization",
			body: `{
				"listeners":{"grpc_socket":"/tmp/duplicate-alias.sock"},
				"apps":{
					"alpha":{"app_id":"app_alpha","app_secret":"secret_alpha","channels":{"ON_CALL":"oc_alpha"}},
					"beta":{"app_id":"app_beta","app_secret":"secret_beta","channels":{"on-call":"oc_beta"}}
				}
			}`,
			wantErr: "channel alias \"on-call\"",
		},
		{
			name: "legacy and named channel aliases share one namespace",
			body: `{
				"feishu":{"app_id":"app_default","app_secret":"secret_default"},
				"listeners":{"grpc_socket":"/tmp/duplicate-legacy-alias.sock"},
				"channels":{"Ops":"oc_default"},
				"apps":{
					"alpha":{"app_id":"app_alpha","app_secret":"secret_alpha","channels":{"ops":"oc_alpha"}}
				}
			}`,
			wantErr: "channel alias \"ops\"",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg, err := loadMultiAppFixture(t, test.body)
			if test.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), test.wantErr) {
					t.Fatalf("error = %v, want substring %q", err, test.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("load: %v", err)
			}
			test.check(t, cfg)
		})
	}
}

func TestLegacyEnvironmentOverridesOnlyDefaultApp(t *testing.T) {
	clearConfigEnv(t)
	path := filepath.Join(t.TempDir(), "feishu-botd.json")
	body := `{
		"feishu":{"app_id":"file_default","app_secret":"file_default_secret"},
		"listeners":{"grpc_socket":"/tmp/env-default.sock"},
		"commands":{"enabled":false,"bot_names":["File Default"]},
		"channels":{"legacy":"oc_file_default"},
		"apps":{
			"alpha":{
				"app_id":"app_alpha",
				"app_secret":"secret_alpha",
				"commands":{"enabled":true,"bot_names":["Alpha Bot"]},
				"channels":{"alpha":"oc_alpha"}
			}
		}
	}`
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("FEISHU_BOTD_CONFIG", path)
	t.Setenv("FEISHU_APP_ID", "env_default")
	t.Setenv("FEISHU_APP_SECRET", "env_default_secret")
	t.Setenv("FEISHU_BOTD_COMMANDS_ENABLED", "true")
	t.Setenv("FEISHU_BOTD_BOT_NAMES", "Env Default")
	t.Setenv("FEISHU_BOTD_CHANNELS_LEGACY", "oc_env_default")

	cfg, err := LoadFromEnv()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	apps := cfg.EffectiveApps()
	if got := apps[DefaultAppAlias]; got.AppID != "env_default" ||
		got.AppSecret != "env_default_secret" ||
		!got.Commands.Enabled ||
		!reflect.DeepEqual(got.Commands.BotNames, []string{"Env Default"}) ||
		got.Channels["legacy"] != "oc_env_default" {
		t.Fatalf("default env override = %#v", got)
	}
	if got := apps["alpha"]; got.AppID != "app_alpha" ||
		!reflect.DeepEqual(got.Commands.BotNames, []string{"Alpha Bot"}) ||
		got.Channels["alpha"] != "oc_alpha" {
		t.Fatalf("named app changed by legacy env: %#v", got)
	}
}

func TestLegacyEnvironmentCreatesDefaultBesideNamedApps(t *testing.T) {
	clearConfigEnv(t)
	path := filepath.Join(t.TempDir(), "feishu-botd.json")
	body := `{
		"listeners":{"grpc_socket":"/tmp/env-created-default.sock"},
		"apps":{
			"alpha":{
				"app_id":"app_alpha",
				"app_secret":"secret_alpha",
				"commands":{"enabled":true}
			}
		}
	}`
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("FEISHU_BOTD_CONFIG", path)
	t.Setenv("FEISHU_APP_ID", "env_default")
	t.Setenv("FEISHU_APP_SECRET", "env_default_secret")
	t.Setenv("FEISHU_BOTD_COMMANDS_ENABLED", "true")

	cfg, err := LoadFromEnv()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if got := cfg.AppAliases(); !reflect.DeepEqual(got, []string{"alpha", DefaultAppAlias}) {
		t.Fatalf("app aliases = %q", got)
	}
	if got := cfg.EffectiveApps()[DefaultAppAlias]; got.AppID != "env_default" || !got.Commands.Enabled {
		t.Fatalf("environment-created default = %#v", got)
	}
}

func TestLegacyEnvironmentDefaultParticipatesInDuplicateAppIDValidation(t *testing.T) {
	clearConfigEnv(t)
	path := filepath.Join(t.TempDir(), "feishu-botd.json")
	body := `{
		"listeners":{"grpc_socket":"/tmp/env-duplicate-default.sock"},
		"apps":{
			"alpha":{
				"app_id":"same_app",
				"app_secret":"secret_alpha",
				"commands":{"enabled":true}
			}
		}
	}`
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("FEISHU_BOTD_CONFIG", path)
	t.Setenv("FEISHU_APP_ID", "same_app")
	t.Setenv("FEISHU_APP_SECRET", "env_default_secret")
	t.Setenv("FEISHU_BOTD_COMMANDS_ENABLED", "true")

	_, err := LoadFromEnv()
	if err == nil || !strings.Contains(err.Error(), "distinct app_id") {
		t.Fatalf("expected duplicate app_id error, got %v", err)
	}
}

func TestAppsOnlyRejectsOrphanLegacyPerAppSettings(t *testing.T) {
	tests := []struct {
		name  string
		extra string
	}{
		{name: "top-level channels", extra: `,"channels":{"legacy":"oc_legacy"}`},
		{name: "top-level commands", extra: `,"commands":{"enabled":true}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			body := `{
				"listeners":{"grpc_socket":"/tmp/orphan-legacy.sock"},
				"apps":{
					"alpha":{"app_id":"app_alpha","app_secret":"secret_alpha","commands":{"enabled":true}}
				}` + test.extra + `
			}`
			_, err := loadMultiAppFixture(t, body)
			if err == nil || !strings.Contains(err.Error(), "legacy commands or channels require") {
				t.Fatalf("expected orphan legacy settings error, got %v", err)
			}
		})
	}
}

func TestProviderAllowedAppsSemantics(t *testing.T) {
	const token = "fixture-agent-token-0123456789abcdef0123456789"
	tests := []struct {
		name           string
		field          string
		wantConfigured bool
		wantAllowed    map[string]bool
		wantErr        string
	}{
		{
			name:           "absent means all",
			wantAllowed:    map[string]bool{"alpha": true, "beta": true},
			wantConfigured: false,
		},
		{
			name:           "present empty means none",
			field:          `,"allowed_apps":[]`,
			wantAllowed:    map[string]bool{"alpha": false, "beta": false},
			wantConfigured: true,
		},
		{
			name:           "present list is exact allowlist",
			field:          `,"allowed_apps":[" alpha ","alpha"]`,
			wantAllowed:    map[string]bool{"alpha": true, "beta": false},
			wantConfigured: true,
		},
		{
			name:    "unknown alias fails closed",
			field:   `,"allowed_apps":["missing"]`,
			wantErr: "unknown app alias",
		},
		{
			name:    "null cannot widen authority",
			field:   `,"allowed_apps":null`,
			wantErr: "must be an array, not null",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			clearConfigEnv(t)
			dir := t.TempDir()
			tokenPath := filepath.Join(dir, "agent-token")
			if err := os.WriteFile(tokenPath, []byte(token+"\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			body := fmt.Sprintf(`{
				"listeners":{"grpc_socket":"/tmp/provider-apps.sock"},
				"apps":{
					"alpha":{"app_id":"app_alpha","app_secret":"secret_alpha","commands":{"enabled":true}},
					"beta":{"app_id":"app_beta","app_secret":"secret_beta","commands":{"enabled":true}}
				},
				"agent_providers":{
					"fixture-agent":{"auth_token_file":%q%s}
				}
			}`, tokenPath, test.field)
			path := filepath.Join(dir, "feishu-botd.json")
			if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
				t.Fatal(err)
			}
			t.Setenv("FEISHU_BOTD_CONFIG", path)

			cfg, err := LoadFromEnv()
			if test.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), test.wantErr) {
					t.Fatalf("error = %v, want substring %q", err, test.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("load: %v", err)
			}
			provider := cfg.AgentProviders["fixture-agent"]
			if provider.AllowedAppsConfigured != test.wantConfigured {
				t.Fatalf("AllowedAppsConfigured = %t, want %t", provider.AllowedAppsConfigured, test.wantConfigured)
			}
			for alias, want := range test.wantAllowed {
				if got := cfg.ProviderAllowsApp("fixture-agent", alias); got != want {
					t.Fatalf("ProviderAllowsApp(%q) = %t, want %t", alias, got, want)
				}
			}
		})
	}
}

func TestProviderAllowsAppTreatsProgrammaticNonNilListAsConfigured(t *testing.T) {
	cfg := Config{AgentProviders: map[string]AgentProviderConfig{
		"none":  {AllowedApps: []string{}},
		"alpha": {AllowedApps: []string{"alpha"}},
		"all":   {},
	}}
	if cfg.ProviderAllowsApp("none", "alpha") {
		t.Fatal("programmatic explicit empty allowlist widened to all apps")
	}
	if !cfg.ProviderAllowsApp("alpha", "alpha") || cfg.ProviderAllowsApp("alpha", "beta") {
		t.Fatal("programmatic non-empty allowlist was not enforced")
	}
	if !cfg.ProviderAllowsApp("all", "beta") {
		t.Fatal("programmatic absent allowlist did not allow all apps")
	}
}

func TestMultiAppStrictDecoding(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{
			name: "unknown app field",
			body: `{
				"listeners":{"grpc_socket":"/tmp/strict-app.sock"},
				"apps":{"alpha":{
					"app_id":"app_alpha",
					"app_secret":"secret_alpha",
					"commands":{"enabled":true},
					"app_secert":"typo"
				}}
			}`,
		},
		{
			name: "unknown nested commands field",
			body: `{
				"listeners":{"grpc_socket":"/tmp/strict-command.sock"},
				"apps":{"alpha":{
					"app_id":"app_alpha",
					"app_secret":"secret_alpha",
					"commands":{"enabled":true,"bot_name":["typo"]}
				}}
			}`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := loadMultiAppFixture(t, test.body)
			if err == nil || !strings.Contains(err.Error(), "unknown field") {
				t.Fatalf("expected strict unknown-field error, got %v", err)
			}
		})
	}
}

func TestPerAppScriptValidationUsesOwnedChannels(t *testing.T) {
	scriptsDir := t.TempDir()
	tests := []struct {
		name        string
		allowedChat string
		wantErr     string
	}{
		{name: "own channel", allowedChat: "alpha"},
		{name: "other app channel", allowedChat: "beta", wantErr: "not a configured channel for app \"alpha\""},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			body := fmt.Sprintf(`{
				"listeners":{"grpc_socket":"/tmp/app-script.sock"},
				"apps":{
					"alpha":{
						"app_id":"app_alpha",
						"app_secret":"secret_alpha",
						"commands":{
							"enabled":true,
							"scripts":{
								"enabled":true,
								"command":"ops",
								"dir":%q,
								"allowed_chats":[%q]
							}
						},
						"channels":{"alpha":"oc_alpha"}
					},
					"beta":{
						"app_id":"app_beta",
						"app_secret":"secret_beta",
						"channels":{"beta":"oc_beta"}
					}
				}
			}`, scriptsDir, test.allowedChat)
			_, err := loadMultiAppFixture(t, body)
			if test.wantErr == "" {
				if err != nil {
					t.Fatalf("load: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("error = %v, want substring %q", err, test.wantErr)
			}
		})
	}
}

func TestEffectiveAppsLegacyFallbackIsDefensive(t *testing.T) {
	cfg := Config{
		AppID:     "legacy_app",
		AppSecret: "legacy_secret",
		Commands:  CommandConfig{Enabled: true, BotNames: []string{"Legacy Bot"}},
		Channels:  map[string]string{"ops": "oc_legacy"},
	}
	apps := cfg.EffectiveApps()
	app := apps[DefaultAppAlias]
	app.Commands.BotNames[0] = "mutated"
	app.Channels["ops"] = "mutated"
	apps[DefaultAppAlias] = app
	delete(apps, DefaultAppAlias)

	fresh := cfg.EffectiveApps()[DefaultAppAlias]
	if fresh.Commands.BotNames[0] != "Legacy Bot" || fresh.Channels["ops"] != "oc_legacy" {
		t.Fatalf("EffectiveApps leaked mutable state: %#v", fresh)
	}
	if alias, chatID, ok := cfg.ResolveChannel("ops"); !ok || alias != DefaultAppAlias || chatID != "oc_legacy" {
		t.Fatalf("legacy ResolveChannel fallback = (%q, %q, %t)", alias, chatID, ok)
	}

	canonicalEmpty := cfg
	canonicalEmpty.ChannelRoutes = map[string]ChannelRoute{}
	if _, _, ok := canonicalEmpty.ResolveChannel("ops"); ok {
		t.Fatal("empty canonical route registry fell back to legacy channels")
	}
}
