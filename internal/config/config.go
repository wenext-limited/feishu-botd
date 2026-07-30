package config

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	// DefaultAppAlias is reserved for the legacy top-level feishu, commands, and
	// channels configuration. Keeping that app's identity stable preserves every
	// existing environment override and daemon-derived conversation id.
	DefaultAppAlias                 = "default"
	defaultDedupeTTL                = 6 * time.Hour
	defaultSendTimeout              = 15 * time.Second
	defaultScriptExecTimeoutSeconds = 120
	minimumBearerTokenBytes         = 32
)

// AppConfig is one Feishu application's private runtime configuration.
// AppID and AppSecret must never be serialized, logged, or sent to providers.
type AppConfig struct {
	AppID     string
	AppSecret string
	Commands  CommandConfig
	Channels  map[string]string
}

// ChannelRoute resolves one globally unique public channel alias to the Feishu
// application and raw chat id that own it. ChatID remains daemon-private.
type ChannelRoute struct {
	AppAlias string
	ChatID   string
}

type Config struct {
	// AppID, AppSecret, Commands, and Channels are retained as compatibility
	// projections for legacy callers and tests. New runtime code should use
	// EffectiveApps and ResolveChannel. Channels is the global alias-to-chat
	// projection; AppID/AppSecret/Commands describe only the default app.
	AppID          string
	AppSecret      string
	SocketPath     string
	BindAddr       string
	GRPCSocketPath string
	GRPCBindAddr   string
	AuthToken      string
	AgentProviders map[string]AgentProviderConfig
	AllowLANBind   bool
	Commands       CommandConfig
	Channels       map[string]string
	Apps           map[string]AppConfig
	ChannelRoutes  map[string]ChannelRoute
	DefaultChannel string
	Services       map[string]ServiceConfig
	DedupeTTL      time.Duration
	SendTimeout    time.Duration
}

// AgentProviderConfig binds one agent (and, when configured, legacy command)
// provider identity to a credential.
// AuthToken is loaded from the provider's configured token file and must never
// be serialized or logged.
type AgentProviderConfig struct {
	AuthToken              string
	AllowedCommands        []string
	AllowUnmatchedMessages bool
	AllowCardActions       bool
	AllowFollowUpMessages  bool
	AllowLegacyCommands    bool
	// AllowedAppsConfigured distinguishes an absent allowed_apps field (all
	// configured apps) from an explicitly empty list (no apps).
	AllowedApps           []string
	AllowedAppsConfigured bool
}

type CommandConfig struct {
	Enabled    bool             `json:"enabled"`
	BotOpenID  string           `json:"bot_open_id"`
	BotUserID  string           `json:"bot_user_id"`
	BotUnionID string           `json:"bot_union_id"`
	BotNames   []string         `json:"bot_names"`
	Scripts    ScriptExecConfig `json:"scripts"`
}

// ScriptExecConfig enables running a local script for a registered inbound
// command. Command is the trigger word (e.g. "ops"); the action word that
// follows it resolves to "<Dir>/<Command>-<action>.sh". Only chats in
// AllowedChats may trigger execution.
type ScriptExecConfig struct {
	Enabled        bool     `json:"enabled"`
	Command        string   `json:"command"`
	Dir            string   `json:"dir"`
	AllowedChats   []string `json:"allowed_chats"`
	TimeoutSeconds int      `json:"timeout_seconds"`
}

type ServiceConfig struct {
	DefaultChannel string `json:"default_channel"`
}

// EffectiveApps returns a defensive copy of the canonical app registry. The
// legacy fallback keeps programmatic Config literals written before multi-app
// support working unchanged.
func (c Config) EffectiveApps() map[string]AppConfig {
	apps := cloneApps(c.Apps)
	if _, exists := apps[DefaultAppAlias]; !exists && (strings.TrimSpace(c.AppID) != "" || strings.TrimSpace(c.AppSecret) != "") {
		channels := make(map[string]string)
		if c.ChannelRoutes != nil {
			for alias, route := range c.ChannelRoutes {
				if route.AppAlias == DefaultAppAlias {
					channels[alias] = route.ChatID
				}
			}
		} else {
			channels = cloneStringMap(c.Channels)
		}
		apps[DefaultAppAlias] = AppConfig{
			AppID:     strings.TrimSpace(c.AppID),
			AppSecret: strings.TrimSpace(c.AppSecret),
			Commands:  cloneCommandConfig(c.Commands),
			Channels:  channels,
		}
	}
	return apps
}

// ResolveChannel maps one public alias to its private app/chat route. The
// fallback preserves direct Config literals that predate ChannelRoutes.
func (c Config) ResolveChannel(alias string) (appAlias, chatID string, ok bool) {
	alias = normalizeChannelName(alias)
	if alias == "" {
		return "", "", false
	}
	if route, exists := c.ChannelRoutes[alias]; exists {
		appAlias = strings.TrimSpace(route.AppAlias)
		if appAlias == "" {
			appAlias = DefaultAppAlias
		}
		chatID = strings.TrimSpace(route.ChatID)
		return appAlias, chatID, chatID != ""
	}
	if c.ChannelRoutes != nil {
		return "", "", false
	}
	chatID = strings.TrimSpace(c.Channels[alias])
	if chatID == "" {
		return "", "", false
	}
	return DefaultAppAlias, chatID, true
}

// AppAliases returns all configured application aliases in deterministic order.
func (c Config) AppAliases() []string {
	apps := c.EffectiveApps()
	aliases := make([]string, 0, len(apps))
	for alias := range apps {
		aliases = append(aliases, alias)
	}
	sort.Strings(aliases)
	return aliases
}

// ProviderAllowsApp reports whether a configured provider may observe or mutate
// state owned by appAlias. An absent provider entry is unrestricted so internal
// in-process subscribers and legacy unscoped deployments retain their behavior.
func (c Config) ProviderAllowsApp(provider, appAlias string) bool {
	provider = strings.TrimSpace(provider)
	appAlias = strings.TrimSpace(appAlias)
	if appAlias == "" {
		appAlias = DefaultAppAlias
	}
	providerCfg, configured := c.AgentProviders[provider]
	if !configured || (!providerCfg.AllowedAppsConfigured && providerCfg.AllowedApps == nil) {
		return true
	}
	for _, allowed := range providerCfg.AllowedApps {
		if allowed == appAlias {
			return true
		}
	}
	return false
}

func LoadFromEnv() (Config, error) {
	fileCfg, err := loadFileConfig(strings.TrimSpace(os.Getenv("FEISHU_BOTD_CONFIG")))
	if err != nil {
		return Config{}, err
	}

	legacyCommands := commandConfigFromEnv(fileCfg.Commands)
	legacyChannels := mergeStringMaps(fileCfg.Channels, loadChannels(os.Environ()))
	cfg := Config{
		AppID:          firstNonEmpty(os.Getenv("FEISHU_APP_ID"), fileCfg.AppID),
		AppSecret:      firstNonEmpty(os.Getenv("FEISHU_APP_SECRET"), fileCfg.AppSecret),
		SocketPath:     firstNonEmpty(os.Getenv("FEISHU_BOTD_SOCKET"), fileCfg.SocketPath),
		BindAddr:       firstNonEmpty(os.Getenv("FEISHU_BOTD_BIND"), fileCfg.BindAddr),
		GRPCSocketPath: firstNonEmpty(os.Getenv("FEISHU_BOTD_GRPC_SOCKET"), fileCfg.GRPCSocketPath),
		GRPCBindAddr:   firstNonEmpty(os.Getenv("FEISHU_BOTD_GRPC_BIND"), fileCfg.GRPCBindAddr),
		AgentProviders: make(map[string]AgentProviderConfig),
		AllowLANBind:   boolFromEnvDefault("FEISHU_BOTD_ALLOW_NON_LOOPBACK_BIND", fileCfg.AllowLANBind),
		Commands:       legacyCommands,
		Apps:           cloneApps(fileCfg.Apps),
		DefaultChannel: firstNonEmpty(os.Getenv("FEISHU_BOTD_DEFAULT_CHANNEL"), fileCfg.DefaultChannel),
		Services:       fileCfg.Services,
		DedupeTTL:      durationFromEnv("FEISHU_BOTD_DEDUPE_TTL_SECONDS", fileCfg.DedupeTTL),
		SendTimeout:    durationFromEnv("FEISHU_BOTD_SEND_TIMEOUT_SECONDS", fileCfg.SendTimeout),
	}

	hasLegacyCredentials := cfg.AppID != "" || cfg.AppSecret != ""
	switch {
	case hasLegacyCredentials:
		if cfg.AppID == "" {
			return Config{}, errors.New("FEISHU_APP_ID or config feishu.app_id is required")
		}
		if cfg.AppSecret == "" {
			return Config{}, errors.New("FEISHU_APP_SECRET or config feishu.app_secret is required")
		}
		cfg.Apps[DefaultAppAlias] = AppConfig{
			AppID:     cfg.AppID,
			AppSecret: cfg.AppSecret,
			Commands:  cloneCommandConfig(legacyCommands),
			Channels:  cloneStringMap(legacyChannels),
		}
	case len(cfg.Apps) == 0:
		return Config{}, errors.New("FEISHU_APP_ID or config feishu.app_id is required")
	case hasLegacyAppSettings(legacyCommands, legacyChannels):
		return Config{}, errors.New("legacy commands or channels require FEISHU_APP_ID and FEISHU_APP_SECRET or config feishu credentials")
	}
	if err := validateApps(cfg.Apps); err != nil {
		return Config{}, err
	}
	channels, routes, err := buildChannelRoutes(cfg.Apps)
	if err != nil {
		return Config{}, err
	}
	cfg.Channels = channels
	cfg.ChannelRoutes = routes
	if cfg.SocketPath == "" && cfg.BindAddr == "" && cfg.GRPCSocketPath == "" && cfg.GRPCBindAddr == "" {
		return Config{}, errors.New("at least one listener is required: set FEISHU_BOTD_SOCKET, FEISHU_BOTD_BIND, FEISHU_BOTD_GRPC_SOCKET, FEISHU_BOTD_GRPC_BIND, or config listeners")
	}
	if err := validateRouting(cfg); err != nil {
		return Config{}, err
	}
	for provider, providerCfg := range fileCfg.AgentProviders {
		token, err := readTokenFile(providerCfg.AuthTokenFile)
		if err != nil {
			return Config{}, fmt.Errorf("load provider %q token: %w", provider, err)
		}
		allowedApps := []string(nil)
		allowedAppsConfigured := providerCfg.AllowedApps.Set
		if allowedAppsConfigured {
			allowedApps = append([]string{}, providerCfg.AllowedApps.Values...)
		}
		cfg.AgentProviders[provider] = AgentProviderConfig{
			AuthToken:              token,
			AllowedCommands:        append([]string(nil), providerCfg.AllowedCommands...),
			AllowUnmatchedMessages: providerCfg.AllowUnmatchedMessages,
			AllowCardActions:       providerCfg.AllowCardActions,
			AllowFollowUpMessages:  providerCfg.AllowFollowUpMessages,
			AllowLegacyCommands:    providerCfg.AllowLegacyCommands,
			AllowedApps:            allowedApps,
			AllowedAppsConfigured:  allowedAppsConfigured,
		}
	}
	if err := validateAgentProviderApps(cfg.AgentProviders, cfg.Apps); err != nil {
		return Config{}, err
	}
	if err := validateAgentProviderTokens(cfg.AgentProviders, ""); err != nil {
		return Config{}, err
	}
	if len(cfg.AgentProviders) > 0 && cfg.DedupeTTL < time.Hour+cfg.SendTimeout {
		return Config{}, fmt.Errorf("dedupe TTL must be at least one hour plus send timeout when agent_providers is configured")
	}

	// The HTTP and gRPC TCP listeners share a single bearer token. Load it once
	// when either TCP listener is enabled. HTTP can explicitly opt into a LAN
	// bind; plaintext gRPC is always loopback-only.
	hasTCPListener := cfg.BindAddr != "" || cfg.GRPCBindAddr != ""
	if hasTCPListener {
		if cfg.BindAddr != "" {
			if err := validateTCPBind("FEISHU_BOTD_BIND", cfg.BindAddr, cfg.AllowLANBind); err != nil {
				return Config{}, err
			}
		}
		if cfg.GRPCBindAddr != "" {
			if err := validatePlaintextGRPCBind("FEISHU_BOTD_GRPC_BIND", cfg.GRPCBindAddr); err != nil {
				return Config{}, err
			}
		}
	}
	tokenFile := firstNonEmpty(os.Getenv("FEISHU_BOTD_AUTH_TOKEN_FILE"), fileCfg.AuthTokenFile)
	if hasTCPListener && tokenFile == "" {
		return Config{}, errors.New("FEISHU_BOTD_AUTH_TOKEN_FILE or config listeners.auth_token_file is required when a TCP listener is set")
	}
	// A scoped Unix deployment may optionally authorize general outbound HTTP
	// and gRPC callers with the same token-file setting. If omitted, those
	// routes/RPCs fail closed while provider and health interfaces remain
	// available.
	if tokenFile != "" && (hasTCPListener || len(cfg.AgentProviders) > 0) {
		token, err := readTokenFile(tokenFile)
		if err != nil {
			return Config{}, err
		}
		if len(token) < minimumBearerTokenBytes {
			return Config{}, fmt.Errorf("general bearer token must be at least %d bytes", minimumBearerTokenBytes)
		}
		cfg.AuthToken = token
	}
	if err := validateAgentProviderTokens(cfg.AgentProviders, cfg.AuthToken); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

type fileConfig struct {
	AppID          string
	AppSecret      string
	SocketPath     string
	BindAddr       string
	GRPCSocketPath string
	GRPCBindAddr   string
	AuthTokenFile  string
	AgentProviders map[string]fileAgentProviderConfig
	AllowLANBind   bool
	Commands       CommandConfig
	Channels       map[string]string
	Apps           map[string]AppConfig
	DefaultChannel string
	Services       map[string]ServiceConfig
	DedupeTTL      time.Duration
	SendTimeout    time.Duration
}

type configFile struct {
	Feishu             fileFeishuConfig                   `json:"feishu"`
	Apps               map[string]fileAppConfig           `json:"apps"`
	Listeners          fileListenersConfig                `json:"listeners"`
	Commands           CommandConfig                      `json:"commands"`
	Channels           map[string]string                  `json:"channels"`
	DefaultChannel     string                             `json:"default_channel"`
	Services           map[string]ServiceConfig           `json:"services"`
	AgentProviders     map[string]fileAgentProviderConfig `json:"agent_providers"`
	DedupeTTLSeconds   int                                `json:"dedupe_ttl_seconds"`
	SendTimeoutSeconds int                                `json:"send_timeout_seconds"`
}

type fileAgentProviderConfig struct {
	AuthTokenFile          string             `json:"auth_token_file"`
	AllowedCommands        []string           `json:"allowed_commands"`
	AllowedApps            optionalStringList `json:"allowed_apps"`
	AllowUnmatchedMessages bool               `json:"allow_unmatched_messages"`
	AllowCardActions       bool               `json:"allow_card_actions"`
	AllowFollowUpMessages  bool               `json:"allow_follow_up_messages"`
	AllowLegacyCommands    bool               `json:"allow_legacy_commands"`
}

// optionalStringList preserves the security-relevant distinction between an
// omitted allowlist and an explicitly empty one. JSON null is rejected because
// treating it as omission would silently widen provider authority.
type optionalStringList struct {
	Values []string
	Set    bool
}

func (o *optionalStringList) UnmarshalJSON(data []byte) error {
	o.Set = true
	if bytes.Equal(bytes.TrimSpace(data), []byte("null")) {
		return errors.New("allowed_apps must be an array, not null")
	}
	var values []string
	if err := json.Unmarshal(data, &values); err != nil {
		return err
	}
	o.Values = values
	return nil
}

type fileAppConfig struct {
	AppID     string            `json:"app_id"`
	AppSecret string            `json:"app_secret"`
	Commands  CommandConfig     `json:"commands"`
	Channels  map[string]string `json:"channels"`
}

type fileFeishuConfig struct {
	AppID     string `json:"app_id"`
	AppSecret string `json:"app_secret"`
}

type fileListenersConfig struct {
	HTTPSocket           string `json:"http_socket"`
	HTTPBind             string `json:"http_bind"`
	GRPCSocket           string `json:"grpc_socket"`
	GRPCBind             string `json:"grpc_bind"`
	AuthTokenFile        string `json:"auth_token_file"`
	AllowNonLoopbackBind bool   `json:"allow_non_loopback_bind"`
}

func loadFileConfig(path string) (fileConfig, error) {
	cfg := fileConfig{
		Channels:    map[string]string{},
		Apps:        map[string]AppConfig{},
		Services:    map[string]ServiceConfig{},
		DedupeTTL:   defaultDedupeTTL,
		SendTimeout: defaultSendTimeout,
	}
	if path == "" {
		return cfg, nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return fileConfig{}, fmt.Errorf("read FEISHU_BOTD_CONFIG: %w", err)
	}
	var raw configFile
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&raw); err != nil {
		return fileConfig{}, fmt.Errorf("parse FEISHU_BOTD_CONFIG: %w", err)
	}
	cfg.AppID = strings.TrimSpace(raw.Feishu.AppID)
	cfg.AppSecret = strings.TrimSpace(raw.Feishu.AppSecret)
	cfg.SocketPath = strings.TrimSpace(raw.Listeners.HTTPSocket)
	cfg.BindAddr = strings.TrimSpace(raw.Listeners.HTTPBind)
	cfg.GRPCSocketPath = strings.TrimSpace(raw.Listeners.GRPCSocket)
	cfg.GRPCBindAddr = strings.TrimSpace(raw.Listeners.GRPCBind)
	cfg.AuthTokenFile = strings.TrimSpace(raw.Listeners.AuthTokenFile)
	agentProviders, err := normalizeAgentProviderConfigs(raw.AgentProviders)
	if err != nil {
		return fileConfig{}, err
	}
	cfg.AgentProviders = agentProviders
	apps, err := normalizeFileApps(raw.Apps)
	if err != nil {
		return fileConfig{}, err
	}
	cfg.Apps = apps
	cfg.AllowLANBind = raw.Listeners.AllowNonLoopbackBind
	cfg.Commands = normalizeCommandConfig(raw.Commands)
	channels, err := normalizeChannelsStrict(raw.Channels, "legacy default app")
	if err != nil {
		return fileConfig{}, err
	}
	cfg.Channels = channels
	cfg.DefaultChannel = normalizeChannelName(raw.DefaultChannel)
	cfg.Services = normalizeServices(raw.Services)
	if raw.DedupeTTLSeconds > 0 {
		cfg.DedupeTTL = time.Duration(raw.DedupeTTLSeconds) * time.Second
	}
	if raw.SendTimeoutSeconds > 0 {
		cfg.SendTimeout = time.Duration(raw.SendTimeoutSeconds) * time.Second
	}
	return cfg, nil
}

func commandConfigFromEnv(base CommandConfig) CommandConfig {
	cfg := normalizeCommandConfig(base)
	cfg.Enabled = boolFromEnvDefault("FEISHU_BOTD_COMMANDS_ENABLED", cfg.Enabled)
	cfg.BotOpenID = firstNonEmpty(os.Getenv("FEISHU_BOTD_BOT_OPEN_ID"), cfg.BotOpenID)
	cfg.BotUserID = firstNonEmpty(os.Getenv("FEISHU_BOTD_BOT_USER_ID"), cfg.BotUserID)
	cfg.BotUnionID = firstNonEmpty(os.Getenv("FEISHU_BOTD_BOT_UNION_ID"), cfg.BotUnionID)
	if raw := strings.TrimSpace(os.Getenv("FEISHU_BOTD_BOT_NAMES")); raw != "" {
		cfg.BotNames = splitList(raw)
	}
	cfg.Scripts.Enabled = boolFromEnvDefault("FEISHU_BOTD_SCRIPTS_ENABLED", cfg.Scripts.Enabled)
	cfg.Scripts.Command = firstNonEmpty(os.Getenv("FEISHU_BOTD_SCRIPTS_COMMAND"), cfg.Scripts.Command)
	cfg.Scripts.Dir = firstNonEmpty(os.Getenv("FEISHU_BOTD_SCRIPTS_DIR"), cfg.Scripts.Dir)
	if raw := strings.TrimSpace(os.Getenv("FEISHU_BOTD_SCRIPTS_ALLOWED_CHATS")); raw != "" {
		cfg.Scripts.AllowedChats = splitList(raw)
	}
	if raw := strings.TrimSpace(os.Getenv("FEISHU_BOTD_SCRIPTS_TIMEOUT_SECONDS")); raw != "" {
		if seconds, err := strconv.Atoi(raw); err == nil && seconds > 0 {
			cfg.Scripts.TimeoutSeconds = seconds
		}
	}
	return normalizeCommandConfig(cfg)
}

func normalizeCommandConfig(in CommandConfig) CommandConfig {
	return CommandConfig{
		Enabled:    in.Enabled,
		BotOpenID:  strings.TrimSpace(in.BotOpenID),
		BotUserID:  strings.TrimSpace(in.BotUserID),
		BotUnionID: strings.TrimSpace(in.BotUnionID),
		BotNames:   normalizeList(in.BotNames),
		Scripts:    normalizeScriptExecConfig(in.Scripts),
	}
}

func cloneCommandConfig(in CommandConfig) CommandConfig {
	return CommandConfig{
		Enabled:    in.Enabled,
		BotOpenID:  in.BotOpenID,
		BotUserID:  in.BotUserID,
		BotUnionID: in.BotUnionID,
		BotNames:   cloneStrings(in.BotNames),
		Scripts: ScriptExecConfig{
			Enabled:        in.Scripts.Enabled,
			Command:        in.Scripts.Command,
			Dir:            in.Scripts.Dir,
			AllowedChats:   cloneStrings(in.Scripts.AllowedChats),
			TimeoutSeconds: in.Scripts.TimeoutSeconds,
		},
	}
}

func cloneStrings(in []string) []string {
	if in == nil {
		return nil
	}
	return append(make([]string, 0, len(in)), in...)
}

func cloneStringMap(in map[string]string) map[string]string {
	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func cloneApps(in map[string]AppConfig) map[string]AppConfig {
	out := make(map[string]AppConfig, len(in)+1)
	for alias, app := range in {
		out[alias] = AppConfig{
			AppID:     app.AppID,
			AppSecret: app.AppSecret,
			Commands:  cloneCommandConfig(app.Commands),
			Channels:  cloneStringMap(app.Channels),
		}
	}
	return out
}

func normalizeFileApps(in map[string]fileAppConfig) (map[string]AppConfig, error) {
	out := make(map[string]AppConfig, len(in))
	rawAliases := make([]string, 0, len(in))
	for rawAlias := range in {
		rawAliases = append(rawAliases, rawAlias)
	}
	sort.Strings(rawAliases)
	for _, rawAlias := range rawAliases {
		app := in[rawAlias]
		alias := strings.TrimSpace(rawAlias)
		if alias == "" {
			return nil, errors.New("apps contains an empty app alias")
		}
		if len(alias) > 64 {
			return nil, fmt.Errorf("app alias %q exceeds 64 bytes", alias)
		}
		if !validAppAlias(alias) {
			return nil, fmt.Errorf("app alias %q must match [A-Za-z0-9][A-Za-z0-9._-]*", alias)
		}
		if strings.EqualFold(alias, DefaultAppAlias) {
			return nil, fmt.Errorf("apps.%s is reserved for the legacy top-level app", DefaultAppAlias)
		}
		if _, duplicate := out[alias]; duplicate {
			return nil, fmt.Errorf("app alias %q is configured more than once after trimming", alias)
		}
		channels, err := normalizeChannelsStrict(app.Channels, fmt.Sprintf("app %q", alias))
		if err != nil {
			return nil, err
		}
		out[alias] = AppConfig{
			AppID:     strings.TrimSpace(app.AppID),
			AppSecret: strings.TrimSpace(app.AppSecret),
			Commands:  normalizeCommandConfig(app.Commands),
			Channels:  channels,
		}
	}
	return out, nil
}

func validAppAlias(alias string) bool {
	for index := 0; index < len(alias); index++ {
		char := alias[index]
		alphaNumeric := char >= 'a' && char <= 'z' ||
			char >= 'A' && char <= 'Z' ||
			char >= '0' && char <= '9'
		if alphaNumeric {
			continue
		}
		if index > 0 && (char == '.' || char == '_' || char == '-') {
			continue
		}
		return false
	}
	return alias != ""
}

func normalizeChannelsStrict(in map[string]string, owner string) (map[string]string, error) {
	out := make(map[string]string, len(in))
	rawAliases := make([]string, 0, len(in))
	for rawAlias := range in {
		rawAliases = append(rawAliases, rawAlias)
	}
	sort.Strings(rawAliases)
	for _, rawAlias := range rawAliases {
		alias := normalizeChannelName(rawAlias)
		chatID := strings.TrimSpace(in[rawAlias])
		if alias == "" || chatID == "" {
			continue
		}
		if _, duplicate := out[alias]; duplicate {
			return nil, fmt.Errorf("%s configures channel alias %q more than once after normalization", owner, alias)
		}
		out[alias] = chatID
	}
	return out, nil
}

func normalizeScriptExecConfig(in ScriptExecConfig) ScriptExecConfig {
	timeout := in.TimeoutSeconds
	if timeout <= 0 {
		timeout = defaultScriptExecTimeoutSeconds
	}
	return ScriptExecConfig{
		Enabled:        in.Enabled,
		Command:        strings.ToLower(strings.TrimSpace(in.Command)),
		Dir:            strings.TrimSpace(in.Dir),
		AllowedChats:   normalizeChannelNames(in.AllowedChats),
		TimeoutSeconds: timeout,
	}
}

func normalizeChannelNames(in []string) []string {
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, value := range in {
		name := normalizeChannelName(value)
		if name == "" {
			continue
		}
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		out = append(out, name)
	}
	return out
}

func splitList(raw string) []string {
	parts := strings.Split(raw, ",")
	values := make([]string, 0, len(parts))
	for _, part := range parts {
		values = append(values, part)
	}
	return values
}

func normalizeList(in []string) []string {
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, value := range in {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		key := strings.ToLower(value)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, value)
	}
	return out
}

func loadChannels(environ []string) map[string]string {
	channels := make(map[string]string)
	for _, entry := range environ {
		key, value, ok := strings.Cut(entry, "=")
		if !ok || !strings.HasPrefix(key, "FEISHU_BOTD_CHANNELS_") {
			continue
		}
		name := normalizeChannelName(strings.TrimPrefix(key, "FEISHU_BOTD_CHANNELS_"))
		value = strings.TrimSpace(value)
		if name != "" && value != "" {
			channels[name] = value
		}
	}
	if raw := strings.TrimSpace(os.Getenv("FEISHU_BOTD_CHANNELS")); raw != "" {
		for _, part := range strings.Split(raw, ",") {
			name, value, ok := strings.Cut(part, "=")
			if !ok {
				continue
			}
			name = normalizeChannelName(name)
			value = strings.TrimSpace(value)
			if name != "" && value != "" {
				channels[name] = value
			}
		}
	}
	return channels
}

func normalizeChannels(in map[string]string) map[string]string {
	channels := make(map[string]string)
	for name, value := range in {
		name = normalizeChannelName(name)
		value = strings.TrimSpace(value)
		if name != "" && value != "" {
			channels[name] = value
		}
	}
	return channels
}

func normalizeServices(in map[string]ServiceConfig) map[string]ServiceConfig {
	services := make(map[string]ServiceConfig)
	for source, svc := range in {
		source = strings.TrimSpace(source)
		channel := normalizeChannelName(svc.DefaultChannel)
		if source != "" && channel != "" {
			services[source] = ServiceConfig{DefaultChannel: channel}
		}
	}
	return services
}

func normalizeChannelName(name string) string {
	name = strings.TrimSpace(name)
	name = strings.ToLower(strings.ReplaceAll(name, "_", "-"))
	return name
}

func mergeStringMaps(base, override map[string]string) map[string]string {
	merged := make(map[string]string, len(base)+len(override))
	for k, v := range base {
		merged[k] = v
	}
	for k, v := range override {
		merged[k] = v
	}
	return merged
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func hasLegacyAppSettings(commands CommandConfig, channels map[string]string) bool {
	return len(channels) > 0 ||
		commands.Enabled ||
		commands.BotOpenID != "" ||
		commands.BotUserID != "" ||
		commands.BotUnionID != "" ||
		len(commands.BotNames) > 0 ||
		commands.Scripts.Enabled ||
		commands.Scripts.Command != "" ||
		commands.Scripts.Dir != "" ||
		len(commands.Scripts.AllowedChats) > 0
}

func validateApps(apps map[string]AppConfig) error {
	aliases := make([]string, 0, len(apps))
	for alias := range apps {
		aliases = append(aliases, alias)
	}
	sort.Strings(aliases)

	appIDOwners := make(map[string]string, len(apps))
	for _, alias := range aliases {
		app := apps[alias]
		if strings.TrimSpace(app.AppID) == "" {
			if alias == DefaultAppAlias {
				return errors.New("FEISHU_APP_ID or config feishu.app_id is required")
			}
			return fmt.Errorf("app %q app_id is required", alias)
		}
		if strings.TrimSpace(app.AppSecret) == "" {
			if alias == DefaultAppAlias {
				return errors.New("FEISHU_APP_SECRET or config feishu.app_secret is required")
			}
			return fmt.Errorf("app %q app_secret is required", alias)
		}
		if other, duplicate := appIDOwners[app.AppID]; duplicate {
			return fmt.Errorf("apps %q and %q must use distinct app_id values", other, alias)
		}
		appIDOwners[app.AppID] = alias
		// A direct-message-only agent has no static chat id to configure.
		if len(app.Channels) == 0 && !app.Commands.Enabled {
			if alias == DefaultAppAlias {
				return errors.New("at least one channel mapping is required unless commands are enabled for direct messages")
			}
			return fmt.Errorf("app %q requires at least one channel mapping unless commands are enabled for direct messages", alias)
		}
		if err := validateAppScripts(alias, app); err != nil {
			return err
		}
	}
	return nil
}

func buildChannelRoutes(apps map[string]AppConfig) (map[string]string, map[string]ChannelRoute, error) {
	channels := make(map[string]string)
	routes := make(map[string]ChannelRoute)
	aliases := make([]string, 0, len(apps))
	for alias := range apps {
		aliases = append(aliases, alias)
	}
	sort.Strings(aliases)
	for _, appAlias := range aliases {
		app := apps[appAlias]
		channelAliases := make([]string, 0, len(app.Channels))
		for channelAlias := range app.Channels {
			channelAliases = append(channelAliases, channelAlias)
		}
		sort.Strings(channelAliases)
		for _, channelAlias := range channelAliases {
			if existing, duplicate := routes[channelAlias]; duplicate {
				return nil, nil, fmt.Errorf(
					"channel alias %q is configured by both app %q and app %q",
					channelAlias, existing.AppAlias, appAlias,
				)
			}
			chatID := app.Channels[channelAlias]
			channels[channelAlias] = chatID
			routes[channelAlias] = ChannelRoute{AppAlias: appAlias, ChatID: chatID}
		}
	}
	return channels, routes, nil
}

func validateRouting(cfg Config) error {
	if cfg.DefaultChannel != "" {
		if _, ok := cfg.Channels[cfg.DefaultChannel]; !ok {
			return fmt.Errorf("default channel %q is not configured", cfg.DefaultChannel)
		}
	}
	for source, svc := range cfg.Services {
		if svc.DefaultChannel == "" {
			return fmt.Errorf("service %q default_channel is required", source)
		}
		if _, ok := cfg.Channels[svc.DefaultChannel]; !ok {
			return fmt.Errorf("service %q default channel %q is not configured", source, svc.DefaultChannel)
		}
	}
	return nil
}

func validateScripts(cfg Config) error {
	return validateAppScripts(DefaultAppAlias, AppConfig{
		AppID:     cfg.AppID,
		AppSecret: cfg.AppSecret,
		Commands:  cfg.Commands,
		Channels:  cfg.Channels,
	})
}

func validateAppScripts(appAlias string, app AppConfig) error {
	s := app.Commands.Scripts
	if !s.Enabled {
		return nil
	}
	prefix := "commands"
	if appAlias != DefaultAppAlias {
		prefix = fmt.Sprintf("apps.%s.commands", appAlias)
	}
	if !app.Commands.Enabled {
		return fmt.Errorf("%s.scripts.enabled requires %s.enabled to be true", prefix, prefix)
	}
	if s.Command == "" {
		return fmt.Errorf("%s.scripts.command is required when %s.scripts.enabled is true", prefix, prefix)
	}
	if s.Dir == "" {
		return fmt.Errorf("%s.scripts.dir is required when %s.scripts.enabled is true", prefix, prefix)
	}
	info, err := os.Stat(s.Dir)
	if err != nil || !info.IsDir() {
		return fmt.Errorf("%s.scripts.dir %q must be an existing directory", prefix, s.Dir)
	}
	if len(s.AllowedChats) == 0 {
		return fmt.Errorf("%s.scripts.allowed_chats must include at least one configured channel", prefix)
	}
	for _, chat := range s.AllowedChats {
		if _, ok := app.Channels[chat]; !ok {
			return fmt.Errorf("%s.scripts.allowed_chats entry %q is not a configured channel for app %q", prefix, chat, appAlias)
		}
	}
	return nil
}

func durationFromEnv(name string, fallback time.Duration) time.Duration {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback
	}
	seconds, err := strconv.Atoi(raw)
	if err != nil || seconds <= 0 {
		return fallback
	}
	return time.Duration(seconds) * time.Second
}

func boolFromEnv(name string) bool {
	return boolFromEnvDefault(name, false)
}

func boolFromEnvDefault(name string, fallback bool) bool {
	raw := strings.ToLower(strings.TrimSpace(os.Getenv(name)))
	if raw == "" {
		return fallback
	}
	return raw == "1" || raw == "true" || raw == "yes" || raw == "on"
}

func validateLoopbackBind(name, addr string) error {
	return validateTCPBind(name, addr, false)
}

// validatePlaintextGRPCBind keeps user prompts, agent output, and bearer
// credentials off non-loopback plaintext links. Cross-host deployments must
// terminate an authenticated encrypted transport before this loopback listener.
func validatePlaintextGRPCBind(name, addr string) error {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("%s must be host:port: %w", name, err)
	}
	if host == "localhost" {
		return nil
	}
	ip := net.ParseIP(host)
	if ip == nil || !ip.IsLoopback() {
		return fmt.Errorf("%s is plaintext and must bind to loopback; use a Unix socket or terminate authenticated TLS before a loopback listener", name)
	}
	return nil
}

func validateTCPBind(name, addr string, allowNonLoopback bool) error {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("%s must be host:port: %w", name, err)
	}
	if allowNonLoopback {
		return nil
	}
	if host == "localhost" {
		return nil
	}
	ip := net.ParseIP(host)
	if ip == nil || !ip.IsLoopback() {
		return fmt.Errorf("%s must bind to loopback unless FEISHU_BOTD_ALLOW_NON_LOOPBACK_BIND=true", name)
	}
	return nil
}

func readTokenFile(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read token file: %w", err)
	}
	line := strings.TrimSpace(strings.SplitN(string(data), "\n", 2)[0])
	if line == "" {
		return "", errors.New("token file is empty")
	}
	for _, r := range line {
		if !(r >= 'A' && r <= 'Z' || r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || strings.ContainsRune("._~+/=-", r)) {
			return "", errors.New("token file contains an invalid bearer token")
		}
	}
	return line, nil
}

func normalizeAgentProviderConfigs(in map[string]fileAgentProviderConfig) (map[string]fileAgentProviderConfig, error) {
	out := make(map[string]fileAgentProviderConfig, len(in))
	for rawProvider, providerCfg := range in {
		provider := strings.TrimSpace(rawProvider)
		if provider == "" {
			return nil, errors.New("agent_providers contains an empty provider name")
		}
		if len(provider) > 64 {
			return nil, fmt.Errorf("provider %q exceeds 64 bytes", provider)
		}
		if _, duplicate := out[provider]; duplicate {
			return nil, fmt.Errorf("provider %q is configured more than once after trimming", provider)
		}
		tokenFile := strings.TrimSpace(providerCfg.AuthTokenFile)
		if tokenFile == "" {
			return nil, fmt.Errorf("provider %q auth_token_file is required", provider)
		}
		commands, err := normalizeProviderCommands(providerCfg.AllowedCommands)
		if err != nil {
			return nil, fmt.Errorf("provider %q: %w", provider, err)
		}
		allowedApps := optionalStringList{}
		if providerCfg.AllowedApps.Set {
			apps, err := normalizeProviderApps(providerCfg.AllowedApps.Values)
			if err != nil {
				return nil, fmt.Errorf("provider %q: %w", provider, err)
			}
			allowedApps = optionalStringList{Values: apps, Set: true}
		}
		out[provider] = fileAgentProviderConfig{
			AuthTokenFile:          tokenFile,
			AllowedCommands:        commands,
			AllowedApps:            allowedApps,
			AllowUnmatchedMessages: providerCfg.AllowUnmatchedMessages,
			AllowCardActions:       providerCfg.AllowCardActions,
			AllowFollowUpMessages:  providerCfg.AllowFollowUpMessages,
			AllowLegacyCommands:    providerCfg.AllowLegacyCommands,
		}
	}
	return out, nil
}

func normalizeProviderApps(in []string) ([]string, error) {
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, rawAlias := range in {
		alias := strings.TrimSpace(rawAlias)
		if alias == "" {
			return nil, errors.New("allowed_apps contains an empty app alias")
		}
		if len(alias) > 64 {
			return nil, errors.New("allowed_apps entry exceeds 64 bytes")
		}
		if _, duplicate := seen[alias]; duplicate {
			continue
		}
		seen[alias] = struct{}{}
		out = append(out, alias)
	}
	return out, nil
}

func normalizeProviderCommands(in []string) ([]string, error) {
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, command := range in {
		command = strings.ToLower(strings.TrimLeft(strings.TrimSpace(command), "/"))
		if command == "" {
			continue
		}
		if len(command) > 64 {
			return nil, errors.New("allowed_commands entry exceeds 64 bytes")
		}
		if _, duplicate := seen[command]; duplicate {
			continue
		}
		seen[command] = struct{}{}
		out = append(out, command)
	}
	return out, nil
}

func validateAgentProviderApps(providers map[string]AgentProviderConfig, apps map[string]AppConfig) error {
	names := make([]string, 0, len(providers))
	for provider := range providers {
		names = append(names, provider)
	}
	sort.Strings(names)
	for _, provider := range names {
		providerCfg := providers[provider]
		if !providerCfg.AllowedAppsConfigured {
			continue
		}
		for _, alias := range providerCfg.AllowedApps {
			if _, exists := apps[alias]; !exists {
				return fmt.Errorf("provider %q allowed_apps references unknown app alias %q", provider, alias)
			}
		}
	}
	return nil
}

func validateAgentProviderTokens(providers map[string]AgentProviderConfig, generalToken string) error {
	owners := make(map[string]string, len(providers))
	for provider, providerCfg := range providers {
		token := strings.TrimSpace(providerCfg.AuthToken)
		if token == "" {
			return fmt.Errorf("provider %q has an empty auth token", provider)
		}
		if len(token) < minimumBearerTokenBytes {
			return fmt.Errorf("provider %q auth token must be at least %d bytes", provider, minimumBearerTokenBytes)
		}
		if other, duplicate := owners[token]; duplicate {
			return fmt.Errorf("providers %q and %q must use distinct auth tokens", other, provider)
		}
		owners[token] = provider
		if generalToken != "" && token == generalToken {
			return fmt.Errorf("provider %q token must differ from the general bearer token", provider)
		}
	}
	return nil
}
