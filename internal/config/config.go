package config

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	defaultDedupeTTL                = 6 * time.Hour
	defaultSendTimeout              = 15 * time.Second
	defaultScriptExecTimeoutSeconds = 120
)

type Config struct {
	AppID          string
	AppSecret      string
	SocketPath     string
	BindAddr       string
	GRPCSocketPath string
	GRPCBindAddr   string
	AuthToken      string
	AllowLANBind   bool
	Commands       CommandConfig
	Channels       map[string]string
	DefaultChannel string
	Services       map[string]ServiceConfig
	DedupeTTL      time.Duration
	SendTimeout    time.Duration
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
// command. Command is the trigger word (e.g. "pls"); the action word that
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

func LoadFromEnv() (Config, error) {
	fileCfg, err := loadFileConfig(strings.TrimSpace(os.Getenv("FEISHU_BOTD_CONFIG")))
	if err != nil {
		return Config{}, err
	}

	cfg := Config{
		AppID:          firstNonEmpty(os.Getenv("FEISHU_APP_ID"), fileCfg.AppID),
		AppSecret:      firstNonEmpty(os.Getenv("FEISHU_APP_SECRET"), fileCfg.AppSecret),
		SocketPath:     firstNonEmpty(os.Getenv("FEISHU_BOTD_SOCKET"), fileCfg.SocketPath),
		BindAddr:       firstNonEmpty(os.Getenv("FEISHU_BOTD_BIND"), fileCfg.BindAddr),
		GRPCSocketPath: firstNonEmpty(os.Getenv("FEISHU_BOTD_GRPC_SOCKET"), fileCfg.GRPCSocketPath),
		GRPCBindAddr:   firstNonEmpty(os.Getenv("FEISHU_BOTD_GRPC_BIND"), fileCfg.GRPCBindAddr),
		AllowLANBind:   boolFromEnvDefault("FEISHU_BOTD_ALLOW_NON_LOOPBACK_BIND", fileCfg.AllowLANBind),
		Commands:       commandConfigFromEnv(fileCfg.Commands),
		Channels:       mergeStringMaps(fileCfg.Channels, loadChannels(os.Environ())),
		DefaultChannel: firstNonEmpty(os.Getenv("FEISHU_BOTD_DEFAULT_CHANNEL"), fileCfg.DefaultChannel),
		Services:       fileCfg.Services,
		DedupeTTL:      durationFromEnv("FEISHU_BOTD_DEDUPE_TTL_SECONDS", fileCfg.DedupeTTL),
		SendTimeout:    durationFromEnv("FEISHU_BOTD_SEND_TIMEOUT_SECONDS", fileCfg.SendTimeout),
	}

	if cfg.AppID == "" {
		return Config{}, errors.New("FEISHU_APP_ID or config feishu.app_id is required")
	}
	if cfg.AppSecret == "" {
		return Config{}, errors.New("FEISHU_APP_SECRET or config feishu.app_secret is required")
	}
	if cfg.SocketPath == "" && cfg.BindAddr == "" && cfg.GRPCSocketPath == "" && cfg.GRPCBindAddr == "" {
		return Config{}, errors.New("at least one listener is required: set FEISHU_BOTD_SOCKET, FEISHU_BOTD_BIND, FEISHU_BOTD_GRPC_SOCKET, FEISHU_BOTD_GRPC_BIND, or config listeners")
	}
	if len(cfg.Channels) == 0 {
		return Config{}, errors.New("at least one channel mapping is required")
	}
	if err := validateRouting(cfg); err != nil {
		return Config{}, err
	}
	if err := validateScripts(cfg); err != nil {
		return Config{}, err
	}
	// The HTTP and gRPC TCP listeners share a single bearer token. Load it once
	// when either TCP listener is enabled. TCP binds stay loopback-only unless
	// the deployment explicitly opts into a non-loopback/LAN bind.
	if cfg.BindAddr != "" || cfg.GRPCBindAddr != "" {
		for _, bind := range []struct{ name, addr string }{
			{"FEISHU_BOTD_BIND", cfg.BindAddr},
			{"FEISHU_BOTD_GRPC_BIND", cfg.GRPCBindAddr},
		} {
			if bind.addr == "" {
				continue
			}
			if err := validateTCPBind(bind.name, bind.addr, cfg.AllowLANBind); err != nil {
				return Config{}, err
			}
		}
		tokenFile := firstNonEmpty(os.Getenv("FEISHU_BOTD_AUTH_TOKEN_FILE"), fileCfg.AuthTokenFile)
		if tokenFile == "" {
			return Config{}, errors.New("FEISHU_BOTD_AUTH_TOKEN_FILE or config listeners.auth_token_file is required when a TCP listener is set")
		}
		token, err := readTokenFile(tokenFile)
		if err != nil {
			return Config{}, err
		}
		cfg.AuthToken = token
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
	AllowLANBind   bool
	Commands       CommandConfig
	Channels       map[string]string
	DefaultChannel string
	Services       map[string]ServiceConfig
	DedupeTTL      time.Duration
	SendTimeout    time.Duration
}

type configFile struct {
	Feishu             fileFeishuConfig         `json:"feishu"`
	Listeners          fileListenersConfig      `json:"listeners"`
	Commands           CommandConfig            `json:"commands"`
	Channels           map[string]string        `json:"channels"`
	DefaultChannel     string                   `json:"default_channel"`
	Services           map[string]ServiceConfig `json:"services"`
	DedupeTTLSeconds   int                      `json:"dedupe_ttl_seconds"`
	SendTimeoutSeconds int                      `json:"send_timeout_seconds"`
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
	cfg.AllowLANBind = raw.Listeners.AllowNonLoopbackBind
	cfg.Commands = normalizeCommandConfig(raw.Commands)
	cfg.Channels = normalizeChannels(raw.Channels)
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
	s := cfg.Commands.Scripts
	if !s.Enabled {
		return nil
	}
	if !cfg.Commands.Enabled {
		return errors.New("commands.scripts.enabled requires commands.enabled to be true")
	}
	if s.Command == "" {
		return errors.New("commands.scripts.command is required when commands.scripts.enabled is true")
	}
	if s.Dir == "" {
		return errors.New("commands.scripts.dir is required when commands.scripts.enabled is true")
	}
	info, err := os.Stat(s.Dir)
	if err != nil || !info.IsDir() {
		return fmt.Errorf("commands.scripts.dir %q must be an existing directory", s.Dir)
	}
	if len(s.AllowedChats) == 0 {
		return errors.New("commands.scripts.allowed_chats must include at least one configured channel")
	}
	for _, chat := range s.AllowedChats {
		if _, ok := cfg.Channels[chat]; !ok {
			return fmt.Errorf("commands.scripts.allowed_chats entry %q is not a configured channel", chat)
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
		return "", fmt.Errorf("read FEISHU_BOTD_AUTH_TOKEN_FILE: %w", err)
	}
	line := strings.TrimSpace(strings.SplitN(string(data), "\n", 2)[0])
	if line == "" {
		return "", errors.New("FEISHU_BOTD_AUTH_TOKEN_FILE is empty")
	}
	for _, r := range line {
		if !(r >= 'A' && r <= 'Z' || r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || strings.ContainsRune("._~+/=-", r)) {
			return "", errors.New("FEISHU_BOTD_AUTH_TOKEN_FILE contains an invalid bearer token")
		}
	}
	return line, nil
}
