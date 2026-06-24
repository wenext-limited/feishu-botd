package config

import (
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	defaultDedupeTTL   = 6 * time.Hour
	defaultSendTimeout = 15 * time.Second
)

type Config struct {
	AppID       string
	AppSecret   string
	SocketPath  string
	BindAddr    string
	AuthToken   string
	Channels    map[string]string
	DedupeTTL   time.Duration
	SendTimeout time.Duration
}

func LoadFromEnv() (Config, error) {
	cfg := Config{
		AppID:       strings.TrimSpace(os.Getenv("FEISHU_APP_ID")),
		AppSecret:   strings.TrimSpace(os.Getenv("FEISHU_APP_SECRET")),
		SocketPath:  strings.TrimSpace(os.Getenv("FEISHU_BOTD_SOCKET")),
		BindAddr:    strings.TrimSpace(os.Getenv("FEISHU_BOTD_BIND")),
		Channels:    loadChannels(os.Environ()),
		DedupeTTL:   durationFromEnv("FEISHU_BOTD_DEDUPE_TTL_SECONDS", defaultDedupeTTL),
		SendTimeout: durationFromEnv("FEISHU_BOTD_SEND_TIMEOUT_SECONDS", defaultSendTimeout),
	}

	if cfg.AppID == "" {
		return Config{}, errors.New("FEISHU_APP_ID is required")
	}
	if cfg.AppSecret == "" {
		return Config{}, errors.New("FEISHU_APP_SECRET is required")
	}
	if cfg.SocketPath == "" && cfg.BindAddr == "" {
		return Config{}, errors.New("FEISHU_BOTD_SOCKET or FEISHU_BOTD_BIND is required")
	}
	if len(cfg.Channels) == 0 {
		return Config{}, errors.New("at least one FEISHU_BOTD_CHANNELS_* mapping is required")
	}
	if cfg.BindAddr != "" {
		if err := validateLoopbackBind(cfg.BindAddr); err != nil {
			return Config{}, err
		}
		tokenFile := strings.TrimSpace(os.Getenv("FEISHU_BOTD_AUTH_TOKEN_FILE"))
		if tokenFile == "" {
			return Config{}, errors.New("FEISHU_BOTD_AUTH_TOKEN_FILE is required when FEISHU_BOTD_BIND is set")
		}
		token, err := readTokenFile(tokenFile)
		if err != nil {
			return Config{}, err
		}
		cfg.AuthToken = token
	}
	return cfg, nil
}

func loadChannels(environ []string) map[string]string {
	channels := make(map[string]string)
	for _, entry := range environ {
		key, value, ok := strings.Cut(entry, "=")
		if !ok || !strings.HasPrefix(key, "FEISHU_BOTD_CHANNELS_") {
			continue
		}
		name := strings.TrimPrefix(key, "FEISHU_BOTD_CHANNELS_")
		name = strings.ToLower(strings.ReplaceAll(name, "_", "-"))
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
			name = strings.ToLower(strings.TrimSpace(name))
			value = strings.TrimSpace(value)
			if name != "" && value != "" {
				channels[name] = value
			}
		}
	}
	return channels
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

func validateLoopbackBind(addr string) error {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("FEISHU_BOTD_BIND must be host:port: %w", err)
	}
	if host == "localhost" {
		return nil
	}
	ip := net.ParseIP(host)
	if ip == nil || !ip.IsLoopback() {
		return errors.New("FEISHU_BOTD_BIND must bind to loopback")
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
