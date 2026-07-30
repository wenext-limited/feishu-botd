package service

import (
	"fmt"
	"sort"
	"strings"

	"feishu-botd/internal/config"
)

// redactor removes configured secrets and raw chat ids from free-form strings
// before they are logged. Response and error bodies use static messages, so
// this only guards log lines, but it runs identically for both the HTTP and
// gRPC transports.
type redactor struct {
	values []string
}

func newRedactor(cfg config.Config) *redactor {
	candidates := []string{cfg.AppID, cfg.AppSecret, cfg.AuthToken}
	for _, app := range cfg.EffectiveApps() {
		candidates = append(candidates, app.AppID, app.AppSecret)
		for _, chatID := range app.Channels {
			candidates = append(candidates, chatID)
		}
	}
	for _, provider := range cfg.AgentProviders {
		candidates = append(candidates, provider.AuthToken)
	}
	for _, chatID := range cfg.Channels {
		candidates = append(candidates, chatID)
	}
	unique := make(map[string]struct{}, len(candidates))
	for _, value := range candidates {
		value = strings.TrimSpace(value)
		if value != "" {
			unique[value] = struct{}{}
		}
	}
	values := make([]string, 0, len(unique))
	for value := range unique {
		values = append(values, value)
	}
	sort.Slice(values, func(i, j int) bool {
		if len(values[i]) != len(values[j]) {
			return len(values[i]) > len(values[j])
		}
		return values[i] < values[j]
	})
	return &redactor{values: values}
}

func (r *redactor) redactString(msg string) string {
	for _, secret := range r.values {
		msg = strings.ReplaceAll(msg, secret, "<redacted>")
	}
	if len(msg) > 180 {
		return msg[:180] + "..."
	}
	return msg
}

func (r *redactor) redact(err error) string {
	if err == nil {
		return ""
	}
	return r.redactString(err.Error())
}

// Redact scrubs configured secrets from an arbitrary value's string form. It is
// used by the gRPC panic-recovery interceptor so the panic path honors the same
// redaction guarantee as the normal error paths.
func (s *Service) Redact(v any) string {
	return s.redactor.redactString(fmt.Sprint(v))
}
