package service

import (
	"fmt"
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
	candidates := []string{cfg.AppSecret, cfg.AuthToken}
	for _, chatID := range cfg.Channels {
		candidates = append(candidates, chatID)
	}
	values := make([]string, 0, len(candidates))
	for _, value := range candidates {
		value = strings.TrimSpace(value)
		if len(value) >= 4 {
			values = append(values, value)
		}
	}
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
