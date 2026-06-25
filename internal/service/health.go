package service

import "context"

// HealthInfo is static liveness information reported on both transports.
type HealthInfo struct {
	Status  string
	Service string
	Version string
}

// Health returns static liveness information. It never touches Feishu.
func (s *Service) Health() HealthInfo {
	return HealthInfo{Status: "ok", Service: "feishu-botd", Version: Version}
}

// Ready performs redacted readiness checks for config, Feishu credentials,
// channels, and dedupe state. It returns whether the service is ready and a
// per-check state map. The only outbound call is a cached tenant-token check;
// it never sends a test message. The returned map never contains secrets or
// raw chat ids.
func (s *Service) Ready(ctx context.Context) (bool, map[string]string) {
	checks := map[string]string{
		"config":       "ok",
		"feishu_auth":  "ok",
		"channels":     "ok",
		"dedupe_store": "ok",
	}
	ready := true

	if s.cfg.AppID == "" || s.cfg.AppSecret == "" {
		checks["feishu_auth"] = "missing_credentials"
		ready = false
	}
	if len(s.cfg.Channels) == 0 {
		checks["channels"] = "missing_channels"
		ready = false
	}
	if !s.store.Ready() {
		checks["dedupe_store"] = "unavailable"
		ready = false
	}

	if ready {
		checkCtx, cancel := context.WithTimeout(ctx, s.cfg.SendTimeout)
		defer cancel()
		if err := s.sender.Ready(checkCtx); err != nil {
			checks["feishu_auth"] = "unavailable"
			ready = false
			s.logger.Warn("readiness auth check failed", "error", s.redactor.redact(err))
		}
	}

	return ready, checks
}
