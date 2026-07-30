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

	apps := s.cfg.EffectiveApps()
	aliases := s.cfg.AppAliases()
	if len(aliases) == 0 {
		checks["feishu_auth"] = "missing_credentials"
		ready = false
	}
	commandsEnabled := false
	for _, app := range apps {
		if app.Commands.Enabled {
			commandsEnabled = true
			break
		}
	}
	if len(s.cfg.Channels) == 0 && !commandsEnabled {
		checks["channels"] = "missing_channels"
		ready = false
	}
	if !s.store.Ready() {
		checks["dedupe_store"] = "unavailable"
		ready = false
	}

	type authCheckResult struct {
		alias string
		state string
		err   error
	}
	authResults := make(chan authCheckResult, len(aliases))
	checkCtx, cancelAuthChecks := context.WithTimeout(ctx, s.cfg.SendTimeout)
	defer cancelAuthChecks()
	pending := make(map[string]struct{}, len(aliases))
	for _, alias := range aliases {
		pending[alias] = struct{}{}
		app := apps[alias]
		if app.AppID == "" || app.AppSecret == "" {
			authResults <- authCheckResult{alias: alias, state: "missing_credentials"}
			continue
		}
		backend, ok := s.backendForApp(alias)
		if !ok {
			authResults <- authCheckResult{alias: alias, state: "unavailable"}
			continue
		}
		go func(alias string, sender interface {
			Ready(context.Context) error
		}) {
			if err := sender.Ready(checkCtx); err != nil {
				authResults <- authCheckResult{alias: alias, state: "unavailable", err: err}
				return
			}
			authResults <- authCheckResult{alias: alias, state: "ok"}
		}(alias, backend.sender)
	}
	authReady := true
	for len(pending) > 0 {
		select {
		case result := <-authResults:
			if _, waiting := pending[result.alias]; !waiting {
				continue
			}
			delete(pending, result.alias)
			checks["feishu_auth."+result.alias] = result.state
			if result.state != "ok" {
				authReady = false
				if result.state == "missing_credentials" {
					checks["feishu_auth"] = "missing_credentials"
				} else if checks["feishu_auth"] == "ok" {
					checks["feishu_auth"] = "unavailable"
				}
			}
			if result.err != nil {
				s.logFeishuFailure("readiness", "health", "feishu:"+result.alias, result.err)
			}
		case <-checkCtx.Done():
			authReady = false
			if checks["feishu_auth"] == "ok" {
				checks["feishu_auth"] = "unavailable"
			}
			for alias := range pending {
				checks["feishu_auth."+alias] = "unavailable"
			}
			pending = nil
		}
	}
	if !authReady {
		ready = false
	}

	s.connectionMu.RLock()
	connectionStates := make(map[string]string, len(s.connectionStates))
	for alias, state := range s.connectionStates {
		connectionStates[alias] = state
	}
	s.connectionMu.RUnlock()
	for alias, state := range connectionStates {
		checks["feishu_connection."+alias] = state
		if state != AppConnectionConnected {
			ready = false
		}
	}

	return ready, checks
}
