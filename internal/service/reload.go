package service

import (
	"time"

	"feishu-botd/internal/config"
	"feishu-botd/internal/feishu"
)

// ReloadStatus is the outcome of the most recent config reload attempt.
type ReloadStatus struct {
	// Result is "" before any reload, then "ok" or "rejected".
	Result string
	At     time.Time
	// RestartRequired names settings changed on disk that the running
	// process still holds at their old values.
	RestartRequired []string
}

// ConfigGeneration counts the configs this service has run on: 1 at start,
// one more for every applied reload.
func (s *Service) ConfigGeneration() uint64 {
	return s.generation.Load()
}

// Config returns the config snapshot in force. Callers must not mutate it.
func (s *Service) Config() config.Config {
	return *s.cfg()
}

// LastReload returns the outcome of the most recent reload attempt.
func (s *Service) LastReload() ReloadStatus {
	s.reloadMu.Lock()
	defer s.reloadMu.Unlock()
	status := s.reload
	status.RestartRequired = append([]string(nil), status.RestartRequired...)
	return status
}

// RecordRejectedReload notes a reload that failed validation. The running
// config, and the settings a previous reload left awaiting restart, stay
// as they were.
func (s *Service) RecordRejectedReload(at time.Time) {
	s.reloadMu.Lock()
	defer s.reloadMu.Unlock()
	s.reload.Result = "rejected"
	s.reload.At = at
}

// ApplyConfig makes next the config in force. senders holds a sender for
// every app the plan adds or re-credits; the caller has already checked
// their credentials. Apps the plan removes lose their backend, and the open
// streams of providers the plan changes or removes are closed so they
// re-authenticate. Nothing else is touched: open streams, deliveries, and
// leases of unaffected providers carry on.
//
// next must already hold the running process's restart-only settings (see
// config.KeepRestartOnly); restartRequired names the ones that differ on
// disk.
func (s *Service) ApplyConfig(
	next config.Config,
	plan config.ReloadPlan,
	senders map[string]feishu.Sender,
	restartRequired []string,
	at time.Time,
) {
	if next.RuntimeChats == nil {
		next.RuntimeChats = s.cfg().RuntimeChats
	}

	s.backendsMu.Lock()
	backends := make(map[string]appBackend, len(s.backends)+len(senders))
	for alias, backend := range s.backends {
		backends[alias] = backend
	}
	for _, alias := range plan.AppsRemoved {
		delete(backends, effectiveAppAlias(alias))
	}
	for alias, sender := range senders {
		backends[effectiveAppAlias(alias)] = newAppBackend(sender)
	}
	s.backends = backends
	s.backendsMu.Unlock()

	s.connectionMu.Lock()
	if s.connectionStates != nil {
		for _, alias := range plan.AppsRemoved {
			delete(s.connectionStates, effectiveAppAlias(alias))
		}
		for _, alias := range append(append([]string(nil), plan.AppsAdded...), plan.AppsRecredited...) {
			s.connectionStates[effectiveAppAlias(alias)] = AppConnectionStarting
		}
	}
	s.connectionMu.Unlock()

	// Keep redacting the old secrets too: a log line about a request that
	// began under the old config may still carry one.
	s.redactorRef.Store(s.redactor().with(newRedactor(next)))
	s.cfgRef.Store(&next)

	reset := make(map[string]struct{}, len(plan.ProvidersChanged)+len(plan.ProvidersRemoved))
	for _, provider := range append(append([]string(nil), plan.ProvidersChanged...), plan.ProvidersRemoved...) {
		reset[provider] = struct{}{}
	}
	if len(reset) > 0 {
		s.agentBroker.closeProviders(reset)
		s.commandBroker.closeProviders(reset)
	}

	s.generation.Add(1)
	s.reloadMu.Lock()
	s.reload = ReloadStatus{
		Result:          "ok",
		At:              at,
		RestartRequired: append([]string(nil), restartRequired...),
	}
	s.reloadMu.Unlock()
}

// ConfigStatus is the config section of a readiness report.
type ConfigStatus struct {
	Generation      uint64
	LastReload      string
	LastReloadAt    time.Time
	RestartRequired []string
}

// ConfigStatus reports the config generation and the last reload outcome.
func (s *Service) ConfigStatus() ConfigStatus {
	status := s.LastReload()
	return ConfigStatus{
		Generation:      s.ConfigGeneration(),
		LastReload:      status.Result,
		LastReloadAt:    status.At,
		RestartRequired: status.RestartRequired,
	}
}
