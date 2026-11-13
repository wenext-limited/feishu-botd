package config

import (
	"reflect"
	"sort"
)

// ReloadPlan is the difference between a running Config and a newly loaded
// one, sorted into what a live reload has to do about each change.
//
// Every list is sorted. A plan with no entries anywhere means the two configs
// are equivalent and the reload has nothing to apply.
type ReloadPlan struct {
	// AppsAdded and AppsRemoved name apps whose Feishu connection the reload
	// starts or stops. AppsRecredited names apps whose app_id or app_secret
	// changed, so their connection restarts with the new credentials.
	AppsAdded      []string
	AppsRemoved    []string
	AppsRecredited []string
	// AppsUpdated names apps whose other settings (channels, commands, bot
	// identity, scripts) changed. Those apply without touching the
	// connection.
	AppsUpdated []string
	// ProvidersAdded, ProvidersRemoved and ProvidersChanged name agent
	// providers. A removed or changed provider's open streams are closed so
	// it re-authenticates under the new token and grants.
	ProvidersAdded   []string
	ProvidersRemoved []string
	ProvidersChanged []string
	// RestartRequired names settings that changed on disk but only take
	// effect when botd restarts. The reload applies everything else and
	// keeps running on the old values for these.
	RestartRequired []string
	// SettingsChanged names other top-level settings that apply live
	// (default_channel, services, send_timeout, the general auth token).
	SettingsChanged []string
}

// Empty reports whether the plan has nothing to apply and nothing to warn
// about.
func (p ReloadPlan) Empty() bool {
	return len(p.AppsAdded) == 0 && len(p.AppsRemoved) == 0 && len(p.AppsRecredited) == 0 &&
		len(p.AppsUpdated) == 0 && len(p.ProvidersAdded) == 0 && len(p.ProvidersRemoved) == 0 &&
		len(p.ProvidersChanged) == 0 && len(p.RestartRequired) == 0 && len(p.SettingsChanged) == 0
}

// AppConnectionChanged reports whether alias's Feishu connection is started,
// stopped, or restarted by the plan.
func (p ReloadPlan) AppConnectionChanged(alias string) bool {
	return contains(p.AppsAdded, alias) || contains(p.AppsRemoved, alias) || contains(p.AppsRecredited, alias)
}

// ProviderReset reports whether provider's open streams must close.
func (p ReloadPlan) ProviderReset(provider string) bool {
	return contains(p.ProvidersRemoved, provider) || contains(p.ProvidersChanged, provider)
}

// PlanReload compares the running config with a newly loaded one.
func PlanReload(running, next Config) ReloadPlan {
	var plan ReloadPlan

	oldApps, newApps := running.EffectiveApps(), next.EffectiveApps()
	for alias, app := range newApps {
		old, existed := oldApps[alias]
		switch {
		case !existed:
			plan.AppsAdded = append(plan.AppsAdded, alias)
		case old.AppID != app.AppID || old.AppSecret != app.AppSecret:
			plan.AppsRecredited = append(plan.AppsRecredited, alias)
		case !reflect.DeepEqual(normalizedApp(old), normalizedApp(app)):
			plan.AppsUpdated = append(plan.AppsUpdated, alias)
		}
	}
	for alias := range oldApps {
		if _, kept := newApps[alias]; !kept {
			plan.AppsRemoved = append(plan.AppsRemoved, alias)
		}
	}

	for name, provider := range next.AgentProviders {
		old, existed := running.AgentProviders[name]
		switch {
		case !existed:
			plan.ProvidersAdded = append(plan.ProvidersAdded, name)
		case !reflect.DeepEqual(normalizedProvider(old), normalizedProvider(provider)):
			plan.ProvidersChanged = append(plan.ProvidersChanged, name)
		}
	}
	for name := range running.AgentProviders {
		if _, kept := next.AgentProviders[name]; !kept {
			plan.ProvidersRemoved = append(plan.ProvidersRemoved, name)
		}
	}

	restart := func(setting string, changed bool) {
		if changed {
			plan.RestartRequired = append(plan.RestartRequired, setting)
		}
	}
	restart("listeners.http_socket", running.SocketPath != next.SocketPath)
	restart("listeners.http_bind", running.BindAddr != next.BindAddr)
	restart("listeners.grpc_socket", running.GRPCSocketPath != next.GRPCSocketPath)
	restart("listeners.grpc_bind", running.GRPCBindAddr != next.GRPCBindAddr)
	restart("listeners.allow_non_loopback_bind", running.AllowLANBind != next.AllowLANBind)
	restart("state_dir", running.StateDir != next.StateDir)
	restart("dedupe_ttl_seconds", running.DedupeTTL != next.DedupeTTL)

	live := func(setting string, changed bool) {
		if changed {
			plan.SettingsChanged = append(plan.SettingsChanged, setting)
		}
	}
	live("default_channel", running.DefaultChannel != next.DefaultChannel)
	live("services", !reflect.DeepEqual(nilIfEmpty(running.Services), nilIfEmpty(next.Services)))
	live("send_timeout_seconds", running.SendTimeout != next.SendTimeout)
	live("listeners.auth_token_file", running.AuthToken != next.AuthToken)

	for _, list := range []*[]string{
		&plan.AppsAdded, &plan.AppsRemoved, &plan.AppsRecredited, &plan.AppsUpdated,
		&plan.ProvidersAdded, &plan.ProvidersRemoved, &plan.ProvidersChanged,
	} {
		sort.Strings(*list)
	}
	return plan
}

// KeepRestartOnly returns next with every restart-only setting replaced by
// running's value, so the live config never claims a listener or state dir
// the process is not actually using. RuntimeChats is carried over too: the
// registry is process state, not configuration.
func KeepRestartOnly(running, next Config) Config {
	next.SocketPath = running.SocketPath
	next.BindAddr = running.BindAddr
	next.GRPCSocketPath = running.GRPCSocketPath
	next.GRPCBindAddr = running.GRPCBindAddr
	next.AllowLANBind = running.AllowLANBind
	next.StateDir = running.StateDir
	next.DedupeTTL = running.DedupeTTL
	next.RuntimeChats = running.RuntimeChats
	return next
}

func normalizedApp(app AppConfig) AppConfig {
	app.Channels = nilIfEmpty(app.Channels)
	app.Commands.BotNames = nilIfEmptySlice(app.Commands.BotNames)
	app.Commands.Scripts.AllowedChats = nilIfEmptySlice(app.Commands.Scripts.AllowedChats)
	return app
}

func normalizedProvider(p AgentProviderConfig) AgentProviderConfig {
	p.AllowedCommands = nilIfEmptySlice(p.AllowedCommands)
	p.AmbientChatAliases = nilIfEmptySlice(p.AmbientChatAliases)
	p.ProactiveChatAliases = nilIfEmptySlice(p.ProactiveChatAliases)
	p.WorkingReactionOverrides = nilIfEmpty(p.WorkingReactionOverrides)
	// AllowedApps keeps nil-vs-empty: an empty list means "no apps", which
	// is a different grant from an absent one. AllowedAppsConfigured already
	// records which it is, so an empty slice and nil compare equal here.
	if len(p.AllowedApps) == 0 {
		p.AllowedApps = nil
	}
	return p
}

func nilIfEmpty[K comparable, V any](in map[K]V) map[K]V {
	if len(in) == 0 {
		return nil
	}
	return in
}

func nilIfEmptySlice(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	return in
}

func contains(list []string, value string) bool {
	for _, item := range list {
		if item == value {
			return true
		}
	}
	return false
}

func sortedUnique(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	out := append([]string(nil), in...)
	sort.Strings(out)
	kept := out[:1]
	for _, value := range out[1:] {
		if value != kept[len(kept)-1] {
			kept = append(kept, value)
		}
	}
	return kept
}
