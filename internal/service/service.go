// Package service holds the transport-agnostic core logic for feishu-botd.
// Both the HTTP compatibility shim and the gRPC server delegate to a single
// *Service so the two transports cannot drift in validation, deduplication,
// sending, or redaction behavior.
package service

import (
	"log/slog"
	"strings"
	"sync"
	"time"

	"feishu-botd/internal/config"
	"feishu-botd/internal/dedupe"
	"feishu-botd/internal/feishu"
	"feishu-botd/internal/ownership"
)

// Version is the reported service version, shared by both transports.
const Version = "0.2.0"

// Provider names the upstream message provider in responses.
const Provider = "feishu"

// App connection states are a fixed, privacy-safe vocabulary shared with the
// process runtime. They contain no SDK error text or tenant identifiers.
const (
	AppConnectionStarting     = "starting"
	AppConnectionConnected    = "connected"
	AppConnectionReconnecting = "reconnecting"
	AppConnectionDisconnected = "disconnected"
	AppConnectionUnavailable  = "unavailable"
)

type appBackend struct {
	sender       feishu.Sender
	dynamicCards feishu.DynamicCards
}

// Service owns the send/dedupe/readiness flow. It is safe for concurrent use
// because its dependencies (sender, store) are themselves concurrency-safe and
// its configuration is an immutable snapshot taken at construction.
type Service struct {
	cfg      config.Config
	backends map[string]appBackend
	store    *dedupe.MemoryStore
	logger   *slog.Logger
	redactor *redactor

	legacyCapabilityCheck bool
	connectionMu          sync.RWMutex
	connectionStates      map[string]string

	inboundRoutes *inboundRouteRegistry
	commandBroker *commandBroker
	agentBroker   *agentBroker
	agentOwners   agentOwnershipStore
}

type agentOwnershipStore interface {
	Put(messageRef, provider string, now time.Time) error
	Lookup(messageRef string, now time.Time) (ownership.Owner, bool, error)
}

// NewService builds a Service from an immutable config snapshot.
// It remains the compatibility constructor for the legacy single-app runtime
// and for existing embedders. New multi-app process composition must use
// NewMultiAppService so every app has an independent sender.
func NewService(cfg config.Config, sender feishu.Sender, store *dedupe.MemoryStore, logger *slog.Logger) *Service {
	return newService(cfg, map[string]feishu.Sender{config.DefaultAppAlias: sender}, store, logger, false)
}

// SetAgentOwnershipStore installs restart-safe routing for reactions on
// agent-authored messages. Process composition calls this before any receiver
// or provider stream starts; tests may inject a deterministic in-memory fake.
func (s *Service) SetAgentOwnershipStore(store agentOwnershipStore) {
	s.agentOwners = store
}

// NewMultiAppService builds one shared service over independent per-app Feishu
// backends. The map key is the public app alias; credentials remain confined to
// each sender. Readiness tracks long-connection state for this constructor.
func NewMultiAppService(cfg config.Config, senders map[string]feishu.Sender, store *dedupe.MemoryStore, logger *slog.Logger) *Service {
	return newService(cfg, senders, store, logger, true)
}

func newService(
	cfg config.Config,
	senders map[string]feishu.Sender,
	store *dedupe.MemoryStore,
	logger *slog.Logger,
	trackConnections bool,
) *Service {
	if logger == nil {
		logger = slog.Default()
	}
	backends := make(map[string]appBackend, len(senders))
	for rawAlias, sender := range senders {
		alias := effectiveAppAlias(rawAlias)
		backend := appBackend{sender: sender}
		if cards, ok := sender.(feishu.DynamicCards); ok {
			backend.dynamicCards = cards
		}
		backends[alias] = backend
	}
	var connectionStates map[string]string
	if trackConnections {
		connectionStates = make(map[string]string)
		for _, alias := range cfg.AppAliases() {
			connectionStates[alias] = AppConnectionStarting
		}
	}
	return &Service{
		cfg:                   cfg,
		backends:              backends,
		store:                 store,
		logger:                logger,
		redactor:              newRedactor(cfg),
		legacyCapabilityCheck: !trackConnections,
		connectionStates:      connectionStates,
		inboundRoutes:         newInboundRouteRegistry(cfg.DedupeTTL),
		commandBroker:         newCommandBroker(cfg.DedupeTTL),
		agentBroker:           newAgentBroker(cfg.DedupeTTL),
	}
}

func effectiveAppAlias(alias string) string {
	alias = strings.TrimSpace(alias)
	if alias == "" {
		return config.DefaultAppAlias
	}
	return alias
}

func isDefaultAppAlias(alias string) bool {
	return effectiveAppAlias(alias) == config.DefaultAppAlias
}

func (s *Service) backendForApp(alias string) (appBackend, bool) {
	backend, ok := s.backends[effectiveAppAlias(alias)]
	return backend, ok && backend.sender != nil
}

func (s *Service) hasDynamicCards() bool {
	for _, backend := range s.backends {
		if backend.dynamicCards != nil {
			return true
		}
	}
	return false
}

func (s *Service) appAllowed(provider, appAlias string) bool {
	return s.cfg.ProviderAllowsApp(provider, effectiveAppAlias(appAlias))
}

// SetAppConnectionState records a fixed-vocabulary receiver state. Unknown app
// aliases and arbitrary states are ignored so readiness can never echo
// untrusted SDK error text.
func (s *Service) SetAppConnectionState(appAlias, state string) {
	appAlias = effectiveAppAlias(appAlias)
	switch state {
	case AppConnectionStarting, AppConnectionConnected, AppConnectionReconnecting,
		AppConnectionDisconnected, AppConnectionUnavailable:
	default:
		return
	}
	s.connectionMu.Lock()
	defer s.connectionMu.Unlock()
	if s.connectionStates == nil {
		return
	}
	if _, configured := s.connectionStates[appAlias]; !configured {
		return
	}
	s.connectionStates[appAlias] = state
}

// SendResult is the transport-agnostic outcome of a delivery.
type SendResult struct {
	Provider  string
	MessageID string
	Duplicate bool
}
