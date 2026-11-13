// Package service holds the transport-agnostic core logic for feishu-botd.
// Both the HTTP compatibility shim and the gRPC server delegate to a single
// *Service so the two transports cannot drift in validation, deduplication,
// sending, or redaction behavior.
package service

import (
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"feishu-botd/internal/chatgrant"
	"feishu-botd/internal/config"
	"feishu-botd/internal/dedupe"
	"feishu-botd/internal/feishu"
	"feishu-botd/internal/messageroute"
	"feishu-botd/internal/ownership"
)

// Version is the reported service version, shared by both transports.
const Version = "0.3.0"

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
	sender          feishu.Sender
	dynamicCards    feishu.DynamicCards
	attachedContext feishu.AttachedContextLookup
	cotMessages     feishu.CoTMessages
	reactions       feishu.ReactionMessages
	contactUsers    feishu.ContactUsers
	images          feishu.ImageUploader
	poster          feishu.MessagePoster
	agentCards      feishu.AgentCards
	pinner          feishu.MessagePinner
	chats           feishu.AgentChats
	// senderEmails resolves and caches senders' company addresses for
	// providers granted allow_sender_email; senderEmailWarned keeps a failing
	// lookup to one log line until the next success.
	senderEmails      *feishu.SenderEmailCache
	senderEmailWarned *atomic.Bool
}

// Service owns the send/dedupe/readiness flow. It is safe for concurrent use
// because its dependencies (sender, store) are themselves concurrency-safe and
// its configuration is an immutable snapshot that a reload replaces whole.
type Service struct {
	cfgRef atomic.Pointer[config.Config]
	// backends is replaced whole, under backendsMu, when a reload starts or
	// stops an app; a reader keeps the map it loaded.
	backendsMu  sync.RWMutex
	backends    map[string]appBackend
	store       *dedupe.MemoryStore
	logger      *slog.Logger
	redactorRef atomic.Pointer[redactor]
	// generation counts the configs this service has run on: 1 at start,
	// one more for every applied reload.
	generation atomic.Uint64
	reloadMu   sync.Mutex
	reload     ReloadStatus

	legacyCapabilityCheck bool
	connectionMu          sync.RWMutex
	connectionStates      map[string]string

	inboundRoutes *inboundRouteRegistry
	commandBroker *commandBroker
	agentBroker   *agentBroker
	agentOwners   agentOwnershipStore
	agentRoutes   agentMessageRouteStore
	agentPosts    *agentPostLedger
	// The PostAgentCard ledger is separate so a card and a message never
	// collide on one operation id.
	agentCardPosts   *agentPostLedger
	agentCardUpdates *agentCardUpdateLedger
	agentCardLanes   *agentCardLanes
	// agentChatMu serializes CreateAgentChat, so one operation id cannot
	// create two chats.
	agentChatMu        sync.Mutex
	conversationLeases *conversationLeases
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
	// Set before anything binds a cfg method value, so every copy shares it.
	if cfg.RuntimeChats == nil {
		cfg.RuntimeChats = chatgrant.NewMemory()
	}
	backends := make(map[string]appBackend, len(senders))
	for rawAlias, sender := range senders {
		backends[effectiveAppAlias(rawAlias)] = newAppBackend(sender)
	}
	var connectionStates map[string]string
	if trackConnections {
		connectionStates = make(map[string]string)
		for _, alias := range cfg.AppAliases() {
			connectionStates[alias] = AppConnectionStarting
		}
	}
	agentBroker := newAgentBroker(cfg.DedupeTTL)
	svc := &Service{
		backends:              backends,
		store:                 store,
		logger:                logger,
		legacyCapabilityCheck: !trackConnections,
		connectionStates:      connectionStates,
		inboundRoutes:         newInboundRouteRegistry(cfg.DedupeTTL),
		commandBroker:         newCommandBroker(cfg.DedupeTTL),
		agentBroker:           agentBroker,
		// Process composition replaces this with the durable journal when a
		// state_dir exists; until then reply routes last the dedupe TTL.
		agentRoutes: messageroute.NewMemory(cfg.DedupeTTL),
		agentPosts:  newAgentPostLedger(cfg.DedupeTTL),

		agentCardPosts:   newAgentPostLedger(cfg.DedupeTTL),
		agentCardUpdates: newAgentCardUpdateLedger(cfg.DedupeTTL),
		agentCardLanes:   newAgentCardLanes(),
	}
	svc.cfgRef.Store(&cfg)
	svc.redactorRef.Store(newRedactor(cfg))
	svc.generation.Store(1)
	// The broker asks the live config, so a reload changes these grants for
	// the next delivery without touching any open stream.
	agentBroker.mentionIdentities = func(provider string) bool {
		return svc.cfg().ProviderAllowsAmbientMessages(provider)
	}
	agentBroker.ambientChat = func(provider, chatAlias string) bool {
		return svc.cfg().ProviderAllowsAmbientChat(provider, chatAlias)
	}
	agentBroker.allowsSenderEmail = func(provider string) bool {
		return svc.cfg().ProviderAllowsSenderEmail(provider)
	}
	return svc
}

// cfg returns the config snapshot in force. Callers must not mutate it;
// tests may, before any concurrent use.
func (s *Service) cfg() *config.Config {
	return s.cfgRef.Load()
}

func (s *Service) redactor() *redactor {
	return s.redactorRef.Load()
}

func (s *Service) backendMap() map[string]appBackend {
	s.backendsMu.RLock()
	defer s.backendsMu.RUnlock()
	return s.backends
}

// newAppBackend binds the optional Feishu capabilities one app's sender
// implements.
func newAppBackend(sender feishu.Sender) appBackend {
	backend := appBackend{sender: sender, senderEmailWarned: &atomic.Bool{}}
	if cards, ok := sender.(feishu.DynamicCards); ok {
		backend.dynamicCards = cards
	}
	if lookup, ok := sender.(feishu.AttachedContextLookup); ok {
		backend.attachedContext = lookup
	}
	if cot, ok := sender.(feishu.CoTMessages); ok {
		backend.cotMessages = cot
	}
	if reactions, ok := sender.(feishu.ReactionMessages); ok {
		backend.reactions = reactions
	}
	if contactUsers, ok := sender.(feishu.ContactUsers); ok {
		backend.contactUsers = contactUsers
	}
	if emails, ok := sender.(feishu.ContactEmails); ok {
		backend.senderEmails = feishu.NewSenderEmailCache(emails)
	}
	if images, ok := sender.(feishu.ImageUploader); ok {
		backend.images = images
	}
	if poster, ok := sender.(feishu.MessagePoster); ok {
		backend.poster = poster
	}
	if cards, ok := sender.(feishu.AgentCards); ok {
		backend.agentCards = cards
	}
	if pinner, ok := sender.(feishu.MessagePinner); ok {
		backend.pinner = pinner
	}
	if chats, ok := sender.(feishu.AgentChats); ok {
		backend.chats = chats
	}
	return backend
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
	backend, ok := s.backendMap()[effectiveAppAlias(alias)]
	return backend, ok && backend.sender != nil
}

func (s *Service) hasDynamicCards() bool {
	for _, backend := range s.backendMap() {
		if backend.dynamicCards != nil {
			return true
		}
	}
	return false
}

func (s *Service) appAllowed(provider, appAlias string) bool {
	return s.cfg().ProviderAllowsApp(provider, effectiveAppAlias(appAlias))
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
