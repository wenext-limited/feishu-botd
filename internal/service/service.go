// Package service holds the transport-agnostic core logic for feishu-botd.
// Both the HTTP compatibility shim and the gRPC server delegate to a single
// *Service so the two transports cannot drift in validation, deduplication,
// sending, or redaction behavior.
package service

import (
	"log/slog"

	"feishu-botd/internal/config"
	"feishu-botd/internal/dedupe"
	"feishu-botd/internal/feishu"
)

// Version is the reported service version, shared by both transports.
const Version = "0.2.0"

// Provider names the upstream message provider in responses.
const Provider = "feishu"

// Service owns the send/dedupe/readiness flow. It is safe for concurrent use
// because its dependencies (sender, store) are themselves concurrency-safe and
// its configuration is an immutable snapshot taken at construction.
type Service struct {
	cfg           config.Config
	sender        feishu.Sender
	dynamicCards  feishu.DynamicCards
	store         *dedupe.MemoryStore
	logger        *slog.Logger
	redactor      *redactor
	inboundRoutes *inboundRouteRegistry
	commandBroker *commandBroker
	agentBroker   *agentBroker
}

// NewService builds a Service from an immutable config snapshot.
func NewService(cfg config.Config, sender feishu.Sender, store *dedupe.MemoryStore, logger *slog.Logger) *Service {
	if logger == nil {
		logger = slog.Default()
	}
	var dynamicCards feishu.DynamicCards
	if cards, ok := sender.(feishu.DynamicCards); ok {
		dynamicCards = cards
	}
	return &Service{
		cfg:           cfg,
		sender:        sender,
		dynamicCards:  dynamicCards,
		store:         store,
		logger:        logger,
		redactor:      newRedactor(cfg),
		inboundRoutes: newInboundRouteRegistry(cfg.DedupeTTL),
		commandBroker: newCommandBroker(cfg.DedupeTTL),
		agentBroker:   newAgentBroker(cfg.DedupeTTL),
	}
}

// SendResult is the transport-agnostic outcome of a delivery.
type SendResult struct {
	Provider  string
	MessageID string
	Duplicate bool
}
