package grpcapi

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	pb "feishu-botd/gen/feishubotd/v1"
	"feishu-botd/internal/config"
	"feishu-botd/internal/notify"
)

type providerPrincipalKey struct{}

type providerPrincipal struct {
	provider               string
	allowedCommands        map[string]struct{}
	allowedApps            []string
	allowedAppsConfigured  bool
	allowUnmatchedMessages bool
	allowCardActions       bool
	allowAttachedContext   bool
	allowFollowUpMessages  bool
	allowMessageReactions  bool
	allowLegacyCommands    bool
}

type providerCredential struct {
	principal providerPrincipal
	digest    [sha256.Size]byte
}

// providerAuthenticator resolves a bearer credential to exactly one configured
// provider. Config validation rejects duplicate credentials; keeping only
// fixed-size digests here also makes every comparison constant-time.
type providerAuthenticator struct {
	credentials []providerCredential
}

func newProviderAuthenticator(providers map[string]config.AgentProviderConfig) providerAuthenticator {
	authenticator := providerAuthenticator{credentials: make([]providerCredential, 0, len(providers))}
	for provider, providerCfg := range providers {
		allowedCommands := make(map[string]struct{}, len(providerCfg.AllowedCommands))
		for _, command := range providerCfg.AllowedCommands {
			if normalized := normalizeSubscriptionCommand(command); normalized != "" {
				allowedCommands[normalized] = struct{}{}
			}
		}
		allowedAppsConfigured := providerCfg.AllowedAppsConfigured || providerCfg.AllowedApps != nil
		var allowedApps []string
		if allowedAppsConfigured {
			// Preserve a configured empty allowlist as a non-nil empty slice. Nil
			// means the field was absent and therefore permits every app.
			allowedApps = append([]string{}, providerCfg.AllowedApps...)
		}
		authenticator.credentials = append(authenticator.credentials, providerCredential{
			principal: providerPrincipal{
				provider:               provider,
				allowedCommands:        allowedCommands,
				allowedApps:            allowedApps,
				allowedAppsConfigured:  allowedAppsConfigured,
				allowUnmatchedMessages: providerCfg.AllowUnmatchedMessages,
				allowCardActions:       providerCfg.AllowCardActions,
				allowAttachedContext:   providerCfg.AllowAttachedContext,
				allowFollowUpMessages:  providerCfg.AllowFollowUpMessages,
				allowMessageReactions:  providerCfg.AllowMessageReactions,
				allowLegacyCommands:    providerCfg.AllowLegacyCommands,
			},
			digest: sha256.Sum256([]byte(providerCfg.AuthToken)),
		})
	}
	return authenticator
}

// isHealthMethod reports whether a fully-qualified gRPC method belongs to a
// health service. Health stays unauthenticated even on the loopback TCP path so
// process managers and grpc_health_probe can poll without a token.
func isHealthMethod(fullMethod string) bool {
	return strings.HasPrefix(fullMethod, "/feishubotd.v1.BotdHealthService/") ||
		strings.HasPrefix(fullMethod, "/grpc.health.v1.Health/")
}

// isAgentMethod is deliberately explicit: the new agent RPCs always require a
// provider credential, and the general notification bearer never grants access.
func isAgentMethod(fullMethod string) bool {
	switch fullMethod {
	case pb.CommandService_SubscribeAgentEvents_FullMethodName,
		pb.CommandService_GetAgentAttachedContext_FullMethodName,
		pb.CommandService_StartAgentResponse_FullMethodName,
		pb.CommandService_UpdateAgentResponse_FullMethodName,
		pb.CommandService_FinishAgentResponse_FullMethodName,
		pb.CommandService_ReplaceAgentResponse_FullMethodName,
		pb.CommandService_SendAgentFollowUp_FullMethodName:
		return true
	default:
		return false
	}
}

func authorizeAgentAttachedContext(ctx context.Context, requested string) error {
	if err := requireProviderIdentity(ctx, requested); err != nil {
		return err
	}
	principal, _ := authenticatedProvider(ctx)
	if !principal.allowAttachedContext {
		return providerScopeDenied(ctx)
	}
	return nil
}

func isLegacyProviderMethod(fullMethod string) bool {
	return fullMethod == pb.CommandService_Subscribe_FullMethodName ||
		fullMethod == pb.CommandService_Respond_FullMethodName
}

func requiresProviderAuth(fullMethod string, scopeLegacy bool) bool {
	return isAgentMethod(fullMethod) || scopeLegacy && isLegacyProviderMethod(fullMethod)
}

func bearerToken(ctx context.Context) (string, bool) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", false
	}
	vals := md.Get("authorization")
	if len(vals) != 1 {
		return "", false
	}
	got := strings.TrimSpace(vals[0])
	if !strings.HasPrefix(got, "Bearer ") {
		return "", false
	}
	got = strings.TrimSpace(strings.TrimPrefix(got, "Bearer "))
	return got, got != ""
}

// authorized performs a constant-time bearer-token check against incoming
// metadata. It mirrors the HTTP shim's Authorization handling.
func authorized(ctx context.Context, expected string) bool {
	if expected == "" {
		return false
	}
	got, ok := bearerToken(ctx)
	if !ok {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(got), []byte(expected)) == 1
}

func (a providerAuthenticator) authenticate(ctx context.Context) (providerPrincipal, bool) {
	token, ok := bearerToken(ctx)
	if !ok {
		return providerPrincipal{}, false
	}
	digest := sha256.Sum256([]byte(token))
	principal := providerPrincipal{}
	matched := 0
	for _, credential := range a.credentials {
		equal := subtle.ConstantTimeCompare(digest[:], credential.digest[:])
		if equal == 1 {
			principal = credential.principal
		}
		matched |= equal
	}
	return principal, matched == 1
}

// unauthenticated returns the stable general-bearer authentication error used
// by both TCP and scoped Unix listeners.
func unauthenticated(ctx context.Context) error {
	// Mirror the HTTP 401 body: a stable code + request id inside a BotdError
	// detail, mapped to codes.Unauthenticated.
	return grpcError(notify.NewAPIError(401, "unauthorized", "missing or invalid bearer token", false), requestIDFromContext(ctx))
}

func providerUnauthenticated(ctx context.Context) error {
	return grpcError(notify.NewAPIError(401, "unauthorized", "missing or invalid provider bearer token", false), requestIDFromContext(ctx))
}

func providerIdentityMismatch(ctx context.Context) error {
	return grpcError(notify.NewAPIError(403, "provider_identity_mismatch", "provider does not match authenticated credential", false), requestIDFromContext(ctx))
}

func providerScopeDenied(ctx context.Context) error {
	return grpcError(notify.NewAPIError(403, "provider_scope_denied", "requested provider capability is not allowed", false), requestIDFromContext(ctx))
}

func authenticatedProvider(ctx context.Context) (providerPrincipal, error) {
	principal, ok := ctx.Value(providerPrincipalKey{}).(providerPrincipal)
	if !ok || principal.provider == "" {
		return providerPrincipal{}, providerUnauthenticated(ctx)
	}
	return principal, nil
}

func authenticatedProviderAppScope(ctx context.Context) (allowedApps []string, configured bool) {
	principal, ok := ctx.Value(providerPrincipalKey{}).(providerPrincipal)
	if !ok || !principal.allowedAppsConfigured {
		return nil, false
	}
	return append([]string{}, principal.allowedApps...), true
}

func requireProviderIdentity(ctx context.Context, requested string) error {
	principal, err := authenticatedProvider(ctx)
	if err != nil {
		return err
	}
	if strings.TrimSpace(requested) != principal.provider {
		return providerIdentityMismatch(ctx)
	}
	return nil
}

func authorizeLegacySubscription(ctx context.Context, requested string, commands []string) error {
	if _, ok := ctx.Value(providerPrincipalKey{}).(providerPrincipal); !ok {
		return nil
	}
	if err := requireProviderIdentity(ctx, requested); err != nil {
		return err
	}
	principal, _ := authenticatedProvider(ctx)
	if !principal.allowLegacyCommands || !commandsAllowed(principal, commands) {
		return providerScopeDenied(ctx)
	}
	return nil
}

func authorizeLegacyResponse(ctx context.Context) (string, error) {
	principal, ok := ctx.Value(providerPrincipalKey{}).(providerPrincipal)
	if !ok {
		return "", nil
	}
	if !principal.allowLegacyCommands {
		return "", providerScopeDenied(ctx)
	}
	return principal.provider, nil
}

func authorizeAgentSubscription(ctx context.Context, requested string, commands []string, unmatched, actions, reactions bool) error {
	if err := requireProviderIdentity(ctx, requested); err != nil {
		return err
	}
	principal, _ := authenticatedProvider(ctx)
	if !commandsAllowed(principal, commands) || unmatched && !principal.allowUnmatchedMessages || actions && !principal.allowCardActions || reactions && !principal.allowMessageReactions {
		return providerScopeDenied(ctx)
	}
	return nil
}

// authorizeAgentFollowUp gates the capability itself. Which conversations the
// provider may address is a separate, narrower check the service applies against
// the events it actually received.
func authorizeAgentFollowUp(ctx context.Context, requested string) error {
	if err := requireProviderIdentity(ctx, requested); err != nil {
		return err
	}
	principal, _ := authenticatedProvider(ctx)
	if !principal.allowFollowUpMessages {
		return providerScopeDenied(ctx)
	}
	return nil
}

func commandsAllowed(principal providerPrincipal, commands []string) bool {
	for _, command := range commands {
		command = normalizeSubscriptionCommand(command)
		if command == "" {
			continue
		}
		if _, allowed := principal.allowedCommands[command]; !allowed {
			return false
		}
	}
	return true
}

func normalizeSubscriptionCommand(command string) string {
	return strings.ToLower(strings.TrimLeft(strings.TrimSpace(command), "/"))
}

func authUnaryInterceptor(token string, scopeLegacy bool) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if isHealthMethod(info.FullMethod) || requiresProviderAuth(info.FullMethod, scopeLegacy) {
			return handler(ctx, req)
		}
		if !authorized(ctx, token) {
			return nil, unauthenticated(ctx)
		}
		return handler(ctx, req)
	}
}

func authStreamInterceptor(token string, scopeLegacy bool) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if isHealthMethod(info.FullMethod) || requiresProviderAuth(info.FullMethod, scopeLegacy) {
			return handler(srv, ss)
		}
		if !authorized(ss.Context(), token) {
			return unauthenticated(ss.Context())
		}
		return handler(srv, ss)
	}
}

// providerAuth interceptors are installed on every listener, including Unix
// sockets. Socket access establishes local reachability; the provider-specific
// bearer establishes which logical provider that peer is allowed to act as.
func providerAuthUnaryInterceptor(authenticator providerAuthenticator, scopeLegacy bool) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if !requiresProviderAuth(info.FullMethod, scopeLegacy) {
			return handler(ctx, req)
		}
		principal, ok := authenticator.authenticate(ctx)
		if !ok {
			return nil, providerUnauthenticated(ctx)
		}
		return handler(context.WithValue(ctx, providerPrincipalKey{}, principal), req)
	}
}

func providerAuthStreamInterceptor(authenticator providerAuthenticator, scopeLegacy bool) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if !requiresProviderAuth(info.FullMethod, scopeLegacy) {
			return handler(srv, ss)
		}
		principal, ok := authenticator.authenticate(ss.Context())
		if !ok {
			return providerUnauthenticated(ss.Context())
		}
		ctx := context.WithValue(ss.Context(), providerPrincipalKey{}, principal)
		return handler(srv, &contextStream{ServerStream: ss, ctx: ctx})
	}
}
