package grpcapi

import (
	"context"
	"log/slog"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	pb "feishu-botd/gen/feishubotd/v1"
	"feishu-botd/internal/config"
	"feishu-botd/internal/dedupe"
	"feishu-botd/internal/feishu"
	"feishu-botd/internal/service"
)

const (
	alphaOnlyProviderToken = "alpha-only-provider-token-0123456789abcdef"
	noAppsProviderToken    = "no-apps-provider-token-0123456789abcdef01"
)

func TestProviderAuthenticatorPreservesAllowedAppsPresence(t *testing.T) {
	const (
		allToken     = "all-apps-provider-token-0123456789abcdef"
		noneToken    = "no-apps-provider-token-0123456789abcdef"
		alphaToken   = "alpha-provider-token-0123456789abcdef01"
		literalToken = "literal-provider-token-0123456789abcdef0"
	)
	authenticator := newProviderAuthenticator(map[string]config.AgentProviderConfig{
		"all": {
			AuthToken: allToken,
		},
		"none": {
			AuthToken:             noneToken,
			AllowedApps:           []string{},
			AllowedAppsConfigured: true,
		},
		"alpha": {
			AuthToken:             alphaToken,
			AllowedApps:           []string{"alpha"},
			AllowedAppsConfigured: true,
		},
		"literal": {
			AuthToken:   literalToken,
			AllowedApps: []string{"beta"},
		},
	})

	tests := []struct {
		name           string
		token          string
		wantConfigured bool
		wantApps       []string
		wantNonNil     bool
	}{
		{name: "absent means all", token: allToken},
		{name: "explicit empty means none", token: noneToken, wantConfigured: true, wantApps: []string{}, wantNonNil: true},
		{name: "configured subset", token: alphaToken, wantConfigured: true, wantApps: []string{"alpha"}, wantNonNil: true},
		{name: "programmatic non-nil list is configured", token: literalToken, wantConfigured: true, wantApps: []string{"beta"}, wantNonNil: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(
				"authorization", "Bearer "+test.token,
			))
			principal, ok := authenticator.authenticate(ctx)
			if !ok {
				t.Fatal("provider credential did not authenticate")
			}
			if principal.allowedAppsConfigured != test.wantConfigured {
				t.Fatalf("allowedAppsConfigured = %t, want %t", principal.allowedAppsConfigured, test.wantConfigured)
			}
			if (principal.allowedApps != nil) != test.wantNonNil {
				t.Fatalf("allowedApps nilness = %t, want non-nil %t", principal.allowedApps == nil, test.wantNonNil)
			}
			if len(principal.allowedApps) != len(test.wantApps) {
				t.Fatalf("allowedApps = %#v, want %#v", principal.allowedApps, test.wantApps)
			}
			for i := range test.wantApps {
				if principal.allowedApps[i] != test.wantApps[i] {
					t.Fatalf("allowedApps = %#v, want %#v", principal.allowedApps, test.wantApps)
				}
			}

			scopeCtx := context.WithValue(context.Background(), providerPrincipalKey{}, principal)
			gotApps, gotConfigured := authenticatedProviderAppScope(scopeCtx)
			if gotConfigured != test.wantConfigured || len(gotApps) != len(test.wantApps) {
				t.Fatalf("context scope = (%#v, %t), want (%#v, %t)", gotApps, gotConfigured, test.wantApps, test.wantConfigured)
			}
			if test.wantConfigured && (gotApps != nil) != test.wantNonNil {
				t.Fatalf("context scope lost configured empty list: %#v", gotApps)
			}
		})
	}
}

func TestProviderAllowedAppsScopeReachesLegacySubscription(t *testing.T) {
	t.Run("configured subset", func(t *testing.T) {
		cfg := multiAppProviderTestConfig()
		conn, svc, _ := startMultiAppProviderUnixServer(t, cfg)
		client := pb.NewCommandServiceClient(conn)

		streamCtx, cancelStream := context.WithTimeout(
			bearerContext(alphaOnlyProviderToken),
			3*time.Second,
		)
		defer cancelStream()
		stream, err := client.Subscribe(streamCtx, &pb.SubscribeRequest{
			Provider: "alpha-only",
			Commands: []string{"ask"},
		})
		if err != nil {
			t.Fatalf("subscribe: %v", err)
		}

		first := receiveLegacyCommand(t, svc, stream, providerTestCommand("alpha", "alpha-1"))
		if first.GetCommand().GetDeliveryId() != "alpha-1" {
			t.Fatalf("first delivery = %q, want alpha-1", first.GetCommand().GetDeliveryId())
		}
		if _, exposed := first.GetCommand().GetMetadata()["app_alias"]; exposed {
			t.Fatalf("legacy command exposed app_alias: %#v", first.GetCommand().GetMetadata())
		}

		received := make(chan legacyReceiveResult, 1)
		go func() {
			response, recvErr := stream.Recv()
			received <- legacyReceiveResult{response: response, err: recvErr}
		}()

		delivered, apiErr := svc.DispatchCommand(
			context.Background(),
			providerTestCommand("beta", "beta-denied"),
		)
		if apiErr != nil {
			t.Fatalf("dispatch beta: %v", apiErr)
		}
		if delivered != 0 {
			t.Fatalf("beta delivered to alpha-only subscription: %d", delivered)
		}
		select {
		case result := <-received:
			t.Fatalf("alpha-only stream received beta event: response=%#v err=%v", result.response, result.err)
		case <-time.After(100 * time.Millisecond):
		}

		delivered, apiErr = svc.DispatchCommand(
			context.Background(),
			providerTestCommand("alpha", "alpha-2"),
		)
		if apiErr != nil {
			t.Fatalf("dispatch second alpha event: %v", apiErr)
		}
		if delivered != 1 {
			t.Fatalf("second alpha delivered = %d, want 1", delivered)
		}
		select {
		case result := <-received:
			if result.err != nil {
				t.Fatalf("receive second alpha event: %v", result.err)
			}
			if result.response.GetCommand().GetDeliveryId() != "alpha-2" {
				t.Fatalf("second delivery = %q, want alpha-2", result.response.GetCommand().GetDeliveryId())
			}
		case <-time.After(time.Second):
			t.Fatal("alpha-only stream did not receive second alpha event")
		}
	})

	t.Run("explicit empty list", func(t *testing.T) {
		cfg := multiAppProviderTestConfig()
		conn, svc, _ := startMultiAppProviderUnixServer(t, cfg)
		client := pb.NewCommandServiceClient(conn)

		streamCtx, cancelStream := context.WithCancel(bearerContext(noAppsProviderToken))
		stream, err := client.Subscribe(streamCtx, &pb.SubscribeRequest{
			Provider: "none",
			Commands: []string{"ask"},
		})
		if err != nil {
			t.Fatalf("subscribe: %v", err)
		}
		received := make(chan legacyReceiveResult, 1)
		go func() {
			response, recvErr := stream.Recv()
			received <- legacyReceiveResult{response: response, err: recvErr}
		}()

		// The subscription RPC is asynchronous. Repeatedly publish fresh events
		// so at least one occurs after registration; none may be enqueued.
		for attempt := 0; attempt < 20; attempt++ {
			for _, appAlias := range []string{"alpha", "beta"} {
				deliveryID := appAlias + "-none-" + time.Now().Format("150405.000000000")
				if _, apiErr := svc.DispatchCommand(
					context.Background(),
					providerTestCommand(appAlias, deliveryID),
				); apiErr != nil {
					t.Fatalf("dispatch %s: %v", appAlias, apiErr)
				}
			}
			select {
			case result := <-received:
				t.Fatalf("empty allowed_apps stream received event: response=%#v err=%v", result.response, result.err)
			case <-time.After(10 * time.Millisecond):
			}
		}

		cancelStream()
		select {
		case result := <-received:
			if status.Code(result.err) != codes.Canceled {
				t.Fatalf("stream cancellation error = %v, want Canceled", result.err)
			}
		case <-time.After(time.Second):
			t.Fatal("empty allowed_apps stream did not stop after cancellation")
		}
	})
}

func TestProviderAllowedAppsDeniedLegacyMutationIsOpaque(t *testing.T) {
	cfg := multiAppProviderTestConfig()
	conn, svc, senders := startMultiAppProviderUnixServer(t, cfg)

	// Seed a beta delivery with the same provider through the compatibility
	// in-process API. This deliberately bypasses subscription filtering and
	// proves mutation authorization independently from the fan-out grant.
	sub, apiErr := svc.SubscribeCommands(context.Background(), "alpha-only", []string{"ask"})
	if apiErr != nil {
		t.Fatalf("seed subscription: %v", apiErr)
	}
	defer sub.Close()
	delivered, apiErr := svc.DispatchCommand(
		context.Background(),
		providerTestCommand("beta", "beta-mutation-denied"),
	)
	if apiErr != nil {
		t.Fatalf("seed beta delivery: %v", apiErr)
	}
	if delivered != 1 {
		t.Fatalf("seed beta delivered = %d, want 1", delivered)
	}
	select {
	case <-sub.C:
	case <-time.After(time.Second):
		t.Fatal("seed subscription did not receive beta delivery")
	}

	_, err := pb.NewCommandServiceClient(conn).Respond(
		bearerContext(alphaOnlyProviderToken),
		&pb.RespondRequest{
			DeliveryId: "beta-mutation-denied",
			Reply: &pb.RespondRequest_Markdown{
				Markdown: &pb.MarkdownContent{Markdown: "must not leave alpha"},
			},
		},
	)
	if status.Code(err) != codes.NotFound {
		t.Fatalf("denied mutation code = %v, want NotFound", status.Code(err))
	}
	detail := botdDetail(t, err)
	if detail.GetCode() != "unknown_delivery" {
		t.Fatalf("denied mutation detail = %#v, want unknown_delivery", detail)
	}
	if senders["alpha"].calls != 0 || senders["beta"].calls != 0 {
		t.Fatalf(
			"denied mutation reached sender: alpha=%d beta=%d",
			senders["alpha"].calls,
			senders["beta"].calls,
		)
	}
}

func TestProviderAllowedAppsScopeReachesAgentSubscription(t *testing.T) {
	cfg := multiAppProviderTestConfig()
	conn, svc, _ := startMultiAppProviderUnixServer(t, cfg)
	client := pb.NewCommandServiceClient(conn)

	streamCtx, cancelStream := context.WithTimeout(
		bearerContext(alphaOnlyProviderToken),
		3*time.Second,
	)
	defer cancelStream()
	stream, err := client.SubscribeAgentEvents(streamCtx, &pb.SubscribeAgentEventsRequest{
		Provider:                 "alpha-only",
		Commands:                 []string{"ask"},
		IncludeUnmatchedMessages: true,
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	first := dispatchAndReceiveAgentEvent(
		t,
		svc,
		stream,
		providerTestCommand("alpha", "alpha-agent-1"),
	)
	if first.GetDeliveryId() != "alpha-agent-1" || first.GetMetadata()["app_alias"] != "alpha" {
		t.Fatalf("first alpha event = %#v", first)
	}

	received := make(chan agentReceiveResult, 1)
	go func() {
		response, recvErr := stream.Recv()
		received <- agentReceiveResult{response: response, err: recvErr}
	}()
	if _, apiErr := svc.DispatchCommand(
		context.Background(),
		providerTestCommand("beta", "beta-agent-denied"),
	); apiErr != nil {
		t.Fatalf("dispatch beta: %v", apiErr)
	}
	select {
	case result := <-received:
		t.Fatalf("alpha-only agent stream received beta event: response=%#v err=%v", result.response, result.err)
	case <-time.After(100 * time.Millisecond):
	}

	if _, apiErr := svc.DispatchCommand(
		context.Background(),
		providerTestCommand("alpha", "alpha-agent-2"),
	); apiErr != nil {
		t.Fatalf("dispatch second alpha event: %v", apiErr)
	}
	select {
	case result := <-received:
		if result.err != nil {
			t.Fatalf("receive second alpha event: %v", result.err)
		}
		event := result.response.GetEvent()
		if event.GetDeliveryId() != "alpha-agent-2" || event.GetMetadata()["app_alias"] != "alpha" {
			t.Fatalf("second alpha event = %#v", event)
		}
	case <-time.After(time.Second):
		t.Fatal("alpha-only agent stream did not receive second alpha event")
	}
}

func TestProviderAllowedAppsDeniedAgentMutationsAreOpaque(t *testing.T) {
	cfg := multiAppProviderTestConfig()
	if cfg.ProviderAllowsApp("alpha-only", "beta") {
		t.Fatal("fixture provider unexpectedly allows beta")
	}
	conn, svc, senders := startMultiAppProviderUnixServer(t, cfg)

	// As in the legacy mutation test, use the compatibility in-process
	// subscription to seed a grant for the same provider on beta. Both gRPC
	// mutations must still re-resolve the handle's app and fail closed.
	sub, apiErr := svc.SubscribeAgentEvents(context.Background(), service.AgentSubscribeOptions{
		Provider: "alpha-only",
		Commands: []string{"ask"},
	})
	if apiErr != nil {
		t.Fatalf("seed agent subscription: %v", apiErr)
	}
	defer sub.Close()
	seed := providerTestCommand("beta", "beta-agent-mutation-denied")
	if _, apiErr := svc.DispatchCommand(context.Background(), seed); apiErr != nil {
		t.Fatalf("seed beta agent delivery: %v", apiErr)
	}
	select {
	case event := <-sub.C:
		if event.DeliveryID != seed.DeliveryID || event.Metadata["app_alias"] != "beta" {
			t.Fatalf("seed event = %#v", event)
		}
	case <-time.After(time.Second):
		t.Fatal("seed agent subscription did not receive beta delivery")
	}

	client := pb.NewCommandServiceClient(conn)
	_, err := client.StartAgentResponse(
		bearerContext(alphaOnlyProviderToken),
		&pb.StartAgentResponseRequest{
			Provider:    "alpha-only",
			DeliveryId:  seed.DeliveryID,
			OperationId: "start-denied",
			Content:     &pb.AgentResponseContent{Markdown: "must not create a card"},
		},
	)
	assertOpaqueNotFound(t, err, "unknown_delivery")

	_, err = client.SendAgentFollowUp(
		bearerContext(alphaOnlyProviderToken),
		&pb.SendAgentFollowUpRequest{
			Provider:       "alpha-only",
			ConversationId: seed.ConversationID,
			OperationId:    "follow-up-denied",
			Markdown:       "must not leave beta",
		},
	)
	assertOpaqueNotFound(t, err, "unknown_conversation")
	if senders["alpha"].calls != 0 || senders["beta"].calls != 0 {
		t.Fatalf(
			"denied agent mutation reached sender: alpha=%d beta=%d",
			senders["alpha"].calls,
			senders["beta"].calls,
		)
	}
}

func assertOpaqueNotFound(t *testing.T, err error, wantCode string) {
	t.Helper()
	if status.Code(err) != codes.NotFound {
		t.Fatalf("%s error code = %v, want NotFound", wantCode, status.Code(err))
	}
	detail := botdDetail(t, err)
	if detail.GetCode() != wantCode {
		t.Fatalf("error detail = %#v, want %s", detail, wantCode)
	}
}

type legacyReceiveResult struct {
	response *pb.SubscribeResponse
	err      error
}

func receiveLegacyCommand(
	t *testing.T,
	svc *service.Service,
	stream pb.CommandService_SubscribeClient,
	input service.CommandInput,
) *pb.SubscribeResponse {
	t.Helper()
	received := make(chan legacyReceiveResult, 1)
	go func() {
		response, err := stream.Recv()
		received <- legacyReceiveResult{response: response, err: err}
	}()
	for attempt := 0; attempt < 100; attempt++ {
		delivered, apiErr := svc.DispatchCommand(context.Background(), input)
		if apiErr != nil {
			t.Fatalf("dispatch command: %v", apiErr)
		}
		if delivered > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	select {
	case result := <-received:
		if result.err != nil {
			t.Fatalf("receive command: %v", result.err)
		}
		return result.response
	case <-time.After(time.Second):
		t.Fatal("subscription did not receive command")
		return nil
	}
}

func providerTestCommand(appAlias, deliveryID string) service.CommandInput {
	chatAlias := appAlias + "-ops"
	return service.CommandInput{
		AppAlias:       appAlias,
		DeliveryID:     deliveryID,
		Command:        "ask",
		Prompt:         "provider auth fixture",
		ConversationID: "conversation-" + deliveryID,
		ChatAlias:      chatAlias,
		SenderID:       "sender-" + appAlias,
		Metadata: map[string]string{
			"message_id":   "message-" + deliveryID,
			"chat_type":    "group",
			"message_type": "text",
		},
	}
}

func multiAppProviderTestConfig() config.Config {
	return config.Config{
		Apps: map[string]config.AppConfig{
			"alpha": {
				AppID:     "app_alpha",
				AppSecret: "secret_alpha",
				Channels:  map[string]string{"alpha-ops": "oc_alpha"},
			},
			"beta": {
				AppID:     "app_beta",
				AppSecret: "secret_beta",
				Channels:  map[string]string{"beta-ops": "oc_beta"},
			},
		},
		Channels: map[string]string{
			"alpha-ops": "oc_alpha",
			"beta-ops":  "oc_beta",
		},
		ChannelRoutes: map[string]config.ChannelRoute{
			"alpha-ops": {AppAlias: "alpha", ChatID: "oc_alpha"},
			"beta-ops":  {AppAlias: "beta", ChatID: "oc_beta"},
		},
		AgentProviders: map[string]config.AgentProviderConfig{
			"alpha-only": {
				AuthToken:              alphaOnlyProviderToken,
				AllowedCommands:        []string{"ask"},
				AllowUnmatchedMessages: true,
				AllowFollowUpMessages:  true,
				AllowLegacyCommands:    true,
				AllowedApps:            []string{"alpha"},
				AllowedAppsConfigured:  true,
			},
			"none": {
				AuthToken:             noAppsProviderToken,
				AllowedCommands:       []string{"ask"},
				AllowLegacyCommands:   true,
				AllowedApps:           []string{},
				AllowedAppsConfigured: true,
			},
		},
		DedupeTTL:   time.Hour,
		SendTimeout: time.Second,
	}
}

func startMultiAppProviderUnixServer(
	t *testing.T,
	cfg config.Config,
) (*grpc.ClientConn, *service.Service, map[string]*fakeSender) {
	t.Helper()
	senders := map[string]*fakeSender{
		"alpha": {messageID: "message-alpha"},
		"beta":  {messageID: "message-beta"},
	}
	senderInterfaces := make(map[string]feishu.Sender, len(senders))
	for appAlias, sender := range senders {
		senderInterfaces[appAlias] = sender
	}
	svc := service.NewMultiAppService(
		cfg,
		senderInterfaces,
		dedupe.NewMemoryStore(time.Hour),
		slog.Default(),
	)
	srv := NewServer(cfg, svc, slog.Default())
	ctx, cancel := context.WithCancel(context.Background())
	socketPath := tempSocket(t)
	errCh := make(chan error, 1)
	go func() { errCh <- srv.ListenAndServeUnix(ctx, socketPath) }()
	t.Cleanup(func() {
		cancel()
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), time.Second)
		defer shutdownCancel()
		_ = srv.Shutdown(shutdownCtx)
	})
	conn := dial(t, func(ctx context.Context) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", socketPath)
	})
	waitHealthy(t, conn, errCh)
	return conn, svc, senders
}
