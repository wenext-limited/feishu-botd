// Command agent is a minimal lifecycle demonstration for feishu-botd. It is
// not a production reliability template: it does not reconnect streams, retry
// retryable operations, or recover an unfinished card after a process failure,
// and it only acknowledges receipt of card-action events in its own logs.
//
// It intentionally has no Feishu credentials and never receives raw Feishu
// routing identifiers. Replace buildAnswer with a model call to turn this echo
// provider into a real agent.
package main

import (
	"context"
	"crypto/sha256"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	pb "feishu-botd/gen/feishubotd/v1"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	defaultSocket   = "/run/feishu-botd/feishu-botd.grpc.sock"
	defaultProvider = "example-agent"
	maxInFlight     = 4
	updateChunkSize = 160
	minUpdateDelay  = 125 * time.Millisecond
)

func main() {
	socket := flag.String("socket", envOr("FEISHU_BOTD_GRPC_SOCKET", defaultSocket), "feishu-botd Unix gRPC socket")
	provider := flag.String("provider", envOr("FEISHU_BOTD_AGENT_PROVIDER", defaultProvider), "stable provider identity")
	tokenFile := flag.String("token-file", envOr("FEISHU_BOTD_AGENT_AUTH_TOKEN_FILE", ""), "file containing this provider's bearer token")
	interval := flag.Duration("update-interval", 250*time.Millisecond, "minimum delay between cumulative snapshots")
	flag.Parse()

	if strings.TrimSpace(*socket) == "" || strings.TrimSpace(*provider) == "" || strings.TrimSpace(*tokenFile) == "" {
		log.Fatal("socket, provider, and token-file must be non-empty")
	}
	if *interval < minUpdateDelay {
		log.Fatal("update-interval must be at least 125ms (the daemon's per-card safety interval)")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	token, err := readProviderToken(*tokenFile)
	if err != nil {
		log.Fatalf("read provider token file: %v", err)
	}

	conn, err := grpc.NewClient(
		"passthrough:///feishu-botd",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", *socket)
		}),
		grpc.WithPerRPCCredentials(providerBearerCredentials{token: token}),
	)
	if err != nil {
		log.Fatalf("create gRPC client: %v", err)
	}
	defer conn.Close()

	client := pb.NewCommandServiceClient(conn)
	stream, err := client.SubscribeAgentEvents(ctx, &pb.SubscribeAgentEventsRequest{
		Provider:                 *provider,
		IncludeUnmatchedMessages: true,
		IncludeCardActions:       true,
	})
	if err != nil {
		log.Fatalf("subscribe to agent events: %v", err)
	}
	log.Printf("example agent is listening on the configured Unix socket")

	sem := make(chan struct{}, maxInFlight)
	var workers sync.WaitGroup
	defer workers.Wait()

	for {
		response, recvErr := stream.Recv()
		if recvErr != nil {
			if errors.Is(recvErr, io.EOF) || ctx.Err() != nil {
				return
			}
			log.Fatalf("receive agent event: %v", recvErr)
		}
		event := response.GetEvent()
		if event == nil {
			continue
		}
		if action := event.GetCardAction(); action != nil {
			// Action identifiers and payload_json are provider-defined and can be
			// sensitive, so this example logs only a fixed event classification.
			log.Printf("received card action")
			continue
		}
		message := event.GetMessage()
		if message == nil {
			continue
		}

		sem <- struct{}{}
		workers.Add(1)
		go func(deliveryID, prompt string) {
			defer workers.Done()
			defer func() { <-sem }()
			if handleErr := respond(ctx, client, *provider, deliveryID, prompt, *interval); handleErr != nil {
				log.Printf("agent response failed: %v", handleErr)
			}
		}(event.GetDeliveryId(), message.GetText())
	}
}

// providerBearerCredentials is safe here because this example always dials a
// local Unix socket. Cross-host plaintext gRPC is rejected by the daemon.
type providerBearerCredentials struct{ token string }

func (c providerBearerCredentials) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	return map[string]string{"authorization": "Bearer " + c.token}, nil
}

func (providerBearerCredentials) RequireTransportSecurity() bool { return false }

func readProviderToken(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	token := strings.TrimSpace(strings.SplitN(string(data), "\n", 2)[0])
	if token == "" {
		return "", errors.New("token file is empty")
	}
	if len(token) < 32 {
		return "", errors.New("provider token must be at least 32 bytes")
	}
	return token, nil
}

func respond(ctx context.Context, client pb.CommandServiceClient, provider, deliveryID, prompt string, interval time.Duration) error {
	start, err := startResponse(ctx, client, &pb.StartAgentResponseRequest{
		Provider:    provider,
		DeliveryId:  deliveryID,
		OperationId: operationID(deliveryID, "start", 0),
		Content: &pb.AgentResponseContent{
			Title:    "Example agent",
			Markdown: "Working…",
			Actions: []*pb.AgentResponseAction{{
				ActionId:    "acknowledge",
				Label:       "Acknowledge",
				PayloadJson: `{"source":"example"}`,
				Style:       pb.AgentResponseActionStyle_AGENT_RESPONSE_ACTION_STYLE_DEFAULT,
			}},
		},
	})
	if err != nil {
		return fmt.Errorf("start response: %w", err)
	}
	receipt := start.GetResponse()
	if receipt == nil {
		return errors.New("start response returned no receipt")
	}

	answer := buildAnswer(prompt)
	runes := []rune(answer)
	revision := receipt.GetRevision()
	step := 0
	for end := updateChunkSize; ; end += updateChunkSize {
		if end > len(runes) {
			end = len(runes)
		}
		step++
		update, updateErr := updateResponse(ctx, client, &pb.UpdateAgentResponseRequest{
			Provider:         provider,
			ResponseId:       receipt.GetResponseId(),
			OperationId:      operationID(deliveryID, "update", step),
			ExpectedRevision: revision,
			Markdown:         string(runes[:end]), // cumulative snapshot, not a token delta
		})
		if updateErr != nil {
			return fmt.Errorf("update response: %w", updateErr)
		}
		if update.GetResponse() == nil {
			return errors.New("update response returned no receipt")
		}
		revision = update.GetResponse().GetRevision()
		if end == len(runes) {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(interval):
		}
	}

	finish, err := finishResponse(ctx, client, &pb.FinishAgentResponseRequest{
		Provider:         provider,
		ResponseId:       receipt.GetResponseId(),
		OperationId:      operationID(deliveryID, "finish", step+1),
		ExpectedRevision: revision,
		Outcome:          pb.AgentResponseOutcome_AGENT_RESPONSE_OUTCOME_COMPLETED,
		Markdown:         answer,
		Summary:          "Example agent completed",
	})
	if err != nil {
		return fmt.Errorf("finish response: %w", err)
	}
	if finish.GetResponse() == nil {
		return errors.New("finish response returned no receipt")
	}
	log.Printf("finished one agent response at revision %d", finish.GetResponse().GetRevision())
	return nil
}

func startResponse(ctx context.Context, client pb.CommandServiceClient, request *pb.StartAgentResponseRequest) (*pb.StartAgentResponseResponse, error) {
	callCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	return client.StartAgentResponse(callCtx, request)
}

func updateResponse(ctx context.Context, client pb.CommandServiceClient, request *pb.UpdateAgentResponseRequest) (*pb.UpdateAgentResponseResponse, error) {
	callCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	return client.UpdateAgentResponse(callCtx, request)
}

func finishResponse(ctx context.Context, client pb.CommandServiceClient, request *pb.FinishAgentResponseRequest) (*pb.FinishAgentResponseResponse, error) {
	callCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	return client.FinishAgentResponse(callCtx, request)
}

func buildAnswer(prompt string) string {
	lines := strings.Split(strings.TrimSpace(prompt), "\n")
	for index, line := range lines {
		lines[index] = "> " + line
	}
	return "**Echo**\n\n" + strings.Join(lines, "\n")
}

// operationID is deterministic for a delivery and step. A transport retry must
// reuse the same request and operation id; changing content under the same id
// is rejected by feishu-botd.
func operationID(deliveryID, phase string, step int) string {
	sum := sha256.Sum256([]byte(deliveryID))
	return fmt.Sprintf("%s-%x-%d", phase, sum[:8], step)
}

func envOr(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}
