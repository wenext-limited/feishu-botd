package compat_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/encoding/protowire"

	"feishu-botd/internal/config"
	"feishu-botd/internal/dedupe"
	"feishu-botd/internal/feishu"
	"feishu-botd/internal/grpcapi"
	"feishu-botd/internal/httpapi"
	"feishu-botd/internal/notify"
	"feishu-botd/internal/service"
)

const (
	oldProviderToken = "old-provider-token-0123456789abcdef0123456789"
	oldGeneralToken  = "old-general-token-0123456789abcdef012345678901"

	oldSubscribeMethod = "/feishubotd.v1.CommandService/SubscribeAgentEvents"
	oldStartMethod     = "/feishubotd.v1.CommandService/StartAgentResponse"
	oldUpdateMethod    = "/feishubotd.v1.CommandService/UpdateAgentResponse"
	oldFinishMethod    = "/feishubotd.v1.CommandService/FinishAgentResponse"
	oldFollowUpMethod  = "/feishubotd.v1.CommandService/SendAgentFollowUp"

	oldOutcomeCompleted int32 = 1
	oldPhaseStreaming   int32 = 1
	oldPhaseCompleted   int32 = 2
)

var oldSubscribeDesc = grpc.StreamDesc{
	StreamName:    "SubscribeAgentEvents",
	ServerStreams: true,
}

// These messages freeze the provider wire contract at branch
// agent-followup-send (8ec4510). They deliberately implement only the legacy
// MessageV1 interface and use hard-coded protobuf field tags: the test does not
// import the daemon's generated bindings, so changing a field number or method
// path breaks this already-compiled-client simulation.
type oldSubscribeAgentEventsRequest struct {
	Provider                 string   `protobuf:"bytes,1,opt,name=provider,proto3"`
	Commands                 []string `protobuf:"bytes,2,rep,name=commands,proto3"`
	IncludeUnmatchedMessages bool     `protobuf:"varint,3,opt,name=include_unmatched_messages,json=includeUnmatchedMessages,proto3"`
	IncludeCardActions       bool     `protobuf:"varint,4,opt,name=include_card_actions,json=includeCardActions,proto3"`
}

func (m *oldSubscribeAgentEventsRequest) Reset() { *m = oldSubscribeAgentEventsRequest{} }
func (*oldSubscribeAgentEventsRequest) String() string {
	return "oldSubscribeAgentEventsRequest"
}
func (*oldSubscribeAgentEventsRequest) ProtoMessage() {}

type oldSubscribeAgentEventsResponse struct {
	Event *oldInboundAgentEvent `protobuf:"bytes,1,opt,name=event,proto3"`
}

func (m *oldSubscribeAgentEventsResponse) Reset() { *m = oldSubscribeAgentEventsResponse{} }
func (*oldSubscribeAgentEventsResponse) String() string {
	return "oldSubscribeAgentEventsResponse"
}
func (*oldSubscribeAgentEventsResponse) ProtoMessage() {}

type oldInboundAgentEvent struct {
	DeliveryID     string                  `protobuf:"bytes,1,opt,name=delivery_id,json=deliveryId,proto3"`
	ConversationID string                  `protobuf:"bytes,2,opt,name=conversation_id,json=conversationId,proto3"`
	ChatAlias      string                  `protobuf:"bytes,3,opt,name=chat_alias,json=chatAlias,proto3"`
	SenderID       string                  `protobuf:"bytes,4,opt,name=sender_id,json=senderId,proto3"`
	Message        *oldInboundAgentMessage `protobuf:"bytes,10,opt,name=message,proto3"`
}

func (m *oldInboundAgentEvent) Reset()       { *m = oldInboundAgentEvent{} }
func (*oldInboundAgentEvent) String() string { return "oldInboundAgentEvent" }
func (*oldInboundAgentEvent) ProtoMessage()  {}
func (m *oldInboundAgentEvent) GetDeliveryID() string {
	if m == nil {
		return ""
	}
	return m.DeliveryID
}

type oldInboundAgentMessage struct {
	Text        string `protobuf:"bytes,1,opt,name=text,proto3"`
	Command     string `protobuf:"bytes,2,opt,name=command,proto3"`
	CommandText string `protobuf:"bytes,3,opt,name=command_text,json=commandText,proto3"`
}

func (m *oldInboundAgentMessage) Reset()       { *m = oldInboundAgentMessage{} }
func (*oldInboundAgentMessage) String() string { return "oldInboundAgentMessage" }
func (*oldInboundAgentMessage) ProtoMessage()  {}

type oldAgentResponseContent struct {
	Title    string `protobuf:"bytes,1,opt,name=title,proto3"`
	Markdown string `protobuf:"bytes,2,opt,name=markdown,proto3"`
}

func (m *oldAgentResponseContent) Reset()       { *m = oldAgentResponseContent{} }
func (*oldAgentResponseContent) String() string { return "oldAgentResponseContent" }
func (*oldAgentResponseContent) ProtoMessage()  {}

type oldStartAgentResponseRequest struct {
	Provider    string                   `protobuf:"bytes,1,opt,name=provider,proto3"`
	DeliveryID  string                   `protobuf:"bytes,2,opt,name=delivery_id,json=deliveryId,proto3"`
	OperationID string                   `protobuf:"bytes,3,opt,name=operation_id,json=operationId,proto3"`
	Content     *oldAgentResponseContent `protobuf:"bytes,10,opt,name=content,proto3"`
}

func (m *oldStartAgentResponseRequest) Reset() { *m = oldStartAgentResponseRequest{} }
func (*oldStartAgentResponseRequest) String() string {
	return "oldStartAgentResponseRequest"
}
func (*oldStartAgentResponseRequest) ProtoMessage() {}

type oldUpdateAgentResponseRequest struct {
	Provider         string `protobuf:"bytes,1,opt,name=provider,proto3"`
	ResponseID       string `protobuf:"bytes,2,opt,name=response_id,json=responseId,proto3"`
	OperationID      string `protobuf:"bytes,3,opt,name=operation_id,json=operationId,proto3"`
	ExpectedRevision uint64 `protobuf:"varint,4,opt,name=expected_revision,json=expectedRevision,proto3"`
	Markdown         string `protobuf:"bytes,10,opt,name=markdown,proto3"`
}

func (m *oldUpdateAgentResponseRequest) Reset() { *m = oldUpdateAgentResponseRequest{} }
func (*oldUpdateAgentResponseRequest) String() string {
	return "oldUpdateAgentResponseRequest"
}
func (*oldUpdateAgentResponseRequest) ProtoMessage() {}

type oldFinishAgentResponseRequest struct {
	Provider         string `protobuf:"bytes,1,opt,name=provider,proto3"`
	ResponseID       string `protobuf:"bytes,2,opt,name=response_id,json=responseId,proto3"`
	OperationID      string `protobuf:"bytes,3,opt,name=operation_id,json=operationId,proto3"`
	ExpectedRevision uint64 `protobuf:"varint,4,opt,name=expected_revision,json=expectedRevision,proto3"`
	Outcome          int32  `protobuf:"varint,5,opt,name=outcome,proto3"`
	Markdown         string `protobuf:"bytes,10,opt,name=markdown,proto3"`
	Summary          string `protobuf:"bytes,11,opt,name=summary,proto3"`
}

func (m *oldFinishAgentResponseRequest) Reset() { *m = oldFinishAgentResponseRequest{} }
func (*oldFinishAgentResponseRequest) String() string {
	return "oldFinishAgentResponseRequest"
}
func (*oldFinishAgentResponseRequest) ProtoMessage() {}

type oldAgentResponseReceipt struct {
	ResponseID string `protobuf:"bytes,1,opt,name=response_id,json=responseId,proto3"`
	Revision   uint64 `protobuf:"varint,2,opt,name=revision,proto3"`
	Phase      int32  `protobuf:"varint,3,opt,name=phase,proto3"`
	Duplicate  bool   `protobuf:"varint,4,opt,name=duplicate,proto3"`
}

func (m *oldAgentResponseReceipt) Reset()       { *m = oldAgentResponseReceipt{} }
func (*oldAgentResponseReceipt) String() string { return "oldAgentResponseReceipt" }
func (*oldAgentResponseReceipt) ProtoMessage()  {}

type oldAgentResponseResponse struct {
	Response *oldAgentResponseReceipt `protobuf:"bytes,1,opt,name=response,proto3"`
}

func (m *oldAgentResponseResponse) Reset()       { *m = oldAgentResponseResponse{} }
func (*oldAgentResponseResponse) String() string { return "oldAgentResponseResponse" }
func (*oldAgentResponseResponse) ProtoMessage()  {}

type oldSendAgentFollowUpRequest struct {
	Provider       string `protobuf:"bytes,1,opt,name=provider,proto3"`
	ConversationID string `protobuf:"bytes,2,opt,name=conversation_id,json=conversationId,proto3"`
	OperationID    string `protobuf:"bytes,3,opt,name=operation_id,json=operationId,proto3"`
	Markdown       string `protobuf:"bytes,10,opt,name=markdown,proto3"`
	Summary        string `protobuf:"bytes,11,opt,name=summary,proto3"`
}

func (m *oldSendAgentFollowUpRequest) Reset() { *m = oldSendAgentFollowUpRequest{} }
func (*oldSendAgentFollowUpRequest) String() string {
	return "oldSendAgentFollowUpRequest"
}
func (*oldSendAgentFollowUpRequest) ProtoMessage() {}

type oldAgentFollowUpReceipt struct {
	FollowUpID string `protobuf:"bytes,1,opt,name=follow_up_id,json=followUpId,proto3"`
	Duplicate  bool   `protobuf:"varint,2,opt,name=duplicate,proto3"`
}

func (m *oldAgentFollowUpReceipt) Reset()       { *m = oldAgentFollowUpReceipt{} }
func (*oldAgentFollowUpReceipt) String() string { return "oldAgentFollowUpReceipt" }
func (*oldAgentFollowUpReceipt) ProtoMessage()  {}

type oldSendAgentFollowUpResponse struct {
	FollowUp *oldAgentFollowUpReceipt `protobuf:"bytes,1,opt,name=follow_up,json=followUp,proto3"`
}

func (m *oldSendAgentFollowUpResponse) Reset() { *m = oldSendAgentFollowUpResponse{} }
func (*oldSendAgentFollowUpResponse) String() string {
	return "oldSendAgentFollowUpResponse"
}
func (*oldSendAgentFollowUpResponse) ProtoMessage() {}

// oldWireCodec encodes the frozen caller types directly from their historical
// field numbers. Besides making the compatibility boundary explicit, this
// avoids the unsafe reflection used by the protobuf MessageV1 adapter, which is
// rejected by Go's race/checkptr instrumentation for hand-written structs.
type oldWireCodec struct{}

func (oldWireCodec) Name() string { return "proto" }

func (oldWireCodec) Marshal(value any) ([]byte, error) {
	switch message := value.(type) {
	case *oldSubscribeAgentEventsRequest:
		var encoded []byte
		encoded = oldAppendString(encoded, 1, message.Provider)
		for _, command := range message.Commands {
			encoded = oldAppendString(encoded, 2, command)
		}
		encoded = oldAppendBool(encoded, 3, message.IncludeUnmatchedMessages)
		return oldAppendBool(encoded, 4, message.IncludeCardActions), nil
	case *oldStartAgentResponseRequest:
		var encoded []byte
		encoded = oldAppendString(encoded, 1, message.Provider)
		encoded = oldAppendString(encoded, 2, message.DeliveryID)
		encoded = oldAppendString(encoded, 3, message.OperationID)
		if message.Content != nil {
			var content []byte
			content = oldAppendString(content, 1, message.Content.Title)
			content = oldAppendString(content, 2, message.Content.Markdown)
			encoded = oldAppendBytes(encoded, 10, content)
		}
		return encoded, nil
	case *oldUpdateAgentResponseRequest:
		var encoded []byte
		encoded = oldAppendString(encoded, 1, message.Provider)
		encoded = oldAppendString(encoded, 2, message.ResponseID)
		encoded = oldAppendString(encoded, 3, message.OperationID)
		encoded = oldAppendUint64(encoded, 4, message.ExpectedRevision)
		return oldAppendString(encoded, 10, message.Markdown), nil
	case *oldFinishAgentResponseRequest:
		var encoded []byte
		encoded = oldAppendString(encoded, 1, message.Provider)
		encoded = oldAppendString(encoded, 2, message.ResponseID)
		encoded = oldAppendString(encoded, 3, message.OperationID)
		encoded = oldAppendUint64(encoded, 4, message.ExpectedRevision)
		encoded = oldAppendUint64(encoded, 5, uint64(message.Outcome))
		encoded = oldAppendString(encoded, 10, message.Markdown)
		return oldAppendString(encoded, 11, message.Summary), nil
	case *oldSendAgentFollowUpRequest:
		var encoded []byte
		encoded = oldAppendString(encoded, 1, message.Provider)
		encoded = oldAppendString(encoded, 2, message.ConversationID)
		encoded = oldAppendString(encoded, 3, message.OperationID)
		encoded = oldAppendString(encoded, 10, message.Markdown)
		return oldAppendString(encoded, 11, message.Summary), nil
	default:
		return nil, fmt.Errorf("old provider codec cannot marshal %T", value)
	}
}

func (oldWireCodec) Unmarshal(encoded []byte, value any) error {
	switch message := value.(type) {
	case *oldSubscribeAgentEventsResponse:
		message.Reset()
		fields, err := oldParseFields(encoded)
		if err != nil {
			return err
		}
		for _, field := range fields {
			if field.number != 1 || field.wireType != protowire.BytesType {
				continue
			}
			message.Event = &oldInboundAgentEvent{}
			if err := oldDecodeInboundAgentEvent(field.bytes, message.Event); err != nil {
				return err
			}
		}
		return nil
	case *oldAgentResponseResponse:
		message.Reset()
		fields, err := oldParseFields(encoded)
		if err != nil {
			return err
		}
		for _, field := range fields {
			if field.number != 1 || field.wireType != protowire.BytesType {
				continue
			}
			message.Response = &oldAgentResponseReceipt{}
			if err := oldDecodeAgentResponseReceipt(field.bytes, message.Response); err != nil {
				return err
			}
		}
		return nil
	case *oldSendAgentFollowUpResponse:
		message.Reset()
		fields, err := oldParseFields(encoded)
		if err != nil {
			return err
		}
		for _, field := range fields {
			if field.number != 1 || field.wireType != protowire.BytesType {
				continue
			}
			message.FollowUp = &oldAgentFollowUpReceipt{}
			if err := oldDecodeAgentFollowUpReceipt(field.bytes, message.FollowUp); err != nil {
				return err
			}
		}
		return nil
	default:
		return fmt.Errorf("old provider codec cannot unmarshal %T", value)
	}
}

func oldAppendString(encoded []byte, number protowire.Number, value string) []byte {
	if value == "" {
		return encoded
	}
	encoded = protowire.AppendTag(encoded, number, protowire.BytesType)
	return protowire.AppendString(encoded, value)
}

func oldAppendBytes(encoded []byte, number protowire.Number, value []byte) []byte {
	encoded = protowire.AppendTag(encoded, number, protowire.BytesType)
	return protowire.AppendBytes(encoded, value)
}

func oldAppendBool(encoded []byte, number protowire.Number, value bool) []byte {
	if !value {
		return encoded
	}
	return oldAppendUint64(encoded, number, 1)
}

func oldAppendUint64(encoded []byte, number protowire.Number, value uint64) []byte {
	if value == 0 {
		return encoded
	}
	encoded = protowire.AppendTag(encoded, number, protowire.VarintType)
	return protowire.AppendVarint(encoded, value)
}

type oldWireField struct {
	number   protowire.Number
	wireType protowire.Type
	bytes    []byte
	varint   uint64
}

func oldParseFields(encoded []byte) ([]oldWireField, error) {
	var fields []oldWireField
	for len(encoded) > 0 {
		number, wireType, tagLength := protowire.ConsumeTag(encoded)
		if tagLength < 0 {
			return nil, protowire.ParseError(tagLength)
		}
		encoded = encoded[tagLength:]
		field := oldWireField{number: number, wireType: wireType}
		switch wireType {
		case protowire.BytesType:
			value, length := protowire.ConsumeBytes(encoded)
			if length < 0 {
				return nil, protowire.ParseError(length)
			}
			field.bytes = value
			encoded = encoded[length:]
		case protowire.VarintType:
			value, length := protowire.ConsumeVarint(encoded)
			if length < 0 {
				return nil, protowire.ParseError(length)
			}
			field.varint = value
			encoded = encoded[length:]
		default:
			length := protowire.ConsumeFieldValue(number, wireType, encoded)
			if length < 0 {
				return nil, protowire.ParseError(length)
			}
			encoded = encoded[length:]
			continue
		}
		fields = append(fields, field)
	}
	return fields, nil
}

func oldDecodeInboundAgentEvent(encoded []byte, event *oldInboundAgentEvent) error {
	fields, err := oldParseFields(encoded)
	if err != nil {
		return err
	}
	for _, field := range fields {
		switch {
		case field.number == 1 && field.wireType == protowire.BytesType:
			event.DeliveryID = string(field.bytes)
		case field.number == 2 && field.wireType == protowire.BytesType:
			event.ConversationID = string(field.bytes)
		case field.number == 3 && field.wireType == protowire.BytesType:
			event.ChatAlias = string(field.bytes)
		case field.number == 4 && field.wireType == protowire.BytesType:
			event.SenderID = string(field.bytes)
		case field.number == 10 && field.wireType == protowire.BytesType:
			event.Message = &oldInboundAgentMessage{}
			if err := oldDecodeInboundAgentMessage(field.bytes, event.Message); err != nil {
				return err
			}
		}
	}
	return nil
}

func oldDecodeInboundAgentMessage(encoded []byte, message *oldInboundAgentMessage) error {
	fields, err := oldParseFields(encoded)
	if err != nil {
		return err
	}
	for _, field := range fields {
		if field.wireType != protowire.BytesType {
			continue
		}
		switch field.number {
		case 1:
			message.Text = string(field.bytes)
		case 2:
			message.Command = string(field.bytes)
		case 3:
			message.CommandText = string(field.bytes)
		}
	}
	return nil
}

func oldDecodeAgentResponseReceipt(encoded []byte, receipt *oldAgentResponseReceipt) error {
	fields, err := oldParseFields(encoded)
	if err != nil {
		return err
	}
	for _, field := range fields {
		switch {
		case field.number == 1 && field.wireType == protowire.BytesType:
			receipt.ResponseID = string(field.bytes)
		case field.number == 2 && field.wireType == protowire.VarintType:
			receipt.Revision = field.varint
		case field.number == 3 && field.wireType == protowire.VarintType:
			receipt.Phase = int32(field.varint)
		case field.number == 4 && field.wireType == protowire.VarintType:
			receipt.Duplicate = field.varint != 0
		}
	}
	return nil
}

func oldDecodeAgentFollowUpReceipt(encoded []byte, receipt *oldAgentFollowUpReceipt) error {
	fields, err := oldParseFields(encoded)
	if err != nil {
		return err
	}
	for _, field := range fields {
		switch {
		case field.number == 1 && field.wireType == protowire.BytesType:
			receipt.FollowUpID = string(field.bytes)
		case field.number == 2 && field.wireType == protowire.VarintType:
			receipt.Duplicate = field.varint != 0
		}
	}
	return nil
}

type recordedSend struct {
	chatID  string
	request notify.Request
}

type recordingBackend struct {
	mu sync.Mutex

	alias           string
	ordinarySends   []recordedSend
	createdCards    []string
	cardSends       []feishu.CardSendRequest
	contentUpdates  []feishu.CardContentUpdate
	settingsUpdates []feishu.CardSettingsUpdate
	batchUpdates    []feishu.CardBatchUpdate
}

func (b *recordingBackend) Ready(context.Context) error { return nil }

func (b *recordingBackend) Send(_ context.Context, chatID string, req notify.Request) (string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.ordinarySends = append(b.ordinarySends, recordedSend{chatID: chatID, request: req})
	return fmt.Sprintf("om_%s_ordinary_%d", b.alias, len(b.ordinarySends)), nil
}

func (b *recordingBackend) CreateCard(_ context.Context, cardJSON string) (string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.createdCards = append(b.createdCards, cardJSON)
	return "card_" + b.alias, nil
}

func (b *recordingBackend) SendCard(_ context.Context, req feishu.CardSendRequest) (string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.cardSends = append(b.cardSends, req)
	return "om_" + b.alias + "_card", nil
}

func (b *recordingBackend) UpdateContent(_ context.Context, req feishu.CardContentUpdate) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.contentUpdates = append(b.contentUpdates, req)
	return nil
}

func (b *recordingBackend) UpdateSettings(_ context.Context, req feishu.CardSettingsUpdate) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.settingsUpdates = append(b.settingsUpdates, req)
	return nil
}

func (b *recordingBackend) BatchUpdate(_ context.Context, req feishu.CardBatchUpdate) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.batchUpdates = append(b.batchUpdates, req)
	return nil
}

type backendSnapshot struct {
	ordinarySends   []recordedSend
	createdCards    []string
	cardSends       []feishu.CardSendRequest
	contentUpdates  []feishu.CardContentUpdate
	settingsUpdates []feishu.CardSettingsUpdate
	batchUpdates    []feishu.CardBatchUpdate
}

func (b *recordingBackend) snapshot() backendSnapshot {
	b.mu.Lock()
	defer b.mu.Unlock()
	return backendSnapshot{
		ordinarySends:   append([]recordedSend(nil), b.ordinarySends...),
		createdCards:    append([]string(nil), b.createdCards...),
		cardSends:       append([]feishu.CardSendRequest(nil), b.cardSends...),
		contentUpdates:  append([]feishu.CardContentUpdate(nil), b.contentUpdates...),
		settingsUpdates: append([]feishu.CardSettingsUpdate(nil), b.settingsUpdates...),
		batchUpdates:    append([]feishu.CardBatchUpdate(nil), b.batchUpdates...),
	}
}

type bearerCredentials struct{ token string }

func (c bearerCredentials) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	return map[string]string{"authorization": "Bearer " + c.token}, nil
}

func (bearerCredentials) RequireTransportSecurity() bool { return false }

type daemonFixture struct {
	svc        *service.Service
	grpcConn   *grpc.ClientConn
	httpClient *http.Client
	alpha      *recordingBackend
	beta       *recordingBackend
}

func newDaemonFixture(t *testing.T) *daemonFixture {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	alpha := &recordingBackend{alias: "alpha"}
	beta := &recordingBackend{alias: "beta"}
	cfg := config.Config{
		AuthToken: oldGeneralToken,
		Apps: map[string]config.AppConfig{
			"alpha": {
				AppID: "cli_alpha", AppSecret: "secret-alpha",
				Commands: config.CommandConfig{Enabled: true},
				Channels: map[string]string{"alpha-alerts": "oc_alpha"},
			},
			"beta": {
				AppID: "cli_beta", AppSecret: "secret-beta",
				Commands: config.CommandConfig{Enabled: true},
				Channels: map[string]string{"beta-support": "oc_beta"},
			},
		},
		Channels: map[string]string{
			"alpha-alerts": "oc_alpha",
			"beta-support": "oc_beta",
		},
		ChannelRoutes: map[string]config.ChannelRoute{
			"alpha-alerts": {AppAlias: "alpha", ChatID: "oc_alpha"},
			"beta-support": {AppAlias: "beta", ChatID: "oc_beta"},
		},
		AgentProviders: map[string]config.AgentProviderConfig{
			"fixture-agent": {
				AuthToken:              oldProviderToken,
				AllowUnmatchedMessages: true,
				AllowFollowUpMessages:  true,
			},
		},
		DedupeTTL:   2 * time.Hour,
		SendTimeout: time.Second,
	}
	store := dedupe.NewMemoryStore(cfg.DedupeTTL)
	svc := service.NewMultiAppService(cfg, map[string]feishu.Sender{
		"alpha": alpha,
		"beta":  beta,
	}, store, logger)
	grpcServer := grpcapi.NewServer(cfg, svc, logger)
	httpServer := httpapi.NewServer(cfg, svc, logger)

	grpcSocket := shortSocketPath("grpc")
	httpSocket := shortSocketPath("http")
	grpcCtx, cancelGRPC := context.WithCancel(context.Background())
	httpCtx, cancelHTTP := context.WithCancel(context.Background())
	grpcErr := make(chan error, 1)
	httpErr := make(chan error, 1)
	go func() { grpcErr <- grpcServer.ListenAndServeUnix(grpcCtx, grpcSocket) }()
	go func() { httpErr <- httpServer.ListenAndServeUnix(httpCtx, httpSocket) }()

	waitForUnixSocket(t, grpcSocket, grpcErr)
	waitForUnixSocket(t, httpSocket, httpErr)

	conn, err := grpc.NewClient(
		"passthrough:///feishu-botd",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", grpcSocket)
		}),
		grpc.WithPerRPCCredentials(bearerCredentials{token: oldProviderToken}),
		grpc.WithDefaultCallOptions(grpc.ForceCodec(oldWireCodec{})),
	)
	if err != nil {
		t.Fatalf("dial gRPC Unix socket: %v", err)
	}
	transport := &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", httpSocket)
		},
	}

	t.Cleanup(func() {
		_ = conn.Close()
		transport.CloseIdleConnections()
		cancelGRPC()
		cancelHTTP()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = grpcServer.Shutdown(shutdownCtx)
		_ = httpServer.Shutdown(shutdownCtx)
		_ = os.Remove(grpcSocket)
		_ = os.Remove(httpSocket)
	})

	return &daemonFixture{
		svc:        svc,
		grpcConn:   conn,
		httpClient: &http.Client{Transport: transport, Timeout: 3 * time.Second},
		alpha:      alpha,
		beta:       beta,
	}
}

func shortSocketPath(kind string) string {
	return filepath.Join("/tmp", fmt.Sprintf("fbd-compat-%d-%d-%s.sock", os.Getpid(), time.Now().UnixNano(), kind))
}

func waitForUnixSocket(t *testing.T, path string, errCh <-chan error) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("unix", path, 20*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return
		}
		select {
		case serveErr := <-errCh:
			t.Fatalf("server for %s exited before accepting connections: %v", path, serveErr)
		default:
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("Unix socket %s did not become ready", path)
}

func TestOldCallerShapesAgainstTwoAppDaemonEndToEnd(t *testing.T) {
	fixture := newDaemonFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	stream, err := fixture.grpcConn.NewStream(ctx, &oldSubscribeDesc, oldSubscribeMethod, grpc.StaticMethod())
	if err != nil {
		t.Fatalf("open old provider subscription: %v", err)
	}
	if err := stream.SendMsg(&oldSubscribeAgentEventsRequest{
		Provider:                 "fixture-agent",
		Commands:                 nil,
		IncludeUnmatchedMessages: true,
		IncludeCardActions:       false,
	}); err != nil {
		t.Fatalf("send old provider subscription: %v", err)
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatalf("close old provider subscription request: %v", err)
	}

	event := dispatchAndReceiveOldEvent(t, fixture.svc, stream, service.CommandInput{
		AppAlias:       "beta",
		DeliveryID:     "delivery_old_provider",
		ConversationID: "conversation_old_provider",
		Command:        "hello",
		Text:           "from beta",
		Prompt:         "hello from beta",
		ChatAlias:      "beta-support",
		ChatID:         "oc_beta",
		SenderID:       "ou_beta",
		Metadata: map[string]string{
			"chat_type":    "group",
			"message_type": "text",
			"message_id":   "om_inbound_beta",
			"root_id":      "om_root_beta",
			"app_alias":    "spoofed-value-must-not-win",
		},
	})
	if event.Event == nil || event.Event.Message == nil {
		t.Fatalf("old provider event = %#v, want message payload", event.Event)
	}
	if event.Event.DeliveryID != "delivery_old_provider" ||
		event.Event.ConversationID != "conversation_old_provider" ||
		event.Event.ChatAlias != "beta-support" ||
		event.Event.SenderID != "ou_beta" {
		t.Fatalf("old provider event envelope = %#v", event.Event)
	}
	if event.Event.Message.Text != "hello from beta" ||
		event.Event.Message.Command != "hello" ||
		event.Event.Message.CommandText != "from beta" {
		t.Fatalf("old provider message = %#v", event.Event.Message)
	}
	// oldInboundAgentEvent intentionally has no metadata field. Successful
	// decoding proves an old client safely ignores the additive app_alias map.

	startRequest := &oldStartAgentResponseRequest{
		Provider:    "fixture-agent",
		DeliveryID:  event.Event.GetDeliveryID(),
		OperationID: "old-start-1",
		Content: &oldAgentResponseContent{
			Title:    "Legacy agent",
			Markdown: "Working",
		},
	}
	var started oldAgentResponseResponse
	if err := fixture.grpcConn.Invoke(ctx, oldStartMethod, startRequest, &started, grpc.StaticMethod()); err != nil {
		t.Fatalf("old StartAgentResponse: %v", err)
	}
	assertOldAgentReceipt(t, started.Response, 1, oldPhaseStreaming, false)
	if started.Response.ResponseID == "" {
		t.Fatal("old StartAgentResponse returned an empty response handle")
	}
	var startReplay oldAgentResponseResponse
	if err := fixture.grpcConn.Invoke(ctx, oldStartMethod, startRequest, &startReplay, grpc.StaticMethod()); err != nil {
		t.Fatalf("replay old StartAgentResponse: %v", err)
	}
	assertOldAgentReceipt(t, startReplay.Response, 1, oldPhaseStreaming, true)
	if startReplay.Response.ResponseID != started.Response.ResponseID {
		t.Fatalf("start replay response_id = %q, want %q", startReplay.Response.ResponseID, started.Response.ResponseID)
	}
	assertCardEgress(t, fixture.alpha.snapshot(), fixture.beta.snapshot())

	updateRequest := &oldUpdateAgentResponseRequest{
		Provider:         "fixture-agent",
		ResponseID:       started.Response.ResponseID,
		OperationID:      "old-update-1",
		ExpectedRevision: 1,
		Markdown:         "Working\n\nDone.",
	}
	var updated oldAgentResponseResponse
	if err := fixture.grpcConn.Invoke(ctx, oldUpdateMethod, updateRequest, &updated, grpc.StaticMethod()); err != nil {
		t.Fatalf("old UpdateAgentResponse: %v", err)
	}
	assertOldAgentReceipt(t, updated.Response, 2, oldPhaseStreaming, false)
	var updateReplay oldAgentResponseResponse
	if err := fixture.grpcConn.Invoke(ctx, oldUpdateMethod, updateRequest, &updateReplay, grpc.StaticMethod()); err != nil {
		t.Fatalf("replay old UpdateAgentResponse: %v", err)
	}
	assertOldAgentReceipt(t, updateReplay.Response, 2, oldPhaseStreaming, true)

	finishRequest := &oldFinishAgentResponseRequest{
		Provider:         "fixture-agent",
		ResponseID:       started.Response.ResponseID,
		OperationID:      "old-finish-1",
		ExpectedRevision: 2,
		Outcome:          oldOutcomeCompleted,
		Markdown:         "Working\n\nDone.",
		Summary:          "Completed",
	}
	var finished oldAgentResponseResponse
	if err := fixture.grpcConn.Invoke(ctx, oldFinishMethod, finishRequest, &finished, grpc.StaticMethod()); err != nil {
		t.Fatalf("old FinishAgentResponse: %v", err)
	}
	assertOldAgentReceipt(t, finished.Response, 3, oldPhaseCompleted, false)
	var finishReplay oldAgentResponseResponse
	if err := fixture.grpcConn.Invoke(ctx, oldFinishMethod, finishRequest, &finishReplay, grpc.StaticMethod()); err != nil {
		t.Fatalf("replay old FinishAgentResponse: %v", err)
	}
	assertOldAgentReceipt(t, finishReplay.Response, 3, oldPhaseCompleted, true)
	alphaAfterLifecycle := fixture.alpha.snapshot()
	betaAfterLifecycle := fixture.beta.snapshot()
	if len(alphaAfterLifecycle.createdCards) != 0 ||
		len(alphaAfterLifecycle.cardSends) != 0 ||
		len(alphaAfterLifecycle.contentUpdates) != 0 ||
		len(alphaAfterLifecycle.settingsUpdates) != 0 {
		t.Fatalf("beta lifecycle mutated alpha backend: %#v", alphaAfterLifecycle)
	}
	if len(betaAfterLifecycle.createdCards) != 1 ||
		len(betaAfterLifecycle.cardSends) != 1 ||
		len(betaAfterLifecycle.contentUpdates) != 1 ||
		len(betaAfterLifecycle.settingsUpdates) != 1 ||
		len(betaAfterLifecycle.batchUpdates) != 0 {
		t.Fatalf("beta lifecycle backend calls = %#v", betaAfterLifecycle)
	}

	followUpRequest := &oldSendAgentFollowUpRequest{
		Provider:       "fixture-agent",
		ConversationID: event.Event.ConversationID,
		OperationID:    "old-follow-up-1",
		Markdown:       "A later result.",
		Summary:        "Later result",
	}
	var followedUp oldSendAgentFollowUpResponse
	if err := fixture.grpcConn.Invoke(ctx, oldFollowUpMethod, followUpRequest, &followedUp, grpc.StaticMethod()); err != nil {
		t.Fatalf("old SendAgentFollowUp: %v", err)
	}
	if followedUp.FollowUp == nil || followedUp.FollowUp.FollowUpID == "" || followedUp.FollowUp.Duplicate {
		t.Fatalf("old follow-up receipt = %#v", followedUp.FollowUp)
	}
	var followUpReplay oldSendAgentFollowUpResponse
	if err := fixture.grpcConn.Invoke(ctx, oldFollowUpMethod, followUpRequest, &followUpReplay, grpc.StaticMethod()); err != nil {
		t.Fatalf("replay old SendAgentFollowUp: %v", err)
	}
	if followUpReplay.FollowUp == nil ||
		followUpReplay.FollowUp.FollowUpID != followedUp.FollowUp.FollowUpID ||
		!followUpReplay.FollowUp.Duplicate {
		t.Fatalf("old follow-up replay = %#v, first = %#v", followUpReplay.FollowUp, followedUp.FollowUp)
	}

	betaAfterAgent := fixture.beta.snapshot()
	if len(betaAfterAgent.ordinarySends) != 1 {
		t.Fatalf("beta ordinary send count = %d, want one follow-up", len(betaAfterAgent.ordinarySends))
	}
	followUpSend := betaAfterAgent.ordinarySends[0]
	if followUpSend.chatID != "oc_beta" ||
		followUpSend.request.Markdown != "A later result." ||
		followUpSend.request.ReplyToMessageID != "om_root_beta" {
		t.Fatalf("beta follow-up egress = %#v", followUpSend)
	}
	if alpha := fixture.alpha.snapshot(); len(alpha.ordinarySends) != 0 {
		t.Fatalf("agent lifecycle escaped through alpha: %#v", alpha.ordinarySends)
	}

	const oldNotifyJSON = `{"source":"old-caller","source_event_id":"notify-1","dedupe_key":"old-caller:notify-1","severity":"info","title":"Legacy notification","markdown":"**still works**","target":{"channel":"alpha-alerts"},"links":[],"metadata":{}}`
	firstNotify := sendOldHTTPNotify(t, fixture.httpClient, oldNotifyJSON)
	if firstNotify.Status != "sent" ||
		firstNotify.Provider != "feishu" ||
		firstNotify.DedupeKey != "old-caller:notify-1" ||
		firstNotify.MessageID == "" ||
		firstNotify.Duplicate {
		t.Fatalf("old HTTP notify response = %#v", firstNotify)
	}
	replayedNotify := sendOldHTTPNotify(t, fixture.httpClient, oldNotifyJSON)
	if !replayedNotify.Duplicate || replayedNotify.MessageID != firstNotify.MessageID {
		t.Fatalf("old HTTP notify replay = %#v, first = %#v", replayedNotify, firstNotify)
	}

	alphaAfterNotify := fixture.alpha.snapshot()
	if len(alphaAfterNotify.ordinarySends) != 1 ||
		alphaAfterNotify.ordinarySends[0].chatID != "oc_alpha" ||
		alphaAfterNotify.ordinarySends[0].request.Target.Channel != "alpha-alerts" {
		t.Fatalf("alpha legacy HTTP egress = %#v", alphaAfterNotify.ordinarySends)
	}
	if got := len(fixture.beta.snapshot().ordinarySends); got != 1 {
		t.Fatalf("HTTP notification escaped through beta: beta ordinary sends = %d", got)
	}
}

func dispatchAndReceiveOldEvent(
	t *testing.T,
	svc *service.Service,
	stream grpc.ClientStream,
	input service.CommandInput,
) oldSubscribeAgentEventsResponse {
	t.Helper()
	type receiveResult struct {
		response oldSubscribeAgentEventsResponse
		err      error
	}
	received := make(chan receiveResult, 1)
	go func() {
		var response oldSubscribeAgentEventsResponse
		received <- receiveResult{response: response, err: stream.RecvMsg(&response)}
	}()

	timer := time.NewTimer(3 * time.Second)
	defer timer.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if _, apiErr := svc.DispatchCommand(context.Background(), input); apiErr != nil {
			t.Fatalf("dispatch app-tagged event: %v", apiErr)
		}
		select {
		case result := <-received:
			if result.err != nil {
				t.Fatalf("receive old provider event: %v", result.err)
			}
			return result.response
		case <-ticker.C:
		case <-timer.C:
			t.Fatal("timed out waiting for old provider event")
			return oldSubscribeAgentEventsResponse{}
		}
	}
}

func assertOldAgentReceipt(t *testing.T, receipt *oldAgentResponseReceipt, revision uint64, phase int32, duplicate bool) {
	t.Helper()
	if receipt == nil ||
		receipt.Revision != revision ||
		receipt.Phase != phase ||
		receipt.Duplicate != duplicate {
		t.Fatalf(
			"old agent receipt = %#v, want revision=%d phase=%d duplicate=%t",
			receipt, revision, phase, duplicate,
		)
	}
}

func assertCardEgress(t *testing.T, alpha, beta backendSnapshot) {
	t.Helper()
	if len(alpha.createdCards) != 0 || len(alpha.cardSends) != 0 {
		t.Fatalf("beta response escaped through alpha: creates=%d sends=%d", len(alpha.createdCards), len(alpha.cardSends))
	}
	if len(beta.createdCards) != 1 || len(beta.cardSends) != 1 {
		t.Fatalf("beta card create/send counts = %d/%d, want 1/1", len(beta.createdCards), len(beta.cardSends))
	}
	if beta.cardSends[0].ReplyToMessageID != "om_inbound_beta" ||
		beta.cardSends[0].ChatID != "" ||
		beta.cardSends[0].CardID != "card_beta" {
		t.Fatalf("beta response egress = %#v", beta.cardSends[0])
	}
}

type oldHTTPNotifyResponse struct {
	Status    string `json:"status"`
	Provider  string `json:"provider"`
	DedupeKey string `json:"dedupe_key"`
	MessageID string `json:"message_id"`
	Duplicate bool   `json:"duplicate"`
}

func sendOldHTTPNotify(t *testing.T, client *http.Client, body string) oldHTTPNotifyResponse {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, "http://feishu-botd/v1/notify", bytes.NewBufferString(body))
	if err != nil {
		t.Fatalf("build old HTTP notify request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+oldGeneralToken)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("send old HTTP notify request: %v", err)
	}
	defer resp.Body.Close()
	responseBody, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read old HTTP notify response: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("old HTTP notify status = %d, body = %s", resp.StatusCode, responseBody)
	}
	var decoded oldHTTPNotifyResponse
	if err := json.Unmarshal(responseBody, &decoded); err != nil {
		t.Fatalf("decode old HTTP notify response: %v", err)
	}
	return decoded
}

var _ feishu.Sender = (*recordingBackend)(nil)
var _ feishu.DynamicCards = (*recordingBackend)(nil)
