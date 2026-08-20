package grpcapi

import (
	"bytes"
	"context"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "feishu-botd/gen/feishubotd/v1"
	"feishu-botd/internal/config"
	"feishu-botd/internal/dedupe"
	"feishu-botd/internal/feishu"
	"feishu-botd/internal/service"
)

// uploadingAgentSender adds the outbound image capability to the shared agent
// fake by embedding it, so the capability is discovered by exactly the type
// assertion the daemon performs on a real sender.
type uploadingAgentSender struct {
	*fakeAgentSender

	mu       sync.Mutex
	uploads  [][]byte
	imageKey string
	err      error
}

func (u *uploadingAgentSender) UploadImage(_ context.Context, data []byte) (string, string, error) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.uploads = append(u.uploads, append([]byte(nil), data...))
	if u.err != nil {
		return "", "", u.err
	}
	return u.imageKey, "image/png", nil
}

func (u *uploadingAgentSender) uploadSnapshot() [][]byte {
	u.mu.Lock()
	defer u.mu.Unlock()
	return append([][]byte(nil), u.uploads...)
}

func startImageUploadServer(
	t *testing.T, sender feishu.Sender, allowImageUpload bool,
) (*grpc.ClientConn, *service.Service) {
	t.Helper()
	cfg := testConfig()
	cfg.AgentProviders = map[string]config.AgentProviderConfig{
		"fixture-agent": {
			AuthToken: fixtureAgentToken, AllowedCommands: []string{"ask"},
			AllowUnmatchedMessages: true, AllowImageUpload: allowImageUpload,
		},
	}
	svc := service.NewService(cfg, sender, dedupe.NewMemoryStore(time.Hour), slog.Default())
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

	conn := dial(t, func(dialCtx context.Context) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(dialCtx, "unix", socketPath)
	}, grpc.WithPerRPCCredentials(testBearerCredentials{token: fixtureAgentToken}))
	waitHealthy(t, conn, errCh)
	return conn, svc
}

// seedImageUploadDelivery drives one inbound event so the provider holds a live
// delivery, which is what an upload has to be attached to.
func seedImageUploadDelivery(t *testing.T, svc *service.Service, client pb.CommandServiceClient) {
	t.Helper()
	subCtx, cancelSub := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancelSub)
	sub, err := client.SubscribeAgentEvents(subCtx, &pb.SubscribeAgentEventsRequest{
		Provider: "fixture-agent", IncludeUnmatchedMessages: true,
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	_ = dispatchAndReceiveAgentEvent(t, svc, sub, service.CommandInput{
		DeliveryID: "delivery_image_fixture", Command: "二维码", Prompt: "给我二维码",
		ConversationID: "conversation_fixture", ChatAlias: "ops", SenderID: "sender_fixture",
		Metadata: map[string]string{"chat_type": "group", "message_id": "om_guide"},
	})
}

func sendImageUpload(
	t *testing.T, client pb.CommandServiceClient, operationID string, chunks [][]byte,
) (*pb.UploadAgentImageResponse, error) {
	t.Helper()
	stream, err := client.UploadAgentImage(context.Background())
	if err != nil {
		t.Fatalf("open upload stream: %v", err)
	}
	header := &pb.UploadAgentImageRequest{
		Frame: &pb.UploadAgentImageRequest_Header{Header: &pb.UploadAgentImageHeader{
			Provider: "fixture-agent", DeliveryId: "delivery_image_fixture", OperationId: operationID,
		}},
	}
	if sendErr := stream.Send(header); sendErr != nil {
		// A server that refuses mid-stream closes it, so a failed Send is the
		// refusal arriving early rather than a test failure.
		return stream.CloseAndRecv()
	}
	for _, chunk := range chunks {
		frame := &pb.UploadAgentImageRequest{
			Frame: &pb.UploadAgentImageRequest_Chunk{Chunk: &pb.UploadAgentImageChunk{Data: chunk}},
		}
		if sendErr := stream.Send(frame); sendErr != nil {
			return stream.CloseAndRecv()
		}
	}
	return stream.CloseAndRecv()
}

func newUploadingSender(imageKey string) *uploadingAgentSender {
	return &uploadingAgentSender{fakeAgentSender: &fakeAgentSender{}, imageKey: imageKey}
}

// pngFixture is a PNG signature plus filler, long enough to need two frames.
func pngFixture(size int) []byte {
	data := append([]byte(nil), "\x89PNG\r\n\x1a\n"...)
	return append(data, bytes.Repeat([]byte{0x42}, size)...)
}

func TestUploadAgentImageReassemblesFramesAndReturnsTheKey(t *testing.T) {
	sender := newUploadingSender("img_v3_qr")
	conn, svc := startImageUploadServer(t, sender, true)
	client := pb.NewCommandServiceClient(conn)
	seedImageUploadDelivery(t, svc, client)

	image := pngFixture(uploadImageMaxChunkBytes + 1024)
	chunks := [][]byte{image[:uploadImageMaxChunkBytes], image[uploadImageMaxChunkBytes:]}
	resp, err := sendImageUpload(t, client, "op_1", chunks)
	if err != nil {
		t.Fatalf("upload: %v", err)
	}
	if resp.GetImageKey() != "img_v3_qr" || resp.GetMediaType() != "image/png" || resp.GetDuplicate() {
		t.Fatalf("response = %#v", resp)
	}
	uploads := sender.uploadSnapshot()
	if len(uploads) != 1 || !bytes.Equal(uploads[0], image) {
		t.Fatalf("uploads=%d reassembled=%d want=%d", len(uploads), len(uploads[0]), len(image))
	}
}

func TestUploadAgentImageRequiresTheProviderGrant(t *testing.T) {
	sender := newUploadingSender("img_v3_qr")
	conn, svc := startImageUploadServer(t, sender, false)
	client := pb.NewCommandServiceClient(conn)
	seedImageUploadDelivery(t, svc, client)

	_, err := sendImageUpload(t, client, "op_1", [][]byte{pngFixture(16)})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("error = %v, want PermissionDenied", err)
	}
	if uploads := sender.uploadSnapshot(); len(uploads) != 0 {
		t.Fatalf("denied upload reached the sender: %d", len(uploads))
	}
}

// The bearer identifies the provider; a header naming a different one must not
// be able to borrow that credential.
func TestUploadAgentImageRejectsAHeaderNamingAnotherProvider(t *testing.T) {
	sender := newUploadingSender("img_v3_qr")
	conn, svc := startImageUploadServer(t, sender, true)
	client := pb.NewCommandServiceClient(conn)
	seedImageUploadDelivery(t, svc, client)

	stream, err := client.UploadAgentImage(context.Background())
	if err != nil {
		t.Fatalf("open upload stream: %v", err)
	}
	_ = stream.Send(&pb.UploadAgentImageRequest{
		Frame: &pb.UploadAgentImageRequest_Header{Header: &pb.UploadAgentImageHeader{
			Provider: "someone-else", DeliveryId: "delivery_image_fixture", OperationId: "op_1",
		}},
	})
	if _, err = stream.CloseAndRecv(); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("error = %v, want PermissionDenied", err)
	}
	if uploads := sender.uploadSnapshot(); len(uploads) != 0 {
		t.Fatalf("impersonated upload reached the sender: %d", len(uploads))
	}
}

func TestUploadAgentImageRejectsMalformedFraming(t *testing.T) {
	sender := newUploadingSender("img_v3_qr")
	conn, svc := startImageUploadServer(t, sender, true)
	client := pb.NewCommandServiceClient(conn)
	seedImageUploadDelivery(t, svc, client)

	chunkFrame := func(size int) *pb.UploadAgentImageRequest {
		return &pb.UploadAgentImageRequest{
			Frame: &pb.UploadAgentImageRequest_Chunk{
				Chunk: &pb.UploadAgentImageChunk{Data: bytes.Repeat([]byte{0x42}, size)},
			},
		}
	}
	headerFrame := &pb.UploadAgentImageRequest{
		Frame: &pb.UploadAgentImageRequest_Header{Header: &pb.UploadAgentImageHeader{
			Provider: "fixture-agent", DeliveryId: "delivery_image_fixture", OperationId: "op_1",
		}},
	}

	for _, testCase := range []struct {
		name   string
		frames []*pb.UploadAgentImageRequest
	}{
		{"no frames at all", nil},
		{"chunk before header", []*pb.UploadAgentImageRequest{chunkFrame(16)}},
		{"header sent twice", []*pb.UploadAgentImageRequest{headerFrame, headerFrame}},
		{"chunk over the frame ceiling", []*pb.UploadAgentImageRequest{
			headerFrame, chunkFrame(uploadImageMaxChunkBytes + 1),
		}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			stream, err := client.UploadAgentImage(context.Background())
			if err != nil {
				t.Fatalf("open upload stream: %v", err)
			}
			for _, frame := range testCase.frames {
				if sendErr := stream.Send(frame); sendErr != nil {
					break
				}
			}
			if _, err = stream.CloseAndRecv(); status.Code(err) != codes.InvalidArgument {
				t.Fatalf("error = %v, want InvalidArgument", err)
			}
		})
	}
	if uploads := sender.uploadSnapshot(); len(uploads) != 0 {
		t.Fatalf("malformed upload reached the sender: %d", len(uploads))
	}
}

func TestUploadAgentImageRefusesAnImageOverTheCeilingWithoutBufferingItAll(t *testing.T) {
	sender := newUploadingSender("img_v3_qr")
	conn, svc := startImageUploadServer(t, sender, true)
	client := pb.NewCommandServiceClient(conn)
	seedImageUploadDelivery(t, svc, client)

	chunk := bytes.Repeat([]byte{0x42}, uploadImageMaxChunkBytes)
	chunks := [][]byte{pngFixture(0)}
	for len(chunks)*uploadImageMaxChunkBytes <= uploadImageMaxBytes {
		chunks = append(chunks, chunk)
	}
	if _, err := sendImageUpload(t, client, "op_1", chunks); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("error = %v, want InvalidArgument", err)
	}
	if uploads := sender.uploadSnapshot(); len(uploads) != 0 {
		t.Fatalf("oversized upload reached the sender: %d", len(uploads))
	}
}

func TestUploadAgentImageReplayReturnsTheSameKey(t *testing.T) {
	sender := newUploadingSender("img_v3_qr")
	conn, svc := startImageUploadServer(t, sender, true)
	client := pb.NewCommandServiceClient(conn)
	seedImageUploadDelivery(t, svc, client)

	image := pngFixture(64)
	first, err := sendImageUpload(t, client, "op_1", [][]byte{image})
	if err != nil {
		t.Fatalf("first upload: %v", err)
	}
	second, err := sendImageUpload(t, client, "op_1", [][]byte{image})
	if err != nil {
		t.Fatalf("replayed upload: %v", err)
	}
	if second.GetImageKey() != first.GetImageKey() || !second.GetDuplicate() {
		t.Fatalf("replay = %#v, first = %#v", second, first)
	}
	if uploads := sender.uploadSnapshot(); len(uploads) != 1 {
		t.Fatalf("replay minted a second key: uploads=%d", len(uploads))
	}
}
