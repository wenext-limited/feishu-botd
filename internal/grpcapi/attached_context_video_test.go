package grpcapi

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	pb "feishu-botd/gen/feishubotd/v1"
	"feishu-botd/internal/feishu"
	"feishu-botd/internal/service"
)

// Video chunks (ADR-0126) follow every image chunk in the stream, in
// video_index order, with the same offset/final chunk-framing contract.
func TestGRPCAgentAttachedContextStreamsVideoChunksAfterAllImageChunks(t *testing.T) {
	imageData := bytes.Repeat([]byte("image-fixture"), 1000)
	video0Data := bytes.Repeat([]byte("video-fixture-zero"), 6000)
	video1Data := bytes.Repeat([]byte("video-fixture-one"), 2000)
	sender := &fakeAgentSender{
		fakeSender: fakeSender{messageID: "unused_fixture"},
		attachedContext: feishu.AttachedContext{
			Status: feishu.AttachedContextFound,
			Messages: []feishu.AttachedContextMessage{{
				AuthorLabel: "participant-1", AuthorType: "user", Text: "crashes on launch",
				Images: []feishu.AttachedContextImage{{MediaType: "image/png", Data: imageData}},
				Videos: []feishu.AttachedContextVideo{
					{MediaType: "video/mp4", Data: video0Data, DurationMs: 9000, FileName: "crash.mp4"},
					{MediaType: "video/webm", Data: video1Data, DurationMs: 4200, FileName: "crash2.webm"},
				},
			}},
		},
	}
	conn, svc := startAgentUnixServer(t, sender)
	client := pb.NewCommandServiceClient(conn)

	subCtx, cancelSub := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelSub()
	sub, err := client.SubscribeAgentEvents(subCtx, &pb.SubscribeAgentEventsRequest{
		Provider: "fixture-agent", IncludeUnmatchedMessages: true,
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	_ = dispatchAndReceiveAgentEvent(t, svc, sub, service.CommandInput{
		DeliveryID: "delivery_video_fixture", Command: "看看", Prompt: "看看这个问题",
		ConversationID: "conversation_fixture", ChatAlias: "ops", SenderID: "sender_fixture",
		Metadata: map[string]string{
			"chat_type": "topic_group", "message_type": "text", "message_id": "om_guide",
			"thread_id": "omt_private", "create_time": "1754380800123",
		},
	})

	stream, err := client.GetAgentAttachedContext(context.Background(), &pb.GetAgentAttachedContextRequest{
		Provider: "fixture-agent", DeliveryId: "delivery_video_fixture",
	})
	if err != nil {
		t.Fatalf("get attached context: %v", err)
	}
	first, err := stream.Recv()
	if err != nil {
		t.Fatalf("receive header: %v", err)
	}
	header := first.GetHeader()
	if header == nil || len(header.GetMessages()) != 1 {
		t.Fatalf("header = %#v", header)
	}
	message := header.GetMessages()[0]
	if len(message.GetImages()) != 1 {
		t.Fatalf("image descriptors = %#v", message.GetImages())
	}
	videoDescriptors := message.GetVideos()
	if len(videoDescriptors) != 2 {
		t.Fatalf("video descriptors = %#v", videoDescriptors)
	}
	wantVideos := []struct {
		mediaType string
		size      uint64
		duration  uint64
		fileName  string
	}{
		{"video/mp4", uint64(len(video0Data)), 9000, "crash.mp4"},
		{"video/webm", uint64(len(video1Data)), 4200, "crash2.webm"},
	}
	for index, want := range wantVideos {
		descriptor := videoDescriptors[index]
		if descriptor.GetVideoIndex() != uint32(index) || descriptor.GetMediaType() != want.mediaType ||
			descriptor.GetByteSize() != want.size || descriptor.GetDurationMs() != want.duration ||
			descriptor.GetFileName() != want.fileName {
			t.Fatalf("video descriptor[%d] = %#v, want %#v", index, descriptor, want)
		}
	}

	var (
		reconstructedImage           []byte
		reconstructedVideos          = map[uint32][]byte{}
		sawImageChunk, sawVideoChunk bool
	)
	for {
		frame, recvErr := stream.Recv()
		if recvErr == io.EOF {
			break
		}
		if recvErr != nil {
			t.Fatalf("receive frame: %v", recvErr)
		}
		if chunk := frame.GetImageChunk(); chunk != nil {
			if sawVideoChunk {
				t.Fatalf("an image chunk arrived after a video chunk started: %#v", chunk)
			}
			sawImageChunk = true
			if chunk.GetOffset() != uint64(len(reconstructedImage)) {
				t.Fatalf("image chunk offset = %d, reconstructed = %d", chunk.GetOffset(), len(reconstructedImage))
			}
			reconstructedImage = append(reconstructedImage, chunk.GetData()...)
			continue
		}
		chunk := frame.GetVideoChunk()
		if chunk == nil {
			t.Fatalf("frame is neither an image nor a video chunk: %#v", frame)
		}
		sawVideoChunk = true
		if !sawImageChunk {
			t.Fatalf("a video chunk arrived before any image chunk: %#v", chunk)
		}
		existing := reconstructedVideos[chunk.GetVideoIndex()]
		if chunk.GetOffset() != uint64(len(existing)) {
			t.Fatalf("video[%d] chunk offset = %d, reconstructed = %d", chunk.GetVideoIndex(), chunk.GetOffset(), len(existing))
		}
		if len(chunk.GetData()) == 0 || len(chunk.GetData()) > attachedContextImageChunkBytes {
			t.Fatalf("video[%d] chunk size = %d", chunk.GetVideoIndex(), len(chunk.GetData()))
		}
		reconstructedVideos[chunk.GetVideoIndex()] = append(existing, chunk.GetData()...)
	}
	if !bytes.Equal(reconstructedImage, imageData) {
		t.Fatalf("reconstructed image mismatch: got %d bytes, want %d", len(reconstructedImage), len(imageData))
	}
	if !bytes.Equal(reconstructedVideos[0], video0Data) {
		t.Fatalf("reconstructed video[0] mismatch: got %d bytes, want %d", len(reconstructedVideos[0]), len(video0Data))
	}
	if !bytes.Equal(reconstructedVideos[1], video1Data) {
		t.Fatalf("reconstructed video[1] mismatch: got %d bytes, want %d", len(reconstructedVideos[1]), len(video1Data))
	}
}

func TestAgentAttachedContextIssueToProtoMapsVideoCodes(t *testing.T) {
	tests := []struct {
		in   feishu.AttachedContextIssueCode
		want pb.AgentAttachedContextIssueCode
	}{
		{feishu.AttachedContextIssueVideoLimit, pb.AgentAttachedContextIssueCode_AGENT_ATTACHED_CONTEXT_ISSUE_CODE_VIDEO_LIMIT},
		{feishu.AttachedContextIssueVideoTooLarge, pb.AgentAttachedContextIssueCode_AGENT_ATTACHED_CONTEXT_ISSUE_CODE_VIDEO_TOO_LARGE},
		{feishu.AttachedContextIssueVideoUnreadable, pb.AgentAttachedContextIssueCode_AGENT_ATTACHED_CONTEXT_ISSUE_CODE_VIDEO_UNREADABLE},
		{feishu.AttachedContextIssueVideoType, pb.AgentAttachedContextIssueCode_AGENT_ATTACHED_CONTEXT_ISSUE_CODE_VIDEO_TYPE_UNSUPPORTED},
		{feishu.AttachedContextIssueVideoTooLong, pb.AgentAttachedContextIssueCode_AGENT_ATTACHED_CONTEXT_ISSUE_CODE_VIDEO_TOO_LONG},
		// VIDEO_OMITTED (12) is pre-existing and now means "no grant"; its
		// mapping must not have changed.
		{feishu.AttachedContextIssueVideoOmitted, pb.AgentAttachedContextIssueCode_AGENT_ATTACHED_CONTEXT_ISSUE_CODE_VIDEO_OMITTED},
	}
	for _, test := range tests {
		if got := agentAttachedContextIssueToProto(test.in); got != test.want {
			t.Errorf("agentAttachedContextIssueToProto(%q) = %v, want %v", test.in, got, test.want)
		}
	}
}
