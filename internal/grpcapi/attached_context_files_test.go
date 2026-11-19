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

// File chunks (ADR-0127) follow every video chunk, in file_index order, with
// the image offset/final contract. An empty file still gets one final frame.
func TestGRPCAgentAttachedContextStreamsFileChunksAfterAllVideoChunks(t *testing.T) {
	videoData := bytes.Repeat([]byte("video-fixture"), 1000)
	file0Data := bytes.Repeat([]byte("file-fixture-zero"), 9000) // > 2 chunks
	file1Data := []byte{}
	file2Data := bytes.Repeat([]byte{0x7F}, attachedContextImageChunkBytes) // exactly one chunk
	sender := &fakeAgentSender{
		fakeSender: fakeSender{messageID: "unused_fixture"},
		attachedContext: feishu.AttachedContext{
			Status: feishu.AttachedContextFound,
			Issues: []feishu.AttachedContextIssue{{Code: feishu.AttachedContextIssueFileLimit, Count: 1}},
			Messages: []feishu.AttachedContextMessage{
				{
					AuthorLabel: "participant-1", AuthorType: "user",
					Videos: []feishu.AttachedContextVideo{{MediaType: "video/mp4", Data: videoData}},
					Files:  []feishu.AttachedContextFile{{MediaType: "application/pdf", Data: file0Data, FileName: "report.pdf"}},
				},
				{
					AuthorLabel: "participant-2", AuthorType: "user",
					Files: []feishu.AttachedContextFile{
						{MediaType: "application/octet-stream", Data: file1Data, FileName: ""},
						{MediaType: "text/plain", Data: file2Data, FileName: "app.log"},
					},
				},
			},
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
		DeliveryID: "delivery_file_fixture", Command: "看看", Prompt: "看看这个文件",
		ConversationID: "conversation_fixture", ChatAlias: "ops", SenderID: "sender_fixture",
		Metadata: map[string]string{
			"chat_type": "topic_group", "message_type": "text", "message_id": "om_guide",
			"thread_id": "omt_private", "create_time": "1754380800123",
		},
	})

	stream, err := client.GetAgentAttachedContext(context.Background(), &pb.GetAgentAttachedContextRequest{
		Provider: "fixture-agent", DeliveryId: "delivery_file_fixture",
	})
	if err != nil {
		t.Fatalf("get attached context: %v", err)
	}
	first, err := stream.Recv()
	if err != nil {
		t.Fatalf("receive header: %v", err)
	}
	header := first.GetHeader()
	if header == nil || len(header.GetMessages()) != 2 {
		t.Fatalf("header = %#v", header)
	}
	if len(header.GetIssues()) != 1 ||
		header.GetIssues()[0].GetCode() != pb.AgentAttachedContextIssueCode_AGENT_ATTACHED_CONTEXT_ISSUE_CODE_FILE_LIMIT {
		t.Fatalf("issues = %#v", header.GetIssues())
	}
	descriptors := append(append([]*pb.AgentAttachedContextFileDescriptor{}, header.GetMessages()[0].GetFiles()...),
		header.GetMessages()[1].GetFiles()...)
	want := []struct {
		mediaType string
		data      []byte
		fileName  string
	}{
		{"application/pdf", file0Data, "report.pdf"},
		{"application/octet-stream", file1Data, ""},
		{"text/plain", file2Data, "app.log"},
	}
	if len(descriptors) != len(want) {
		t.Fatalf("file descriptors = %#v", descriptors)
	}
	for index, w := range want {
		d := descriptors[index]
		if d.GetFileIndex() != uint32(index) || d.GetMediaType() != w.mediaType ||
			d.GetByteSize() != uint64(len(w.data)) || d.GetFileName() != w.fileName {
			t.Fatalf("file descriptor[%d] = %#v", index, d)
		}
	}

	reconstructed := map[uint32][]byte{}
	finals := map[uint32]int{}
	sawVideo := false
	lastFileIndex := -1
	for {
		frame, recvErr := stream.Recv()
		if recvErr == io.EOF {
			break
		}
		if recvErr != nil {
			t.Fatalf("receive frame: %v", recvErr)
		}
		if frame.GetVideoChunk() != nil {
			if lastFileIndex >= 0 {
				t.Fatal("a video chunk arrived after a file chunk started")
			}
			sawVideo = true
			continue
		}
		chunk := frame.GetFileChunk()
		if chunk == nil {
			t.Fatalf("unexpected frame: %#v", frame)
		}
		if !sawVideo {
			t.Fatal("a file chunk arrived before the video chunks")
		}
		index := chunk.GetFileIndex()
		if int(index) < lastFileIndex {
			t.Fatalf("file chunk for %d arrived after file %d", index, lastFileIndex)
		}
		lastFileIndex = int(index)
		existing := reconstructed[index]
		if chunk.GetOffset() != uint64(len(existing)) {
			t.Fatalf("file[%d] offset = %d, reconstructed = %d", index, chunk.GetOffset(), len(existing))
		}
		if len(chunk.GetData()) > attachedContextImageChunkBytes {
			t.Fatalf("file[%d] chunk size = %d", index, len(chunk.GetData()))
		}
		reconstructed[index] = append(existing, chunk.GetData()...)
		if chunk.GetFinal() {
			finals[index]++
			if uint64(len(reconstructed[index])) != descriptors[index].GetByteSize() {
				t.Fatalf("file[%d] final at %d bytes, want %d", index, len(reconstructed[index]), descriptors[index].GetByteSize())
			}
		}
	}
	for index, w := range want {
		if !bytes.Equal(reconstructed[uint32(index)], w.data) {
			t.Fatalf("reconstructed file[%d] = %d bytes, want %d", index, len(reconstructed[uint32(index)]), len(w.data))
		}
		if finals[uint32(index)] != 1 {
			t.Fatalf("file[%d] final frames = %d, want exactly 1", index, finals[uint32(index)])
		}
	}
}

func TestAgentAttachedContextIssueToProtoMapsFileCodes(t *testing.T) {
	tests := []struct {
		in   feishu.AttachedContextIssueCode
		want pb.AgentAttachedContextIssueCode
	}{
		{feishu.AttachedContextIssueFileOmitted, pb.AgentAttachedContextIssueCode_AGENT_ATTACHED_CONTEXT_ISSUE_CODE_FILE_OMITTED},
		{feishu.AttachedContextIssueFileLimit, pb.AgentAttachedContextIssueCode_AGENT_ATTACHED_CONTEXT_ISSUE_CODE_FILE_LIMIT},
		{feishu.AttachedContextIssueFileTooLarge, pb.AgentAttachedContextIssueCode_AGENT_ATTACHED_CONTEXT_ISSUE_CODE_FILE_TOO_LARGE},
		{feishu.AttachedContextIssueTotalFileLimit, pb.AgentAttachedContextIssueCode_AGENT_ATTACHED_CONTEXT_ISSUE_CODE_TOTAL_FILE_LIMIT},
		{feishu.AttachedContextIssueFileUnreadable, pb.AgentAttachedContextIssueCode_AGENT_ATTACHED_CONTEXT_ISSUE_CODE_FILE_UNREADABLE},
	}
	for _, test := range tests {
		if got := agentAttachedContextIssueToProto(test.in); got != test.want {
			t.Errorf("agentAttachedContextIssueToProto(%q) = %v, want %v", test.in, got, test.want)
		}
	}
}
