package grpcapi

import (
	pb "feishu-botd/gen/feishubotd/v1"
	"feishu-botd/internal/feishu"
	"feishu-botd/internal/service"
)

const attachedContextImageChunkBytes = 64 * 1024

type attachedContextStreamImage struct {
	index uint32
	data  []byte
}

// attachedContextStreamVideo mirrors attachedContextStreamImage: video bytes
// stream with the same chunk-framing contract, after every image chunk
// (ADR-0126).
type attachedContextStreamVideo struct {
	index uint32
	data  []byte
}

func (c *commandServer) GetAgentAttachedContext(
	in *pb.GetAgentAttachedContextRequest,
	stream pb.CommandService_GetAgentAttachedContextServer,
) error {
	if err := authorizeAgentAttachedContext(stream.Context(), in.GetProvider()); err != nil {
		return err
	}
	result, apiErr := c.svc.GetAgentAttachedContext(stream.Context(), service.AgentAttachedContextInput{
		Provider: in.GetProvider(), DeliveryID: in.GetDeliveryId(),
	})
	if apiErr != nil {
		return grpcError(apiErr, requestIDFromContext(stream.Context()))
	}
	header, images, videos := agentAttachedContextHeaderToProto(result)
	if err := stream.Send(&pb.GetAgentAttachedContextResponse{
		Frame: &pb.GetAgentAttachedContextResponse_Header{Header: header},
	}); err != nil {
		return err
	}
	for _, image := range images {
		for offset := 0; offset < len(image.data); offset += attachedContextImageChunkBytes {
			end := offset + attachedContextImageChunkBytes
			if end > len(image.data) {
				end = len(image.data)
			}
			if err := stream.Send(&pb.GetAgentAttachedContextResponse{
				Frame: &pb.GetAgentAttachedContextResponse_ImageChunk{
					ImageChunk: &pb.AgentAttachedContextImageChunk{
						ImageIndex: image.index,
						Offset:     uint64(offset),
						Data:       image.data[offset:end],
						Final:      end == len(image.data),
					},
				},
			}); err != nil {
				return err
			}
		}
	}
	// Video chunks (ADR-0126) always follow every image chunk, in
	// video_index order — the resolver reads images and then videos.
	for _, video := range videos {
		for offset := 0; offset < len(video.data); offset += attachedContextImageChunkBytes {
			end := offset + attachedContextImageChunkBytes
			if end > len(video.data) {
				end = len(video.data)
			}
			if err := stream.Send(&pb.GetAgentAttachedContextResponse{
				Frame: &pb.GetAgentAttachedContextResponse_VideoChunk{
					VideoChunk: &pb.AgentAttachedContextVideoChunk{
						VideoIndex: video.index,
						Offset:     uint64(offset),
						Data:       video.data[offset:end],
						Final:      end == len(video.data),
					},
				},
			}); err != nil {
				return err
			}
		}
	}
	return nil
}

func agentAttachedContextHeaderToProto(
	in feishu.AttachedContext,
) (*pb.AgentAttachedContextHeader, []attachedContextStreamImage, []attachedContextStreamVideo) {
	header := &pb.AgentAttachedContextHeader{
		Status:    agentAttachedContextStatusToProto(in.Status),
		Truncated: in.Truncated,
	}
	for _, issue := range in.Issues {
		header.Issues = append(header.Issues, &pb.AgentAttachedContextIssue{
			Code: agentAttachedContextIssueToProto(issue.Code), Count: issue.Count,
		})
	}
	images := make([]attachedContextStreamImage, 0)
	videos := make([]attachedContextStreamVideo, 0)
	for _, message := range in.Messages {
		out := &pb.AgentAttachedContextMessage{
			AuthorLabel: message.AuthorLabel,
			AuthorType:  message.AuthorType,
			Text:        message.Text,
		}
		for _, image := range message.Images {
			index := uint32(len(images))
			out.Images = append(out.Images, &pb.AgentAttachedContextImageDescriptor{
				ImageIndex: index, MediaType: image.MediaType, ByteSize: uint64(len(image.Data)),
			})
			images = append(images, attachedContextStreamImage{index: index, data: image.Data})
		}
		for _, video := range message.Videos {
			index := uint32(len(videos))
			out.Videos = append(out.Videos, &pb.AgentAttachedContextVideoDescriptor{
				VideoIndex: index, MediaType: video.MediaType, ByteSize: uint64(len(video.Data)),
				DurationMs: video.DurationMs, FileName: video.FileName,
			})
			videos = append(videos, attachedContextStreamVideo{index: index, data: video.Data})
		}
		header.Messages = append(header.Messages, out)
	}
	return header, images, videos
}

func agentAttachedContextStatusToProto(in feishu.AttachedContextStatus) pb.AgentAttachedContextStatus {
	switch in {
	case feishu.AttachedContextFound:
		return pb.AgentAttachedContextStatus_AGENT_ATTACHED_CONTEXT_STATUS_FOUND
	case feishu.AttachedContextMissing:
		return pb.AgentAttachedContextStatus_AGENT_ATTACHED_CONTEXT_STATUS_MISSING
	case feishu.AttachedContextUnreadable:
		return pb.AgentAttachedContextStatus_AGENT_ATTACHED_CONTEXT_STATUS_UNREADABLE
	default:
		return pb.AgentAttachedContextStatus_AGENT_ATTACHED_CONTEXT_STATUS_UNSPECIFIED
	}
}

func agentAttachedContextIssueToProto(in feishu.AttachedContextIssueCode) pb.AgentAttachedContextIssueCode {
	switch in {
	case feishu.AttachedContextIssueNoThread:
		return pb.AgentAttachedContextIssueCode_AGENT_ATTACHED_CONTEXT_ISSUE_CODE_NO_THREAD
	case feishu.AttachedContextIssueHistoryUnreadable:
		return pb.AgentAttachedContextIssueCode_AGENT_ATTACHED_CONTEXT_ISSUE_CODE_HISTORY_UNREADABLE
	case feishu.AttachedContextIssueBoundaryNotFound:
		return pb.AgentAttachedContextIssueCode_AGENT_ATTACHED_CONTEXT_ISSUE_CODE_BOUNDARY_NOT_FOUND
	case feishu.AttachedContextIssueBoundaryScanLimit:
		return pb.AgentAttachedContextIssueCode_AGENT_ATTACHED_CONTEXT_ISSUE_CODE_BOUNDARY_SCAN_LIMIT
	case feishu.AttachedContextIssueMessageLimit:
		return pb.AgentAttachedContextIssueCode_AGENT_ATTACHED_CONTEXT_ISSUE_CODE_MESSAGE_LIMIT
	case feishu.AttachedContextIssueTextLimit:
		return pb.AgentAttachedContextIssueCode_AGENT_ATTACHED_CONTEXT_ISSUE_CODE_TEXT_LIMIT
	case feishu.AttachedContextIssueImageLimit:
		return pb.AgentAttachedContextIssueCode_AGENT_ATTACHED_CONTEXT_ISSUE_CODE_IMAGE_LIMIT
	case feishu.AttachedContextIssueImageTooLarge:
		return pb.AgentAttachedContextIssueCode_AGENT_ATTACHED_CONTEXT_ISSUE_CODE_IMAGE_TOO_LARGE
	case feishu.AttachedContextIssueTotalImageLimit:
		return pb.AgentAttachedContextIssueCode_AGENT_ATTACHED_CONTEXT_ISSUE_CODE_TOTAL_IMAGE_LIMIT
	case feishu.AttachedContextIssueImageUnreadable:
		return pb.AgentAttachedContextIssueCode_AGENT_ATTACHED_CONTEXT_ISSUE_CODE_IMAGE_UNREADABLE
	case feishu.AttachedContextIssueImageType:
		return pb.AgentAttachedContextIssueCode_AGENT_ATTACHED_CONTEXT_ISSUE_CODE_IMAGE_TYPE_UNSUPPORTED
	case feishu.AttachedContextIssueVideoOmitted:
		return pb.AgentAttachedContextIssueCode_AGENT_ATTACHED_CONTEXT_ISSUE_CODE_VIDEO_OMITTED
	case feishu.AttachedContextIssueVideoLimit:
		return pb.AgentAttachedContextIssueCode_AGENT_ATTACHED_CONTEXT_ISSUE_CODE_VIDEO_LIMIT
	case feishu.AttachedContextIssueVideoTooLarge:
		return pb.AgentAttachedContextIssueCode_AGENT_ATTACHED_CONTEXT_ISSUE_CODE_VIDEO_TOO_LARGE
	case feishu.AttachedContextIssueVideoUnreadable:
		return pb.AgentAttachedContextIssueCode_AGENT_ATTACHED_CONTEXT_ISSUE_CODE_VIDEO_UNREADABLE
	case feishu.AttachedContextIssueVideoType:
		return pb.AgentAttachedContextIssueCode_AGENT_ATTACHED_CONTEXT_ISSUE_CODE_VIDEO_TYPE_UNSUPPORTED
	case feishu.AttachedContextIssueVideoTooLong:
		return pb.AgentAttachedContextIssueCode_AGENT_ATTACHED_CONTEXT_ISSUE_CODE_VIDEO_TOO_LONG
	case feishu.AttachedContextIssueUnsupportedMessage:
		return pb.AgentAttachedContextIssueCode_AGENT_ATTACHED_CONTEXT_ISSUE_CODE_UNSUPPORTED_MESSAGE
	case feishu.AttachedContextIssueMalformedMessage:
		return pb.AgentAttachedContextIssueCode_AGENT_ATTACHED_CONTEXT_ISSUE_CODE_MALFORMED_MESSAGE
	default:
		return pb.AgentAttachedContextIssueCode_AGENT_ATTACHED_CONTEXT_ISSUE_CODE_UNSPECIFIED
	}
}
