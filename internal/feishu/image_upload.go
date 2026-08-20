package feishu

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"strings"

	larkcore "github.com/larksuite/oapi-sdk-go/v3/core"
	larkim "github.com/larksuite/oapi-sdk-go/v3/service/im/v1"
)

// maxOutboundImageBytes mirrors the per-image ceiling on the inbound attached
// context path. Feishu itself accepts 10 MiB; staying under that keeps the two
// directions describable by one number and leaves headroom for the multipart
// envelope.
const maxOutboundImageBytes = 5 * 1024 * 1024

// outboundImageTypes is the closed set botd is willing to hand to Feishu. It is
// matched against sniffed bytes, never against a caller-declared type: an
// upload endpoint that forwards whatever it is given is a way to push arbitrary
// files out through the bot's identity.
var outboundImageTypes = map[string]struct{}{
	"image/png":  {},
	"image/jpeg": {},
	"image/gif":  {},
	"image/webp": {},
}

// ImageUploader mints the image_key a card needs to render a picture. It is an
// optional capability discovered by type assertion, exactly like DynamicCards,
// so a Sender that cannot upload simply does not implement it.
type ImageUploader interface {
	// UploadImage returns the Feishu image key and the media type botd detected
	// from the bytes.
	UploadImage(ctx context.Context, data []byte) (string, string, error)
}

// ImageRejectedError is a refusal decided by botd rather than by Feishu. It is
// separated from a transport or API failure because a caller can fix it by
// sending different bytes, and retrying the same ones never helps.
type ImageRejectedError struct {
	Reason string
}

func (e *ImageRejectedError) Error() string {
	return "feishu image upload rejected: " + e.Reason
}

// DetectOutboundImageType reports the sniffed media type of an image botd is
// willing to upload. ok is false for every other byte sequence, including a
// well-formed file of an unsupported image format.
func DetectOutboundImageType(data []byte) (string, bool) {
	// http.DetectContentType reads at most 512 bytes and always returns a type,
	// so the allowlist below is what actually decides.
	detected := http.DetectContentType(data)
	if index := strings.IndexByte(detected, ';'); index >= 0 {
		detected = detected[:index]
	}
	detected = strings.TrimSpace(strings.ToLower(detected))
	_, ok := outboundImageTypes[detected]
	return detected, ok
}

func (s *ChannelSender) UploadImage(ctx context.Context, data []byte) (string, string, error) {
	if s == nil || s.imageAPI == nil {
		return "", "", fmt.Errorf("feishu image upload is not configured")
	}
	if len(data) == 0 {
		return "", "", &ImageRejectedError{Reason: "image is empty"}
	}
	if len(data) > maxOutboundImageBytes {
		return "", "", &ImageRejectedError{Reason: "image exceeds the daemon size limit"}
	}
	mediaType, ok := DetectOutboundImageType(data)
	if !ok {
		return "", "", &ImageRejectedError{Reason: "image type is not supported"}
	}

	body := larkim.NewCreateImageReqBodyBuilder().
		ImageType(larkim.CreateImageImageTypeMessage).
		Image(bytes.NewReader(data)).
		Build()
	req := larkim.NewCreateImageReqBuilder().Body(body).Build()
	// Same reason as CreateCard: the generated builder keeps the body only in a
	// private ApiReq, so mirror it for request decorators and tests.
	req.Body = body

	resp, err := s.imageAPI.Create(ctx, req)
	if err != nil {
		return "", "", fmt.Errorf("feishu image upload failed: %w", err)
	}
	if resp == nil {
		return "", "", fmt.Errorf("feishu image upload returned an empty response")
	}
	if !resp.Success() {
		return "", "", dynamicCardResponseError("image upload", resp.ApiResp, resp.Code, resp.Msg)
	}
	if resp.Data == nil || resp.Data.ImageKey == nil || strings.TrimSpace(*resp.Data.ImageKey) == "" {
		return "", "", fmt.Errorf("feishu image upload returned no image key")
	}
	return strings.TrimSpace(*resp.Data.ImageKey), mediaType, nil
}

type imageAPI interface {
	Create(context.Context, *larkim.CreateImageReq, ...larkcore.RequestOptionFunc) (*larkim.CreateImageResp, error)
}
