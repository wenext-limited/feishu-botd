package service

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"feishu-botd/internal/config"
	"feishu-botd/internal/dedupe"
	"feishu-botd/internal/feishu"
)

// pngBytes is the smallest thing http.DetectContentType calls an image/png:
// the 8-byte signature is enough, and a real encoder is not needed to test the
// sniffing boundary.
var pngBytes = []byte("\x89PNG\r\n\x1a\n" + "the rest is not inspected")

type imageUploadTestBackend struct {
	*fakeSender
	imageKey  string
	mediaType string
	err       error
	calls     int
	uploaded  [][]byte
}

func (b *imageUploadTestBackend) UploadImage(_ context.Context, data []byte) (string, string, error) {
	b.calls++
	b.uploaded = append(b.uploaded, append([]byte(nil), data...))
	if b.err != nil {
		return "", "", b.err
	}
	key := b.imageKey
	if key == "" {
		key = "img_v3_default"
	}
	mediaType := b.mediaType
	if mediaType == "" {
		mediaType = "image/png"
	}
	return key, mediaType, nil
}

func newImageUploadTestService(backend *imageUploadTestBackend) *Service {
	cfg := config.Config{
		AppID: "cli_test", AppSecret: "secret",
		Channels:    map[string]string{"ops": "oc_ops"},
		DedupeTTL:   time.Hour,
		SendTimeout: time.Second,
		AgentProviders: map[string]config.AgentProviderConfig{
			"ibot":  {AllowImageUpload: true},
			"other": {AllowImageUpload: true},
		},
	}
	return NewService(cfg, backend, dedupe.NewMemoryStore(time.Hour), slog.Default())
}

func seedImageUploadDelivery(t *testing.T, svc *Service, provider, deliveryID string) {
	t.Helper()
	sub := mustSubscribeAgent(t, svc, AgentSubscribeOptions{
		Provider: provider, IncludeUnmatchedMessages: true,
	})
	t.Cleanup(sub.Close)
	mustDispatchAgentPrompt(t, svc, CommandInput{
		DeliveryID: deliveryID, Command: "给我二维码", Prompt: "给我二维码",
		ConversationID: "conv_qr", ChatAlias: "ops", SenderID: "ou_sender",
		Metadata: map[string]string{"chat_type": "group", "message_id": "om_guide"},
	})
	_ = receiveAgentEvent(t, sub)
}

func newSeededImageUploadService(t *testing.T, backend *imageUploadTestBackend) *Service {
	t.Helper()
	svc := newImageUploadTestService(backend)
	seedImageUploadDelivery(t, svc, "ibot", "delivery_qr")
	return svc
}

func uploadInput(operationID string, data []byte) AgentImageUploadInput {
	return AgentImageUploadInput{
		Provider: "ibot", DeliveryID: "delivery_qr", OperationID: operationID, Data: data,
	}
}

func TestUploadAgentImageReturnsKeyForAnAuthorizedDelivery(t *testing.T) {
	backend := &imageUploadTestBackend{
		fakeSender: &fakeSender{messageID: "om_answer"},
		imageKey:   "img_v3_qr", mediaType: "image/png",
	}
	svc := newSeededImageUploadService(t, backend)

	got, apiErr := svc.UploadAgentImage(context.Background(), uploadInput("op_1", pngBytes))
	if apiErr != nil {
		t.Fatalf("upload: %v", apiErr)
	}
	if got.ImageKey != "img_v3_qr" || got.MediaType != "image/png" || got.Duplicate {
		t.Fatalf("result = %#v", got)
	}
	if backend.calls != 1 || string(backend.uploaded[0]) != string(pngBytes) {
		t.Fatalf("calls=%d uploaded=%q", backend.calls, backend.uploaded)
	}
}

func TestUploadAgentImageRequiresExplicitProviderGrant(t *testing.T) {
	backend := &imageUploadTestBackend{fakeSender: &fakeSender{messageID: "om_answer"}}
	svc := newSeededImageUploadService(t, backend)
	svc.cfg.AgentProviders["ibot"] = config.AgentProviderConfig{}

	_, apiErr := svc.UploadAgentImage(context.Background(), uploadInput("op_1", pngBytes))
	if apiErr == nil || apiErr.Code != "provider_scope_denied" {
		t.Fatalf("scope error = %v", apiErr)
	}
	if backend.calls != 0 {
		t.Fatalf("denied upload reached Feishu: calls=%d", backend.calls)
	}
}

func TestUploadAgentImageDoesNotCrossProviderOrDeliveryOwnership(t *testing.T) {
	backend := &imageUploadTestBackend{fakeSender: &fakeSender{messageID: "om_answer"}}
	svc := newSeededImageUploadService(t, backend)

	for _, testCase := range []struct {
		name  string
		input AgentImageUploadInput
	}{
		{"other provider", AgentImageUploadInput{
			Provider: "other", DeliveryID: "delivery_qr", OperationID: "op_1", Data: pngBytes,
		}},
		{"unknown delivery", AgentImageUploadInput{
			Provider: "ibot", DeliveryID: "delivery_missing", OperationID: "op_1", Data: pngBytes,
		}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			_, apiErr := svc.UploadAgentImage(context.Background(), testCase.input)
			if apiErr == nil || apiErr.Code != "unknown_delivery" {
				t.Fatalf("error = %v", apiErr)
			}
		})
	}
	if backend.calls != 0 {
		t.Fatalf("unauthorized upload reached Feishu: calls=%d", backend.calls)
	}
}

func TestUploadAgentImageReplayReturnsTheFirstKeyWithoutUploadingAgain(t *testing.T) {
	backend := &imageUploadTestBackend{
		fakeSender: &fakeSender{messageID: "om_answer"}, imageKey: "img_v3_qr",
	}
	svc := newSeededImageUploadService(t, backend)

	first, apiErr := svc.UploadAgentImage(context.Background(), uploadInput("op_1", pngBytes))
	if apiErr != nil {
		t.Fatalf("first upload: %v", apiErr)
	}
	second, apiErr := svc.UploadAgentImage(context.Background(), uploadInput("op_1", pngBytes))
	if apiErr != nil {
		t.Fatalf("replayed upload: %v", apiErr)
	}
	if second.ImageKey != first.ImageKey || !second.Duplicate {
		t.Fatalf("replay = %#v, first = %#v", second, first)
	}
	if backend.calls != 1 {
		t.Fatalf("replay minted a second key: calls=%d", backend.calls)
	}
}

func TestUploadAgentImageRejectsAnOperationIDReusedForDifferentBytes(t *testing.T) {
	backend := &imageUploadTestBackend{fakeSender: &fakeSender{messageID: "om_answer"}}
	svc := newSeededImageUploadService(t, backend)

	if _, apiErr := svc.UploadAgentImage(context.Background(), uploadInput("op_1", pngBytes)); apiErr != nil {
		t.Fatalf("first upload: %v", apiErr)
	}
	other := append(append([]byte(nil), pngBytes...), " and more"...)
	_, apiErr := svc.UploadAgentImage(context.Background(), uploadInput("op_1", other))
	if apiErr == nil || apiErr.Code != "operation_conflict" {
		t.Fatalf("error = %v", apiErr)
	}
	if backend.calls != 1 {
		t.Fatalf("conflicting replay reached Feishu: calls=%d", backend.calls)
	}
}

func TestUploadAgentImageBoundsHowManyImagesOneDeliveryCanMint(t *testing.T) {
	backend := &imageUploadTestBackend{fakeSender: &fakeSender{messageID: "om_answer"}}
	svc := newSeededImageUploadService(t, backend)

	for index := 0; index < maxDeliveryImages; index++ {
		data := append(append([]byte(nil), pngBytes...), byte(index))
		if _, apiErr := svc.UploadAgentImage(context.Background(), uploadInput(string(rune('a'+index)), data)); apiErr != nil {
			t.Fatalf("upload %d: %v", index, apiErr)
		}
	}
	_, apiErr := svc.UploadAgentImage(context.Background(), uploadInput("overflow", pngBytes))
	if apiErr == nil || apiErr.Code != "too_many_images" {
		t.Fatalf("error = %v", apiErr)
	}
	if backend.calls != maxDeliveryImages {
		t.Fatalf("calls=%d", backend.calls)
	}
}

func TestUploadAgentImageSurfacesADaemonRefusalAsNonRetryable(t *testing.T) {
	backend := &imageUploadTestBackend{
		fakeSender: &fakeSender{messageID: "om_answer"},
		err:        &feishu.ImageRejectedError{Reason: "image type is not supported"},
	}
	svc := newSeededImageUploadService(t, backend)

	_, apiErr := svc.UploadAgentImage(context.Background(), uploadInput("op_1", pngBytes))
	if apiErr == nil || apiErr.Code != "invalid_image" || apiErr.Retryable {
		t.Fatalf("error = %#v", apiErr)
	}
}

func TestUploadAgentImageSurfacesATransportFailureAsRetryable(t *testing.T) {
	backend := &imageUploadTestBackend{
		fakeSender: &fakeSender{messageID: "om_answer"},
		err:        errors.New("connection reset"),
	}
	svc := newSeededImageUploadService(t, backend)

	_, apiErr := svc.UploadAgentImage(context.Background(), uploadInput("op_1", pngBytes))
	if apiErr == nil || !apiErr.Retryable {
		t.Fatalf("error = %#v", apiErr)
	}
}

func TestUploadAgentImageRejectsIncompleteRequests(t *testing.T) {
	backend := &imageUploadTestBackend{fakeSender: &fakeSender{messageID: "om_answer"}}
	svc := newSeededImageUploadService(t, backend)

	for _, testCase := range []struct {
		name  string
		input AgentImageUploadInput
		code  string
	}{
		{"no provider", AgentImageUploadInput{DeliveryID: "delivery_qr", OperationID: "op", Data: pngBytes}, "missing_provider"},
		{"no delivery", AgentImageUploadInput{Provider: "ibot", OperationID: "op", Data: pngBytes}, "missing_delivery_id"},
		{"no operation", AgentImageUploadInput{Provider: "ibot", DeliveryID: "delivery_qr", Data: pngBytes}, "missing_operation_id"},
		{"no bytes", AgentImageUploadInput{Provider: "ibot", DeliveryID: "delivery_qr", OperationID: "op"}, "missing_image"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			_, apiErr := svc.UploadAgentImage(context.Background(), testCase.input)
			if apiErr == nil || apiErr.Code != testCase.code {
				t.Fatalf("error = %v, want %s", apiErr, testCase.code)
			}
		})
	}
	if backend.calls != 0 {
		t.Fatalf("invalid request reached Feishu: calls=%d", backend.calls)
	}
}

// An app whose sender cannot upload must say so rather than fail obscurely at
// the point a card tries to render a key that was never minted.
func TestUploadAgentImageReportsAnAppThatCannotUpload(t *testing.T) {
	// A plain fakeSender implements Sender but not ImageUploader, which is
	// exactly the shape of an app whose credentials predate this capability.
	svc := newImageUploadTestService(&imageUploadTestBackend{})
	svc = NewService(svc.cfg, &fakeSender{messageID: "om_answer"},
		dedupe.NewMemoryStore(time.Hour), slog.Default())
	seedImageUploadDelivery(t, svc, "ibot", "delivery_qr")

	_, apiErr := svc.UploadAgentImage(context.Background(), uploadInput("op_1", pngBytes))
	if apiErr == nil || apiErr.Code != "image_upload_unsupported" {
		t.Fatalf("error = %v", apiErr)
	}
}
