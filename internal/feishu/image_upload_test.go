package feishu

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	larkcore "github.com/larksuite/oapi-sdk-go/v3/core"
	larkim "github.com/larksuite/oapi-sdk-go/v3/service/im/v1"
)

type fakeImageAPI struct {
	req  *larkim.CreateImageReq
	resp *larkim.CreateImageResp
	err  error
	body []byte
}

func (f *fakeImageAPI) Create(_ context.Context, req *larkim.CreateImageReq, _ ...larkcore.RequestOptionFunc) (*larkim.CreateImageResp, error) {
	f.req = req
	if req != nil && req.Body != nil && req.Body.Image != nil {
		f.body, _ = io.ReadAll(req.Body.Image)
	}
	return f.resp, f.err
}

func imageKeyResp(key string) *larkim.CreateImageResp {
	resp := &larkim.CreateImageResp{Data: &larkim.CreateImageRespData{ImageKey: &key}}
	resp.Code = 0
	return resp
}

// pngHeader is the PNG signature; http.DetectContentType needs no more.
var pngHeader = []byte("\x89PNG\r\n\x1a\n")

func TestUploadImageSendsBytesAndReturnsTheKey(t *testing.T) {
	api := &fakeImageAPI{resp: imageKeyResp("img_v3_qr")}
	sender := &ChannelSender{imageAPI: api}

	data := append(append([]byte(nil), pngHeader...), "payload"...)
	key, mediaType, err := sender.UploadImage(context.Background(), data)
	if err != nil {
		t.Fatalf("upload: %v", err)
	}
	if key != "img_v3_qr" || mediaType != "image/png" {
		t.Fatalf("key=%q mediaType=%q", key, mediaType)
	}
	if !bytes.Equal(api.body, data) {
		t.Fatalf("uploaded %q, want %q", api.body, data)
	}
	if api.req == nil || api.req.Body == nil || api.req.Body.ImageType == nil ||
		*api.req.Body.ImageType != larkim.CreateImageImageTypeMessage {
		t.Fatalf("request body = %#v", api.req)
	}
}

// The declared type is never consulted, so a caller cannot push a non-image
// out through the bot's identity by labelling it one.
func TestUploadImageRefusesBytesThatAreNotAnAllowedImage(t *testing.T) {
	for _, testCase := range []struct {
		name string
		data []byte
	}{
		{"empty", nil},
		{"html", []byte("<!DOCTYPE html><html><body>hi</body></html>")},
		{"zip", []byte("PK\x03\x04and then some archive bytes")},
		{"plain text", []byte("just a note, definitely not a picture")},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			api := &fakeImageAPI{resp: imageKeyResp("img_v3_qr")}
			sender := &ChannelSender{imageAPI: api}

			_, _, err := sender.UploadImage(context.Background(), testCase.data)
			var rejected *ImageRejectedError
			if !errors.As(err, &rejected) {
				t.Fatalf("error = %v, want a rejection", err)
			}
			if api.req != nil {
				t.Fatalf("rejected bytes still reached Feishu")
			}
		})
	}
}

func TestUploadImageRefusesAnImageOverTheDaemonCeiling(t *testing.T) {
	api := &fakeImageAPI{resp: imageKeyResp("img_v3_qr")}
	sender := &ChannelSender{imageAPI: api}

	data := append(append([]byte(nil), pngHeader...), bytes.Repeat([]byte{0}, maxOutboundImageBytes)...)
	_, _, err := sender.UploadImage(context.Background(), data)
	var rejected *ImageRejectedError
	if !errors.As(err, &rejected) || !strings.Contains(rejected.Reason, "size limit") {
		t.Fatalf("error = %v", err)
	}
	if api.req != nil {
		t.Fatalf("oversized image still reached Feishu")
	}
}

func TestUploadImageSurfacesAFeishuRejection(t *testing.T) {
	resp := &larkim.CreateImageResp{}
	resp.Code = 234001
	resp.Msg = "image is invalid"
	api := &fakeImageAPI{resp: resp}
	sender := &ChannelSender{imageAPI: api}

	_, _, err := sender.UploadImage(context.Background(), pngHeader)
	var apiErr *DynamicCardAPIError
	if !errors.As(err, &apiErr) || apiErr.Code != 234001 {
		t.Fatalf("error = %v", err)
	}
	// A Feishu-side refusal is not an ImageRejectedError: botd did not decide it
	// and cannot tell the caller that different bytes would fail the same way.
	var rejected *ImageRejectedError
	if errors.As(err, &rejected) {
		t.Fatalf("Feishu rejection was reported as a daemon refusal")
	}
}

func TestUploadImageRequiresAnImageKeyInTheResponse(t *testing.T) {
	for _, testCase := range []struct {
		name string
		resp *larkim.CreateImageResp
		err  error
	}{
		{"transport failure", nil, errors.New("connection reset")},
		{"empty response", nil, nil},
		{"no key", imageKeyResp(" "), nil},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			sender := &ChannelSender{imageAPI: &fakeImageAPI{resp: testCase.resp, err: testCase.err}}
			if _, _, err := sender.UploadImage(context.Background(), pngHeader); err == nil {
				t.Fatalf("expected an error")
			}
		})
	}
}

func TestUploadImageReportsAnUnconfiguredSender(t *testing.T) {
	sender := &ChannelSender{}
	if _, _, err := sender.UploadImage(context.Background(), pngHeader); err == nil {
		t.Fatalf("expected an error")
	}
}

func TestDetectOutboundImageTypeAcceptsExactlyTheRenderableFormats(t *testing.T) {
	for _, testCase := range []struct {
		name string
		data []byte
		want string
		ok   bool
	}{
		{"png", pngHeader, "image/png", true},
		{"jpeg", []byte("\xff\xd8\xff\xe0"), "image/jpeg", true},
		{"gif", []byte("GIF89a"), "image/gif", true},
		{"webp", []byte("RIFF\x00\x00\x00\x00WEBPVP8 "), "image/webp", true},
		{"bmp is an image Feishu will not render", []byte("BM\x00\x00\x00\x00"), "image/bmp", false},
		{"pdf", []byte("%PDF-1.7\n"), "application/pdf", false},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			got, ok := DetectOutboundImageType(testCase.data)
			if got != testCase.want || ok != testCase.ok {
				t.Fatalf("detected %q ok=%t, want %q ok=%t", got, ok, testCase.want, testCase.ok)
			}
		})
	}
}
