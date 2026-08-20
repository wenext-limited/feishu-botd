package feishu

import (
	"context"
	"errors"
	"io"
	"net/http"
	"testing"

	lark "github.com/larksuite/oapi-sdk-go/v3"
	larkcore "github.com/larksuite/oapi-sdk-go/v3/core"
	larkim "github.com/larksuite/oapi-sdk-go/v3/service/im/v1"
)

type fakeReactionAPI struct {
	createReq *larkim.CreateMessageReactionReq
	deleteReq *larkim.DeleteMessageReactionReq

	createResp *larkim.CreateMessageReactionResp
	deleteResp *larkim.DeleteMessageReactionResp

	createErr error
	deleteErr error
}

func (f *fakeReactionAPI) Create(_ context.Context, req *larkim.CreateMessageReactionReq, _ ...larkcore.RequestOptionFunc) (*larkim.CreateMessageReactionResp, error) {
	f.createReq = req
	return f.createResp, f.createErr
}

func (f *fakeReactionAPI) Delete(_ context.Context, req *larkim.DeleteMessageReactionReq, _ ...larkcore.RequestOptionFunc) (*larkim.DeleteMessageReactionResp, error) {
	f.deleteReq = req
	return f.deleteResp, f.deleteErr
}

func TestAddReactionSendsTheConfiguredEmojiToTheGivenMessage(t *testing.T) {
	reactionID := "reaction_fixture_1"
	api := &fakeReactionAPI{createResp: &larkim.CreateMessageReactionResp{
		Data: &larkim.CreateMessageReactionRespData{ReactionId: &reactionID},
	}}
	sender := &ChannelSender{reactionAPI: api}

	got, err := sender.AddReaction(context.Background(), AddReactionRequest{
		MessageID: "om_trigger", EmojiType: "OnIt",
	})
	if err != nil {
		t.Fatalf("add reaction: %v", err)
	}
	if got != reactionID {
		t.Fatalf("reaction id = %q, want %q", got, reactionID)
	}
	if api.createReq.Body == nil || api.createReq.Body.ReactionType == nil ||
		api.createReq.Body.ReactionType.EmojiType == nil || *api.createReq.Body.ReactionType.EmojiType != "OnIt" {
		t.Fatalf("request body = %#v", api.createReq.Body)
	}
}

func TestAddReactionRequiresMessageIDAndEmojiType(t *testing.T) {
	sender := &ChannelSender{reactionAPI: &fakeReactionAPI{}}

	if _, err := sender.AddReaction(context.Background(), AddReactionRequest{EmojiType: "OnIt"}); err == nil {
		t.Fatal("missing message id: want an error")
	}
	if _, err := sender.AddReaction(context.Background(), AddReactionRequest{MessageID: "om_trigger"}); err == nil {
		t.Fatal("missing emoji type: want an error")
	}
}

func TestAddReactionSurfacesARejectedResponse(t *testing.T) {
	api := &fakeReactionAPI{createResp: &larkim.CreateMessageReactionResp{
		ApiResp: &larkcore.ApiResp{
			StatusCode: http.StatusBadRequest,
			Header:     http.Header{larkcore.HttpHeaderKeyLogId: []string{"log-reaction-1"}},
		},
		CodeError: larkcore.CodeError{Code: 230001, Msg: "message not found"},
	}}
	sender := &ChannelSender{reactionAPI: api}

	_, err := sender.AddReaction(context.Background(), AddReactionRequest{MessageID: "om_trigger", EmojiType: "OnIt"})
	var apiErr *DynamicCardAPIError
	if !errors.As(err, &apiErr) || apiErr.Code != 230001 || apiErr.RequestID != "log-reaction-1" {
		t.Fatalf("error = %v, want a classified DynamicCardAPIError", err)
	}
}

func TestAddReactionWrapsATransportFailure(t *testing.T) {
	sender := &ChannelSender{reactionAPI: &fakeReactionAPI{createErr: errors.New("dial tcp 10.0.0.1: connection refused")}}

	_, err := sender.AddReaction(context.Background(), AddReactionRequest{MessageID: "om_trigger", EmojiType: "OnIt"})
	if err == nil {
		t.Fatal("want a transport error")
	}
}

func TestRemoveReactionRequestBodyRoundTrips(t *testing.T) {
	api := &fakeReactionAPI{deleteResp: &larkim.DeleteMessageReactionResp{}}
	sender := &ChannelSender{reactionAPI: api}

	if err := sender.RemoveReaction(context.Background(), RemoveReactionRequest{
		MessageID: "om_trigger", ReactionID: "reaction_fixture_1",
	}); err != nil {
		t.Fatalf("remove reaction: %v", err)
	}
	if api.deleteReq == nil {
		t.Fatal("delete was not called")
	}
}

func TestRemoveReactionRequiresMessageIDAndReactionID(t *testing.T) {
	sender := &ChannelSender{reactionAPI: &fakeReactionAPI{}}

	if err := sender.RemoveReaction(context.Background(), RemoveReactionRequest{ReactionID: "r1"}); err == nil {
		t.Fatal("missing message id: want an error")
	}
	if err := sender.RemoveReaction(context.Background(), RemoveReactionRequest{MessageID: "om_trigger"}); err == nil {
		t.Fatal("missing reaction id: want an error")
	}
}

func TestRemoveReactionSurfacesARejectedResponse(t *testing.T) {
	api := &fakeReactionAPI{deleteResp: &larkim.DeleteMessageReactionResp{
		ApiResp:   &larkcore.ApiResp{StatusCode: http.StatusForbidden},
		CodeError: larkcore.CodeError{Code: 230002, Msg: "not the original adder"},
	}}
	sender := &ChannelSender{reactionAPI: api}

	err := sender.RemoveReaction(context.Background(), RemoveReactionRequest{MessageID: "om_trigger", ReactionID: "reaction_fixture_1"})
	var apiErr *DynamicCardAPIError
	if !errors.As(err, &apiErr) || apiErr.Code != 230002 {
		t.Fatalf("error = %v, want a classified DynamicCardAPIError", err)
	}
}

func TestReactionMessagesRequireConfiguredClient(t *testing.T) {
	sender := &ChannelSender{}

	if _, err := sender.AddReaction(context.Background(), AddReactionRequest{MessageID: "om_trigger", EmojiType: "OnIt"}); err == nil {
		t.Fatal("unconfigured sender: want an error")
	}
	if err := sender.RemoveReaction(context.Background(), RemoveReactionRequest{MessageID: "om_trigger", ReactionID: "r1"}); err == nil {
		t.Fatal("unconfigured sender: want an error")
	}
}

// TestRemoveReactionEscapesMessageAndReactionIDsInThePath is the one thing
// the fake above cannot prove: DeleteMessageReactionReq exposes no field to
// inspect the identifiers it was built with (only an unexported *ApiReq), so
// this drives the request through a real *lark.Client and a stub transport,
// exactly like message_cot_test.go does, to see the actual outbound path.
func TestRemoveReactionEscapesMessageAndReactionIDsInThePath(t *testing.T) {
	httpClient := &reactionStubHTTPClient{status: http.StatusOK, body: `{"code":0,"msg":"success","data":{}}`}
	client := lark.NewClient("cli_reaction_test", "secret", lark.WithHttpClient(httpClient))
	sender := &ChannelSender{client: client, reactionAPI: client.Im.V1.MessageReaction}

	if err := sender.RemoveReaction(context.Background(), RemoveReactionRequest{
		MessageID: "om trigger", ReactionID: "reaction/1",
	}); err != nil {
		t.Fatalf("remove reaction: %v", err)
	}
	if len(httpClient.requests) != 1 {
		t.Fatalf("requests = %d, want 1: %#v", len(httpClient.requests), httpClient.requests)
	}
	got := httpClient.requests[0]
	if got.Method != http.MethodDelete {
		t.Fatalf("method = %q, want DELETE", got.Method)
	}
	want := "/open-apis/im/v1/messages/om%20trigger/reactions/reaction%2F1"
	if got.EscapedPath != want {
		t.Fatalf("path = %q, want %q", got.EscapedPath, want)
	}
}

type reactionRecordedRequest struct {
	Method      string
	EscapedPath string
}

type reactionStubHTTPClient struct {
	requests []reactionRecordedRequest
	status   int
	body     string
}

func (c *reactionStubHTTPClient) Do(req *http.Request) (*http.Response, error) {
	if req.URL.Path == cotTestTokenPath {
		return cotStubResponse(req, http.StatusOK, cotTestTokenBody, nil), nil
	}
	if req.Body != nil {
		_, _ = io.ReadAll(req.Body)
	}
	c.requests = append(c.requests, reactionRecordedRequest{
		Method: req.Method, EscapedPath: req.URL.EscapedPath(),
	})
	status := c.status
	if status == 0 {
		status = http.StatusOK
	}
	body := c.body
	if body == "" {
		body = `{"code":0,"msg":"success"}`
	}
	return cotStubResponse(req, status, body, nil), nil
}
