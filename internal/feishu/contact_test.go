package feishu

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"testing"

	lark "github.com/larksuite/oapi-sdk-go/v3"
	larkcore "github.com/larksuite/oapi-sdk-go/v3/core"
	larkcontact "github.com/larksuite/oapi-sdk-go/v3/service/contact/v3"
)

type fakeContactUserAPI struct {
	calls int
	req   *larkcontact.GetUserReq

	resp *larkcontact.GetUserResp
	err  error
}

func (f *fakeContactUserAPI) Get(_ context.Context, req *larkcontact.GetUserReq, _ ...larkcore.RequestOptionFunc) (*larkcontact.GetUserResp, error) {
	f.calls++
	f.req = req
	return f.resp, f.err
}

func contactUserResp(name string) *larkcontact.GetUserResp {
	return &larkcontact.GetUserResp{
		Data: &larkcontact.GetUserRespData{User: &larkcontact.User{Name: &name}},
	}
}

func TestDisplayNameResolvesTheUsersName(t *testing.T) {
	api := &fakeContactUserAPI{resp: contactUserResp("王鑫禹")}
	sender := &ChannelSender{contactUserAPI: api}

	got, err := sender.DisplayName(context.Background(), "ou_sender_1")
	if err != nil {
		t.Fatalf("display name: %v", err)
	}
	if got != "王鑫禹" {
		t.Fatalf("name = %q, want 王鑫禹", got)
	}
	if api.calls != 1 {
		t.Fatalf("calls = %d, want 1", api.calls)
	}
}

func TestDisplayNameCachesSuccessfulLookups(t *testing.T) {
	api := &fakeContactUserAPI{resp: contactUserResp("王鑫禹")}
	sender := &ChannelSender{contactUserAPI: api}

	for i := 0; i < 3; i++ {
		got, err := sender.DisplayName(context.Background(), "ou_sender_1")
		if err != nil {
			t.Fatalf("display name call %d: %v", i, err)
		}
		if got != "王鑫禹" {
			t.Fatalf("name = %q, want 王鑫禹", got)
		}
	}
	if api.calls != 1 {
		t.Fatalf("calls = %d, want 1 (cached after the first)", api.calls)
	}
}

func TestDisplayNameDoesNotCacheAFailedLookup(t *testing.T) {
	api := &fakeContactUserAPI{err: errors.New("dial tcp 10.0.0.1: connection refused")}
	sender := &ChannelSender{contactUserAPI: api}

	if _, err := sender.DisplayName(context.Background(), "ou_sender_1"); err == nil {
		t.Fatal("want a transport error")
	}
	api.err = nil
	api.resp = contactUserResp("王鑫禹")
	got, err := sender.DisplayName(context.Background(), "ou_sender_1")
	if err != nil {
		t.Fatalf("display name: %v", err)
	}
	if got != "王鑫禹" {
		t.Fatalf("name = %q, want 王鑫禹", got)
	}
	if api.calls != 2 {
		t.Fatalf("calls = %d, want 2 (no caching of the failure)", api.calls)
	}
}

func TestDisplayNameRequiresUserID(t *testing.T) {
	sender := &ChannelSender{contactUserAPI: &fakeContactUserAPI{}}

	if _, err := sender.DisplayName(context.Background(), ""); err == nil {
		t.Fatal("missing user id: want an error")
	}
}

func TestDisplayNameSurfacesARejectedResponse(t *testing.T) {
	api := &fakeContactUserAPI{resp: &larkcontact.GetUserResp{
		ApiResp: &larkcore.ApiResp{
			StatusCode: http.StatusForbidden,
			Header:     http.Header{larkcore.HttpHeaderKeyLogId: []string{"log-contact-1"}},
		},
		CodeError: larkcore.CodeError{Code: 99991672, Msg: "missing scope"},
	}}
	sender := &ChannelSender{contactUserAPI: api}

	_, err := sender.DisplayName(context.Background(), "ou_sender_1")
	var apiErr *DynamicCardAPIError
	if !errors.As(err, &apiErr) || apiErr.Code != 99991672 || apiErr.RequestID != "log-contact-1" {
		t.Fatalf("error = %v, want a classified DynamicCardAPIError", err)
	}
}

func TestDisplayNameWrapsATransportFailure(t *testing.T) {
	sender := &ChannelSender{contactUserAPI: &fakeContactUserAPI{err: errors.New("dial tcp 10.0.0.1: connection refused")}}

	if _, err := sender.DisplayName(context.Background(), "ou_sender_1"); err == nil {
		t.Fatal("want a transport error")
	}
}

func TestDisplayNameRejectsAnEmptyName(t *testing.T) {
	empty := "   "
	api := &fakeContactUserAPI{resp: &larkcontact.GetUserResp{
		Data: &larkcontact.GetUserRespData{User: &larkcontact.User{Name: &empty}},
	}}
	sender := &ChannelSender{contactUserAPI: api}

	if _, err := sender.DisplayName(context.Background(), "ou_sender_1"); err == nil {
		t.Fatal("blank name: want an error")
	}
}

func TestContactUsersRequireConfiguredClient(t *testing.T) {
	sender := &ChannelSender{}

	if _, err := sender.DisplayName(context.Background(), "ou_sender_1"); err == nil {
		t.Fatal("unconfigured sender: want an error")
	}
}

// TestDisplayNameQueriesByOpenID is the one thing the fake above cannot
// prove: GetUserReq exposes no field to inspect the path/query params it was
// built with (only an unexported *apiReq), so this drives the request
// through a real *lark.Client and a stub transport, exactly like
// reaction_add_remove_test.go's path-escaping test does.
func TestDisplayNameQueriesByOpenID(t *testing.T) {
	httpClient := &contactStubHTTPClient{
		status: http.StatusOK,
		body:   `{"code":0,"msg":"success","data":{"user":{"name":"王鑫禹"}}}`,
	}
	client := lark.NewClient("cli_contact_test", "secret", lark.WithHttpClient(httpClient))
	sender := &ChannelSender{client: client, contactUserAPI: client.Contact.V3.User}

	got, err := sender.DisplayName(context.Background(), "ou_sender_1")
	if err != nil {
		t.Fatalf("display name: %v", err)
	}
	if got != "王鑫禹" {
		t.Fatalf("name = %q, want 王鑫禹", got)
	}
	if len(httpClient.requests) != 1 {
		t.Fatalf("requests = %d, want 1: %#v", len(httpClient.requests), httpClient.requests)
	}
	req := httpClient.requests[0]
	if req.Path != "/open-apis/contact/v3/users/ou_sender_1" {
		t.Fatalf("path = %q", req.Path)
	}
	if req.Query.Get("user_id_type") != "open_id" {
		t.Fatalf("user_id_type = %q, want open_id", req.Query.Get("user_id_type"))
	}
}

type contactRecordedRequest struct {
	Path  string
	Query url.Values
}

type contactStubHTTPClient struct {
	requests []contactRecordedRequest
	status   int
	body     string
}

func (c *contactStubHTTPClient) Do(req *http.Request) (*http.Response, error) {
	if req.URL.Path == cotTestTokenPath {
		return cotStubResponse(req, http.StatusOK, cotTestTokenBody, nil), nil
	}
	if req.Body != nil {
		_, _ = io.ReadAll(req.Body)
	}
	c.requests = append(c.requests, contactRecordedRequest{Path: req.URL.Path, Query: req.URL.Query()})
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
