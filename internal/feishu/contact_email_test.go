package feishu

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"

	larkcore "github.com/larksuite/oapi-sdk-go/v3/core"
	larkcontact "github.com/larksuite/oapi-sdk-go/v3/service/contact/v3"
)

func contactEmailResp(enterprise, personal *string) *larkcontact.GetUserResp {
	return &larkcontact.GetUserResp{
		Data: &larkcontact.GetUserRespData{User: &larkcontact.User{EnterpriseEmail: enterprise, Email: personal}},
	}
}

// The order matches dial's own (enterprise_email, then email), so the
// address botd hands over is the one dial signed the person in with.
func TestSenderEmailPrefersEnterpriseEmail(t *testing.T) {
	for _, testCase := range []struct {
		name       string
		enterprise *string
		personal   *string
		want       string
	}{
		{name: "enterprise first", enterprise: ptr("lisi@zhenxinmail.com"), personal: ptr("lisi@gmail.com"), want: "lisi@zhenxinmail.com"},
		{name: "email when enterprise is empty", enterprise: ptr(""), personal: ptr("lisi@zhenxinmail.com"), want: "lisi@zhenxinmail.com"},
		{name: "email when enterprise is absent", personal: ptr(" lisi@zhenxinmail.com "), want: "lisi@zhenxinmail.com"},
		{name: "neither (or no scope for either)", want: ""},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			api := &fakeContactUserAPI{resp: contactEmailResp(testCase.enterprise, testCase.personal)}
			sender := &ChannelSender{contactUserAPI: api}
			got, err := sender.SenderEmail(context.Background(), "ou_sender_1")
			if err != nil {
				t.Fatalf("sender email: %v", err)
			}
			if got != testCase.want {
				t.Fatalf("email = %q, want %q", got, testCase.want)
			}
			if api.calls != 1 {
				t.Fatalf("calls = %d, want 1", api.calls)
			}
		})
	}
}

func TestSenderEmailReportsAFailedLookup(t *testing.T) {
	sender := &ChannelSender{contactUserAPI: &fakeContactUserAPI{err: errors.New("connection refused")}}
	if _, err := sender.SenderEmail(context.Background(), "ou_sender_1"); err == nil {
		t.Fatal("a transport failure must be reported")
	}
	denied := &larkcontact.GetUserResp{
		ApiResp:   &larkcore.ApiResp{StatusCode: http.StatusBadRequest},
		CodeError: larkcore.CodeError{Code: 41050, Msg: "no user authority"},
	}
	sender = &ChannelSender{contactUserAPI: &fakeContactUserAPI{resp: denied}}
	_, err := sender.SenderEmail(context.Background(), "ou_sender_1")
	var apiErr *DynamicCardAPIError
	if !errors.As(err, &apiErr) || apiErr.Code != 41050 {
		t.Fatalf("err = %v, want a Feishu API error with its code", err)
	}
}

type fakeContactEmails struct {
	emails map[string]string
	err    error
	calls  int
}

func (f *fakeContactEmails) SenderEmail(_ context.Context, openID string) (string, error) {
	f.calls++
	if f.err != nil {
		return "", f.err
	}
	return f.emails[openID], nil
}

func TestSenderEmailCacheRemembersAndExpires(t *testing.T) {
	lookup := &fakeContactEmails{emails: map[string]string{"ou_1": "a@zhenxinmail.com"}}
	cache := NewSenderEmailCache(lookup)
	now := time.Unix(1_000_000, 0)
	cache.now = func() time.Time { return now }

	first := cache.Resolve(context.Background(), "ou_1")
	second := cache.Resolve(context.Background(), "ou_1")
	if first.Email != "a@zhenxinmail.com" || second.Email != "a@zhenxinmail.com" || first.Cached || !second.Cached {
		t.Fatalf("first=%#v second=%#v", first, second)
	}
	if lookup.calls != 1 {
		t.Fatalf("calls = %d, want 1", lookup.calls)
	}
	now = now.Add(senderEmailTTL + time.Second)
	cache.Resolve(context.Background(), "ou_1")
	if lookup.calls != 2 {
		t.Fatalf("calls after expiry = %d, want 2", lookup.calls)
	}
}

// A missing scope looks like "no address". Remembering that briefly keeps an
// app without the scope from calling the API on every message, and letting
// it lapse means granting the scope takes effect without a restart.
func TestSenderEmailCacheRemembersMissesBriefly(t *testing.T) {
	lookup := &fakeContactEmails{err: errors.New("boom")}
	cache := NewSenderEmailCache(lookup)
	now := time.Unix(1_000_000, 0)
	cache.now = func() time.Time { return now }

	failed := cache.Resolve(context.Background(), "ou_1")
	if failed.Email != "" || failed.Err == nil || failed.Cached {
		t.Fatalf("failed = %#v", failed)
	}
	again := cache.Resolve(context.Background(), "ou_1")
	if again.Email != "" || again.Err == nil || !again.Cached || lookup.calls != 1 {
		t.Fatalf("again = %#v calls=%d", again, lookup.calls)
	}
	lookup.err = nil
	lookup.emails = map[string]string{"ou_1": "a@zhenxinmail.com"}
	now = now.Add(senderEmailMissTTL + time.Second)
	if got := cache.Resolve(context.Background(), "ou_1"); got.Email != "a@zhenxinmail.com" {
		t.Fatalf("after the miss lapsed = %#v", got)
	}
}

func TestSenderEmailCacheKeepsNoAnswerCutShortByTheCaller(t *testing.T) {
	lookup := &fakeContactEmails{err: context.DeadlineExceeded}
	cache := NewSenderEmailCache(lookup)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	cache.Resolve(ctx, "ou_1")
	cache.Resolve(context.Background(), "ou_1")
	if lookup.calls != 2 {
		t.Fatalf("calls = %d, want 2: a cancelled lookup must not be remembered", lookup.calls)
	}
}

func TestSenderEmailCacheIsBounded(t *testing.T) {
	lookup := &fakeContactEmails{emails: map[string]string{}}
	cache := NewSenderEmailCache(lookup)
	now := time.Unix(1_000_000, 0)
	cache.now = func() time.Time { now = now.Add(time.Millisecond); return now }
	for i := 0; i < senderEmailMaxEntries+10; i++ {
		id := fmt.Sprintf("ou_%d", i)
		lookup.emails[id] = id + "@zhenxinmail.com"
		cache.Resolve(context.Background(), id)
	}
	if got := len(cache.entries); got > senderEmailMaxEntries {
		t.Fatalf("entries = %d, want at most %d", got, senderEmailMaxEntries)
	}
	if _, kept := cache.entries["ou_0"]; kept {
		t.Fatal("the oldest entry should have been evicted")
	}
}

func TestSenderEmailCacheIgnoresAnEmptySender(t *testing.T) {
	lookup := &fakeContactEmails{}
	if got := NewSenderEmailCache(lookup).Resolve(context.Background(), "  "); got.Email != "" || lookup.calls != 0 {
		t.Fatalf("got %#v calls=%d", got, lookup.calls)
	}
}
