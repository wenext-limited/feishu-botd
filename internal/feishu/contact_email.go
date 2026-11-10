package feishu

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	larkcontact "github.com/larksuite/oapi-sdk-go/v3/service/contact/v3"
)

const (
	// senderEmailTTL bounds how long a resolved address is trusted. An
	// address changes rarely, but a person who leaves or is renamed should
	// not keep the old one for the life of the process.
	senderEmailTTL = time.Hour
	// senderEmailMissTTL bounds how long "no address" or a failed lookup is
	// remembered. Short, so granting the scope takes effect soon, and long
	// enough that an app without the scope does not call the Contact API on
	// every message.
	senderEmailMissTTL    = 10 * time.Minute
	senderEmailMaxEntries = 4096
	maxSenderEmailBytes   = 254
)

// ContactEmails resolves a sender's company email address from their open_id.
//
// The address is enterprise_email, or email when that is empty — the same
// order dial uses when a person signs in with Feishu, so the two sides name a
// person with the same string. Reading enterprise_email needs one of
// contact:user.employee:readonly, contact:contact:readonly_as_app,
// contact:contact:readonly or contact:contact:access_as_app; reading email
// needs contact:user.email:readonly. Feishu leaves out a field the app has no
// scope for rather than failing the call, so a missing scope reads as "no
// address" (empty string, nil error), which is indistinguishable from a user
// who has none.
type ContactEmails interface {
	SenderEmail(ctx context.Context, openID string) (string, error)
}

// SenderEmail looks openID up through the Contact API. It does not cache:
// SenderEmailCache does, with expiry and a bound.
func (s *ChannelSender) SenderEmail(ctx context.Context, openID string) (string, error) {
	if s == nil || s.contactUserAPI == nil {
		return "", fmt.Errorf("feishu contact lookup is not configured")
	}
	openID, err := validateRequired("user_id", openID, maxContactIdentifierLength)
	if err != nil {
		return "", err
	}
	req := larkcontact.NewGetUserReqBuilder().
		UserId(openID).
		UserIdType(larkcontact.UserIdTypeOpenId).
		Build()
	resp, callErr := s.contactUserAPI.Get(ctx, req)
	if callErr != nil {
		return "", fmt.Errorf("feishu contact lookup failed: %w", callErr)
	}
	if resp == nil {
		return "", fmt.Errorf("feishu contact lookup returned an empty response")
	}
	if !resp.Success() {
		return "", dynamicCardResponseError("contact lookup", resp.ApiResp, resp.Code, resp.Msg)
	}
	if resp.Data == nil || resp.Data.User == nil {
		return "", nil
	}
	return preferredEmail(resp.Data.User), nil
}

// preferredEmail is enterprise_email when it is set, else email. Anything
// longer than an address can be is treated as absent rather than truncated.
func preferredEmail(user *larkcontact.User) string {
	for _, candidate := range []*string{user.EnterpriseEmail, user.Email} {
		if candidate == nil {
			continue
		}
		email := strings.TrimSpace(*candidate)
		if email != "" && len(email) <= maxSenderEmailBytes {
			return email
		}
	}
	return ""
}

var _ ContactEmails = (*ChannelSender)(nil)

// SenderEmailResult is one cached answer: the address, or empty with the
// reason it is empty.
type SenderEmailResult struct {
	Email string
	// Err is the lookup failure behind an empty Email, nil when the lookup
	// succeeded (with or without an address).
	Err error
	// Cached is true when no Contact API call was made for this answer.
	Cached bool
}

type senderEmailEntry struct {
	email     string
	err       error
	cachedAt  time.Time
	expiresAt time.Time
}

// SenderEmailCache memoizes ContactEmails by open_id, bounded and expiring.
// Failures and empty answers are remembered too, for less time, so an app
// without the contact scope does not call the API on every message.
type SenderEmailCache struct {
	mu      sync.Mutex
	lookup  ContactEmails
	entries map[string]senderEmailEntry
	now     func() time.Time
}

func NewSenderEmailCache(lookup ContactEmails) *SenderEmailCache {
	return &SenderEmailCache{
		lookup:  lookup,
		entries: make(map[string]senderEmailEntry),
		now:     time.Now,
	}
}

// Resolve answers openID's address. It never fails: a failure is an empty
// address with Err set.
func (c *SenderEmailCache) Resolve(ctx context.Context, openID string) SenderEmailResult {
	openID = strings.TrimSpace(openID)
	if c == nil || c.lookup == nil || openID == "" {
		return SenderEmailResult{}
	}
	now := c.now()
	c.mu.Lock()
	if entry, ok := c.entries[openID]; ok && now.Before(entry.expiresAt) {
		c.mu.Unlock()
		return SenderEmailResult{Email: entry.email, Err: entry.err, Cached: true}
	}
	c.mu.Unlock()

	email, err := c.lookup.SenderEmail(ctx, openID)
	if err != nil {
		email = ""
	}
	ttl := senderEmailTTL
	if email == "" {
		ttl = senderEmailMissTTL
	}
	// A lookup cut short by the caller's own deadline says nothing about
	// the user; do not remember it.
	if ctx.Err() == nil {
		c.store(openID, senderEmailEntry{email: email, err: err, cachedAt: now, expiresAt: now.Add(ttl)})
	}
	return SenderEmailResult{Email: email, Err: err}
}

func (c *SenderEmailCache) store(openID string, entry senderEmailEntry) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, exists := c.entries[openID]; !exists && len(c.entries) >= senderEmailMaxEntries {
		for key, existing := range c.entries {
			if !entry.cachedAt.Before(existing.expiresAt) {
				delete(c.entries, key)
			}
		}
		if len(c.entries) >= senderEmailMaxEntries {
			oldestKey := ""
			var oldest time.Time
			for key, existing := range c.entries {
				if oldestKey == "" || existing.cachedAt.Before(oldest) {
					oldestKey = key
					oldest = existing.cachedAt
				}
			}
			delete(c.entries, oldestKey)
		}
	}
	c.entries[openID] = entry
}
