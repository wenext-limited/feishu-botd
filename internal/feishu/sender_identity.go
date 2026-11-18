package feishu

import (
	larkim "github.com/larksuite/oapi-sdk-go/v3/service/im/v1"
	"strings"
	"unicode"
)

// SenderIdentity is private ingress evidence from the authenticated event and
// configured receiving app. It never comes from mentions or command metadata.
type SenderIdentity struct {
	AppID      string
	TenantKey  string
	SenderType string
	UnionID    string
	OpenID     string
	UserID     string
}

func (*SenderIdentity) String() string   { return "SenderIdentity(<redacted>)" }
func (*SenderIdentity) GoString() string { return "SenderIdentity(<redacted>)" }

func identityComponent(s string) bool {
	return s != "" && len(s) <= 320 && strings.IndexFunc(s, func(r rune) bool { return unicode.IsControl(r) || unicode.IsSpace(r) }) < 0
}

// ValidFor also binds the unchanged opaque routing identity and receiving app.
func (s *SenderIdentity) ValidFor(appID, routingID string) bool {
	if s == nil || s.SenderType != "user" || s.AppID != appID || !identityComponent(s.AppID) || !identityComponent(s.TenantKey) {
		return false
	}
	for _, id := range []string{s.UnionID, s.OpenID, s.UserID} {
		if id != "" && !identityComponent(id) {
			return false
		}
	}
	if s.UnionID != "" && (!strings.HasPrefix(s.UnionID, "on_") || len(s.UnionID) <= 3) {
		return false
	}
	if s.OpenID != "" && (!strings.HasPrefix(s.OpenID, "ou_") || len(s.OpenID) <= 3) {
		return false
	}
	return routingID != "" && (routingID == s.OpenID || routingID == s.UserID || routingID == s.UnionID)
}

func senderIdentity(appID string, sender *larkim.EventSender) *SenderIdentity {
	if sender == nil || sender.SenderId == nil {
		return nil
	}
	identity := &SenderIdentity{AppID: appID, TenantKey: deref(sender.TenantKey), SenderType: deref(sender.SenderType),
		UnionID: deref(sender.SenderId.UnionId), OpenID: deref(sender.SenderId.OpenId), UserID: deref(sender.SenderId.UserId)}
	if !identity.ValidFor(appID, senderID(sender)) {
		return nil
	}
	return identity
}
