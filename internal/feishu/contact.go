package feishu

import (
	"context"
	"fmt"
	"strings"

	larkcore "github.com/larksuite/oapi-sdk-go/v3/core"
	larkcontact "github.com/larksuite/oapi-sdk-go/v3/service/contact/v3"
)

const maxContactIdentifierLength = 128

// ContactUsers resolves a sender's Feishu display name from their stable
// identity. It exists because Feishu's message-receive event carries only an
// id (open_id, almost always — see senderID in receiver.go), never a name, so
// daemon-driven behavior that needs to recognize a specific person by name
// (e.g. AgentProviderConfig.WorkingReactionOverrides) has no other source for
// one. Requires the contact:user.base:readonly scope granted to the app in
// the Feishu admin console; an app without that scope gets a rejected
// response here, which callers treat as "no match" rather than a hard error.
type ContactUsers interface {
	DisplayName(ctx context.Context, userID string) (string, error)
}

type contactUserAPI interface {
	Get(context.Context, *larkcontact.GetUserReq, ...larkcore.RequestOptionFunc) (*larkcontact.GetUserResp, error)
}

// DisplayName resolves userID (an open_id) to this tenant's display name for
// that user, caching successful lookups for the process lifetime. A display
// name is effectively permanent compared to how often the same small set of
// senders re-triggers this call, so an unbounded, un-expiring cache is the
// right tradeoff here rather than one more moving part.
func (s *ChannelSender) DisplayName(ctx context.Context, userID string) (string, error) {
	if s == nil || s.contactUserAPI == nil {
		return "", fmt.Errorf("feishu contact lookup is not configured")
	}
	userID, err := validateRequired("user_id", userID, maxContactIdentifierLength)
	if err != nil {
		return "", err
	}
	if cached, ok := s.contactNameCache.Load(userID); ok {
		return cached.(string), nil
	}

	req := larkcontact.NewGetUserReqBuilder().
		UserId(userID).
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
	if resp.Data == nil || resp.Data.User == nil || resp.Data.User.Name == nil {
		return "", fmt.Errorf("feishu contact lookup returned no name")
	}
	name := strings.TrimSpace(*resp.Data.User.Name)
	if name == "" {
		return "", fmt.Errorf("feishu contact lookup returned an empty name")
	}
	s.contactNameCache.Store(userID, name)
	return name, nil
}
