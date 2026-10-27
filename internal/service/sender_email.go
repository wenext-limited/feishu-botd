package service

import (
	"context"
	"errors"
	"time"

	"feishu-botd/internal/feishu"
)

// senderEmailLookupTimeout bounds the Contact API call on the inbound path.
// The address is optional, so a slow lookup costs the message its address,
// never its delivery.
const senderEmailLookupTimeout = 3 * time.Second

// resolveSenderEmail fills in.SenderEmail when some provider that could
// receive this message holds allow_sender_email. It never fails the message:
// any failure leaves the address empty. Failures are logged once until the
// next successful lookup, and never with the address or the sender's id.
func (s *Service) resolveSenderEmail(ctx context.Context, in *CommandInput, replyProvider string) {
	if in.SenderID == "" {
		return
	}
	if replyProvider != "" {
		if !s.cfg.ProviderAllowsSenderEmail(replyProvider) {
			return
		}
	} else if !s.cfg.AnyProviderAllowsSenderEmail(in.AppAlias) {
		return
	}
	backend, ok := s.backendForApp(in.AppAlias)
	if !ok || backend.senderEmails == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	lookupCtx, cancel := context.WithTimeout(ctx, senderEmailLookupTimeout)
	defer cancel()
	result := backend.senderEmails.Resolve(lookupCtx, in.SenderID)
	if result.Email != "" {
		in.SenderEmail = result.Email
		backend.senderEmailWarned.Store(false)
		return
	}
	if result.Cached || !backend.senderEmailWarned.CompareAndSwap(false, true) {
		return
	}
	if result.Err != nil {
		attrs := []any{
			"app", effectiveAppAlias(in.AppAlias),
			"error_class", senderEmailErrorClass(result.Err),
		}
		var apiErr *feishu.DynamicCardAPIError
		if errors.As(result.Err, &apiErr) {
			attrs = append(attrs, "http_status", apiErr.HTTPStatus, "code", apiErr.Code, "request_id", apiErr.RequestID)
		}
		s.logger.Warn("sender email lookup failed; providers receive an empty sender_email", attrs...)
		return
	}
	s.logger.Warn("sender email lookup returned no address; check the app's contact scopes "+
		"(contact:user.employee:readonly for enterprise_email, contact:user.email:readonly for email) "+
		"and its contact visibility range",
		"app", effectiveAppAlias(in.AppAlias),
	)
}

func senderEmailErrorClass(err error) string {
	var apiErr *feishu.DynamicCardAPIError
	switch {
	case errors.Is(err, context.Canceled):
		return "context_canceled"
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline_exceeded"
	case errors.As(err, &apiErr):
		return "api_error"
	default:
		return "transport"
	}
}
