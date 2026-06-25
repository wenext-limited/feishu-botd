package notify

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/url"
	"strings"
)

type Request struct {
	Source        string            `json:"source"`
	SourceEventID string            `json:"source_event_id"`
	DedupeKey     string            `json:"dedupe_key"`
	Severity      string            `json:"severity"`
	Title         string            `json:"title"`
	Markdown      string            `json:"markdown"`
	Target        Target            `json:"target"`
	Links         []Link            `json:"links"`
	Metadata      map[string]string `json:"metadata"`
}

type Target struct {
	Channel string `json:"channel"`
}

type Link struct {
	Label string `json:"label"`
	URL   string `json:"url"`
}

type Response struct {
	Status    string `json:"status"`
	Provider  string `json:"provider"`
	DedupeKey string `json:"dedupe_key"`
	MessageID string `json:"message_id"`
	Duplicate bool   `json:"duplicate"`
}

type ErrorResponse struct {
	Error ErrorBody `json:"error"`
}

type ErrorBody struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	Retryable bool   `json:"retryable"`
	RequestID string `json:"request_id"`
}

func (r Request) Fingerprint() string {
	clone := r
	body, _ := json.Marshal(clone)
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}

func (r Request) Validate(channels map[string]string) *APIError {
	if strings.TrimSpace(r.Source) == "" {
		return BadRequest("missing_source", "source is required")
	}
	if strings.TrimSpace(r.SourceEventID) == "" {
		return BadRequest("missing_source_event_id", "source_event_id is required")
	}
	if strings.TrimSpace(r.DedupeKey) == "" {
		return BadRequest("missing_dedupe_key", "dedupe_key is required")
	}
	if strings.TrimSpace(r.Title) == "" {
		return BadRequest("missing_title", "title is required")
	}
	if strings.TrimSpace(r.Markdown) == "" {
		return BadRequest("missing_markdown", "markdown is required")
	}
	if r.Severity != "info" && r.Severity != "warning" && r.Severity != "critical" {
		return BadRequest("invalid_severity", "severity must be info, warning, or critical")
	}
	if strings.TrimSpace(r.Target.Channel) == "" {
		return BadRequest("missing_channel", "target.channel is required")
	}
	if _, ok := channels[r.Target.Channel]; !ok {
		return NewAPIError(404, "unknown_channel", "unknown channel", false)
	}
	if len(r.Source) > 64 || len(r.SourceEventID) > 160 || len(r.DedupeKey) > 240 || len(r.Title) > 200 || len(r.Markdown) > 8000 {
		return BadRequest("field_too_large", "one or more fields are too large")
	}
	if len(r.Links) > 8 || len(r.Metadata) > 32 {
		return BadRequest("too_many_items", "too many links or metadata entries")
	}
	for _, link := range r.Links {
		if len(link.Label) > 80 || len(link.URL) > 500 {
			return BadRequest("field_too_large", "link field is too large")
		}
		u, err := url.Parse(link.URL)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			return BadRequest("invalid_link_url", "link url must be http or https")
		}
	}
	for k, v := range r.Metadata {
		if len(k) > 80 || len(v) > 500 {
			return BadRequest("field_too_large", "metadata field is too large")
		}
	}
	return nil
}

type APIError struct {
	Status    int
	Code      string
	Message   string
	Retryable bool
}

func NewAPIError(status int, code, message string, retryable bool) *APIError {
	return &APIError{Status: status, Code: code, Message: message, Retryable: retryable}
}

func BadRequest(code, message string) *APIError {
	return NewAPIError(400, code, message, false)
}

func NotImplemented(code, message string) *APIError {
	return NewAPIError(501, code, message, false)
}

func (e *APIError) Error() string { return e.Code + ": " + e.Message }
