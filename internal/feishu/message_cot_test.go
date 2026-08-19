package feishu

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	lark "github.com/larksuite/oapi-sdk-go/v3"
	larkcore "github.com/larksuite/oapi-sdk-go/v3/core"
)

const (
	cotTestTokenPath = "/open-apis/auth/v3/tenant_access_token/internal"
	cotTestTokenBody = `{"code":0,"msg":"success","tenant_access_token":"t-test","expire":7200}`
	cotTestOKBody    = `{"code":0,"msg":"success"}`
)

// cotTestEventTime is a fixed instant so timestamp encoding is asserted against
// a value the test controls rather than the wall clock.
var cotTestEventTime = time.Date(2026, 8, 19, 10, 30, 0, 0, time.UTC)

type cotRecordedRequest struct {
	Method string
	// EscapedPath keeps the wire form of the path so path escaping is provable;
	// url.URL.Path would hand back the decoded value.
	EscapedPath string
	RawQuery    string
	Body        []byte
}

// cotStubHTTPClient serves the tenant token endpoint unconditionally and
// records every message_cot request, so tests assert the exact hand-built wire
// request the adapter produces.
type cotStubHTTPClient struct {
	requests []cotRecordedRequest

	status       int
	body         string
	bodies       []string
	contentType  string
	header       http.Header
	transportErr error
}

func (c *cotStubHTTPClient) Do(req *http.Request) (*http.Response, error) {
	if req.URL.Path == cotTestTokenPath {
		return cotStubResponse(req, http.StatusOK, cotTestTokenBody, nil), nil
	}
	body, err := io.ReadAll(req.Body)
	if err != nil {
		return nil, err
	}
	c.requests = append(c.requests, cotRecordedRequest{
		Method:      req.Method,
		EscapedPath: req.URL.EscapedPath(),
		RawQuery:    req.URL.RawQuery,
		Body:        body,
	})
	if c.transportErr != nil {
		return nil, c.transportErr
	}
	response := cotStubResponse(req, c.responseStatus(), c.responseBody(len(c.requests)-1), c.header)
	if c.contentType != "" {
		response.Header.Set("Content-Type", c.contentType)
	}
	return response, nil
}

func (c *cotStubHTTPClient) responseStatus() int {
	if c.status == 0 {
		return http.StatusOK
	}
	return c.status
}

func (c *cotStubHTTPClient) responseBody(index int) string {
	if len(c.bodies) > 0 {
		if index >= len(c.bodies) {
			index = len(c.bodies) - 1
		}
		return c.bodies[index]
	}
	if c.body == "" {
		return cotTestOKBody
	}
	return c.body
}

func cotStubResponse(req *http.Request, status int, body string, header http.Header) *http.Response {
	responseHeader := http.Header{"Content-Type": []string{"application/json"}}
	for key, values := range header {
		for _, value := range values {
			responseHeader.Add(key, value)
		}
	}
	return &http.Response{
		StatusCode: status,
		Header:     responseHeader,
		Body:       io.NopCloser(strings.NewReader(body)),
		Request:    req,
	}
}

func newCoTTestSender(httpClient *cotStubHTTPClient) *ChannelSender {
	return &ChannelSender{
		client: lark.NewClient("cli_cot_test", "secret", lark.WithHttpClient(httpClient)),
	}
}

func decodeCoTBody(t *testing.T, raw []byte) map[string]any {
	t.Helper()
	var body map[string]any
	if err := json.Unmarshal(raw, &body); err != nil {
		t.Fatalf("decode request body %q: %v", raw, err)
	}
	return body
}

func cotTestRunStarted(t *testing.T, threadID, runID string) CoTEvent {
	t.Helper()
	event, err := NewCoTRunStartedEvent(threadID, runID, cotTestEventTime)
	if err != nil {
		t.Fatalf("build RUN_STARTED event: %v", err)
	}
	return event
}

func cotTestRunFinished(t *testing.T, threadID, runID string, outcome CoTOutcome) CoTEvent {
	t.Helper()
	event, err := NewCoTRunFinishedEvent(threadID, runID, outcome, cotTestEventTime)
	if err != nil {
		t.Fatalf("build RUN_FINISHED event: %v", err)
	}
	return event
}

func cotTestStepStarted(t *testing.T, stepID, stepName string) CoTEvent {
	t.Helper()
	event, err := NewCoTStepStartedEvent(stepID, stepName, cotTestEventTime)
	if err != nil {
		t.Fatalf("build STEP_STARTED event: %v", err)
	}
	return event
}

func cotTestStepFinished(t *testing.T, stepID, stepName string) CoTEvent {
	t.Helper()
	event, err := NewCoTStepFinishedEvent(stepID, stepName, cotTestEventTime)
	if err != nil {
		t.Fatalf("build STEP_FINISHED event: %v", err)
	}
	return event
}

func TestCoTMessagesCreateSendsBoundOriginMessageRequest(t *testing.T) {
	httpClient := &cotStubHTTPClient{
		body: `{"code":0,"msg":"success","data":{"cot_id":"cot_7355","message_id":"om_cot"}}`,
	}
	sender := newCoTTestSender(httpClient)

	cotID, messageID, err := sender.Create(context.Background(), CoTCreateRequest{
		ChatID:          "oc_test",
		OriginMessageID: "om_trigger",
	})
	if err != nil {
		t.Fatalf("create CoT: %v", err)
	}
	if cotID != "cot_7355" || messageID != "om_cot" {
		t.Fatalf("identifiers = %q/%q, want cot_7355/om_cot", cotID, messageID)
	}
	if len(httpClient.requests) != 1 {
		t.Fatalf("request count = %d, want 1", len(httpClient.requests))
	}
	got := httpClient.requests[0]
	if got.Method != http.MethodPost || got.EscapedPath != "/open-apis/im/v1/message_cot" {
		t.Fatalf("request = %s %s, want POST /open-apis/im/v1/message_cot", got.Method, got.EscapedPath)
	}
	if got.RawQuery != "receive_id_type=chat_id" {
		t.Fatalf("query = %q, want receive_id_type=chat_id", got.RawQuery)
	}
	body := decodeCoTBody(t, got.Body)
	if len(body) != 2 || body["receive_id"] != "oc_test" || body["origin_message_id"] != "om_trigger" {
		t.Fatalf("request body = %#v", body)
	}
}

func TestCoTMessagesCreateRequiresChatAndOriginMessage(t *testing.T) {
	sender := newCoTTestSender(&cotStubHTTPClient{})
	cases := map[string]CoTCreateRequest{
		"missing chat id":        {OriginMessageID: "om_trigger"},
		"missing origin message": {ChatID: "oc_test"},
		"blank chat id":          {ChatID: "   ", OriginMessageID: "om_trigger"},
	}
	for name, request := range cases {
		t.Run(name, func(t *testing.T) {
			if _, _, err := sender.Create(context.Background(), request); err == nil {
				t.Fatal("expected boundary validation error")
			}
		})
	}
}

func TestCoTMessagesCreateRejectsResponseWithoutIdentifiers(t *testing.T) {
	cases := map[string]cotStubHTTPClient{
		"no data":       {body: `{"code":0,"msg":"success"}`},
		"no cot id":     {body: `{"code":0,"msg":"success","data":{"message_id":"om_cot"}}`},
		"no message id": {body: `{"code":0,"msg":"success","data":{"cot_id":"cot_7355"}}`},
		"not json":      {body: `<html>gateway</html>`, contentType: "text/html"},
	}
	for name, stub := range cases {
		t.Run(name, func(t *testing.T) {
			sender := newCoTTestSender(&stub)
			_, _, err := sender.Create(context.Background(), CoTCreateRequest{
				ChatID:          "oc_test",
				OriginMessageID: "om_trigger",
			})
			var apiErr *CoTAPIError
			if !errors.As(err, &apiErr) || apiErr.Class != "invalid_response" {
				t.Fatalf("error = %v, want invalid_response CoTAPIError", err)
			}
		})
	}
}

func TestCoTMessagesAppendEventsSendsProvenEventStream(t *testing.T) {
	httpClient := &cotStubHTTPClient{}
	sender := newCoTTestSender(httpClient)

	events := []CoTEvent{
		cotTestRunStarted(t, "thread-1", "run-1"),
		cotTestStepStarted(t, "step-1", "Reading files"),
	}
	if err := sender.AppendEvents(context.Background(), CoTAppendRequest{
		CoTID:     "cot_7355",
		MessageID: "om_cot",
		Events:    events,
	}); err != nil {
		t.Fatalf("append events: %v", err)
	}
	if len(httpClient.requests) != 1 {
		t.Fatalf("request count = %d, want 1", len(httpClient.requests))
	}
	got := httpClient.requests[0]
	if got.Method != http.MethodPut || got.EscapedPath != "/open-apis/im/v1/message_cot" {
		t.Fatalf("request = %s %s, want PUT /open-apis/im/v1/message_cot", got.Method, got.EscapedPath)
	}
	if got.RawQuery != "" {
		t.Fatalf("query = %q, want no query parameters", got.RawQuery)
	}
	body := decodeCoTBody(t, got.Body)
	if body["cot_id"] != "cot_7355" || body["message_id"] != "om_cot" {
		t.Fatalf("request body = %#v", body)
	}
	wire, ok := body["events"].([]any)
	if !ok || len(wire) != 2 {
		t.Fatalf("events = %#v, want two entries", body["events"])
	}
	first, ok := wire[0].(map[string]any)
	if !ok {
		t.Fatalf("event = %#v, want object", wire[0])
	}
	if first["event_type"] != string(CoTEventRunStarted) {
		t.Fatalf("event_type = %#v, want %s", first["event_type"], CoTEventRunStarted)
	}
	// content must travel as a marshalled JSON string, not a nested object.
	content, ok := first["content"].(string)
	if !ok {
		t.Fatalf("content = %#v, want JSON string", first["content"])
	}
	if content != `{"threadId":"thread-1","runId":"run-1"}` {
		t.Fatalf("content = %q", content)
	}
}

func TestCoTMessagesAppendEventsSendsIntegerMillisecondTimestamps(t *testing.T) {
	httpClient := &cotStubHTTPClient{}
	sender := newCoTTestSender(httpClient)

	if err := sender.AppendEvents(context.Background(), CoTAppendRequest{
		CoTID:     "cot_7355",
		MessageID: "om_cot",
		Events:    []CoTEvent{cotTestRunStarted(t, "thread-1", "run-1")},
	}); err != nil {
		t.Fatalf("append events: %v", err)
	}
	body := decodeCoTBody(t, httpClient.requests[0].Body)
	event := body["events"].([]any)[0].(map[string]any)
	// A JSON number decodes into float64; a string would decode into string.
	timestamp, ok := event["timestamp"].(float64)
	if !ok {
		t.Fatalf("timestamp = %#v, want a JSON number", event["timestamp"])
	}
	if int64(timestamp) != cotTestEventTime.UnixMilli() {
		t.Fatalf("timestamp = %d, want %d", int64(timestamp), cotTestEventTime.UnixMilli())
	}
}

func TestCoTEventConstructorsProduceProvenContentShapes(t *testing.T) {
	cases := []struct {
		name        string
		event       CoTEvent
		err         error
		wantType    CoTEventType
		wantContent string
	}{
		{
			name:        "run started",
			wantType:    CoTEventRunStarted,
			wantContent: `{"threadId":"thread-1","runId":"run-1"}`,
		},
		{
			name:        "step started",
			wantType:    CoTEventStepStarted,
			wantContent: `{"stepId":"step-1","stepName":"Reading files"}`,
		},
		{
			name:        "step finished",
			wantType:    CoTEventStepFinished,
			wantContent: `{"stepId":"step-1","stepName":"Reading files"}`,
		},
		{
			name:        "run finished",
			wantType:    CoTEventRunFinished,
			wantContent: `{"threadId":"thread-1","runId":"run-1","status":"done"}`,
		},
	}
	cases[0].event, cases[0].err = NewCoTRunStartedEvent("thread-1", "run-1", cotTestEventTime)
	cases[1].event, cases[1].err = NewCoTStepStartedEvent("step-1", "Reading files", cotTestEventTime)
	cases[2].event, cases[2].err = NewCoTStepFinishedEvent("step-1", "Reading files", cotTestEventTime)
	cases[3].event, cases[3].err = NewCoTRunFinishedEvent("thread-1", "run-1", CoTOutcomeDone, cotTestEventTime)

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			if testCase.err != nil {
				t.Fatalf("build event: %v", testCase.err)
			}
			if testCase.event.Type != testCase.wantType {
				t.Fatalf("type = %q, want %q", testCase.event.Type, testCase.wantType)
			}
			if testCase.event.Content != testCase.wantContent {
				t.Fatalf("content = %q, want %q", testCase.event.Content, testCase.wantContent)
			}
			if !testCase.event.At.Equal(cotTestEventTime) {
				t.Fatalf("event time = %v, want %v", testCase.event.At, cotTestEventTime)
			}
		})
	}
}

func TestCoTRunFinishedEventCarriesErrorOutcome(t *testing.T) {
	event, err := NewCoTRunFinishedEvent("thread-1", "run-1", CoTOutcomeError, cotTestEventTime)
	if err != nil {
		t.Fatalf("build run finished event: %v", err)
	}
	if event.Content != `{"threadId":"thread-1","runId":"run-1","status":"error"}` {
		t.Fatalf("content = %q", event.Content)
	}
}

func TestCoTEventConstructorsRejectMissingFields(t *testing.T) {
	cases := map[string]func() (CoTEvent, error){
		"run started without thread": func() (CoTEvent, error) {
			return NewCoTRunStartedEvent("", "run-1", cotTestEventTime)
		},
		"run started without run": func() (CoTEvent, error) {
			return NewCoTRunStartedEvent("thread-1", "", cotTestEventTime)
		},
		"step started without id": func() (CoTEvent, error) {
			return NewCoTStepStartedEvent("", "Reading files", cotTestEventTime)
		},
		"step finished without name": func() (CoTEvent, error) {
			return NewCoTStepFinishedEvent("step-1", "", cotTestEventTime)
		},
		"run finished with unknown outcome": func() (CoTEvent, error) {
			return NewCoTRunFinishedEvent("thread-1", "run-1", CoTOutcome("cancelled"), cotTestEventTime)
		},
		"event without time": func() (CoTEvent, error) {
			return NewCoTRunStartedEvent("thread-1", "run-1", time.Time{})
		},
	}
	for name, build := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := build(); err == nil {
				t.Fatal("expected boundary validation error")
			}
		})
	}
}

func TestCoTMessagesAppendEventsValidatesEventBoundary(t *testing.T) {
	valid := cotTestRunStarted(t, "thread-1", "run-1")
	cases := map[string]CoTAppendRequest{
		"missing cot id":     {MessageID: "om_cot", Events: []CoTEvent{valid}},
		"missing message id": {CoTID: "cot_7355", Events: []CoTEvent{valid}},
		"no events":          {CoTID: "cot_7355", MessageID: "om_cot"},
		"untyped event": {CoTID: "cot_7355", MessageID: "om_cot", Events: []CoTEvent{
			{Content: `{"threadId":"thread-1"}`, At: cotTestEventTime},
		}},
		"non object content": {CoTID: "cot_7355", MessageID: "om_cot", Events: []CoTEvent{
			{Type: CoTEventRunStarted, Content: `["thread-1"]`, At: cotTestEventTime},
		}},
		"event without time": {CoTID: "cot_7355", MessageID: "om_cot", Events: []CoTEvent{
			{Type: CoTEventRunStarted, Content: `{"threadId":"thread-1"}`},
		}},
	}
	for name, request := range cases {
		t.Run(name, func(t *testing.T) {
			httpClient := &cotStubHTTPClient{}
			sender := newCoTTestSender(httpClient)
			if err := sender.AppendEvents(context.Background(), request); err == nil {
				t.Fatal("expected boundary validation error")
			}
			if len(httpClient.requests) != 0 {
				t.Fatalf("rejected request reached the API: %#v", httpClient.requests)
			}
		})
	}
}

func TestCoTMessagesAppendEventsDoesNotMutateCallerEvents(t *testing.T) {
	sender := newCoTTestSender(&cotStubHTTPClient{})
	events := []CoTEvent{cotTestStepStarted(t, "step-1", "Reading files")}
	before := events[0]

	if err := sender.AppendEvents(context.Background(), CoTAppendRequest{
		CoTID:     "cot_7355",
		MessageID: "om_cot",
		Events:    events,
	}); err != nil {
		t.Fatalf("append events: %v", err)
	}
	if events[0] != before {
		t.Fatalf("caller event mutated: %#v, want %#v", events[0], before)
	}
}

func TestCoTMessagesCompleteEscapesPathAndQueryValues(t *testing.T) {
	httpClient := &cotStubHTTPClient{}
	sender := newCoTTestSender(httpClient)

	if err := sender.Complete(context.Background(), CoTCompleteRequest{
		CoTID:     "cot/7355 a",
		MessageID: "om&cot=1",
		Reason:    CoTOutcomeDone,
	}); err != nil {
		t.Fatalf("complete CoT: %v", err)
	}
	if len(httpClient.requests) != 1 {
		t.Fatalf("request count = %d, want 1", len(httpClient.requests))
	}
	got := httpClient.requests[0]
	if got.Method != http.MethodPost {
		t.Fatalf("method = %s, want POST", got.Method)
	}
	wantPath := "/open-apis/im/v1/message_cot/complete/cot%2F7355%20a"
	if got.EscapedPath != wantPath {
		t.Fatalf("path = %q, want %q", got.EscapedPath, wantPath)
	}
	if got.RawQuery != "message_id=om%26cot%3D1&reason=done" {
		t.Fatalf("query = %q", got.RawQuery)
	}
	// Completion carries everything in the path and query, so no body is sent.
	if len(got.Body) != 0 {
		t.Fatalf("body = %q, want no request body", got.Body)
	}
}

func TestCoTMessagesCompleteRejectsUnknownReason(t *testing.T) {
	httpClient := &cotStubHTTPClient{}
	sender := newCoTTestSender(httpClient)
	cases := map[string]CoTOutcome{
		"empty":   "",
		"unknown": "cancelled",
	}
	for name, reason := range cases {
		t.Run(name, func(t *testing.T) {
			err := sender.Complete(context.Background(), CoTCompleteRequest{
				CoTID:     "cot_7355",
				MessageID: "om_cot",
				Reason:    reason,
			})
			if err == nil {
				t.Fatal("expected reason validation error")
			}
			if len(httpClient.requests) != 0 {
				t.Fatalf("rejected request reached the API: %#v", httpClient.requests)
			}
		})
	}
}

func TestCoTMessagesCompleteTreatsAlreadyTerminalAsSuccess(t *testing.T) {
	httpClient := &cotStubHTTPClient{
		status: http.StatusBadRequest,
		body:   `{"code":232035,"msg":"cot message is already in terminal status"}`,
	}
	sender := newCoTTestSender(httpClient)

	if err := sender.Complete(context.Background(), CoTCompleteRequest{
		CoTID:     "cot_7355",
		MessageID: "om_cot",
		Reason:    CoTOutcomeError,
	}); err != nil {
		t.Fatalf("complete on an already terminal message = %v, want success", err)
	}
}

func TestCoTMessagesClassifyTerminalStatusAsSentinel(t *testing.T) {
	httpClient := &cotStubHTTPClient{
		status: http.StatusBadRequest,
		body:   `{"code":232035,"msg":"COT message is ALREADY IN TERMINAL STATUS"}`,
	}
	sender := newCoTTestSender(httpClient)

	err := sender.AppendEvents(context.Background(), CoTAppendRequest{
		CoTID:     "cot_7355",
		MessageID: "om_cot",
		Events:    []CoTEvent{cotTestRunStarted(t, "thread-1", "run-1")},
	})
	if !errors.Is(err, ErrCoTAlreadyTerminal) {
		t.Fatalf("error = %v, want ErrCoTAlreadyTerminal", err)
	}
	var apiErr *CoTAPIError
	if !errors.As(err, &apiErr) || apiErr.Class != "already_terminal" {
		t.Fatalf("error = %v, want already_terminal CoTAPIError", err)
	}
}

func TestCoTMessagesRedactsRejectedResponses(t *testing.T) {
	secret := "om_private_user_content"
	httpClient := &cotStubHTTPClient{
		status: http.StatusBadRequest,
		body:   `{"code":232001,"msg":"invalid origin_message_id ` + secret + `"}`,
		header: http.Header{larkcore.HttpHeaderKeyLogId: []string{"log-cot-1"}},
	}
	sender := newCoTTestSender(httpClient)

	_, _, err := sender.Create(context.Background(), CoTCreateRequest{
		ChatID:          "oc_test",
		OriginMessageID: "om_trigger",
	})
	var apiErr *CoTAPIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("error = %v, want CoTAPIError", err)
	}
	if apiErr.Code != 232001 || apiErr.HTTPStatus != http.StatusBadRequest || apiErr.RequestID != "log-cot-1" {
		t.Fatalf("unexpected API error: %#v", apiErr)
	}
	if apiErr.Class != "api_rejected" {
		t.Fatalf("class = %q, want api_rejected", apiErr.Class)
	}
	if strings.Contains(err.Error(), secret) {
		t.Fatalf("error text leaked the Feishu response message: %q", err.Error())
	}
}

func TestCoTMessagesPreservesContextCancellation(t *testing.T) {
	httpClient := &cotStubHTTPClient{transportErr: context.Canceled}
	sender := newCoTTestSender(httpClient)

	_, _, err := sender.Create(context.Background(), CoTCreateRequest{
		ChatID:          "oc_test",
		OriginMessageID: "om_trigger",
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
	var apiErr *CoTAPIError
	if !errors.As(err, &apiErr) || apiErr.Class != "context_canceled" {
		t.Fatalf("error = %v, want context_canceled CoTAPIError", err)
	}
}

func TestCoTMessagesRedactsTransportFailures(t *testing.T) {
	httpClient := &cotStubHTTPClient{transportErr: errors.New("dial tcp 10.0.0.1:443: connection reset")}
	sender := newCoTTestSender(httpClient)

	err := sender.Complete(context.Background(), CoTCompleteRequest{
		CoTID:     "cot_7355",
		MessageID: "om_cot",
		Reason:    CoTOutcomeDone,
	})
	var apiErr *CoTAPIError
	if !errors.As(err, &apiErr) || apiErr.Class != "transport" {
		t.Fatalf("error = %v, want transport CoTAPIError", err)
	}
	if strings.Contains(err.Error(), "10.0.0.1") {
		t.Fatalf("error text leaked the transport error: %q", err.Error())
	}
}

func TestCoTMessagesRequireConfiguredClient(t *testing.T) {
	sender := &ChannelSender{}
	if _, _, err := sender.Create(context.Background(), CoTCreateRequest{ChatID: "oc_test", OriginMessageID: "om_trigger"}); err == nil {
		t.Fatal("expected not configured error from Create")
	}
	if err := sender.AppendEvents(context.Background(), CoTAppendRequest{CoTID: "cot_7355", MessageID: "om_cot"}); err == nil {
		t.Fatal("expected not configured error from AppendEvents")
	}
	if err := sender.Complete(context.Background(), CoTCompleteRequest{CoTID: "cot_7355", MessageID: "om_cot", Reason: CoTOutcomeDone}); err == nil {
		t.Fatal("expected not configured error from Complete")
	}
}

func TestCoTMessagesCreateAppendCompleteWireSequence(t *testing.T) {
	httpClient := &cotStubHTTPClient{bodies: []string{
		`{"code":0,"msg":"success","data":{"cot_id":"cot_7355","message_id":"om_cot"}}`,
		cotTestOKBody,
		cotTestOKBody,
	}}
	sender := newCoTTestSender(httpClient)
	ctx := context.Background()

	cotID, messageID, err := sender.Create(ctx, CoTCreateRequest{ChatID: "oc_test", OriginMessageID: "om_trigger"})
	if err != nil {
		t.Fatalf("create CoT: %v", err)
	}
	if err := sender.AppendEvents(ctx, CoTAppendRequest{
		CoTID:     cotID,
		MessageID: messageID,
		Events: []CoTEvent{
			cotTestRunStarted(t, "thread-1", "run-1"),
			cotTestStepStarted(t, "step-1", "Reading files"),
			cotTestStepFinished(t, "step-1", "Reading files"),
			cotTestRunFinished(t, "thread-1", "run-1", CoTOutcomeDone),
		},
	}); err != nil {
		t.Fatalf("append events: %v", err)
	}
	if err := sender.Complete(ctx, CoTCompleteRequest{
		CoTID:     cotID,
		MessageID: messageID,
		Reason:    CoTOutcomeDone,
	}); err != nil {
		t.Fatalf("complete CoT: %v", err)
	}

	if len(httpClient.requests) != 3 {
		t.Fatalf("request count = %d, want 3", len(httpClient.requests))
	}
	want := []struct {
		method string
		path   string
	}{
		{http.MethodPost, "/open-apis/im/v1/message_cot"},
		{http.MethodPut, "/open-apis/im/v1/message_cot"},
		{http.MethodPost, "/open-apis/im/v1/message_cot/complete/cot_7355"},
	}
	for index, expected := range want {
		got := httpClient.requests[index]
		if got.Method != expected.method || got.EscapedPath != expected.path {
			t.Fatalf("request[%d] = %s %s, want %s %s", index, got.Method, got.EscapedPath, expected.method, expected.path)
		}
	}
	events := decodeCoTBody(t, httpClient.requests[1].Body)["events"].([]any)
	wantTypes := []CoTEventType{CoTEventRunStarted, CoTEventStepStarted, CoTEventStepFinished, CoTEventRunFinished}
	for index, wantType := range wantTypes {
		event := events[index].(map[string]any)
		if event["event_type"] != string(wantType) {
			t.Fatalf("events[%d].event_type = %#v, want %s", index, event["event_type"], wantType)
		}
	}
}

func TestCoTMessagesAppendEventsRejectsOversizedBatches(t *testing.T) {
	httpClient := &cotStubHTTPClient{}
	sender := newCoTTestSender(httpClient)
	events := make([]CoTEvent, 0, maxCoTEventsPerRequest+1)
	for index := 0; index <= maxCoTEventsPerRequest; index++ {
		events = append(events, cotTestStepStarted(t, "step-1", "Reading files"))
	}

	if err := sender.AppendEvents(context.Background(), CoTAppendRequest{
		CoTID:     "cot_7355",
		MessageID: "om_cot",
		Events:    events,
	}); err == nil {
		t.Fatal("expected batch size validation error")
	}
	if len(httpClient.requests) != 0 {
		t.Fatalf("rejected request reached the API: %#v", httpClient.requests)
	}
}

func TestCoTMessagesRejectOversizedIdentifiers(t *testing.T) {
	sender := newCoTTestSender(&cotStubHTTPClient{})
	oversized := strings.Repeat("x", maxCoTIdentifierLength+1)

	if _, _, err := sender.Create(context.Background(), CoTCreateRequest{
		ChatID:          oversized,
		OriginMessageID: "om_trigger",
	}); err == nil {
		t.Fatal("expected chat id length error")
	}
	if err := sender.Complete(context.Background(), CoTCompleteRequest{
		CoTID:     oversized,
		MessageID: "om_cot",
		Reason:    CoTOutcomeDone,
	}); err == nil {
		t.Fatal("expected cot id length error")
	}
}
