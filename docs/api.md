# HTTP API (compatibility shim)

This HTTP API is a compatibility shim retained for existing webhook-style
callers. It delegates to the same internal service logic as the gRPC contract;
new integrations should prefer gRPC. See [ipc.md](./ipc.md) for the
protobuf-first contract, the equivalent gRPC RPCs, and the shared error model.

The request shapes do not change for a multi-app daemon: there is no app field.
Every caller still supplies a bare channel alias (or uses a configured service
default), and botd resolves that globally unique alias to the owning app and
private Feishu chat id. App aliases are public configuration names, but app ids,
secrets, and raw chat ids never enter HTTP bodies or responses.

TCP requests to the two `POST` routes require the general bearer token. Unix
socket requests also require that same distinct general bearer whenever
`agent_providers` is configured; a provider token does not authorize HTTP
sends, and omitting the general token makes those routes fail closed. Unix
deployments without `agent_providers` retain legacy local trust. `GET /healthz`
and `GET /readyz` remain unauthenticated on both transports.

## `GET /healthz`

Returns process liveness only.

```json
{"status":"ok","service":"feishu-botd","version":"0.2.0"}
```

## `GET /readyz`

Returns redacted readiness checks for config, Feishu credentials, channels, and
dedupe state. Existing aggregate keys remain unchanged. Multi-app readiness
adds `feishu_auth.<alias>` (`ok`, `missing_credentials`, or `unavailable`) and
`feishu_connection.<alias>` (`starting`, `connected`, `reconnecting`,
`disconnected`, or `unavailable`). Any failed app auth check or connection
state other than `connected` returns `503 unready`. It does not send a test
message or expose SDK error text.

## `POST /v1/notify`

Sends one notification to a configured local channel.
The alias selects its owning app internally; callers do not pass an app alias
separately. Global and per-service default channels use the same resolution.

```json
{
  "source": "monitoring",
  "source_event_id": "REPLACE_WITH_SOURCE_EVENT_ID",
  "dedupe_key": "monitoring:service-health:event-001:ops",
  "severity": "critical",
  "title": "Service health check failed",
  "markdown": "**Service**: example-api",
  "target": { "channel": "ops" },
  "links": [],
  "metadata": { "trigger": "health_check_failed" }
}
```

Successful response:

```json
{
  "status": "sent",
  "provider": "feishu",
  "dedupe_key": "monitoring:service-health:event-001:ops",
  "message_id": "REPLACE_WITH_MESSAGE_ID",
  "duplicate": false
}
```

Errors are redacted:

```json
{
  "error": {
    "code": "feishu_unavailable",
    "message": "Feishu send failed",
    "retryable": true,
    "request_id": "REPLACE_WITH_REQUEST_ID"
  }
}
```

`card_json` and `reply_to_message_id` are accepted on the request body (they're
additive fields on the shared internal contract) but are always cleared before
sending — `/v1/notify` is the stable markdown-only, fresh-message contract.
Card delivery and reply-threading are both `/v1/message`-only, and
reply-threading there is internal-only (see [ipc.md](./ipc.md)); no HTTP/gRPC
caller can currently set it.

## `POST /v1/message`

Lower-level send path for callers that need a specific message content type.
It supports markdown and Feishu interactive cards. `dedupe_key` is optional;
when present, duplicate sends return the original `message_id`. `target.channel`
may be omitted when `source` has a configured service default. Dedupe state is
scoped to the resolved app, so reusing the same source/key against aliases owned
by two different apps does not create a cross-app duplicate or conflict.

Markdown:

```json
{
  "source": "build-monitor",
  "dedupe_key": "build-monitor:build:123",
  "markdown": {
    "title": "Build succeeded",
    "markdown": "**Project**: example-app"
  }
}
```

Interactive card:

```json
{
  "source": "build-monitor",
  "dedupe_key": "build-monitor:build:123",
  "msg_type": "interactive",
  "card": {
    "type": "template",
    "data": {
      "template_id": "REPLACE_WITH_TEMPLATE_ID",
      "template_version_name": "1.0.3",
      "template_variable": {
        "title": "Build succeeded"
      }
    }
  }
}
```

Successful response:

```json
{
  "provider": "feishu",
  "message_id": "REPLACE_WITH_MESSAGE_ID",
  "duplicate": false
}
```
