# HTTP API (compatibility shim)

This HTTP API is a compatibility shim retained for existing webhook-style
callers. It delegates to the same internal service logic as the gRPC contract;
new integrations should prefer gRPC. See [ipc.md](./ipc.md) for the
protobuf-first contract, the equivalent gRPC RPCs, and the shared error model.

## `GET /healthz`

Returns process liveness only.

```json
{"status":"ok","service":"feishu-botd","version":"0.1.0"}
```

## `GET /readyz`

Returns redacted readiness checks for config, Feishu credentials, channels, and
dedupe state. It does not send a test message.

## `POST /v1/notify`

Sends one notification to a configured local channel.

```json
{
  "source": "xipe",
  "source_event_id": "01J...",
  "dedupe_key": "xipe:account-condition:01J...:ops",
  "severity": "critical",
  "title": "Xipe account needs re-auth",
  "markdown": "**Account**: acct_123",
  "target": { "channel": "ops" },
  "links": [],
  "metadata": { "trigger": "reauth_required" }
}
```

Successful response:

```json
{
  "status": "sent",
  "provider": "feishu",
  "dedupe_key": "xipe:account-condition:01J...:ops",
  "message_id": "om_xxx",
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
    "request_id": "req_..."
  }
}
```

## `POST /v1/message`

Lower-level send path for callers that need a specific message content type.
It supports markdown and Feishu interactive cards. `dedupe_key` is optional;
when present, duplicate sends return the original `message_id`.

Markdown:

```json
{
  "source": "jenkins",
  "dedupe_key": "jenkins:build:123",
  "target": { "channel": "ci" },
  "markdown": {
    "title": "Build succeeded",
    "markdown": "**Project**: WeNext"
  }
}
```

Interactive card:

```json
{
  "source": "jenkins",
  "dedupe_key": "jenkins:build:123",
  "target": { "channel": "ci" },
  "msg_type": "interactive",
  "card": {
    "type": "template",
    "data": {
      "template_id": "AAqBgzXLgNKzZ",
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
  "message_id": "om_xxx",
  "duplicate": false
}
```
