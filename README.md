# feishu-botd

`feishu-botd` is a small local sidecar for sending Feishu/Lark bot
notifications on behalf of Xipe and other local services. It owns Feishu app
credentials and exposes a narrow local API:

```text
GET  /healthz
GET  /readyz
POST /v1/notify
```

The v1 scope is notification sending only. Interactive chat, inbound Feishu
events, card actions, streaming replies, durable outbox semantics, and native
Xipe delivery backends are follow-up work.

## Configuration

Required:

```text
FEISHU_APP_ID=cli_xxx
FEISHU_APP_SECRET=...
FEISHU_BOTD_CHANNELS_OPS=oc_xxx
```

Choose at least one local listener:

```text
FEISHU_BOTD_SOCKET=/run/feishu-botd/feishu-botd.sock
```

or:

```text
FEISHU_BOTD_BIND=127.0.0.1:7345
FEISHU_BOTD_AUTH_TOKEN_FILE=/run/secrets/feishu-botd-token
```

Optional:

```text
FEISHU_BOTD_DEDUPE_TTL_SECONDS=21600
FEISHU_BOTD_SEND_TIMEOUT_SECONDS=15
```

## Development

```sh
go test ./...
go test -race ./...
go vet ./...
```
