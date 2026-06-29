# feishu-botd

`feishu-botd` is a small local gateway for Feishu/Lark bots. It owns the
Feishu/Lark SDK, app credentials, and token lifecycle, and lets local services
send notifications without ever handling raw chat ids. It is general-purpose: it
is not tied to any single project or organization.

## Contract

The contract is **protobuf-first**. The `.proto` files under
[`proto/feishubotd/v1/`](proto/feishubotd/v1) are the shared, language-neutral
definition; clients (Go, Rust, …) generate their own bindings — there is no
shared client crate. The daemon serves these over gRPC, preferring a Unix domain
socket:

| Service | RPCs |
| --- | --- |
| `BotdHealthService` | `Health`, `Ready` |
| `NotificationService` | `SendNotification`, `SendMessage` |
| `CommandService` / `ProviderService` | future inbound commands + data providers (skeletons) |

The legacy HTTP API stays as a compatibility shim over the **same** internal
service logic:

```text
GET  /healthz
POST /v1/notify   (and GET /readyz)
POST /v1/message  (lower-level markdown/card send)
```

See [docs/ipc.md](docs/ipc.md) for the full gRPC contract and error model, and
[docs/api.md](docs/api.md) for the HTTP shim.

Interactive chat, inbound Feishu events, card actions, streaming replies, and
durable outbox semantics are follow-up work. Outbound interactive card sends
are supported through the lower-level message surface.

## Configuration

Required:

```text
FEISHU_APP_ID=cli_xxx
FEISHU_APP_SECRET=...
FEISHU_BOTD_CHANNELS_OPS=oc_xxx
```

Choose at least one listener. Each transport has a Unix-socket form (local-first,
preferred) and a TCP form (bearer-token auth required):

```text
# gRPC (preferred)
FEISHU_BOTD_GRPC_SOCKET=/run/feishu-botd/feishu-botd.grpc.sock
# FEISHU_BOTD_GRPC_BIND=127.0.0.1:7346

# HTTP compatibility shim
FEISHU_BOTD_SOCKET=/run/feishu-botd/feishu-botd.sock
# FEISHU_BOTD_BIND=127.0.0.1:7345
```

Any TCP listener (`FEISHU_BOTD_BIND` or `FEISHU_BOTD_GRPC_BIND`) requires a
shared bearer token. TCP binds are loopback-only by default; expose them on a
LAN only with an explicit opt-in:

```text
FEISHU_BOTD_AUTH_TOKEN_FILE=/run/secrets/feishu-botd-token
FEISHU_BOTD_ALLOW_NON_LOOPBACK_BIND=true
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

Generated gRPC bindings under `gen/` are committed, so a normal build never runs
codegen. Regenerate only after editing `proto/`:

```sh
make proto        # buf generate + gofmt
make proto-lint   # buf lint
```

Build a local binary or container image:

```sh
make build
make image
```

Run beside Xipe's Docker Compose stack with the optional overlay:

```sh
cd /path/to/xipe
docker compose \
  -f docker-compose.yml \
  -f ../feishu-botd/deploy/docker-compose.xipe.yml \
  --profile feishu \
  up -d --build
```

The overlay shares a Unix socket volume with the `xipe` container and leaves the
sidecar optional. Xipe proxy traffic and startup do not depend on
`feishu-botd`.
