# IPC contract (protobuf / gRPC)

`feishu-botd` is moving to a **protobuf-first** local IPC contract. The
`.proto` files under [`proto/feishubotd/v1/`](../proto/feishubotd/v1) are the
shared, language-neutral contract. The Go daemon owns the Feishu/Lark SDK,
credentials, token lifecycle, channel-alias resolution, and (in future)
inbound events. Client apps — Go, Rust, or otherwise — talk to the daemon over
gRPC and generate their own bindings from `proto/`. There is no shared client
crate.

The legacy HTTP `POST /v1/notify` endpoint remains as a compatibility shim over
the exact same internal service logic (see [api.md](./api.md)).

## Transports

gRPC is the preferred transport. Two listeners are available; enable whichever
fits the deployment (both can run at once):

| Env var | Listener | Auth |
| --- | --- | --- |
| `FEISHU_BOTD_GRPC_SOCKET` | Unix domain socket (preferred, local-first) | none (local trust) |
| `FEISHU_BOTD_GRPC_BIND` | loopback TCP (Docker / process managers) | bearer token, required |

The Unix socket is created `0o660` and removed-then-rebound on start, mirroring
the HTTP socket. The loopback TCP listener requires `FEISHU_BOTD_AUTH_TOKEN_FILE`
(the **same** token shared with the HTTP TCP listener) and rejects non-loopback
binds. Health RPCs are exempt from auth on the TCP listener so process managers
can probe without a token.

Dial examples (Go):

```go
// Unix socket
conn, _ := grpc.NewClient(
    "passthrough:///feishu-botd",
    grpc.WithTransportCredentials(insecure.NewCredentials()),
    grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
        return (&net.Dialer{}).DialContext(ctx, "unix", "/run/feishu-botd/feishu-botd.grpc.sock")
    }),
)

// Loopback TCP: attach the bearer token as request metadata
ctx := metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)
```

## Services

All services live in package `feishubotd.v1`.

### `BotdHealthService`

| RPC | Purpose |
| --- | --- |
| `Health` | Process liveness. Mirrors `GET /healthz`. |
| `Ready` | Redacted readiness checks (config, credentials, channels, dedupe). Mirrors `GET /readyz`. |

The daemon also registers the standard `grpc.health.v1.Health` service so
`grpc_health_probe` works for gRPC-only deployments.

### `NotificationService`

| RPC | Purpose |
| --- | --- |
| `SendNotification` | The ergonomic, deduped, webhook-replacement path. Exact shape `POST /v1/notify` maps onto. |
| `SendMessage` | Lower-level send with a forward-compatible content `oneof`. v1 implements markdown and raw interactive card JSON. |

`SendNotification` keeps the same required fields as the HTTP contract —
`source`, `source_event_id`, `dedupe_key`, `severity`, `title`, `markdown`,
`target` — so `source` + `dedupe_key` make every call idempotent. Callers route
with a stable **channel alias** (`target.channel = "ops"`); raw Feishu chat ids
and app credentials live only in daemon config.

`SendMessage` carries a `content` oneof (`markdown` | `text` | `card`).
`markdown` and `card.card_json` are implemented in v1; `text` returns
`UNIMPLEMENTED`. `card_json` must be a Feishu interactive-card JSON object,
such as a template card payload. Deduplication applies only when a `dedupe_key`
is supplied.

### `CommandService` / `ProviderService` (skeletons)

Defined to pin the package shape for future inbound bot commands and
data-provider flows. They are **not registered** on the server in this slice;
the field numbers and streaming shapes are reserved so they can be added without
breaking `v1`.

## Error model

One error vocabulary, two encodings. The daemon's internal error carries a
stable machine `code`, a redacted `message`, a `retryable` flag, and a
`request_id`.

- **HTTP** serializes these into the existing JSON error envelope.
- **gRPC** maps them onto a canonical status code and attaches the same fields
  as a neutral, in-contract `BotdError` detail (via `status.WithDetails`). Dumb
  clients get a sensible code; richer clients branch on the stable string `code`
  without vendoring `google.rpc`.

| HTTP status / code | gRPC code |
| --- | --- |
| 400 (`missing_*`, `invalid_severity`, `invalid_json`, `field_too_large`, ...) | `INVALID_ARGUMENT` |
| 401 `unauthorized` | `UNAUTHENTICATED` |
| 404 `unknown_channel`, `not_found` | `NOT_FOUND` |
| 409 `dedupe_conflict` | `ALREADY_EXISTS` |
| 409 `dedupe_in_flight` | `ABORTED` |
| 501 unimplemented content/services | `UNIMPLEMENTED` |
| 502 `feishu_unavailable` | `UNAVAILABLE` |
| other / internal | `INTERNAL` |

## Regenerating bindings

Generated Go under [`gen/feishubotd/v1/`](../gen/feishubotd/v1) is **committed**,
so `go build`/`go test` and the Docker build never run codegen. Regenerate only
when the `.proto` files change:

```sh
make proto        # buf generate + gofmt
make proto-lint   # buf lint
make proto-check  # fail if committed gen/ is stale
```

Tooling (installed under `$(go env GOPATH)/bin`):

- `buf` v1.50.0 (bundles its own protobuf compiler — no standalone `protoc`)
- `protoc-gen-go` v1.36.6
- `protoc-gen-go-grpc` v1.5.1

Non-Go clients run their own generator against `proto/` (e.g. `tonic`/`prost`
for Rust). The `.proto` files are the only shared artifact.
