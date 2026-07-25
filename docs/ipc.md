# IPC contract (protobuf / gRPC)

`feishu-botd` is moving to a **protobuf-first** local IPC contract. The
`.proto` files under [`proto/feishubotd/v1/`](../proto/feishubotd/v1) are the
shared, language-neutral contract. The Go daemon owns the Feishu/Lark SDK,
credentials, token lifecycle, channel-alias resolution, inbound events, and
CardKit response lifecycle. Client apps — Go, Rust, or otherwise — talk to the
daemon over gRPC and generate their own bindings from `proto/`. There is no
shared client crate.

The legacy HTTP `POST /v1/notify` endpoint remains as a compatibility shim over
the exact same internal service logic (see [api.md](./api.md)).

## Transports

gRPC is the preferred transport. Two listeners are available; enable whichever
fits the deployment (both can run at once):

| Env var | Listener | Auth |
| --- | --- | --- |
| `FEISHU_BOTD_GRPC_SOCKET` | Unix domain socket (preferred, local-first) | with `agent_providers`: provider bearer for agent/scoped inbound RPCs and general bearer for other non-health RPCs; without it: legacy local trust |
| `FEISHU_BOTD_GRPC_BIND` | plaintext TCP (loopback only) | provider bearer for agent/scoped inbound RPCs; general bearer for other non-health RPCs |

The Unix socket is created `0o660` and removed-then-rebound on start, mirroring
the HTTP socket. Agent RPCs require a provider-specific token from
`agent_providers` even on Unix; filesystem access alone does not establish a
provider identity. When `agent_providers` is non-empty, every other non-health
gRPC RPC on the Unix socket requires the distinct general bearer. Configure it
with `listeners.auth_token_file` or `FEISHU_BOTD_AUTH_TOKEN_FILE`; if omitted,
general RPCs fail closed while provider and health RPCs remain available. A
provider token never grants outbound `NotificationService` authority. Only
deployments without `agent_providers` retain the existing Unix local-trust
mode. TCP always requires the general bearer for general RPCs. Provider tokens
and the general token must be distinct. Health RPCs remain unauthenticated.

gRPC TCP has no built-in TLS in this release, so it is always loopback-only;
`FEISHU_BOTD_ALLOW_NON_LOOPBACK_BIND` applies to HTTP, not gRPC. Cross-host gRPC
must use an authenticated encrypted tunnel or TLS proxy terminating onto the
loopback listener. The server checks both the configured address and the actual
bound listener address.

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

// General loopback TCP RPC: attach the general bearer as request metadata.
ctx := metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)

// Agent/scoped provider RPC, including over Unix: attach that provider's
// distinct bearer and send its matching provider id in the request.
ctx := metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+providerToken)
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

`SendNotification` keeps the same identity fields as the HTTP contract —
`source`, `source_event_id`, `dedupe_key`, `severity`, `title`, `markdown`,
`target` — so `source` + `dedupe_key` make every call idempotent. Callers can
route with a stable **channel alias** (`target.channel = "ops"`), or omit the
target when `source` has a configured service default. Raw Feishu chat ids and
app credentials live only in daemon config.

`SendMessage` carries a `content` oneof (`markdown` | `text` | `card`).
`markdown` and `card.card_json` are implemented in v1; `text` returns
`UNIMPLEMENTED`. `card_json` must be a Feishu interactive-card JSON object,
such as a template card payload. Deduplication applies only when a `dedupe_key`
is supplied.

### `CommandService`

| RPC | Purpose |
| --- | --- |
| `Subscribe` | Provider opens a server stream for one or more command names. botd pushes matching `InboundCommand` values. |
| `Respond` | Provider sends a markdown or card reply for a previously delivered `delivery_id`. |
| `SubscribeAgentEvents` | Agent opens a server stream for exact commands, unmatched conversational messages, card actions, or any combination. |
| `StartAgentResponse` | Creates a streaming CardKit 2.0 reply for one inbound delivery and returns an opaque response handle at revision 1. |
| `UpdateAgentResponse` | Applies a complete accumulated markdown snapshot at the expected revision. |
| `FinishAgentResponse` | Applies final content, records the outcome, and disables CardKit streaming mode. |

Inbound Feishu events are enabled only when `commands.enabled` or
`FEISHU_BOTD_COMMANDS_ENABLED=true` is set. Users invoke commands as
`@<bot-name> <COMMAND> [args...]`. The daemon opens the Feishu long connection,
handles `im.message.receive_v1`, accepts text messages from configured channel
aliases, verifies the configured bot-name or bot-id mention, strips Feishu's
mention token, and dispatches the first word as `command` with the rest as
`text`.

`InboundCommand.chat_alias` is always a configured alias; raw Feishu chat ids are
never exposed. Its metadata is limited to `chat_type` and `message_type`; raw
event, message, thread, root, and parent ids remain daemon-private. Unknown or
ambiguous chat ids are ignored. `Respond` sends the
reply back to the original channel alias using the existing `SendMessage` path,
threaded as a Feishu reply to the original inbound message (not a fresh
top-level post), and rejects unknown, expired, in-flight, or already-answered
`delivery_id` values. The legacy stream remains group-only; use the agent stream
for direct messages.

`commands.scripts` (see [README.md](../README.md#local-script-execution))
registers an in-process subscriber — `internal/scriptexec` — that runs a local
script for one command word and calls `Respond` itself. It uses the exact same
`Subscribe`/`Respond` path as an external provider, just without a gRPC
round-trip, so it's a drop-in alternative to (or can run alongside) a real
gRPC provider — **as long as they subscribe to different command words**.
`dispatch` delivers at most once per provider, even if that provider has
overlapping streams. Distinct providers can each receive the same command, so if
`commands.scripts.command` collides with a word an external provider also
subscribes to, both run and race `Respond`; the loser gets a rejected,
un-retried `response_in_flight` and its reply is silently dropped.

External agent RPCs always require an `agent_providers` credential. When
`agent_providers` is non-empty, external legacy `Subscribe` and `Respond` are
also provider-scoped: the request provider must match the authenticated
principal, `allow_legacy_commands` must be true, requested words must be in
`allowed_commands`, and only a provider that actually received a delivery may respond.
This prevents a notification-only bearer holder from reading or suppressing
inbound commands. Legacy deployments with no `agent_providers` map keep their
existing Unix/global-token mode; the in-process script executor is not a gRPC
peer and remains internal.

#### Agent ingress and routing

`SubscribeAgentEventsRequest` requires a stable `provider` and at least one of:

- non-empty `commands` for exact, normalized first-word matches;
- `include_unmatched_messages = true` for natural conversational prompts;
- `include_card_actions = true` for actions on cards owned by that provider.

The `provider` must match the identity authenticated by the provider bearer;
the general notification bearer cannot use an agent RPC. The daemon also checks
requested selectors against that provider's `allowed_commands`,
`allow_unmatched_messages`, and `allow_card_actions` before registering the
stream. Legacy streams additionally require `allow_legacy_commands`. Grant
unmatched-message access carefully because it includes direct-message prompts.

Exact command subscribers win over unmatched subscribers. A successfully
delivered legacy group command wins before either agent selector. For a single
agent, the usual subscription has no command list and enables both booleans.
Multiple matching providers can receive a message, but the first valid response
start attempt claims that delivery even if its Feishu call later fails;
deployments should avoid overlapping ownership.

Once any route accepts an inbound delivery, that route is sticky for the
dedupe lifetime. A Feishu retry never re-arbitrates between legacy and agent
paths or after subscriber churn. Delivery is still best-effort: enqueue is
non-blocking and there is no provider ACK or durable replay log.

Direct (`p2p`) text needs no mention and is exposed as `chat_alias = "direct"`.
Group and topic-group text must originate in a uniquely configured channel
alias and mention the configured bot. `InboundAgentMessage.text` contains the
complete mention-stripped prompt, while `command` and `command_text` expose the
optional first-word view. `conversation_id` is an opaque hash of the raw chat
and optional thread. Only `chat_type` and `message_type` pass through metadata.
Raw event/chat/message/card ids and callback credentials never enter the public
stream. `delivery_id` and `conversation_id` are stable, domain-separated opaque
handles derived by botd.

#### Agent response state machine

`StartAgentResponse` accepts `provider`, `delivery_id`, `operation_id`, and an
`AgentResponseContent`. The content contains a title, initial markdown, and up
to eight unique actions. botd builds a CardKit JSON 2.0 entity with streaming
mode enabled, replies to the inbound message, and returns an opaque
`response_id`, revision `1`, phase `STREAMING`, and a `duplicate` flag.

`UpdateAgentResponse` accepts that handle, a new `operation_id`, the current
receipt's revision in `expected_revision`, and the complete accumulated
markdown. It serializes CardKit mutations, updates the card's `agent_answer`
element, and increments the semantic revision once. It does not accept token
deltas. Feishu's typewriter effect is preserved when the previous snapshot is a
prefix of the new snapshot.

`FinishAgentResponse` has the same concurrency fields plus `outcome`, final
markdown, and an optional preview `summary`. Outcome must be `COMPLETED`,
`FAILED`, or `CANCELLED`. The daemon applies changed final content, disables
streaming mode, and increments the semantic revision once even when finalizing
requires two CardKit operations.

Every operation is idempotent within botd's process state. A transport retry
must reuse the same `operation_id` and semantic request. A completed retry
returns `duplicate = true`; update and finish retries return their recorded
operation receipt, while a start retry returns the response handle's current
state. Reuse with different fields is an `operation_conflict`. A stale
`expected_revision` is a `revision_conflict`, a different concurrent mutation
is `operation_in_flight`, and a terminal response is `response_closed`. The
`provider` must own both the inbound delivery and the response handle.

botd enforces one card-wide sequence domain and at least 125 ms between CardKit
mutation attempts (about 8 Hz), leaving margin below Feishu's 10
operations/second/card limit. Providers should still coalesce model tokens into
cumulative snapshots at about 5–8 Hz.
Feishu automatically closes streaming mode after roughly 10 minutes and allows
CardKit entity operations for 14 days. botd's agent state is deliberately
process-local: handles and operation receipts expire after the configured
dedupe TTL (six hours by default) and are lost on restart, so an old card cannot
be resumed through its prior `response_id`. Run one active long-connection
client per Feishu app: Feishu cluster-delivers each event to one random client,
and this release has no shared ownership store for active-active replicas.

With `agent_providers` configured, startup requires the dedupe TTL to be at
least one hour plus `send_timeout`. That keeps an ambiguous Start claim alive
through Feishu's one-hour IM message-UUID retry window.

Feishu's card-create API has no idempotency key. An ambiguous create failure can
therefore leave an unused entity that botd cannot recover. Once botd knows the
`card_id`, an ambiguous send can be retried with that same card and the same
message UUID; update and finish retries retain their original CardKit UUID and
sequence.

Transport failures, HTTP 5xx, Feishu IM code `230020`, CardKit codes `200810`
and `300120`, and HTTP 429 return retryable `feishu_unavailable` and retain the
exact request, operation ID, UUID, and sequence. HTTP 429, `230020`, and
`200810` are definitive temporary rejections; transport failures, HTTP 5xx,
and `300120` are ambiguous commit outcomes.

A definitive non-retryable rejection on the first attempt returns
`feishu_rejected` and releases the uncommitted delivery or operation so a
corrected request can use a new operation ID. If a later definitive rejection
follows an ambiguous attempt, botd cannot safely replace it: Start closes as
`send_state_unknown`, while Update/Finish close as `operation_state_unknown`.
The claim and original operation identity remain retained. After an ambiguous
Start, botd also stops before Feishu's one-hour message-UUID dedupe window can
expire and returns `send_retry_expired` without another Feishu call.

#### Agent card actions

Each `AgentResponseAction` carries a provider-defined `action_id`, label,
optional JSON-object `payload_json`, and default/primary/danger style. A
`card.action.trigger` callback is acknowledged by botd and routed only to an
action-enabled subscription for the provider that owns the response.

`InboundCardAction` contains the opaque response handle, action id, and a
normalized JSON object with `tag`, `name`, `option`, `timezone`, `input_value`,
`options`, `checked`, `value`, and `form_value`. The callback token and raw
Feishu card/message/chat ids are removed. Callback delivery ids are deduplicated
in memory. If the provider queue is full or unavailable, the user gets a
generic unavailable response rather than a leaked internal error.

The response API authors buttons, not form cards. `name` remains an independent
Feishu form-component identifier and `form_value` is preserved only for
forward-compatible callback normalization. An action is not eligible for
`StartAgentResponse`; it may reference the owning response for Update/Finish
only while that response remains streaming.

See [agent.md](./agent.md) for Feishu app setup and a runnable provider.

### `ProviderService` (skeleton)

Defined to pin the package shape for future data-provider pull flows. It is not
registered on the server in this slice.

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
| 403 `provider_identity_mismatch`, `provider_scope_denied` | `PERMISSION_DENIED` |
| 404 `unknown_channel`, `unknown_delivery`, `unknown_response`, `unknown_action`, `not_found` | `NOT_FOUND` |
| 409 `dedupe_conflict`, `already_responded` | `ALREADY_EXISTS` |
| 409 `operation_conflict` | `ALREADY_EXISTS` |
| 409 `dedupe_in_flight`, `response_in_flight`, `operation_in_flight`, `revision_conflict` | `ABORTED` |
| 412 `response_closed`, `feishu_rejected`, `send_retry_expired`, `send_state_unknown`, `operation_state_unknown` | `FAILED_PRECONDITION` |
| 429 `agent_queue_full` | `RESOURCE_EXHAUSTED` |
| 501 unimplemented content/services | `UNIMPLEMENTED` |
| 502 `feishu_unavailable`, 503 `agent_unavailable` | `UNAVAILABLE` |
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
