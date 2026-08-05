# IPC contract (protobuf / gRPC)

`feishu-botd` is moving to a **protobuf-first** local IPC contract. The
`.proto` files under [`proto/feishubotd/v1/`](../proto/feishubotd/v1) are the
shared, language-neutral contract. The Go daemon owns the Feishu/Lark SDK,
credentials, token lifecycle, channel-alias resolution, inbound events, and
CardKit response lifecycle for one or more Feishu apps. Client apps — Go, Rust,
or otherwise — talk to the daemon over gRPC and generate their own bindings
from `proto/`. There is no shared client crate.

The legacy HTTP `POST /v1/notify` endpoint remains as a compatibility shim over
the exact same internal service logic (see [api.md](./api.md)).

## Multi-app routing without wire changes

No protobuf or HTTP request has an app field. Multi-app routing uses identities
that callers already send:

- A bare channel alias resolves through one global directory to the owning app
  and private Feishu chat id.
- An inbound `delivery_id`, `response_id`, or `conversation_id` is an opaque
  daemon-issued handle whose internal state records the source app.
- `InboundAgentEvent.metadata["app_alias"]` is optional, public context for a
  provider that wants it. It is never required for routing or authorization,
  and old clients may ignore the additive map entry.

This keeps existing generated clients and exact request state machines
unchanged. Replies, CardKit operations, callback dedupe, delivery claims,
operation receipts, and follow-up grants are app-scoped internally; a handle
created by app A can send only through app A. App aliases may cross the provider
boundary, while `app_id`, `app_secret`, raw routing ids, and SDK connection
details may not.

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

`Ready` keeps the existing aggregate `config`, `feishu_auth`, `channels`, and
`dedupe_store` entries, then adds `feishu_auth.<alias>` for each app. Per-app
auth is `ok`, `missing_credentials`, or `unavailable`. The multi-app process also
reports `feishu_connection.<alias>` as `starting`, `connected`,
`reconnecting`, `disconnected`, or `unavailable`. Any app auth failure or any
connection state other than `connected` makes `ready = false`. Keys contain
only public aliases; credential and SDK error text is never returned.

The daemon also registers the standard `grpc.health.v1.Health` service so
`grpc_health_probe` works for gRPC-only deployments. Like `/healthz`, that
standard service is static liveness; use `BotdHealthService.Ready` for the
per-app readiness states.

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
app credentials live only in daemon config. Channel aliases are globally unique
across apps and resolve internally to `(app, chat)`. Deduplication is scoped to
that resolved app, so the same caller key used for two different apps does not
cause a cross-app conflict.

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
| `SubscribeAgentEvents` | Agent opens a server stream for exact commands, unmatched conversational messages, card actions, native message reactions, or any combination. |
| `StartAgentResponse` | Creates a streaming CardKit 2.0 reply and returns an opaque response handle, provider-safe message reference, and revision 1. |
| `UpdateAgentResponse` | Applies a complete accumulated markdown snapshot, and any timeline part it carries, at the expected revision. |
| `FinishAgentResponse` | Applies final content, records the outcome, and disables CardKit streaming mode. |
| `SendAgentFollowUp` | Posts one later, standalone message into a conversation the provider has already received an agent event from. |

Inbound Feishu events are dispatched only for an app whose `commands.enabled`
is true (the existing `FEISHU_BOTD_COMMANDS_ENABLED` override affects the
reserved `default` app). Users invoke commands as
`@<bot-name> <COMMAND> [args...]`. The daemon opens one Feishu long connection
per configured app and handles `im.message.receive_v1`. It accepts text messages
from globally unique configured channel aliases after either verifying that
app's configured bot-name/bot-id mention or proving that an unmentioned reply's
parent is owned by an agent provider. Mention tokens are stripped. For exact
command routing it derives the first word as `command` with the rest as `text`;
the complete prompt remains authoritative for conversational routing.

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
gRPC provider — **as long as they subscribe to different command words**. A
named app configures this under `apps.<alias>.commands.scripts`; its internal
subscription and allowed chats are restricted to that app.
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

An agent provider may also configure `allowed_apps`. When it is absent, the
provider retains access to every configured app; an explicit empty list allows
none. The daemon applies this config-only scope before legacy or agent event
fan-out and rechecks the app resolved from every opaque mutation handle.
Disallowed handles return the same `unknown_delivery`, `unknown_response`, or
`unknown_conversation` errors as nonexistent handles.

#### Agent ingress and routing

`SubscribeAgentEventsRequest` requires a stable `provider` and at least one of:

- non-empty `commands` for exact, normalized first-word matches;
- `include_unmatched_messages = true` for natural conversational prompts;
- `include_card_actions = true` for actions on cards owned by that provider;
- `include_message_reactions = true` for native thumbs attached to messages
  authored by that provider.

The `provider` must match the identity authenticated by the provider bearer;
the general notification bearer cannot use an agent RPC. The daemon also checks
requested selectors against that provider's `allowed_commands`,
`allow_unmatched_messages`, `allow_card_actions`, and
`allow_message_reactions` before registering the
stream. Legacy streams additionally require `allow_legacy_commands`, and
follow-up sends require `allow_follow_up_messages`. Event candidates are then
filtered by `allowed_apps` before exact-command/fallback arbitration. Grant
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
Group and topic-group text must mention that app's configured bot. By default,
the chat must have a globally unique configured channel alias; an app with
`commands.allow_unconfigured_group_chats = true` also accepts unknown chat ids
through a stable opaque alias. These unconfigured-group routes are agent-only
and do not enter the outbound channel directory, legacy command stream, or
local script executor. `InboundAgentMessage.text`
contains the complete mention-stripped prompt, while `command` and
`command_text` expose an optional bounded first-word view. The daemon omits both
view fields when the command exceeds 64 bytes or its remainder exceeds 8,000
bytes; the complete prompt still reaches unmatched agents up to its independent
32 KiB limit. This prevents language-specific word boundaries from rejecting a
valid conversational message. `conversation_id` is an
opaque hash of the raw chat and optional thread. The `default` app preserves
the historical derivation byte-for-byte; only additional apps are namespaced.
Agent-event metadata is limited to `chat_type`, `message_type`, and the trusted
public `app_alias`. Legacy command metadata remains limited to `chat_type` and
`message_type`. Raw event/chat/message/card ids and callback credentials never
enter the public stream. `delivery_id` and `conversation_id` are stable,
domain-separated opaque handles derived by botd.
An explicit reply adds `InboundAgentMessage.reply_to_message_ref`, derived from
the exact parent message. Group messages may add the current bounded
`conversation_title`; lookup failure leaves it empty. Neither value contains a
raw Feishu route, and the title must never be used as authorization.
An unmentioned group reply is delivered only when the persisted 24-hour owner
record identifies an app-allowed provider; it is pinned to that provider and
cannot enter the legacy command or local-script paths. Unknown and expired
parents are ignored. Feishu delivery of mentionless replies additionally
requires the sensitive `im:message.group_msg` scope; `group_at_msg` only covers
messages that mention the bot.

Native reaction events contain `message_ref`, exact `reaction_type`
(`THUMBSUP` or `ThumbsDown`), and `ADDED` or `REMOVED`. They route only to the
provider that authored that message. `state_dir` is mandatory when any provider
is granted reactions; botd atomically persists the opaque ownership mapping for
24 hours so a restart does not make old messages ownerless. Deleted reactions
are retractions, not opposite votes.

#### Agent response state machine

`StartAgentResponse` accepts `provider`, `delivery_id`, `operation_id`, and an
`AgentResponseContent`. The content contains a title, initial markdown, up
to eight unique actions, and the optional timeline pair below. botd builds a
CardKit JSON 2.0 entity with streaming mode enabled, replies to the inbound
message, and returns an opaque `response_id`, revision `1`, phase `STREAMING`,
and a `duplicate` flag.

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

#### Agent response timeline panel

`AgentResponseContent`, `UpdateAgentResponseRequest`, and
`FinishAgentResponseRequest` each carry an optional `timeline_markdown` and
`timeline_title`. They are additive fields on new numbers; a daemon built
before them ignores them as proto3 unknowns and answers with its ordinary
receipt.

| Field | Semantics |
| --- | --- |
| `timeline_markdown` | The expanded panel body. A complete accumulated snapshot on every request, exactly like `markdown`. |
| `timeline_title` | The collapsed panel header, rendered as plain text. Normally the step currently running; a completed-state line on Finish. |

A response has a collapsible timeline panel if and only if `StartAgentResponse`
carried a non-empty value for either field. That decision is fixed for the
handle's lifetime. On Update and Finish an empty field means "leave that part
unchanged" and a non-empty field replaces it; a value equal to what the card
already shows makes no Feishu call. Both fields are ignored, not rejected, on a
response with no panel.

Sizes: `markdown` and `timeline_markdown` share the single 30 KiB card budget —
Feishu's limit is per card, not per element — measured against whichever part
the request leaves unchanged. `timeline_title` is capped at 200 bytes like
`title` and `summary`. Either overrun returns `field_too_large`.

The panel body is written with the same streaming content API as the answer;
the panel header is written with a `batch_update` `partial_update_element`,
because the content API only writes text elements. One Update can therefore
make up to three Feishu calls and one Finish up to four, all behind the same
125 ms serializer and all yielding exactly one revision. An operation reserves
every call's sequence and UUID on its first attempt and records each as Feishu
accepts it, so a retry with the same `operation_id` repeats only the call still
in doubt. Calls run answer, timeline body, timeline header, settings, so a
partially applied operation never advertises a step the body does not list and
never closes streaming mode early.

Every operation is idempotent within botd's app-scoped process state. A
transport retry must reuse the same `operation_id` and semantic request. A
completed retry returns `duplicate = true`; update and finish retries return
their recorded operation receipt, while a start retry returns the response
handle's current state. Reuse with different fields is an
`operation_conflict`. A stale
`expected_revision` is a `revision_conflict`, a different concurrent mutation
is `operation_in_flight`, and a terminal response is `response_closed`. The
`provider` must own both the inbound delivery and the response handle.

botd enforces one card-wide sequence domain and at least 125 ms between CardKit
mutation attempts (about 8 Hz), leaving margin below Feishu's 10
operations/second/card limit. Providers should still coalesce model tokens into
cumulative snapshots at about 5–8 Hz.
Feishu automatically closes streaming mode after roughly 10 minutes and allows
CardKit entity operations for 14 days. Response handles and operation receipts
remain process-local and expire after the configured dedupe TTL (six hours by
default), so an old card cannot be resumed through its prior `response_id`.
Native-reaction ownership alone is persisted under `state_dir` for 24 hours;
the snapshot is local to one daemon and is not an active-active coordination
store. Run one active botd process owning a given Feishu app.

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

#### Agent follow-up sends

`SendAgentFollowUp` is the one path that writes into a conversation without an
inbound delivery to answer. It accepts `provider`, a `conversation_id` from a
previously delivered `InboundAgentEvent`, an `operation_id`, the complete
`markdown`, and an optional `summary` used as the title and notification
preview. It returns an opaque `follow_up_id` and a `duplicate` flag. There is no
revision and no later edit: this is an ordinary message, not a CardKit entity.

botd records a private, app-scoped reverse map from `conversation_id` to the
owning app, concrete chat, and optional thread each time it delivers an agent
event, and keeps it for the dedupe TTL. The app is resolved from the handle, not
provider metadata, so a follow-up cannot leave through another app's sender.
Three checks run before any Feishu call: the provider bearer must match the
request `provider`, that provider must have
`allow_follow_up_messages` in its config (default false, granted separately from
the ingress selectors because it lets an agent speak unprompted), and it must
have received an agent event for that exact conversation inside the TTL. A
conversation botd cannot route and a conversation the caller was never spoken to
in both return `unknown_conversation`, so the RPC cannot enumerate chats.

A thread-scoped conversation is answered inside its thread; a flat chat or
direct message receives a new top-level message. `markdown` is capped at 30 KiB
and `summary` at 200 bytes. Configured groups resolve through their aliases;
allowed unconfigured groups use the daemon-private ingress route retained for
that conversation.

Idempotency follows the Update/Finish rules: a completed retry of the same
`operation_id` returns `duplicate = true` without sending again, reuse with
different content is an `operation_conflict`, and a concurrent second attempt is
a retryable `operation_in_flight`. The follow-up handle seeds the Feishu message
UUID, so retrying an ambiguous attempt reuses that UUID and cannot post the
message twice; botd returns non-retryable `send_retry_expired` rather than retry
outside Feishu's one-hour UUID window. A definitive rejection with no earlier
ambiguous attempt releases the operation id for corrected content.

Older daemons do not implement this RPC and return `UNIMPLEMENTED`.

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
| 404 `unknown_channel`, `unknown_delivery`, `unknown_response`, `unknown_action`, `unknown_conversation`, `not_found` | `NOT_FOUND` |
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
when the `.proto` files change. Multi-app support is internal routing plus
configuration and does not regenerate these bindings:

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
