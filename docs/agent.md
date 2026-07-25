# Building a Feishu agent

`feishu-botd` can be the Feishu-facing runtime for a local agent. The daemon
owns the app credentials, long connection, chat routing, CardKit entities, and
callback acknowledgement. The agent process sees only the protobuf contract
over local gRPC:

```text
Feishu message -> feishu-botd -> SubscribeAgentEvents -> agent
Feishu card    <- feishu-botd <- Start / Update / Finish <- agent
Feishu action  -> feishu-botd -> SubscribeAgentEvents -> agent
```

Raw chat IDs, message IDs, card IDs, callback tokens, app credentials, and
tenant keys never cross the provider boundary. Treat `sender_id`, message text,
form values, and action payloads as user data and avoid logging them by default.

## Configure the Feishu app

In the [Feishu developer console](https://open.feishu.cn/app), use an enterprise
custom app (long-connection delivery does not support store apps):

1. Add the bot capability.
2. Grant `im:message:send_as_bot` and `cardkit:card:write`.
3. For direct messages, grant `im:message.p2p_msg:readonly`. For group mentions,
   grant `im:message.group_at_msg:readonly`. Broader message scopes are not
   required for the routing implemented here.
4. Under **Events and Callbacks**, choose long-connection delivery and subscribe
   to the `im.message.receive_v1` event.
5. To receive button callbacks, also subscribe to the
   `card.action.trigger` callback using long-connection delivery.
6. Publish a new app version, make the bot available to the intended users, and
   add it to every group in which it should answer.

The callback subscription has no additional permission scope. Feishu requires
callback acknowledgement within three seconds; `feishu-botd` acknowledges the
callback itself and delivers a normalized event to the agent asynchronously.

Useful primary references:

- [Receive message event](https://open.feishu.cn/document/uAjLw4CM/ukTMukTMukTM/reference/im-v1/message/events/receive)
- [Card action callback](https://open.feishu.cn/document/uAjLw4CM/ukzMukzMukzM/feishu-cards/card-callback-communication)
- [CardKit streaming updates](https://open.feishu.cn/document/cardkit-v1/streaming-updates-openapi-overview)
- [Create a CardKit entity](https://open.feishu.cn/document/cardkit-v1/card/create)
- [Receive events over a long connection](https://open.feishu.cn/document/server-docs/event-subscription-guide/event-subscription-configure-/request-url-configuration-case)

## Configure and run the daemon

Enable inbound commands and a Unix gRPC listener. Static channels are optional
for a direct-message-only agent; group messages are accepted only from chat IDs
that have a unique configured alias.

Generate one provider credential. Keep the token file outside version control
and make it readable only by the daemon and that provider process:

```sh
umask 077
openssl rand -base64 32 > /run/secrets/feishu-botd-example-agent-token
```

```json
{
  "feishu": {
    "app_id": "REPLACE_WITH_APP_ID",
    "app_secret": "REPLACE_WITH_APP_SECRET"
  },
  "listeners": {
    "grpc_socket": "/run/feishu-botd/feishu-botd.grpc.sock"
  },
  "agent_providers": {
    "example-agent": {
      "auth_token_file": "/run/secrets/feishu-botd-example-agent-token",
      "allowed_commands": [],
      "allow_unmatched_messages": true,
      "allow_card_actions": true,
      "allow_legacy_commands": false
    }
  },
  "commands": {
    "enabled": true,
    "bot_open_id": "REPLACE_WITH_BOT_OPEN_ID",
    "bot_names": ["Example Agent"]
  },
  "channels": {
    "support": "REPLACE_WITH_SUPPORT_CHAT_ID"
  }
}
```

Keep the real config outside version control, restrict its permissions, and run
the daemon with `FEISHU_BOTD_CONFIG` pointing to it. An agent does not need the
Feishu app ID, app secret, general notification bearer, or raw routing IDs.
Each configured provider token must contain at least 32 bytes, be unique, and
differ from the general bearer token.

The runnable Go example subscribes to unmatched messages and card actions,
then streams cumulative echo snapshots:

```sh
go run ./examples/agent \
  -socket /run/feishu-botd/feishu-botd.grpc.sock \
  -provider example-agent \
  -token-file /run/secrets/feishu-botd-example-agent-token
```

`FEISHU_BOTD_GRPC_SOCKET`, `FEISHU_BOTD_AGENT_PROVIDER`, and
`FEISHU_BOTD_AGENT_AUTH_TOKEN_FILE` are equivalent to the three flags. The
example reads the credential from a file; do not pass a raw token on the command
line or place one in source. Replace `buildAnswer` in
[`examples/agent/main.go`](../examples/agent/main.go) with a model call for a
real agent. Keep model/API credentials in the agent process, never in card
content or action payloads.

This example is deliberately a minimal lifecycle demonstration, not a
production reliability template. It logs only that an action arrived; it does
not start a new response from an action. It also omits stream reconnection,
exact-operation retries, and best-effort failure finalization. Production
agents must implement the retry and `FinishAgentResponse` rules below.

Provider authentication is mandatory for all agent RPCs on both Unix sockets
and loopback TCP. The request's `provider` must match the identity bound to the
bearer credential; a general notification bearer cannot subscribe, claim a
delivery, or mutate an agent response. With `agent_providers` configured, a
Unix peer also needs the distinct general bearer to call outbound
`NotificationService` RPCs or the HTTP shim's `POST /v1/notify` and
`POST /v1/message` routes. Set `listeners.auth_token_file` (or
`FEISHU_BOTD_AUTH_TOKEN_FILE`) only for callers that need that authority; when
it is omitted, those outbound interfaces fail closed while provider and health
interfaces remain available. The legacy Unix local-trust behavior remains only
when no provider map is configured. Plaintext gRPC TCP is never served on a
non-loopback address. For a cross-host agent, use a shared Unix socket where
appropriate or an authenticated encrypted tunnel/TLS proxy that terminates
onto the daemon's loopback listener.

## Message routing

`SubscribeAgentEvents` has two independent message selectors:

- `commands` selects normalized first-word commands. Exact command subscribers
  win over unmatched subscribers.
- `include_unmatched_messages` is the natural conversational mode. It receives
  direct messages and group mentions for which there is no exact command
  subscriber.

An existing legacy `Subscribe` consumer takes precedence when it successfully
receives the same group command. Multiple providers matching the same agent
message receive their own copy, but the first valid `StartAgentResponse`
attempt claims the response even if its Feishu call later fails. Use
non-overlapping command ownership, or run one unmatched agent provider, to
avoid races.

The first successful ingress route is sticky for the dedupe lifetime. A Feishu
retry does not switch from a legacy consumer to an agent, from one selector to
another, or from an agent back to legacy after subscriber churn.

Each provider can request only the selectors authorized in its config:
`allowed_commands`, `allow_unmatched_messages`, and `allow_card_actions`.
`allow_legacy_commands` separately permits legacy `Subscribe`/`Respond`, using
the same `allowed_commands` allowlist. Requests outside this scope fail before
broker registration. Grant only the minimum selectors needed; unmatched-message
access includes direct-message prompts. Card actions are additionally restricted
to the provider that owns the response.

Routing differs by chat type:

| Chat type | Accepted input | Public route |
| --- | --- | --- |
| Direct (`p2p`) | Any non-empty text; no mention required | `chat_alias = "direct"` |
| Group / topic group | Text from a uniquely configured channel alias that mentions this bot | Configured alias |

Group routing also requires a configured bot identity (`bot_open_id`,
`bot_user_id`, `bot_union_id`, or a bot name). Strong Feishu IDs are
authoritative: when one is configured, a same-named mention with a different ID
is rejected. Direct-message routing does not require a mention identity.

`InboundAgentMessage.text` is the complete mention-stripped prompt and preserves
newlines. `command` is the normalized first word and `command_text` is its
remainder. `conversation_id` is a stable, opaque hash of the chat and optional
thread; raw routing IDs are retained only inside the daemon. Public metadata is
limited to `chat_type` and `message_type`.

## Progressive response lifecycle

The agent response RPCs form a strict state machine:

| RPC | Input identity | Result |
| --- | --- | --- |
| `StartAgentResponse` | `provider`, inbound `delivery_id`, `operation_id` | Creates and replies with one CardKit 2.0 entity; successful revision is `1` |
| `UpdateAgentResponse` | `provider`, opaque `response_id`, `operation_id`, current `expected_revision` | Replaces the markdown element with a cumulative snapshot and increments the revision |
| `FinishAgentResponse` | Same identity plus outcome, final markdown, and optional summary | Applies final content if needed, disables streaming mode, and increments the revision once |

The provider must carry the returned `response_id` and `revision` forward. Send
the complete accumulated markdown on every update—never a token delta. Feishu's
typewriter behavior depends on the previous text being a prefix of the next
snapshot.

Every mutation needs a provider-generated `operation_id`. On an ambiguous
transport failure, retry the same semantic request with the same ID. A
completed retry returns `duplicate = true`; update and finish retries return
their recorded operation receipt, while a start retry returns the response
handle's current state. Reusing an ID with different fields returns
`operation_conflict`. A new operation whose `expected_revision` does not equal
the latest receipt returns `revision_conflict`. Only one operation may be in
flight for a response, and a finished response rejects later updates.

Transport failures, HTTP 5xx, Feishu IM code `230020`, CardKit codes `200810`
and `300120`, and HTTP 429 return retryable `feishu_unavailable`. Retry the
byte-equivalent semantic request with the same `operation_id`; Start also
reuses the same Feishu message UUID. HTTP 429, `230020`, and `200810` are
definitive temporary rejections (`200810` means a card interaction is in
progress). Transport failures, HTTP 5xx, and `300120` are ambiguous because the
platform may have committed the operation before the error was observed.

A definitive non-retryable business rejection on the first attempt returns
`feishu_rejected` and releases the uncommitted delivery or operation, allowing
a corrected request with a new operation ID at the unchanged revision. A
definitive rejection after an ambiguous attempt cannot prove whether that
earlier attempt committed. botd therefore fails closed with
`send_state_unknown` for Start or `operation_state_unknown` for Update/Finish,
retains the claim and exact operation identity, and rejects replacements.

Feishu deduplicates an IM message UUID for one hour. After an ambiguous Start,
botd refuses another attempt when its send timeout would reach that window and
returns non-retryable `send_retry_expired`; it makes no new Feishu call and
retains the delivery claim. These unknown/expired states require operator
reconciliation rather than a new operation.

The outcome must be `COMPLETED`, `FAILED`, or `CANCELLED`. `FinishAgentResponse`
is required even when generation fails: it closes CardKit streaming mode and
sets the final preview summary.

`StartAgentResponse.content.actions` may contain up to eight unique buttons.
Each has an `action_id`, label, optional JSON-object payload, and default,
primary, or danger style. Set `include_card_actions = true` on the provider's
subscription to receive them.

## Card actions

An `InboundCardAction` is routed only to the provider that owns the originating
response. It contains the opaque `response_id`, provider-defined `action_id`,
and normalized `payload_json`. The payload preserves typed callback fields:
`tag`, `name`, `option`, `timezone`, `input_value`, `options`, `checked`,
`value`, and `form_value`.

The response API authors buttons only. `name` is preserved as Feishu's
independent form-component identifier and `form_value` is normalized for
forward compatibility with callbacks; this release cannot author a form card.
An action event can identify and mutate its existing response only while that
response is still streaming. It cannot be used as a `StartAgentResponse`
delivery, and an action clicked after Finish is an event for the provider to
handle outside the closed card lifecycle.

The callback token and Feishu card, message, and chat IDs are intentionally
omitted. Do not place secrets or raw routing identifiers in a button's
provider-defined payload: Feishu sends that value back to the callback and the
agent receives it unchanged inside `value.payload`.

## Limits and persistence

- Feishu permits at most 10 CardKit card/component operations per second per
  card. `feishu-botd` serializes mutations and spaces attempted calls by at
  least 125 ms (about 8 Hz), leaving margin below that ceiling. Agents should
  still coalesce tokens into snapshots around 5–8 Hz; the example uses 250 ms.
- Feishu automatically closes streaming mode about 10 minutes after it was
  enabled. Finish comfortably before that deadline; do not treat auto-close as
  successful completion.
- A CardKit entity can be operated on for 14 days and can be sent only once.
  Card creation itself has no Feishu idempotency key, so an ambiguous create
  failure can leave an unused entity that botd cannot recover. Once botd has a
  `card_id`, a send retry reuses that card and the same message UUID.
- Agent deliveries, response handles, operation receipts, action ownership,
  and callback deduplication are in memory. They expire after the configured
  dedupe TTL (six hours by default) and are lost when the daemon restarts.
  After restart, an old `response_id` cannot be resumed even if Feishu still
  retains the card. Restart does not disable streaming on the remote card;
  Feishu eventually auto-closes it. Finish active responses before planned
  restarts.
- Run exactly one active `feishu-botd` long-connection client per Feishu app.
  Feishu permits up to 50 connections but delivers each event to one random
  client in cluster mode, not to every client. Because response/action
  ownership is process-local, active-active replicas can split a message and
  its later action across different state stores. Shared durable routing is
  required before active-active operation is safe.
- Ingress is best-effort and process-local: botd acknowledges Feishu after a
  non-blocking provider enqueue and has no provider ACK/replay log. A provider
  disconnect, full queue, or process restart can lose an event; deduplication
  prevents duplicate handling but does not provide exactly-once delivery.
- When `agent_providers` is configured, startup requires the dedupe TTL to be
  at least one hour plus `send_timeout`. This retains an ambiguous Start claim
  through Feishu's one-hour IM message-UUID retry window.
- Generated cards are limited to 30 KiB by the daemon. Keep final answers below
  that bound and summarize or link to larger artifacts.

## Verification boundary

`go test ./...` verifies protocol behavior, routing, callback normalization,
CardKit request construction, revisions, and retry semantics against test
doubles. The 125 ms mutation interval is a runtime invariant rather than a
wall-clock test. Tests do not prove that a particular Feishu tenant has published
the correct app version, granted the scopes, installed the bot, or enabled both
long-connection subscriptions.

Before production use, run a live-tenant smoke test: send one direct message,
send one mentioned message in a configured group, observe multiple cumulative
card snapshots, finish the response, and click an action button. Confirm nominal
events reach the intended provider, exercise retry/queue-failure behavior, and
verify that no raw identifiers or user content appear in logs.
