# Building a Feishu agent

`feishu-botd` can be the Feishu-facing runtime for a local agent. One daemon can
own several Feishu apps, each with independent credentials, bot identity,
sender, and long connection. The daemon owns app selection, chat routing,
CardKit entities, and callback acknowledgement. The agent process sees only the
unchanged protobuf contract over local gRPC:

```text
Feishu message -> feishu-botd -> SubscribeAgentEvents -> agent
Feishu card    <- feishu-botd <- Start / Update / Finish <- agent
Feishu action  -> feishu-botd -> SubscribeAgentEvents -> agent
Feishu reaction -> feishu-botd -> SubscribeAgentEvents -> agent
```

Raw chat IDs, message IDs, card IDs, callback tokens, app credentials, and
tenant keys never cross the provider boundary. App aliases are public and may
appear in provider-safe metadata; `app_id` and `app_secret` values are not
exposed to providers or logs. Treat `sender_id`, message text, form values, and
action payloads as user data and avoid logging them by default.

## Configure the Feishu apps

For every app served by this process, use an enterprise custom app in the
[Feishu developer console](https://open.feishu.cn/app) (long-connection
delivery does not support store apps):

1. Add the bot capability.
2. Grant `im:message:send_as_bot` and `cardkit:card:write`.
3. For direct messages, grant `im:message.p2p_msg:readonly`. For group mentions,
   grant `im:message.group_at_msg:readonly`. Broader message scopes are not
   required for the routing implemented here.
4. Under **Events and Callbacks**, choose long-connection delivery and subscribe
   to the `im.message.receive_v1` event.
5. To receive button callbacks, also subscribe to the
   `card.action.trigger` callback using long-connection delivery.
6. To receive native reactions attached to agent answers, grant the message
   reaction read permission and subscribe to both
   `im.message.reaction.created_v1` and `im.message.reaction.deleted_v1`.
   To expose the current group title, grant the permission required by
   `GET /open-apis/im/v1/chats/:chat_id`; title lookup is fail-soft.
7. Publish a new app version, make the bot available to the intended users, and
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

Enable inbound commands for the app and configure a Unix gRPC listener. Static
channels are optional for a direct-message-only agent. By default, group
messages are accepted only from chat IDs that have a globally unique configured
alias. An app can explicitly opt into mentioned messages from unconfigured
groups as described under Message routing below.

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
  "state_dir": "/var/lib/feishu-botd",
  "agent_providers": {
    "example-agent": {
      "auth_token_file": "/run/secrets/feishu-botd-example-agent-token",
      "allowed_apps": ["default"],
      "allowed_commands": [],
      "allow_unmatched_messages": true,
      "allow_card_actions": true,
      "allow_follow_up_messages": false,
      "allow_message_reactions": true,
      "allow_legacy_commands": false
    }
  },
  "commands": {
    "enabled": true,
    "allow_unconfigured_group_chats": false,
    "bot_open_id": "REPLACE_WITH_BOT_OPEN_ID",
    "bot_names": ["Example Agent"]
  },
  "channels": {
    "support": "REPLACE_WITH_SUPPORT_CHAT_ID"
  }
}
```

This is the unchanged legacy single-app shape. It becomes the reserved app
alias `default`. To serve more apps, add a top-level `apps` map whose entries
contain their own `app_id`, `app_secret`, `commands`, and `channels`; see the
full coexistence example in
[`config/feishu-botd.example.json`](../config/feishu-botd.example.json).
The top-level `feishu`, `commands`, and `channels` blocks may coexist with
`apps`, but `apps.default` is reserved and rejected. Existing Feishu credential,
command, script, and channel environment variables affect only `default`; named
apps come from JSON.

Channel aliases are global, not per-provider or per-request. They are trimmed,
lowercased, and have `_` replaced with `-`; each normalized alias must occur
exactly once across all apps. That gives a bare alias one unambiguous internal
route to an app and chat. Duplicate app ids and duplicate channel aliases fail
startup. Per-app bot ids and names are used only for mentions received by that
app.

The process constructs one sender and one long-connection receiver for every
configured app, including apps with inbound commands disabled. Before opening
HTTP or gRPC listeners, it checks every credential set, starts every receiver,
and waits for every long connection to reach its first connected state. A bad
credential or initial connection failure therefore fails the whole daemon
rather than leaving a partially serving app set.

After startup, readiness keeps the aggregate `feishu_auth` check and adds
`feishu_auth.<alias>` plus `feishu_connection.<alias>` for every app.
Connection values are `starting`, `connected`, `reconnecting`, `disconnected`,
or `unavailable`; any value other than `connected` makes the daemon unready.
The check names contain only public aliases, never credential or SDK error
text. `/healthz` and standard gRPC health remain static liveness.

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

`allowed_apps` is an optional provider allowlist of public app aliases. An
absent field preserves the existing permissive behavior and allows every app;
an explicit `[]` allows none. `null` and unknown aliases fail strict config
loading. The daemon filters disallowed apps before event fan-out, then resolves
each opaque delivery, response, or conversation handle and checks its owning app
again for every mutation. A denied foreign-app handle returns the same
`unknown_delivery`, `unknown_response`, or `unknown_conversation` response as an
unknown handle, so the allowlist cannot be used to enumerate another app's
state.

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
`allowed_commands`, `allow_unmatched_messages`, `allow_card_actions`, and
`allow_message_reactions`.
`allow_legacy_commands` separately permits legacy `Subscribe`/`Respond`, using
the same `allowed_commands` allowlist, and `allow_follow_up_messages` separately
permits the follow-up send described below. Requests outside this scope fail
before broker registration. Event selection also considers `allowed_apps`: a
disallowed exact-command subscriber cannot suppress an allowed unmatched
subscriber. Grant only the minimum selectors needed; unmatched-message access
includes direct-message prompts. Card actions are additionally restricted to
the provider that owns the response.

Routing differs by chat type:

| Chat type | Accepted input | Public route |
| --- | --- | --- |
| Direct (`p2p`) | Any non-empty text; no mention required | `chat_alias = "direct"` |
| Configured group / topic group | Text that mentions this app's bot | Configured alias |
| Unconfigured group / topic group | Mentioned text when this app sets `commands.allow_unconfigured_group_chats = true` | Stable opaque alias |

Group routing also requires a configured bot identity (`bot_open_id`,
`bot_user_id`, `bot_union_id`, or a bot name). Strong Feishu IDs are
authoritative: when one is configured, a same-named mention with a different ID
is rejected. Direct-message routing does not require a mention identity.

`allow_unconfigured_group_chats` is per app and defaults to `false`. When it is
enabled, only messages that mention that app's configured bot identity are
accepted; merely adding the bot to a group is not enough. These dynamic routes
are agent-only and cannot trigger legacy command subscribers or local scripts.
They also do not populate the static channel directory used by outbound
notification APIs. The daemon retains the raw chat id privately so the agent
can start a response and send authorized conversation follow-ups; providers see
only opaque delivery, conversation, and chat-alias values. This setting grants
every member of every invited group that can mention the bot access to the
agent, so deployments with corpus or user-level authorization requirements
should keep explicit channel aliases instead.

`InboundAgentMessage.text` is the complete mention-stripped prompt and preserves
newlines. `command` is the normalized first word and `command_text` is its
remainder. `conversation_id` is a stable, opaque hash of the chat and optional
thread; raw routing IDs are retained only inside the daemon. The reserved
`default` app keeps the pre-multi-app conversation-id derivation byte-for-byte
so durable provider state continues to resolve. Only additional apps are
namespaced into the derivation.

For an explicit Feishu reply, `reply_to_message_ref` is the app-scoped opaque
identity of the exact parent message. It is empty for a non-reply. Group
messages also carry the current, trimmed `conversation_title` when the bounded
five-minute lookup succeeds. The title is user-controlled context, not an
authorization value; direct messages and failed lookups carry an empty title.

When `include_message_reactions = true`, the stream also receives native
`THUMBSUP` and `ThumbsDown` reactions attached to messages authored by that
same provider. Each event carries an opaque `message_ref`, the reacting
`sender_id`, and `ADDED` or `REMOVED`; removal retracts the prior reaction and
does not assert its opposite. Unknown messages are not broadcast. This selector
requires `allow_message_reactions = true` and a configured `state_dir` so owner
routing survives restart. Ownership expires after 24 hours.

`InboundAgentEvent.metadata` may contain the provider-safe keys `chat_type`,
`message_type`, and `app_alias`; daemon ingress supplies the source
`app_alias`. The alias is informational: old clients may ignore the additive
map entry, and no request or routing decision requires a provider to echo it.
Legacy `InboundCommand.metadata` remains limited to `chat_type` and
`message_type`. Raw event, message, thread, root, parent, card, and chat ids are
still omitted from both public streams.

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
snapshot. Update and Finish also carry the optional timeline fields described
in [the run timeline panel](#the-run-timeline-panel).

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

## The run timeline panel

An answer and the steps that produced it do not belong in the same markdown
snapshot: the step log pushes the answer down the card and keeps moving while
the user is trying to read it. A response can instead carry its timeline in a
collapsible panel above the answer. Collapsed — the default — the panel header
is the step currently running; expanded, it shows the timeline markdown.

The panel is optional and decided once. It exists for the response's whole
lifetime if and only if `StartAgentResponse.content` carried a non-empty
`timeline_markdown` or `timeline_title`. A provider that sends neither gets the
byte-identical card it got before this feature existed.

| Field | Where | Meaning |
| --- | --- | --- |
| `timeline_markdown` | Start, Update, Finish | The expanded panel body. Like `markdown`, always the complete accumulated snapshot, never a delta. |
| `timeline_title` | Start, Update, Finish | The collapsed panel header. Rendered as plain text, so step text cannot inject card markup. |

On Update and Finish each field is independent and **empty means "leave that
part unchanged"**, so a provider can advance the header without resending the
body, or the reverse. A non-empty value replaces that part; a value that
already matches the card is not sent to Feishu again. A panel-less response
ignores both fields rather than rejecting them, so one provider implementation
can talk to both card shapes. `FinishAgentResponse` keeps the last streaming
header unless `timeline_title` carries the completed-state line the run should
settle on, such as `Completed in 12 steps`.

Sizes are shared, because Feishu's ceiling applies to the rendered card rather
than to any one element: the answer `markdown` and the panel's
`timeline_markdown` together must stay within the 30 KiB card limit, measured
against whatever the request leaves unchanged. `timeline_title` is capped at
200 bytes like `title` and `summary`. Both overruns return `field_too_large`.

Budget for the card, not for the fields. `StartAgentResponse` additionally
rejects a card whose **serialized JSON** exceeds 30 KiB, and that JSON carries
scaffolding the field lengths do not: measured at roughly 150 bytes for a
plain card, 750 with a timeline panel and a title, and 1200 with two action
buttons as well. A provider that fills the field budget exactly is therefore
rejected before its first card is ever created — this predates the panel, but
a panel widens the gap. Keep total content near 29 KiB and clip toward
whichever part matters less. Note too that Go's JSON encoder escapes `<` and
`>`, so an `<br>` costs 14 bytes inside the card JSON while counting as 4
against the field gate.

Line breaks are the panel author's problem, not botd's: Feishu renders a
single `\n` as a *soft* break that the client may collapse, so step lines
joined by one newline can run together into a paragraph. Use `<br>`, a
markdown list, or a blank line between entries.

Applying a timeline costs extra CardKit operations. The panel body streams
through the same content API as the answer, which is what preserves the
typewriter effect, while the header moves through a `batch_update`
`partial_update_element`. One Update can therefore make up to three Feishu
calls and one Finish up to four; they stay behind the same 125 ms serializer
and still produce exactly one revision. Coalesce accordingly — an agent that
advances both the answer and the timeline on every tick should aim nearer
2–3 Hz than 8 Hz.

Retries are unchanged in shape. An operation reserves the sequence numbers and
idempotency UUIDs for all of its calls on its first attempt and records each
call as Feishu accepts it, so retrying an ambiguous failure with the same
`operation_id` repeats only the call that is still in doubt. The calls run in
the order answer, timeline body, timeline header, then settings: a partially
applied operation can leave the header lagging the steps it summarizes but
never leading them, and streaming mode is disabled only after every content
call has landed.

A daemon built before this feature ignores the fields as proto3 unknowns and
returns its ordinary receipt, so a provider cannot tell a panel was dropped.
Treat the panel as presentation, not as a channel for anything the answer does
not also say.

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

## Follow-up sends

A CardKit response is closed by `FinishAgentResponse` and Feishu auto-closes
streaming mode after about ten minutes, so a long or detached run cannot report
back through the card it started. `SendAgentFollowUp` posts one later,
standalone message into the same conversation instead.

The request carries `provider`, the `conversation_id` from any previously
delivered `InboundAgentEvent`, an `operation_id`, the complete `markdown`, and
an optional `summary` used as the message title and notification preview. The
response returns an opaque `follow_up_id` and a `duplicate` flag. There is no
revision: a follow-up is an ordinary message and cannot be edited afterwards.

botd records an app-scoped reverse map from `conversation_id` to the concrete
route each time it delivers an agent event, and resolves both the app and chat
privately at send time. A follow-up for a conversation received through app A
can therefore use only app A's sender, even when another app has colliding raw
Feishu ids. Three checks run before any Feishu call:

1. The provider bearer must match the request `provider`.
2. That provider must have `allow_follow_up_messages` in its config. It defaults
   to false; granting it lets the agent start a message the user did not
   prompt, so it is deliberately separate from the ingress selectors.
3. That provider must have received an agent event for that exact conversation
   within the dedupe TTL. Scope follows the conversation, not the daemon: an
   agent can only follow up where it was spoken to.

A conversation botd has no route for and a conversation the caller was never
spoken to in both return `unknown_conversation`, so the RPC cannot be used to
probe which chats exist.

Message shape and limits:

- A thread-scoped conversation is answered inside its thread. A flat chat or
  direct message gets a new top-level message rather than a reply threaded under
  a prompt the user scrolled past hours ago.
- `markdown` is capped at 30 KiB and `summary` at 200 bytes. Long bodies are
  split across several Feishu messages by the ordinary send path.
- A configured group conversation routes through its channel alias. Removing
  that alias from config makes the conversation unaddressable. An explicitly
  allowed unconfigured group uses the private ingress route retained for the
  same process-local conversation lifetime.

Retries follow the same rules as `UpdateAgentResponse` and
`FinishAgentResponse`. Reuse the `operation_id` for the byte-equivalent request:
a completed retry returns `duplicate = true` without sending again, and reuse
with different content returns `operation_conflict`. A second attempt while the
first is still in flight returns retryable `operation_in_flight`. The follow-up
handle seeds the Feishu message UUID, so an ambiguous attempt that Feishu
silently accepted cannot be posted twice by its retry; botd refuses a retry that
would fall outside Feishu's one-hour UUID deduplication window and returns
non-retryable `send_retry_expired`. A definitive rejection with nothing in doubt
releases the operation id so corrected content can reuse it.

Conversation routes, grants, and follow-up receipts are in memory alongside the
rest of the agent state and are lost on restart. A daemon built before this RPC
returns `UNIMPLEMENTED`; treat that as "no follow-up channel available" and
degrade to finishing inside the original card.

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
  conversation routes, follow-up grants, and callback deduplication are in
  memory. They expire after the configured dedupe TTL (six hours by default) and
  are lost when the daemon restarts.
  After restart, an old `response_id` cannot be resumed even if Feishu still
  retains the card, and a `conversation_id` cannot be followed up until that
  conversation speaks to the agent again. Restart does not disable streaming on
  the remote card; Feishu eventually auto-closes it. Finish active responses
  before planned restarts.
- Native-reaction provider ownership is the exception: an atomic snapshot under
  `state_dir` retains only opaque `message_ref` to provider mappings for 24
  hours. A malformed snapshot fails startup rather than widening delivery.
- Run exactly one active `feishu-botd` process owning a given Feishu app. One
  multi-app process opens one connection for each configured app. Feishu permits
  up to 50 connections but delivers each event to one random client in cluster
  mode, not to every client. Because response/action ownership is process-local,
  active-active replicas can split a message and its later action across
  different state stores. Shared durable routing is required before
  active-active operation is safe.
- Ingress is best-effort and process-local: botd acknowledges Feishu after a
  non-blocking provider enqueue and has no provider ACK/replay log. A provider
  disconnect, full queue, or process restart can lose an event; deduplication
  prevents duplicate handling but does not provide exactly-once delivery.
- When `agent_providers` is configured, startup requires the dedupe TTL to be
  at least one hour plus `send_timeout`. This retains an ambiguous Start claim
  through Feishu's one-hour IM message-UUID retry window.
- Generated cards are limited to 30 KiB by the daemon. Keep final answers below
  that bound and summarize or link to larger artifacts. A timeline panel shares
  that one budget with the answer rather than adding to it.

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
