# feishu-botd

`feishu-botd` is a small local gateway and agent runtime for Feishu/Lark bots.
One process can own one or more apps, each with its own credentials, bot
identity, sender, and inbound long connection. The daemon owns the Feishu/Lark
SDK, token lifecycle, chat routing, and dynamic CardKit messages. Local services
and agents use a protobuf/gRPC contract without handling raw chat ids or
selecting an app on each request.

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
| `CommandService` | Legacy `Subscribe`/`Respond`, plus agent event streaming and progressive CardKit responses |
| `ProviderService` | future data providers (skeleton) |

The legacy HTTP API stays as a compatibility shim over the **same** internal
service logic:

```text
GET  /healthz
POST /v1/notify   (and GET /readyz)
POST /v1/message  (lower-level markdown/card send)
```

See [docs/ipc.md](docs/ipc.md) for the full gRPC contract and error model, and
[docs/api.md](docs/api.md) for the HTTP shim.

Inbound text commands and conversational agent prompts are supported for each
app whose `commands.enabled` is true and that has bot capability, the required
message/CardKit permissions, and long-connection subscriptions for
`im.message.receive_v1` and (when actions are used) `card.action.trigger`.
Agents can progressively update one CardKit 2.0 response and receive normalized
button callbacks without seeing raw Feishu routing ids or callback tokens.
Long-connection agent mode requires enterprise custom apps and one active
`feishu-botd` process owning each app; Feishu does not broadcast events across
multiple connected clients, and this release keeps ownership state in process.

See [Building a Feishu agent](docs/agent.md) for app setup, routing semantics,
the response state machine, platform limits, and a minimal public-safe Go
lifecycle example under [`examples/agent/`](examples/agent/).

Optionally, `commands.scripts` lets the daemon itself run a local script for
one registered command, instead of (or alongside) an external gRPC provider —
see [Local script execution](#local-script-execution) below.

## Configuration

For Docker/manual deployment, use a config file:

```sh
cp config/feishu-botd.example.json config/feishu-botd.json
$EDITOR config/feishu-botd.json
```

The config file contains Feishu app credentials, listeners, channel aliases,
and per-caller defaults. This example keeps the existing top-level app as
`default` and adds a second app named `support`:

```json
{
  "feishu": {
    "app_id": "REPLACE_WITH_DEFAULT_APP_ID",
    "app_secret": "REPLACE_WITH_DEFAULT_APP_SECRET"
  },
  "apps": {
    "support": {
      "app_id": "REPLACE_WITH_SUPPORT_APP_ID",
      "app_secret": "REPLACE_WITH_SUPPORT_APP_SECRET",
      "commands": {
        "enabled": true,
        "bot_open_id": "REPLACE_WITH_SUPPORT_BOT_OPEN_ID",
        "bot_names": ["Support Bot"]
      },
      "channels": {
        "support": "REPLACE_WITH_SUPPORT_CHAT_ID"
      }
    }
  },
  "listeners": {
    "http_bind": "0.0.0.0:7345",
    "grpc_socket": "/run/feishu-botd/feishu-botd.grpc.sock",
    "auth_token_file": "/run/secrets/feishu-botd-token",
    "allow_non_loopback_bind": true
  },
  "agent_providers": {
    "example-agent": {
      "auth_token_file": "/run/secrets/feishu-botd-example-agent-token",
      "allowed_apps": ["default", "support"],
      "allowed_commands": [],
      "allow_unmatched_messages": true,
      "allow_card_actions": true,
      "allow_follow_up_messages": false,
      "allow_legacy_commands": false
    }
  },
  "commands": {
    "enabled": true,
    "bot_open_id": "REPLACE_WITH_DEFAULT_BOT_OPEN_ID",
    "bot_names": ["Default Bot"]
  },
  "channels": {
    "ci": "REPLACE_WITH_CI_CHAT_ID",
    "ops": "REPLACE_WITH_OPS_CHAT_ID"
  },
  "services": { "build-monitor": { "default_channel": "ci" } }
}
```

The top-level `feishu`, `commands`, and `channels` blocks are the legacy
single-app shape. They remain valid without migration and form the reserved app
alias `default`. They may coexist with `apps`; an explicit `apps.default`
(case-insensitive) is rejected. `apps` may also be used without the legacy
blocks, in which case no `default` app exists. Named aliases are trimmed,
limited to 64 bytes, and must match `[A-Za-z0-9][A-Za-z0-9._-]*`. Every app
needs a distinct `app_id`. Strict unknown-field rejection also applies inside
each app and provider entry.

Commands, bot identity, local scripts, and channel mappings belong to their
app. Channel aliases still form one global namespace: they are trimmed,
lowercased, and have `_` replaced with `-`; duplicates after that normalization,
within or across apps, fail startup. The global `default_channel` and
`services.*.default_channel` settings may point to any alias. Callers keep
sending a bare channel alias; botd resolves it internally to the owning app and
private chat id.

At startup, botd checks every app's credentials, starts one long-connection
receiver per app, and waits for every receiver's first connected state before
opening HTTP or gRPC listeners. Readiness preserves the aggregate checks and
adds `feishu_auth.<alias>` plus `feishu_connection.<alias>`; any app auth
failure or any connection state other than `connected` makes the process
unready. These keys contain public aliases only.

Start with:

```text
FEISHU_BOTD_CONFIG=/etc/feishu-botd/config.json
```

Environment variables are still supported and override the file when set:

```text
FEISHU_APP_ID=REPLACE_WITH_APP_ID
FEISHU_APP_SECRET=REPLACE_WITH_APP_SECRET
FEISHU_BOTD_CHANNELS=ci=REPLACE_WITH_CI_CHAT_ID,ops=REPLACE_WITH_OPS_CHAT_ID
FEISHU_BOTD_BIND=127.0.0.1:7345
FEISHU_BOTD_GRPC_SOCKET=/run/feishu-botd/feishu-botd.grpc.sock
FEISHU_BOTD_AUTH_TOKEN_FILE=/run/secrets/feishu-botd-token
FEISHU_BOTD_COMMANDS_ENABLED=false
FEISHU_BOTD_BOT_OPEN_ID=REPLACE_WITH_BOT_OPEN_ID
FEISHU_BOTD_BOT_NAMES=Example Bot
FEISHU_BOTD_SCRIPTS_ENABLED=false
FEISHU_BOTD_SCRIPTS_COMMAND=ops
FEISHU_BOTD_SCRIPTS_DIR=/etc/feishu-botd/scripts
FEISHU_BOTD_SCRIPTS_ALLOWED_CHATS=ci
FEISHU_BOTD_SCRIPTS_TIMEOUT_SECONDS=300
FEISHU_BOTD_DEDUPE_TTL_SECONDS=21600
FEISHU_BOTD_SEND_TIMEOUT_SECONDS=15
```

The existing credential, command, script, and channel environment variables
apply only to the reserved `default` app; if necessary, credentials from the
environment create that app alongside entries in `apps`. Named apps are
configured only in JSON. This keeps existing single-app files and
`FEISHU_APP_ID` / `FEISHU_APP_SECRET` deployments unchanged.

`agent_providers` is configured in the JSON file because each provider maps to
a separate token file. The general bearer and every provider token must contain
at least 32 bytes. Provider tokens must be unique and must differ from the
general bearer. Token loading trims and uses only the first line of each file.
Existing deployments with a shorter general bearer must rotate it before
upgrading because the daemon now fails closed at startup. The five agent RPCs
require the matching provider token on both Unix and loopback TCP. When this map
is non-empty, legacy command `Subscribe`/`Respond` is scoped the same way; existing
legacy deployments without the map keep their current local/global-token mode.
On scoped Unix listeners, HTTP `POST /v1/notify` and `POST /v1/message` plus
other non-health gRPC RPCs require the distinct general bearer from
`listeners.auth_token_file`; if it is omitted, those outbound interfaces fail
closed. A provider credential never grants outbound notification authority.
HTTP health/readiness and gRPC health remain public.
Each subscription is limited by `allowed_commands`,
`allow_unmatched_messages`, `allow_card_actions`, and
`allow_legacy_commands`; `allow_follow_up_messages` separately permits sending a
later message into a conversation the provider has already answered in.
`allowed_apps` optionally limits that provider to configured app aliases. When
the field is absent, all apps are allowed; an explicit empty list allows none,
and `null` is rejected. botd applies the allowlist before event fan-out and
again when a delivery, response, or conversation handle is mutated.
Plaintext gRPC TCP is loopback-only even when the HTTP
LAN opt-in is enabled.

No HTTP or protobuf request gains an app field. Outbound sends route through
the global channel-alias directory, while replies, CardKit mutations, and
follow-ups route through opaque daemon-issued handles. `InboundAgentEvent`
metadata may include the public `app_alias` key for providers that want to
observe the source app; clients may ignore it and nothing requires it for
routing. `app_id`, `app_secret`, raw chat ids, and SDK connection details remain
inside the daemon and are excluded from provider events and logs.

### Local script execution

Setting `commands.scripts.enabled` lets the daemon run a local script directly
for one registered command word — no separate provider process needed. For a
named app, configure the same block under `apps.<alias>.commands.scripts`. Its
`allowed_chats` entries must be channels owned by that app; a script executor is
never shared across app routes. Given:

```json
"scripts": {
  "enabled": true,
  "command": "ops",
  "dir": "/etc/feishu-botd/scripts",
  "allowed_chats": ["ci"],
  "timeout_seconds": 300
}
```

A message `@<bot-name> ops run example-job staging` in the `ci` chat resolves
to `/etc/feishu-botd/scripts/ops-run.sh example-job staging`: the first word
after the mention (`ops`) must match `command`; the second word (`run`) is the
*action* and is resolved by naming convention to `<command>-<action>.sh` in
`dir`; every remaining word is passed through unmodified as a positional
argument (`example-job`, `staging`) via `argv`, never through a shell — arguments
like `` $(whoami) `` or `; rm -rf` are passed literally and are never
interpreted. The resolved script must live inside `dir` (no path traversal),
exist, and be executable, and the invoking chat must be in `allowed_chats`
(default-deny — no chats are permitted by default). Combined stdout/stderr is
captured (truncated past ~4000 bytes) and replied in chat as the script's exit
code plus output; the script is killed, along with any of its own
subprocesses, if it runs past `timeout_seconds`.

[`scripts/ops-run.example.sh`](scripts/ops-run.example.sh) is an environment-
driven, allowlisted Jenkins example for that command shape. Copy it to
`ops-run.sh` in the configured private script directory and provide credentials
through the service environment; do not commit a deployment copy.

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

Run the standalone container with the checked-in Compose file:

```sh
docker compose up -d --build
```

The base Compose file is notification-only. Running an agent also requires an
`agent_providers` entry and the matching provider token mounted read-only into
both the daemon and agent containers; see the concrete mount example in
[docs/deployment.md](docs/deployment.md#sharing-a-unix-socket-with-another-container).

See [docs/deployment.md](docs/deployment.md) for Unix-socket, TCP, Docker, and
process-manager deployment guidance.
