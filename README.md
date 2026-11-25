# feishu-botd

A lightweight local gateway and conversational agent runtime daemon for Feishu / Lark bots.

`feishu-botd` runs as a single-host sidecar that owns Feishu API credentials, tenant token lifecycles, WebSocket long connections, channel-alias routing, CardKit 2.0 streaming state, and callback acknowledgement across one or more Feishu apps. Local services, CI/CD pipelines, and AI agents talk to `feishu-botd` over protobuf/gRPC (or an HTTP compatibility shim) using stable aliases and opaque handles—without ever handling raw Feishu chat IDs, message IDs, callback tokens, or app secrets.

---

## Features

- **Multi-App Gateway**: Serve a default app alongside any number of named Feishu apps in one process, each with its own credentials, bot identity, sender, long connection, and per-app readiness checks (`feishu_auth.<alias>`, `feishu_connection.<alias>`).
- **Protobuf / gRPC-First IPC**: Strongly typed, language-neutral contracts in [`proto/feishubotd/v1/`](proto/feishubotd/v1) over Unix domain sockets (preferred) or loopback TCP, with no shared client SDK required.
- **HTTP Compatibility Shim**: REST endpoints (`/healthz`, `/readyz`, `/v1/notify`, `/v1/message`) backed by the exact same internal service layer as gRPC.
- **Channel Alias Directory & Email DMs**: Route notifications to globally unique channel aliases (e.g. `ops`, `ci`) or per-service defaults, and optionally send direct messages by recipient email (`target.user_email`) behind explicit domain and source allowlists.
- **Full Conversational & Proactive Agent Runtime**:
  - Push-based event streaming for direct messages, group `@mentions`, mentionless replies to agent-owned messages, interactive card button callbacks, native emoji reactions, and scoped ambient group observation.
  - Progressive CardKit 2.0 streaming responses with collapsible run timeline panels, typed timeline steps, terminal in-place card replacement, threaded replies, and working/agent reactions.
  - Lazy attached context retrieval for thread history, images, videos, files, and interactive cards, plus client-streaming outbound image uploads.
  - Follow-ups, durable scheduled conversation leases, proactive posts, updatable typed task cards, message pins, runtime group creation/adoption/release, and managed group announcement sections.
- **Live Configuration Reload**: Automatically detects edits to the config and bearer-token files (and handles `SIGHUP`), validates before applying, starts/stops/reconnects only changed apps, and resets only modified providers without restarting the daemon.
- **Local Script Execution**: Optionally dispatch `@<bot> <command> <action> [args...]` directly to local shell scripts without running an external agent service.
- **Least-Privilege Security Boundary**: Distinct general and per-provider bearer tokens (≥ 32 bytes), default-off per-provider capability flags, per-provider `allowed_apps` scoping, and AES-256-GCM sealed persistent reply routes under `state_dir`.

---

## Architecture

```text
+-----------------------------------------------------------------------+
|                           Host / Container                            |
|                                                                       |
|  +--------------------+   +----------------------+   +-------------+  |
|  | Microservices / CI |   |  AI Agent Providers  |   | Local Shell |  |
|  +---------+----------+   +----------+-----------+   |   Scripts   |  |
|            | gRPC / HTTP             | gRPC Stream   +------+------+  |
|            | (NotificationSvc)       | (CommandSvc)         |         |
|            v                         v                      v         |
|  +-----------------------------------------------------------------+  |
|  |                           feishu-botd                           |  |
|  |  * Multi-app auth & WS long-conn   * Channel alias directory    |  |
|  |  * CardKit 2.0 streaming & panels  * Per-provider auth & ACLs   |  |
|  |  * Live config reload (watch/HUP)  * Private state_dir stores   |  |
|  +--------------------------------+--------------------------------+  |
+-----------------------------------|-----------------------------------+
                                    | WebSocket / HTTPS
                                    v
                          +-------------------+
                          |   Feishu / Lark   |
                          +-------------------+
```

---

## Quick Start

### 1. Create a bearer token

General bearer tokens (and every agent provider token) must be at least 32 bytes:

```sh
mkdir -p secrets
umask 077 && openssl rand -base64 32 > secrets/feishu-botd-token
```

### 2. Create a configuration file

Create `config/feishu-botd.json` (keep real configs out of version control):

```json
{
  "feishu": {
    "app_id": "cli_a1b2c3d4e5",
    "app_secret": "your_feishu_app_secret"
  },
  "listeners": {
    "http_bind": "127.0.0.1:7345",
    "grpc_socket": "/run/feishu-botd/feishu-botd.grpc.sock",
    "auth_token_file": "secrets/feishu-botd-token"
  },
  "channels": {
    "ops": "oc_1234567890abcdef"
  },
  "default_channel": "ops"
}
```

> For multi-app setups, `agent_providers`, `direct_messages`, `state_dir`, and `commands.scripts`, see [`config/feishu-botd.example.json`](config/feishu-botd.example.json).

### 3. Run the daemon

**From source** (Go 1.22+):

```sh
mkdir -p /tmp/feishu-botd
FEISHU_BOTD_CONFIG=config/feishu-botd.json \
  FEISHU_BOTD_GRPC_SOCKET=/tmp/feishu-botd/feishu-botd.grpc.sock \
  go run ./cmd/feishu-botd
```

**With Docker Compose** (uses `/run/secrets/feishu-botd-token` inside the container and named volumes `feishu-botd-run` and `feishu-botd-state`):

```sh
cp config/feishu-botd.example.json config/feishu-botd.json
cp .env.example .env
# Edit config/feishu-botd.json with your Feishu credentials and chat IDs
docker compose up -d --build
```

**Native macOS `launchd`** (for local agents running outside Docker):

```sh
# Place host-path config at ~/.feishu-botd/config.json, then:
deploy/install.sh
```

See [Deployment Guide](docs/deployment.md) for cross-container Unix socket sharing, LAN binding, and launchd details.

### 4. Verify and send a notification

Check liveness and per-app readiness (unauthenticated):

```sh
curl -fsS http://127.0.0.1:7345/healthz
curl -fsS http://127.0.0.1:7345/readyz
```

Send a deduplicated notification via the HTTP compatibility shim:

```sh
TOKEN="$(cat secrets/feishu-botd-token)"
curl -fsS -X POST http://127.0.0.1:7345/v1/notify \
  -H "Authorization: Bearer ${TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{
    "source": "deploy-cli",
    "source_event_id": "deploy-2026-001",
    "dedupe_key": "deploy-cli:web-gateway:2026-001",
    "severity": "info",
    "title": "Deployment Completed",
    "markdown": "Service **web-gateway** successfully deployed.",
    "target": { "channel": "ops" }
  }'
```

---

## Interfaces

### gRPC Services (`feishubotd.v1`)

Defined in [`proto/feishubotd/v1/`](proto/feishubotd/v1). Served over `FEISHU_BOTD_GRPC_SOCKET` (Unix socket, preferred) and/or `FEISHU_BOTD_GRPC_BIND` (loopback TCP only). Standard `grpc.health.v1.Health` is also registered for `grpc_health_probe`.

| Service | RPCs | Purpose |
| --- | --- | --- |
| `BotdHealthService` | `Health`, `Ready` | Process liveness, per-app auth/connection readiness, and live config reload status. |
| `NotificationService` | `SendNotification`, `SendMessage` | Deduplicated structured notifications and lower-level markdown / raw interactive card sends (including gated `target.user_email` direct messages). |
| `CommandService` (legacy) | `Subscribe`, `Respond` | Group `@mention` first-word command streaming and threaded replies. |
| `CommandService` (agent ingress) | `SubscribeAgentEvents`, `GetAgentAttachedContext`, `UploadAgentImage` | Full-fidelity agent event stream (DMs, mentions, mentionless replies, card actions, reactions, ambient messages), lazy topic/media context snapshots, and outbound image uploads. |
| `CommandService` (agent responses) | `StartAgentResponse`, `UpdateAgentResponse`, `FinishAgentResponse`, `ReplaceAgentResponse`, `AddAgentReaction` | Idempotent CardKit 2.0 streaming responses, collapsible run timeline panels, terminal in-place replacement, and provider-chosen reactions. |
| `CommandService` (follow-ups & leases) | `SendAgentFollowUp`, `RetainAgentConversation`, `ReleaseAgentConversation` | Standalone conversation follow-ups and durable, bounded leases for scheduled tasks. |
| `CommandService` (proactive & chats) | `PostAgentMessage`, `PostAgentCard`, `UpdateAgentCard`, `PinAgentMessage`, `CreateAgentChat`, `InviteAgentChatMembers`, `RenameAgentChat`, `AdoptAgentChat`, `ReleaseAgentChat`, `SetAgentChatAnnouncementSection` | Proactive posts, updatable typed task cards, chat pins, runtime group lifecycle management, and managed group announcement sections. |

See [IPC & gRPC Contract](docs/ipc.md) for the full wire contract, idempotency rules, and unified error vocabulary.

### HTTP API (Compatibility Shim)

Retained for simple webhook-style integrations over Unix socket (`FEISHU_BOTD_SOCKET`) or TCP (`FEISHU_BOTD_BIND`):

- `GET /healthz` — Process liveness.
- `GET /readyz` — Redacted readiness checks (`config`, `feishu_auth.<alias>`, `feishu_connection.<alias>`, `channels`, `dedupe_store`) plus live reload status.
- `POST /v1/notify` — Deduplicated markdown notification to a channel alias (or service default).
- `POST /v1/message` — Lower-level markdown or interactive card message to a channel alias or gated `target.user_email`.

See [HTTP API Reference](docs/api.md) for full request/response schemas.

---

## Conversational Agents & Security Model

`feishu-botd` isolates AI agents from Feishu credentials and raw platform identifiers:

- **Opaque boundary**: Raw `chat_id`, `message_id`, `thread_id`, `card_id`, callback tokens, `app_id`, and `app_secret` never leave the daemon. Providers interact using opaque `delivery_id`, `conversation_id`, `response_id`, `message_ref`, `thread_ref`, and public aliases.
- **Per-provider authentication & least privilege**: Every provider in `agent_providers` has its own bearer token file (distinct from the general notification bearer) and explicit default-off capability flags (`allow_unmatched_messages`, `allow_card_actions`, `allow_attached_context`, `allow_attached_video`, `allow_attached_files`, `allow_image_upload`, `allow_follow_up_messages`, `allow_conversation_leases`, `allow_message_reactions`, `allow_agent_reactions`, `allow_cot_progress`, `allow_ambient_messages`, `allow_proactive_messages`, `allow_pins`, `allow_create_chats`, `allow_adopt_chats`, `allow_announcements`, `allow_sender_email`, `allow_sender_identity`, plus `allowed_apps`).
- **Private durable state (`state_dir`)**: Persists 24-hour message ownership, 7-day AES-256-GCM sealed reply routes (`agent-message-routes.jsonl`), runtime chat grants (`agent-chat-grants.json`), conversation leases (`agent-conversation-leases.json`), and announcement section state (`agent-chat-announcements.json`). Keep `state_dir` private to the daemon.
- **Single active daemon per app**: Run one active `feishu-botd` instance per Feishu app, because Feishu distributes long-connection events across replicas while response/action state is process-local.

See [Building a Feishu Agent](docs/agent.md) for required Feishu app scopes and lifecycle details, and [`examples/agent/`](examples/agent/) for a minimal runnable Go agent.

---

## Live Configuration Reload

When `FEISHU_BOTD_CONFIG` is set, `feishu-botd` polls the config file and every referenced token file every 2 seconds (configurable via `FEISHU_BOTD_CONFIG_WATCH`, or `off` to disable polling) and also reloads on `SIGHUP`:

```sh
docker compose kill -s HUP feishu-botd
# or on macOS launchd:
launchctl kill HUP gui/$(id -u)/technology.wenext.feishu-botd
```

Invalid reloads are rejected without disrupting traffic. Channels, services, `direct_messages`, commands, scripts, apps, and provider tokens/grants apply live; listener addresses, `state_dir`, and `dedupe_ttl_seconds` are flagged under `config.restart_required` in `/readyz` and gRPC `Ready` until restart.

---

## Local Script Execution

For simple operational tasks, an app can execute local scripts directly on `@mention` without an external agent process:

```text
@Bot ops run staging
  │   │    └── Arguments passed to script (argv)
  │   └─────── Action maps to <commands.scripts.dir>/ops-run.sh
  └─────────── Configured command word (commands.scripts.command)
```

Execution is restricted to `commands.scripts.allowed_chats` and bounded by `timeout_seconds`. Captured output is sent back as a threaded reply. See [`scripts/ops-run.example.sh`](scripts/ops-run.example.sh) for an example.

---

## Development

Generated Go protobuf bindings under [`gen/feishubotd/v1/`](gen/feishubotd/v1) are committed, so building and testing require only Go 1.22+.

```sh
make fmt          # Format Go sources with gofmt
make vet          # Run go vet ./...
make test         # Run go test ./...
make race         # Run go test -race ./...
make check        # fmt + test + race + vet
make build        # Build bin/feishu-botd
make image        # Build Docker image (feishu-botd:latest)

# Only needed when proto/ changes (requires buf v1.50.0, protoc-gen-go, protoc-gen-go-grpc):
make proto-lint   # Lint .proto files with buf
make proto        # Regenerate gen/ and gofmt
make proto-check  # Verify committed gen/ matches proto/
```

---

## Documentation

- [Building a Feishu Agent](docs/agent.md) — Feishu app scopes, message routing, CardKit 2.0 streaming, timeline panels, attached media/files, proactive cards, runtime chat grants, announcements, and leases.
- [IPC & gRPC Contract](docs/ipc.md) — Protobuf services, transports, auth rules, idempotency, and error model.
- [HTTP API Reference](docs/api.md) — Compatibility endpoints, readiness payload, and request examples.
- [Deployment Guide](docs/deployment.md) — Local source runs, Docker Compose, cross-container Unix sockets, live reload, and macOS `launchd`.
- [ADR 0001: Runtime Chat Grants](docs/adr/0001-runtime-chat-grants.md) — Design record for provider-created and adopted group chat grants.
