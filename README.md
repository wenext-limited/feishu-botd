# feishu-botd

A lightweight gateway and conversational agent runtime daemon for Feishu / Lark bots.

`feishu-botd` acts as a local sidecar: it manages Feishu API connectivity, WebSocket long connections, token lifecycles, and chat routing. Local microservices, CI/CD pipelines, and AI agents interact with `feishu-botd` over high-performance gRPC or HTTP using clean channel aliases, without handling raw Feishu chat IDs or credentials.

---

## Features

- **Multi-App Gateway**: Manage multiple Feishu bot identities within a single daemon process.
- **Protobuf / gRPC First**: Strongly typed, language-agnostic contracts ([`proto/feishubotd/v1/`](proto/feishubotd/v1)) over Unix sockets or loopback TCP.
- **HTTP Compatibility Shim**: REST endpoints (`/v1/notify`, `/v1/message`) for webhook-style integrations.
- **Conversational Agent Runtime**: Native bidirectional streaming for AI agents, supporting real-time CardKit 2.0 progressive updates and interactive button callbacks.
- **Channel Alias Directory**: Send messages to logical names (e.g. `ops`, `ci`) mapped internally to private Feishu chat IDs.
- **Local Script Dispatch**: Optionally trigger local scripts directly from bot `@mentions` without running an external agent service.
- **Secure by Default**: Per-provider token authentication, privilege sandboxing, and opaque conversation handles.

---

## Architecture

```
+-------------------------------------------------------------+
|                      Host / Container                       |
|                                                             |
|  +--------------------+         +-----------------------+   |
|  | Microservices / CI |         |   AI Agent Services   |   |
|  +---------+----------+         +-----------+-----------+   |
|            | HTTP / gRPC                    | gRPC Stream   |
|            | (/v1/notify)                   | (CommandSvc)  |
|            v                                v               |
|  +-------------------------------------------------------+  |
|  |                      feishu-botd                      |  |
|  |  * Token lifecycle       * Channel alias directory    |  |
|  |  * Long-conn receiver    * CardKit 2.0 streaming     |  |
|  +-----------------------------+-------------------------+  |
+--------------------------------|----------------------------+
                                 | WebSocket / HTTPS
                                 v
                       +-------------------+
                       |    Feishu / Lark  |
                       +-------------------+
```

---

## Quick Start

### 1. Configuration

Create a configuration file `config/feishu-botd.json`:

```json
{
  "feishu": {
    "app_id": "cli_a1b2c3d4e5",
    "app_secret": "your_feishu_app_secret"
  },
  "channels": {
    "ops": "oc_1234567890abcdef"
  },
  "listeners": {
    "http_bind": "127.0.0.1:7345",
    "grpc_socket": "/run/feishu-botd/feishu-botd.grpc.sock"
  }
}
```

> For multi-app configurations, agent provider permissions, and script runner settings, see [`config/feishu-botd.example.json`](config/feishu-botd.example.json).

### 2. Run the Daemon

**With Docker Compose:**

```sh
# Create a general auth token
mkdir -p secrets && openssl rand -base64 32 > secrets/feishu-botd-token

# Start feishu-botd
docker compose up -d
```

**From Source:**

```sh
FEISHU_BOTD_CONFIG=config/feishu-botd.json go run ./cmd/feishu-botd
```

### 3. Send a Notification

Send a test notification to the `ops` channel via the HTTP compatibility endpoint:

```sh
curl -X POST http://127.0.0.1:7345/v1/notify \
  -H "Content-Type: application/json" \
  -d '{
    "severity": "info",
    "title": "Deployment Completed",
    "markdown": "Service **web-gateway** successfully deployed.",
    "target": { "channel": "ops" }
  }'
```

---

## Interfaces

### gRPC Contract (`proto/feishubotd/v1/`)

The daemon serves protobuf contracts defined in [`proto/feishubotd/v1/`](proto/feishubotd/v1) over Unix sockets and loopback TCP:

| Service | Description |
| --- | --- |
| `BotdHealthService` | Liveness and readiness probes (`Health`, `Ready`). |
| `NotificationService` | Dispatch structured notifications and CardKit messages (`SendNotification`, `SendMessage`). |
| `CommandService` | Conversational agent runtime: inbound event streaming, progressive CardKit responses, and button callbacks. |

See [IPC & gRPC Contract](docs/ipc.md) for full interface specifications and error handling.

### HTTP API (Compatibility Shim)

Retained for simple webhook-style integrations:

- `GET /healthz` & `GET /readyz` — Process health and readiness status.
- `POST /v1/notify` — High-level notification dispatch with deduplication and severity levels.
- `POST /v1/message` — Low-level Markdown or CardKit message dispatch.

See [HTTP API Reference](docs/api.md) for request schemas and response examples.

---

## Conversational Agents

`feishu-botd` enables AI agents to interact with users via Feishu bots:

- Inbound `@mentions` and direct messages are received via WebSocket long connection.
- Agents stream responses directly to a Feishu CardKit 2.0 card, updating output progressively as it generates.
- Interactive card buttons dispatch action events back to the agent without exposing raw callback tokens or chat IDs.

See [Building a Feishu Agent](docs/agent.md) for app setup and card lifecycles, and refer to [`examples/agent/`](examples/agent/) for a minimal Go agent implementation.

---

## Local Script Execution

For simple operational tasks, `feishu-botd` can execute local scripts directly upon `@mention` without needing an external agent process:

```text
@Bot ops run staging
  │   │    └── Arguments passed to script (argv)
  │   └─────── Action maps to /etc/feishu-botd/scripts/ops-run.sh
  └─────────── Command word
```

Script output is captured and sent back to the chat as a reply. See [`scripts/ops-run.example.sh`](scripts/ops-run.example.sh) for an example Jenkins trigger script.

---

## Development

```sh
# Run tests
go test ./...
go test -race ./...

# Build binary
make build

# Regenerate protobuf bindings (buf required)
make proto
```

---

## Documentation

- [Agent Development Guide](docs/agent.md) — App configuration, message routing, CardKit 2.0 streaming, and permissions.
- [IPC & gRPC Contract](docs/ipc.md) — Detailed protobuf definitions, RPC contracts, and error model.
- [HTTP API Reference](docs/api.md) — Compatibility endpoint schemas and examples.
- [Deployment Guide](docs/deployment.md) — Docker, systemd, Unix domain socket setup, and security considerations.
