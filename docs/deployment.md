# Deployment

Prefer a Unix domain socket when `feishu-botd` runs on the same host as its
clients. The socket directory should be owned by the deployment user and not
world writable. gRPC (`FEISHU_BOTD_GRPC_SOCKET`) is the preferred transport; the
HTTP socket (`FEISHU_BOTD_SOCKET`) remains for the compatibility shim. The two
transports use distinct socket paths and can run simultaneously during
migration. See [ipc.md](./ipc.md) for the gRPC contract.

HTTP TCP is intended for Docker or process-manager deployments. It is
loopback-only by default; a non-loopback HTTP bind requires
`FEISHU_BOTD_ALLOW_NON_LOOPBACK_BIND=true` and the general bearer loaded from
`FEISHU_BOTD_AUTH_TOKEN_FILE`. This HTTP LAN opt-in does not apply to gRPC.
HTTP TCP is also plaintext: a bearer authenticates a caller but does not encrypt
the token or message content. Use it only on a trusted network, or put an HTTPS
reverse proxy or encrypted tunnel in front of the loopback listener.

gRPC TCP has no built-in TLS in this release and is always loopback-only. Its
general non-health RPCs use the same `FEISHU_BOTD_AUTH_TOKEN_FILE`, while agent
and scoped legacy command RPCs use distinct per-provider token files configured
under `agent_providers`. The same privilege split applies to both Unix sockets
when `agent_providers` is non-empty: HTTP `POST /v1/notify` and
`POST /v1/message` plus general non-health gRPC RPCs require the distinct
general token from `listeners.auth_token_file` or
`FEISHU_BOTD_AUTH_TOKEN_FILE`. Provider tokens authorize only their scoped
gRPC methods and never authorize HTTP sends. If the general token is omitted
from a scoped Unix-only deployment, those outbound interfaces fail closed.
Only deployments without `agent_providers` retain legacy Unix local trust. For
cross-host gRPC, use an authenticated encrypted tunnel or TLS proxy terminating
onto loopback; a bearer token alone does not encrypt prompts, responses, or
credentials. HTTP health/readiness and gRPC health remain unauthenticated.

Inbound agent mode requires a Feishu enterprise custom app and exactly one
active `feishu-botd` long-connection client per app. Feishu uses cluster/random
delivery across multiple clients rather than broadcast. Because botd's
delivery, response, and action ownership state is process-local, active-active
replicas can split related events and are unsupported until that state is made
durable and shared. An active/standby setup must ensure only the active process
opens the Feishu connection.

If any provider enables `allow_message_reactions`, configure `state_dir` on
durable local storage. The checked-in Compose file mounts the named
`feishu-botd-state` volume at `/var/lib/feishu-botd`; the example config points
`state_dir` there. This snapshot contains only opaque message references and
provider names, expires ownership after 24 hours, and must remain private to the
daemon. It survives restart but is not shared active-active state.

The general bearer and every provider bearer must contain at least 32 bytes.
The loader trims and uses only the first line of each token file; later lines
are ignored. Before upgrading, replace any shorter general token and update all
of its callers. For example, create a new file with restrictive permissions:

```sh
umask 077
openssl rand -base64 32 > /run/secrets/feishu-botd-token
```

Feishu app credentials and raw chat ids stay in sidecar configuration. Hook
definitions should use stable channel names such as `ops`.

Rollback is stopping the sidecar or disabling the caller integration that uses
it.

## Local Source Run

```sh
export FEISHU_APP_ID=REPLACE_WITH_APP_ID
export FEISHU_APP_SECRET=REPLACE_WITH_APP_SECRET
export FEISHU_BOTD_SOCKET=/tmp/feishu-botd/feishu-botd.sock
export FEISHU_BOTD_GRPC_SOCKET=/tmp/feishu-botd/feishu-botd.grpc.sock
export FEISHU_BOTD_CHANNELS_OPS=REPLACE_WITH_OPS_CHAT_ID

mkdir -p /tmp/feishu-botd
go run ./cmd/feishu-botd
```

Verify liveness and readiness over the HTTP shim:

```sh
curl --unix-socket /tmp/feishu-botd/feishu-botd.sock http://localhost/healthz
curl --unix-socket /tmp/feishu-botd/feishu-botd.sock http://localhost/readyz
```

The gRPC listener is on `feishu-botd.grpc.sock`. See [ipc.md](./ipc.md) to dial
it (e.g. `grpc_health_probe -addr unix:///tmp/feishu-botd/feishu-botd.grpc.sock`).

## Standalone Docker on a LAN

Create a token file that all LAN callers will use:

```sh
mkdir -p secrets
openssl rand -base64 32 > secrets/feishu-botd-token
chmod 600 secrets/feishu-botd-token
```

Copy the checked-in template and edit it:

```sh
cp config/feishu-botd.example.json config/feishu-botd.json
cp .env.example .env
$EDITOR config/feishu-botd.json
$EDITOR .env
```

Put Feishu app credentials, channel aliases, and service defaults in
`config/feishu-botd.json`. For example,
`services.build-monitor.default_channel = "ci"` lets a build monitor send with
`"source": "build-monitor"` without repeating
`"target": { "channel": "ci" }` in every request.

Set `FEISHU_BOTD_HOST_IP` to the Docker host's LAN IP when possible, such as
`192.0.2.10` (a documentation address; replace it with the real host address).
Leaving it as `0.0.0.0` exposes the service on every host
interface allowed by the host firewall.

Start it:

```sh
docker compose up -d --build
```

From another LAN machine:

```sh
TOKEN="$(ssh botd-host.example cat /srv/feishu-botd/secrets/feishu-botd-token)"
curl http://192.0.2.10:7345/v1/message \
  -H "Authorization: Bearer ${TOKEN}" \
  -H 'Content-Type: application/json' \
  -d '{
    "source": "build-monitor",
    "dedupe_key": "build-monitor:build:123",
    "msg_type": "interactive",
    "card": {
      "type": "template",
      "data": {
        "template_id": "REPLACE_WITH_TEMPLATE_ID",
        "template_version_name": "1.0.3",
        "template_variable": { "title": "Build succeeded" }
      }
    }
  }'
```

## Sharing a Unix socket with another container

The repo's own `docker-compose.yml` keeps both daemon sockets in a NAMED
volume with the fixed name `feishu-botd-run` (it inherits the image's
`10001:10001` ownership on first use). A consumer in another Compose
project on the same engine declares it `external: true` under that name,
mounts it at any path, and connects to `feishu-botd.grpc.sock` inside it —
with membership of gid `10001` to pass the socket's `0o660` group bits. A
named volume rather than a host bind is deliberate: unix sockets cannot
bind on macOS host-shared paths (Docker Desktop's file sharing), so a bind
would crash-loop the daemon on Mac hosts. On a Linux host where a NATIVE
(non-container) process must reach the socket, replace the named volume
with a host-directory bind owned by `10001:10001` — that platform supports
it. The compose file also mounts the whole `./secrets` directory read-only
at `/run/secrets`, so adding a per-provider token (see `agent_providers` in
[agent.md](./agent.md)) is one new file plus a config entry — no compose
edit.

The same Compose file keeps reaction ownership in the separate named volume
`feishu-botd-state`. Do not mount that volume into provider containers.

When a caller and `feishu-botd` run in the same Compose project instead,
mount one named volume at `/run/feishu-botd` in both services. Configure the
daemon with `FEISHU_BOTD_GRPC_SOCKET=/run/feishu-botd/feishu-botd.grpc.sock`
and give the caller permission to connect to the socket's group (`0o660`).
Keep the caller's startup independent unless bot delivery is a hard
availability requirement.

[`deploy/docker-compose.consumer.example.yml`](../deploy/docker-compose.consumer.example.yml)
is a generic overlay template. Rename its `consumer` service key to the caller's
service key, then combine it with that service's base Compose file.

An outbound notification-only caller needs only the socket path and generated
protobuf bindings. An agent also needs its own provider token file mounted
read-only; it must not receive the Feishu app ID, app secret, raw chat IDs,
general bearer, or another provider's token.

The checked-in base Compose file is notification-only. For an agent, add its
provider entry to botd's private config and mount the same provider-token file
read-only at the configured path in exactly the daemon and that agent:

```yaml
services:
  example-agent:
    volumes:
      - ./secrets/example-agent-token:/run/secrets/example-agent-token:ro
    environment:
      FEISHU_BOTD_AGENT_PROVIDER: example-agent
      FEISHU_BOTD_AGENT_AUTH_TOKEN_FILE: /run/secrets/example-agent-token

  feishu-botd:
    volumes:
      - ./secrets/example-agent-token:/run/secrets/example-agent-token:ro
```

The corresponding private config uses
`agent_providers.example-agent.auth_token_file =
"/run/secrets/example-agent-token"`. Do not mount the general bearer into the
agent container.

## Reloading configuration

botd re-reads its configuration without a restart. It reloads on `SIGHUP`
and, when `FEISHU_BOTD_CONFIG` is set, whenever the config file or any token
file it names changes. The watcher checks file contents every two seconds and
waits for two checks to agree, so it never reads a half-written file. Set
`FEISHU_BOTD_CONFIG_WATCH=off` to rely on `SIGHUP` alone, or to a duration
such as `5s` to change the interval.

```sh
launchctl kill HUP gui/$(id -u)/technology.wenext.feishu-botd   # launchd
docker compose kill -s HUP feishu-botd                          # Compose
```

A reload validates the whole new config first, using the same loader as
startup, and checks the credentials of every app it adds or re-credits. If
anything fails, botd logs `config reload rejected` with the reason and keeps
running on the previous config. Otherwise it applies only what changed:

| Change | Effect |
| --- | --- |
| Channels, `default_channel`, `services`, commands, bot identity, scripts, `send_timeout_seconds` | Applied in place. No Feishu connection is touched. |
| An app added or removed | Only that app's connection starts or stops. |
| An app's `app_id` or `app_secret` | Only that app reconnects; its old connection closes once the new one is up. |
| A provider added | Its token works at once. |
| A provider's token file contents or grants changed, or the provider removed | Only that provider's open streams end with `UNAVAILABLE` (`provider_reconfigured`); a client that reconnects gets the new grants. Every other provider's streams stay open. |
| `listeners.auth_token_file` contents | The general bearer changes at once. |

These settings take effect only after a restart: `listeners.*` socket paths
and binds, `listeners.allow_non_loopback_bind`, `state_dir`, and
`dedupe_ttl_seconds`. So does the reply-route journal when the first provider
gets `allow_proactive_messages` (until then its reply routes live in memory).
A reload that changes one of them applies everything else and logs a warning
naming them. `/readyz` lists them under `config.restart_required` until the
restart. Environment variables are read once, at process start.

Each applied reload logs `config reload applied` with the new generation, the
apps started, stopped, reconnected, or updated, and the providers added or
reset. `/readyz` and gRPC `Ready` report the generation and last outcome under
`config` (see [api.md](./api.md#get-readyz)).

## macOS launchd

On a Mac whose agent providers also run natively, run the daemon natively too:
its Unix sockets are then files every local client can open, with no
container boundary and no relay in between.

Everything it owns lives under `~/.feishu-botd`. Write
`~/.feishu-botd/config.json` with host paths: `listeners.http_socket` and
`listeners.grpc_socket` under `~/.feishu-botd/run/`, `state_dir` as
`~/.feishu-botd/state`, credential files by their absolute host paths, and a
script `dir` the account can read. Then:

```sh
deploy/install.sh                 # builds with this Mac's Go
deploy/install.sh path/to/binary  # or installs a darwin binary built elsewhere
tail -f ~/.feishu-botd/logs/feishu-botd.log
```

`deploy/install.sh` installs `~/.feishu-botd/bin/feishu-botd`, renders
`deploy/launchd/feishu-botd.plist` to
`~/Library/LaunchAgents/technology.wenext.feishu-botd.plist` and (re)starts the
`technology.wenext.feishu-botd` job. A local script runs as the same account
with macOS's `/bin/bash` 3.2, so scripts must not rely on bash 4 features such
as associative arrays.

Moving an existing container deployment: stop the container, copy
`/var/lib/feishu-botd` out of the state volume into `~/.feishu-botd/state`, then
install. Repoint every client at the new socket paths in the same window.

## Rollback

Disable or drain callers first, then stop the sidecar:

```sh
docker compose stop feishu-botd
```

For launchd:

```sh
launchctl bootout gui/$(id -u)/technology.wenext.feishu-botd
```
