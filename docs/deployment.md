# Deployment

Prefer a Unix domain socket when `feishu-botd` runs on the same host as its
clients. The socket directory should be owned by the deployment user and not
world writable. gRPC (`FEISHU_BOTD_GRPC_SOCKET`) is the preferred transport; the
HTTP socket (`FEISHU_BOTD_SOCKET`) remains for the compatibility shim. The two
transports use distinct socket paths and can run simultaneously during
migration. See [ipc.md](./ipc.md) for the gRPC contract.

TCP is intended for Docker or process-manager deployments. TCP binds are
loopback-only by default. To expose `feishu-botd` to other machines on a LAN,
bind to a non-loopback address such as `0.0.0.0:7345` and set
`FEISHU_BOTD_ALLOW_NON_LOOPBACK_BIND=true`. In TCP mode, both transports require
a bearer token loaded from
`FEISHU_BOTD_AUTH_TOKEN_FILE`: HTTP `POST /v1/notify` expects an
`Authorization: Bearer <token>` header, and gRPC expects the same token as
`authorization` request metadata. The single token is shared by
`FEISHU_BOTD_BIND` and `FEISHU_BOTD_GRPC_BIND`; health RPCs are exempt. Use a
host firewall or Docker's host-IP port publishing to keep the listener on the
trusted LAN.

Feishu app credentials and raw chat ids stay in sidecar configuration. Hook
definitions should use stable channel names such as `ops`.

Rollback is stopping the sidecar or disabling the Xipe hook that calls it.

## Local Source Run

```sh
export FEISHU_APP_ID=cli_xxx
export FEISHU_APP_SECRET=...
export FEISHU_BOTD_SOCKET=/tmp/feishu-botd/feishu-botd.sock
export FEISHU_BOTD_GRPC_SOCKET=/tmp/feishu-botd/feishu-botd.grpc.sock
export FEISHU_BOTD_CHANNELS_OPS=oc_xxx

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
cp .env.example .env
$EDITOR .env
```

Set `FEISHU_BOTD_HOST_IP` to the Docker host's LAN IP when possible, such as
`192.168.1.10`. Leaving it as `0.0.0.0` exposes the service on every host
interface allowed by the host firewall.

Start it:

```sh
docker compose up -d --build
```

From another LAN machine:

```sh
TOKEN="$(ssh botd-host cat /path/to/feishu-botd/secrets/feishu-botd-token)"
curl http://192.168.1.10:7345/v1/message \
  -H "Authorization: Bearer ${TOKEN}" \
  -H 'Content-Type: application/json' \
  -d '{
    "source": "jenkins",
    "dedupe_key": "jenkins:build:123",
    "target": { "channel": "ci" },
    "msg_type": "interactive",
    "card": {
      "type": "template",
      "data": {
        "template_id": "AAqBgzXLgNKzZ",
        "template_version_name": "1.0.3",
        "template_variable": { "title": "Build succeeded" }
      }
    }
  }'
```

## Docker Beside Xipe

The checked-in Docker overlay is intended to be used with Xipe's Compose file
from the Xipe repository root:

```sh
docker compose \
  -f docker-compose.yml \
  -f ../feishu-botd/deploy/docker-compose.xipe.yml \
  --profile feishu \
  up -d --build
```

Add these sidecar-only values to Xipe's `.env` or another deployment-owned env
file before enabling the profile:

```text
FEISHU_APP_ID=cli_xxx
FEISHU_APP_SECRET=...
FEISHU_BOTD_CHANNEL=ops
FEISHU_BOTD_CHANNELS=ops=oc_xxx
```

The overlay mounts `feishu-botd-run` into both containers and sets
`FEISHU_BOTD_SOCKET=/run/feishu-botd/feishu-botd.sock` for Xipe. Dashboard hook
definitions still only need the bundled command:

```text
Working directory: /usr/local/share/xipe
Command: /usr/bin/env bash scripts/xipe-feishu-botd-hook.sh
```

The `xipe` service has no `depends_on` edge to `feishu-botd`. A stopped or
unready sidecar is visible only as hook delivery failure.

## macOS launchd

`deploy/launchd/feishu-botd.plist` is a template for local macOS
development. Install a built binary at `/usr/local/bin/feishu-botd`, replace the
placeholder Feishu app values and channel ids, then load it:

```sh
mkdir -p /tmp/feishu-botd
cp deploy/launchd/feishu-botd.plist ~/Library/LaunchAgents/
launchctl load ~/Library/LaunchAgents/feishu-botd.plist
```

Use a secrets manager or a local deployment-specific plist outside version
control for real app secrets.

## Rollback

Disable the Xipe account-condition hook first. Then stop the sidecar:

```sh
docker compose \
  -f docker-compose.yml \
  -f ../feishu-botd/deploy/docker-compose.xipe.yml \
  --profile feishu \
  stop feishu-botd
```

For launchd:

```sh
launchctl unload ~/Library/LaunchAgents/feishu-botd.plist
```
