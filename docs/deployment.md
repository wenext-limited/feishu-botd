# Deployment

Prefer a Unix domain socket when `feishu-botd` runs on the same host as Xipe.
The socket directory should be owned by the deployment user and not world
writable.

Loopback TCP is intended for Docker or process-manager deployments. In TCP mode,
`POST /v1/notify` requires `Authorization: Bearer <token>` and the token must be
loaded from `FEISHU_BOTD_AUTH_TOKEN_FILE`.

Feishu app credentials and raw chat ids stay in sidecar configuration. Xipe hook
definitions should use stable channel names such as `ops`.

Rollback is stopping the sidecar or disabling the Xipe hook that calls it.

## Local Source Run

```sh
export FEISHU_APP_ID=cli_xxx
export FEISHU_APP_SECRET=...
export FEISHU_BOTD_SOCKET=/tmp/feishu-botd/feishu-botd.sock
export FEISHU_BOTD_CHANNELS_OPS=oc_xxx

mkdir -p /tmp/feishu-botd
go run ./cmd/feishu-botd
```

Verify liveness and readiness:

```sh
curl --unix-socket /tmp/feishu-botd/feishu-botd.sock http://localhost/healthz
curl --unix-socket /tmp/feishu-botd/feishu-botd.sock http://localhost/readyz
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

`deploy/launchd/com.oops-rs.feishu-botd.plist` is a template for local macOS
development. Install a built binary at `/usr/local/bin/feishu-botd`, replace the
placeholder Feishu app values and channel ids, then load it:

```sh
mkdir -p /tmp/feishu-botd
cp deploy/launchd/com.oops-rs.feishu-botd.plist ~/Library/LaunchAgents/
launchctl load ~/Library/LaunchAgents/com.oops-rs.feishu-botd.plist
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
launchctl unload ~/Library/LaunchAgents/com.oops-rs.feishu-botd.plist
```
