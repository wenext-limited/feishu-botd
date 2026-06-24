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
