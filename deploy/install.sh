#!/bin/bash
# Installs feishu-botd as a native launchd job of the current account.
#
#   deploy/install.sh              build with this Mac's Go, install, restart
#   deploy/install.sh <binary>     install a prebuilt darwin binary instead
#
# A Mac without Go can take a binary built elsewhere:
#   GOOS=darwin GOARCH=arm64 CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' \
#     -o bin/feishu-botd-darwin-arm64 ./cmd/feishu-botd
#
# ~/.feishu-botd/config.json is operator-owned and never written here. Its
# listeners name sockets under ~/.feishu-botd/run, so a native client — an
# agent provider on the same Mac — connects to them directly.
set -euo pipefail

readonly REPO="$(cd "$(dirname "$0")/.." && pwd)"
readonly ROOT="${HOME:?}/.feishu-botd"
readonly JOB="technology.wenext.feishu-botd"
readonly PLIST="${HOME}/Library/LaunchAgents/${JOB}.plist"

[ -f "${ROOT}/config.json" ] || {
  echo "install: ${ROOT}/config.json is missing" >&2
  exit 1
}
mkdir -p "${ROOT}"/{bin,state,run,logs}

if [ "$#" -ge 1 ]; then
  BINARY="$1"
else
  (cd "$REPO" && CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o bin/feishu-botd ./cmd/feishu-botd)
  BINARY="${REPO}/bin/feishu-botd"
fi
install -m 755 "$BINARY" "${ROOT}/bin/feishu-botd.new"
mv -f "${ROOT}/bin/feishu-botd.new" "${ROOT}/bin/feishu-botd"

mkdir -p "$(dirname "$PLIST")"
sed "s|__HOME__|${HOME}|g" "${REPO}/deploy/launchd/feishu-botd.plist" >"${PLIST}.new"
mv -f "${PLIST}.new" "$PLIST"

if launchctl print "gui/$(id -u)/${JOB}" >/dev/null 2>&1; then
  launchctl bootout "gui/$(id -u)/${JOB}" || true
fi
# A job still unloading refuses bootstrap with an I/O error; wait it out.
for _ in $(seq 60); do
  launchctl bootstrap "gui/$(id -u)" "$PLIST" 2>/dev/null && break
  sleep 1
done
launchctl print "gui/$(id -u)/${JOB}" >/dev/null || {
  echo "install: ${JOB} did not start" >&2
  exit 1
}
echo "started ${JOB}"
