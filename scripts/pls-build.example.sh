#!/usr/bin/env bash
set -euo pipefail

# Invoked by feishu-botd's commands.scripts executor for "@<bot> pls build <project> <branch> [env] [zentao]".
# Mirrors the `jenkins` target in each project's own Makefile (e.g.
# lama-ludo-ios/Makefile): fires a remote Jenkins buildWithParameters call
# rather than building anything locally, since the container running this
# script has no project checkouts.

# Fill these in before deploying this script to the host — they are not read
# from the environment so this file is self-contained and can be copied
# directly to the remote service.
JENKINS_URL="http://10.86.20.10"
JENKINS_USER="wenextci"
JENKINS_TOKEN="REPLACE_WITH_JENKINS_PASSWORD_OR_API_TOKEN"
JENKINS_JOB_TOKEN="REPLACE_WITH_JENKINS_JOB_BUILD_TOKEN"

usage() {
  echo "usage: pls-build.sh <project> <branch> [env] [zentao]" >&2
  echo "  project: one of: ${!JENKINS_JOBS[*]}" >&2
  echo "  env:     Test (default) | Production" >&2
  echo "  zentao:  No (default) | Yes" >&2
  exit 1
}

declare -A JENKINS_JOBS=(
  [ludo]=lamaludo-ios
  [lama]=Lama-iOS
  [fungo]=Fungo-ios
  [wyak]=wyak-ios
  [yoki]=Yoki-ios
)

PROJECT="${1:-}"
BRANCH="${2:-}"
ENV_NAME="${3:-Test}"
ZENTAO="${4:-No}"

[ -n "$PROJECT" ] && [ -n "$BRANCH" ] || usage

JOB="${JENKINS_JOBS[$PROJECT]:-}"
if [ -z "$JOB" ]; then
  echo "unknown project '${PROJECT}': known projects: ${!JENKINS_JOBS[*]}" >&2
  exit 1
fi

case "$ENV_NAME" in
  Test | Production) ;;
  *)
    echo "invalid env '${ENV_NAME}': must be Test or Production" >&2
    exit 1
    ;;
esac

case "$ZENTAO" in
  Yes | No) ;;
  *)
    echo "invalid zentao '${ZENTAO}': must be Yes or No" >&2
    exit 1
    ;;
esac

case "$JENKINS_TOKEN$JENKINS_JOB_TOKEN" in
  *REPLACE_WITH_*)
    echo "pls-build.sh: fill in JENKINS_TOKEN/JENKINS_JOB_TOKEN at the top of this file before use" >&2
    exit 1
    ;;
esac

curl -fsS -X POST "${JENKINS_URL}/job/${JOB}/buildWithParameters?token=${JENKINS_JOB_TOKEN}" \
  -u "${JENKINS_USER}:${JENKINS_TOKEN}" \
  --data "branch=${BRANCH}" \
  --data "env=${ENV_NAME}" \
  --data "zentao=${ZENTAO}"

echo "triggered ${JOB} (branch=${BRANCH}, env=${ENV_NAME}, zentao=${ZENTAO})"
