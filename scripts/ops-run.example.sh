#!/usr/bin/env bash
set -euo pipefail

# Example target for feishu-botd's commands.scripts executor:
#   "@<bot> ops run <job-alias> <branch> [environment]"
#
# Keep deployment-specific names and credentials outside this public example.
# The allowlist maps chat-facing aliases to Jenkins job names, for example:
#   JENKINS_JOB_MAP='mobile=example-mobile-job,api=example-api-job'
#
# Required environment variables:
#   JENKINS_URL, JENKINS_USER, JENKINS_API_TOKEN, JENKINS_JOB_MAP
# Optional environment variables:
#   JENKINS_BRANCH_PARAMETER (default: branch)
#   JENKINS_ENVIRONMENT_PARAMETER (default: environment)

require_env() {
  local name="$1"
  if [ -z "${!name:-}" ]; then
    echo "ops-run.sh: required environment variable ${name} is not set" >&2
    exit 1
  fi
}

require_env JENKINS_URL
require_env JENKINS_USER
require_env JENKINS_API_TOKEN
require_env JENKINS_JOB_MAP

case "$JENKINS_URL" in
  http://* | https://*) ;;
  *)
    echo "ops-run.sh: JENKINS_URL must use http:// or https://" >&2
    exit 1
    ;;
esac

JENKINS_URL="${JENKINS_URL%/}"
JENKINS_BRANCH_PARAMETER="${JENKINS_BRANCH_PARAMETER:-branch}"
JENKINS_ENVIRONMENT_PARAMETER="${JENKINS_ENVIRONMENT_PARAMETER:-environment}"

usage() {
  echo "usage: ops-run.sh <job-alias> <branch> [environment]" >&2
  exit 1
}

resolve_job() {
  local requested_alias="$1"
  local mapping alias job
  local -a mappings

  IFS=',' read -r -a mappings <<<"$JENKINS_JOB_MAP"
  for mapping in "${mappings[@]}"; do
    alias="${mapping%%=*}"
    job="${mapping#*=}"
    if [ "$alias" = "$requested_alias" ] && [ "$mapping" != "$job" ] && [ -n "$job" ]; then
      printf '%s\n' "$job"
      return 0
    fi
  done

  return 1
}

JOB_ALIAS="${1:-}"
BRANCH="${2:-}"
ENVIRONMENT="${3:-}"

[ "$#" -le 3 ] && [ -n "$JOB_ALIAS" ] && [ -n "$BRANCH" ] || usage

if ! JOB="$(resolve_job "$JOB_ALIAS")"; then
  echo "unknown job alias '${JOB_ALIAS}'" >&2
  exit 1
fi

case "$JOB" in
  *[!A-Za-z0-9._-]* | '')
    echo "invalid Jenkins job name configured for alias '${JOB_ALIAS}'" >&2
    exit 1
    ;;
esac

curl_args=(
  -fsS
  -X POST
  "${JENKINS_URL}/job/${JOB}/buildWithParameters"
  -u "${JENKINS_USER}:${JENKINS_API_TOKEN}"
  --data-urlencode "${JENKINS_BRANCH_PARAMETER}=${BRANCH}"
)

if [ -n "$ENVIRONMENT" ]; then
  curl_args+=(--data-urlencode "${JENKINS_ENVIRONMENT_PARAMETER}=${ENVIRONMENT}")
fi

curl "${curl_args[@]}"

echo "triggered ${JOB_ALIAS} (branch=${BRANCH}${ENVIRONMENT:+, environment=${ENVIRONMENT}})"
