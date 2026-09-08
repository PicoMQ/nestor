#!/usr/bin/env bash
# Runs the end to end scenarios in nestor-e2e against RustFS in docker compose.
#
#   nestor-e2e/e2e.sh                  all scenarios
#   nestor-e2e/e2e.sh single cluster   a subset
#   KEEP=1 nestor-e2e/e2e.sh single    leave the stack running afterwards
#   NESTOR_IMAGE=... SKIP_BUILD=1    use an already built nestor image
#
# Scenarios: single, cluster, library

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SCENARIOS=("$@")
if [ ${#SCENARIOS[@]} -eq 0 ]; then
  SCENARIOS=(single cluster library)
fi

log() { printf '\n\033[1;34m==> %s\033[0m\n' "$*"; }

compose() {
  docker compose -f "$ROOT/nestor-e2e/$1/compose.yml" "${@:2}"
}

run() {
  local scenario="$1"
  local status=0
  log "$scenario: starting stack"
  compose "$scenario" up --detach || status=$?

  if [ "$status" -eq 0 ]; then
    log "$scenario: running tests"
    (cd "$ROOT" && cargo test --locked -p nestor-e2e --test "$scenario" -- --ignored --test-threads=1 --nocapture) || status=$?
  fi

  if [ "$status" -ne 0 ]; then
    log "$scenario: service logs"
    compose "$scenario" logs --no-log-prefix --tail=200
  fi
  if [ "${KEEP:-0}" != "1" ]; then
    log "$scenario: tearing down"
    compose "$scenario" down --volumes --remove-orphans
  fi
  return "$status"
}

export NESTOR_IMAGE="${NESTOR_IMAGE:-nestor-e2e:local}"
if [ "${SKIP_BUILD:-0}" != "1" ] && [[ " ${SCENARIOS[*]} " == *" single "* || " ${SCENARIOS[*]} " == *" cluster "* ]]; then
  log "building $NESTOR_IMAGE"
  docker build -t "$NESTOR_IMAGE" "$ROOT"
fi

log "building test binaries"
(cd "$ROOT" && cargo test --locked -p nestor-e2e --no-run)

failed=()
for scenario in "${SCENARIOS[@]}"; do
  run "$scenario" || failed+=("$scenario")
done

if [ ${#failed[@]} -ne 0 ]; then
  log "failed: ${failed[*]}"
  exit 1
fi
log "all scenarios passed"
