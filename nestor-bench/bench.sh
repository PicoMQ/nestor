#!/usr/bin/env bash
# Runs bench scenarios against the nestor-bench compose stack and writes one report per run.
#
#   nestor-bench/bench.sh                          the default matrix, all targets
#   nestor-bench/bench.sh sequential fanout        a subset of scenarios
#   TARGETS="library origin" nestor-bench/bench.sh   a subset of targets
#   OBJECTS=64 PROFILE=stream-set SEED=1           dataset shape
#   OUT=reports                                    report directory
#   KEEP=1                                         leave the stack running afterwards
#   NESTOR_IMAGE=... SKIP_BUILD=1                  use an already built nestor image
#
# Scenarios: cold-open sequential fanout fanout-staggered stream-read small-objects scan-pollution
#            delete working-set tail errors herd restart concurrency soak
# Targets:   library endpoint origin

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE="$ROOT/nestor-bench/compose.yml"
SCENARIOS=("$@")
if [ ${#SCENARIOS[@]} -eq 0 ]; then
  SCENARIOS=(cold-open sequential fanout fanout-staggered stream-read scan-pollution working-set tail errors herd restart)
fi
read -r -a TARGETS <<< "${TARGETS:-library endpoint origin}"
SEED="${SEED:-1}"
OBJECTS="${OBJECTS:-32}"
PROFILE="${PROFILE:-stream-set}"
OUT="${OUT:-$ROOT/nestor-bench/reports}"

log() { printf '\n\033[1;34m==> %s\033[0m\n' "$*"; }

bench() {
  (cd "$ROOT" && cargo run --locked --release -q -p nestor-bench --bin bench -- "$@")
}

export NESTOR_IMAGE="${NESTOR_IMAGE:-nestor-e2e:local}"
if [ "${SKIP_BUILD:-0}" != "1" ] && [[ " ${TARGETS[*]} " == *" endpoint "* ]]; then
  log "building $NESTOR_IMAGE"
  docker build -t "$NESTOR_IMAGE" "$ROOT"
fi

log "starting stack"
docker compose -f "$COMPOSE" up --detach --wait

log "building bench"
(cd "$ROOT" && cargo build --locked --release -q -p nestor-bench)

log "dataset seed=$SEED objects=$OBJECTS profile=$PROFILE"
bench dataset --seed "$SEED" --objects "$OBJECTS" --profile "$PROFILE"

mkdir -p "$OUT"
failed=()
for scenario in "${SCENARIOS[@]}"; do
  for target in "${TARGETS[@]}"; do
    if [ "$scenario" = "restart" ] && [ "$target" != "endpoint" ]; then
      continue
    fi
    log "$scenario / $target"
    bench run --target "$target" --scenario "$scenario" \
      --seed "$SEED" --objects "$OBJECTS" --profile "$PROFILE" \
      --compose "$COMPOSE" --assert \
      --out "$OUT/$scenario-$target.json" || failed+=("$scenario/$target")
  done
done

if [ "${KEEP:-0}" != "1" ]; then
  log "tearing down"
  docker compose -f "$COMPOSE" down --volumes --remove-orphans
fi

if [ ${#failed[@]} -ne 0 ]; then
  log "failed: ${failed[*]}"
  exit 1
fi
log "reports in $OUT"
