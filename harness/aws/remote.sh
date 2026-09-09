#!/usr/bin/env bash
# Runs on the instance as root. Builds the checked out tree, installs the nestor binary as a systemd
# unit, runs the matrix against S3 and uploads the reports to the bench bucket.
#
#   RUN=<id>                  report prefix in the bucket, runs/<id>/
#   TARGETS="library endpoint origin"   OBJECTS=32 PROFILE=stream-set SEED=1
#   remote.sh cold-open sequential      a subset of scenarios
#
# tail and errors inject faults through the proxy, which a direct origin bypasses, so they are not
# in the default matrix here.

set -euo pipefail

source /etc/nestor-bench/env
export PATH="/usr/local/cargo/bin:$PATH"
export RUSTUP_HOME=/usr/local/rustup CARGO_HOME=/usr/local/cargo CARGO_TARGET_DIR=/mnt/nvme/target
export AWS_REGION="$REGION"
export BENCH_ORIGIN="https://s3.$REGION.amazonaws.com" BENCH_BUCKET="$BUCKET"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SCENARIOS=("$@")
if [ ${#SCENARIOS[@]} -eq 0 ]; then
  SCENARIOS=(cold-open sequential fanout fanout-staggered stream-read scan-pollution working-set herd restart)
fi
read -r -a TARGETS <<< "${TARGETS:-library endpoint origin}"
SEED="${SEED:-1}"
OBJECTS="${OBJECTS:-32}"
PROFILE="${PROFILE:-stream-set}"
RUN="${RUN:-$(date -u +%Y%m%dT%H%M%SZ)}"
OUT="/mnt/nvme/reports/$RUN"

log() { printf '\n==> %s\n' "$*"; }

bench() { "$CARGO_TARGET_DIR/release/bench" "$@"; }

cd "$ROOT"
log "building $(git rev-parse --short HEAD)"
cargo build --locked --release -p nestor-cli -p nestor-bench

log "installing nestor"
install -m 755 "$CARGO_TARGET_DIR/release/nestor" /usr/local/bin/nestor
install -m 644 harness/aws/nestor.toml /etc/nestor/nestor.toml
printf 'NESTOR_ORIGIN__ENDPOINT=%s\nNESTOR_ORIGIN__REGION=%s\n' "$BENCH_ORIGIN" "$REGION" > /etc/nestor/env
install -m 644 harness/aws/nestor.service /etc/systemd/system/nestor.service
systemctl daemon-reload
systemctl enable --now nestor

log "dataset seed=$SEED objects=$OBJECTS profile=$PROFILE"
bench dataset --seed "$SEED" --objects "$OBJECTS" --profile "$PROFILE"

imds_token=$(curl -fsS -X PUT -H 'X-aws-ec2-metadata-token-ttl-seconds: 60' http://169.254.169.254/latest/api/token)
instance_type=$(curl -fsS -H "X-aws-ec2-metadata-token: $imds_token" http://169.254.169.254/latest/meta-data/instance-type)
mkdir -p "$OUT"
cat > "$OUT/run.json" <<EOF
{
  "run": "$RUN",
  "commit": "$(git rev-parse HEAD)",
  "region": "$REGION",
  "instance_type": "$instance_type",
  "kernel": "$(uname -r)"
}
EOF

failed=()
for scenario in "${SCENARIOS[@]}"; do
  for target in "${TARGETS[@]}"; do
    if [ "$scenario" = "restart" ] && [ "$target" != "endpoint" ]; then
      continue
    fi
    args=()
    case "$target" in
      library) args=(--disk-path /mnt/nvme/bench) ;;
      endpoint) args=(--systemd nestor --disk-path /mnt/nvme/nestor) ;;
    esac
    log "$scenario / $target"
    bench run --target "$target" --scenario "$scenario" \
      --seed "$SEED" --objects "$OBJECTS" --profile "$PROFILE" \
      "${args[@]}" --assert \
      --out "$OUT/$scenario-$target.json" || failed+=("$scenario/$target")
  done
done

log "uploading to s3://$BUCKET/runs/$RUN/"
aws s3 sync --quiet "$OUT" "s3://$BUCKET/runs/$RUN/"

if [ ${#failed[@]} -ne 0 ]; then
  log "failed: ${failed[*]}"
  exit 1
fi
