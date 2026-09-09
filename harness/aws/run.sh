#!/usr/bin/env bash
# Benches a commit on AWS: applies the Terraform in this directory, runs remote.sh on the instance
# through SSM, pulls the reports and destroys everything. Needs terraform and the aws CLI with
# credentials for the account.
#
#   harness/aws/run.sh                       the default matrix at HEAD, then destroy
#   harness/aws/run.sh sequential fanout     a subset of scenarios
#   REF=main TARGETS="library origin" OBJECTS=64 PROFILE=stream-set SEED=1
#   KEEP=1                                   leave the instance up for another run
#   OUT=harness/aws/reports                  where runs land, one directory per run
#   TF_VAR_instance_type=i4i.2xlarge TF_VAR_region=eu-west-1
#
# The ref must be reachable from the repository the instance clones, so push before running.

set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
REF="${REF:-$(git -C "$ROOT" rev-parse HEAD)}"
OUT="${OUT:-$HERE/reports}"
SCENARIOS=("$@")

log() { printf '\n\033[1;34m==> %s\033[0m\n' "$*"; }

tf() { terraform -chdir="$HERE" "$@"; }

ssm() {
  local timeout="$1" prefix="$2"
  shift 2
  local command_id status
  command_id=$(aws ssm send-command \
    --region "$REGION" \
    --instance-ids "$INSTANCE" \
    --document-name AWS-RunShellScript \
    --timeout-seconds 600 \
    --parameters "{\"commands\":[\"$*\"],\"executionTimeout\":[\"$timeout\"]}" \
    --output-s3-bucket-name "$BUCKET" \
    --output-s3-key-prefix "$prefix" \
    --query Command.CommandId --output text)
  while :; do
    status=$(aws ssm get-command-invocation --region "$REGION" --command-id "$command_id" \
      --instance-id "$INSTANCE" --query Status --output text 2>/dev/null || echo Pending)
    case "$status" in
      Pending | InProgress | Delayed) sleep 15 ;;
      *) break ;;
    esac
  done
  aws ssm get-command-invocation --region "$REGION" --command-id "$command_id" --instance-id "$INSTANCE" \
    --query '[StandardOutputContent, StandardErrorContent]' --output text
  [ "$status" = Success ]
}

log "applying"
tf init -input=false >/dev/null
tf apply -input=false -auto-approve
INSTANCE=$(tf output -raw instance_id)
BUCKET=$(tf output -raw bucket)
REGION=$(tf output -raw region)

log "waiting for $INSTANCE"
until aws ssm describe-instance-information --region "$REGION" \
    --filters "Key=InstanceIds,Values=$INSTANCE" \
    --query 'InstanceInformationList[0].PingStatus' --output text 2>/dev/null | grep -q Online; do
  sleep 10
done
ssm 1800 bootstrap "cloud-init status --wait >/dev/null && test -f /var/lib/nestor-bench/ready"

RUN="$(date -u +%Y%m%dT%H%M%SZ)-${REF:0:12}"
log "run $RUN on $(tf output -raw instance_type), follow with: aws ssm start-session --region $REGION --target $INSTANCE"
ssm 14400 "runs/$RUN/ssm" \
  "cd /opt/nestor && git fetch -q origin '$REF' && git checkout -q FETCH_HEAD" \
  "&& RUN='$RUN' TARGETS='${TARGETS:-library endpoint origin}' OBJECTS='${OBJECTS:-32}' PROFILE='${PROFILE:-stream-set}' SEED='${SEED:-1}'" \
  "harness/aws/remote.sh ${SCENARIOS[*]}" || failed=1

log "pulling reports"
mkdir -p "$OUT/$RUN"
aws s3 sync --quiet --region "$REGION" "s3://$BUCKET/runs/$RUN/" "$OUT/$RUN/"

if [ "${KEEP:-0}" != "1" ]; then
  log "destroying"
  tf destroy -input=false -auto-approve
fi

if [ "${failed:-0}" = "1" ]; then
  log "bench failed, see $OUT/$RUN/ssm"
  exit 1
fi
log "reports in $OUT/$RUN"
