#!/usr/bin/env bash
# Topic set v2.1 (plan section 7, as revised). Runs before any consumer starts; safe to re-run.
# Every topic is keyed by vehicle_id, so one vehicle's records stay in order on one
# partition all the way through the pipeline.
set -euo pipefail

DAY_MS=86400000

# Consumers must never create topics implicitly (v1 audit defect 17).
rpk cluster config set auto_create_topics_enabled false

create() {
  local name=$1 partitions=$2
  shift 2
  if rpk topic describe "$name" >/dev/null 2>&1; then
    echo "exists  $name"
  else
    rpk topic create "$name" --partitions "$partitions" --replicas 1 "$@"
  fi
}

retain() { echo "--topic-config=retention.ms=$(( $1 * DAY_MS ))"; }

create telemetry.raw.v1        12 "$(retain 7)"   # gateway output, as accepted
create telemetry.quarantine.v1  3 "$(retain 30)"  # rejected, with reason codes
# The processor's single ordered input: telemetry plus control records (TICK,
# RULES_ACTIVATED, ASSIGNMENT_CHANGED, OPERATOR_COMMAND). Replaying this log alone
# reproduces every output, which is what makes replay deterministic.
create wt.input.v1             12 "$(retain 7)"
create telemetry.late.v1        3 "$(retain 30)"
create telemetry.minutes.v1    12 "$(retain 3)"   # minute buckets
create risk.assessments.v1     12 "$(retain 14)"
create alerts.events.v1        12 "$(retain 30)"
create fleet.state.v1          12 --topic-config=cleanup.policy=compact --topic-config=segment.ms=600000

for service in processor projector archiver notifier; do
  create "$service.dlq.v1" 3 "$(retain 30)"
done

rpk topic list
