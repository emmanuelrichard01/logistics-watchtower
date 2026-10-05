#!/usr/bin/env bash
# Topics from plan section 7. Runs before any consumer starts; safe to re-run.
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

create telemetry.raw.v1        12 "$(retain 7)"
create telemetry.quarantine.v1  3 "$(retain 30)"
create telemetry.clean.v1      12 "$(retain 7)"
create telemetry.late.v1        3 "$(retain 30)"
create risk.assessments.v1      6 "$(retain 14)"
create alerts.events.v1         6 "$(retain 30)"
create fleet.state.v1          12 --topic-config=cleanup.policy=compact
create rules.config.v1          1 --topic-config=cleanup.policy=compact

for service in processor projector archiver notifier; do
  create "$service.dlq.v1" 3 "$(retain 30)"
done

rpk topic list
