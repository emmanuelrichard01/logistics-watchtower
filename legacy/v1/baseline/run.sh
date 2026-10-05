#!/usr/bin/env bash
# v1 baseline: delivered throughput, producer-to-WebSocket latency and resource use
# at several fleet sizes. Usage: legacy/v1/baseline/run.sh [fleet sizes...]
set -euo pipefail

cd "$(dirname "$0")/.."
PROJECT=wt-v1
WARMUP=20
DURATION=60
OUT="baseline/results/$(date -u +%Y%m%dT%H%M%SZ).jsonl"
mkdir -p baseline/results

trap 'docker compose -p "$PROJECT" down -v --remove-orphans >/dev/null 2>&1 || true' EXIT

for fleet in "${@:-3 100 1000}"; do
  for n in $fleet; do
    echo "== fleet $n" >&2
    FLEET_SIZE=$n DEMO_MODE=true docker compose -p "$PROJECT" up -d --build --quiet-pull >&2
    # Workaround for a v1 defect: the API subscribes before the processor creates the
    # alerts topic and only notices it on the 5-minute metadata refresh.
    until docker exec redpanda rpk topic list 2>/dev/null | grep -q '^alerts'; do sleep 2; done
    docker compose -p "$PROJECT" restart api >&2

    # Git Bash would rewrite "/c/...:/b:ro" as a path list; pass a native path instead.
    MSYS_NO_PATHCONV=1 docker run --rm --network "${PROJECT}_default" \
      -v "$(pwd -W 2>/dev/null || pwd)/baseline:/b:ro" python:3.12-slim \
      sh -c "pip install -q --disable-pip-version-check websockets >/dev/null 2>&1 && \
             python /b/measure_ws.py ws://api:8000/ws $n $WARMUP $DURATION" >"$OUT.part" &
    client=$!

    sleep $((WARMUP + DURATION / 2 + 15))  # sample resources mid-measurement
    stats=$(docker stats --no-stream --format '{{json .}}' \
      | uv run --no-project python -c 'import json,sys; print(json.dumps({r["Name"]: {"cpu": r["CPUPerc"], "mem": r["MemUsage"]} for r in map(json.loads, sys.stdin) if r["Name"].startswith(("logistics-", "redpanda"))}))')
    wait "$client"

    uv run --no-project python -c 'import json,sys; r=json.loads(open(sys.argv[1]).read()); r["resources"]=json.loads(sys.argv[2]); print(json.dumps(r))' \
      "$OUT.part" "$stats" >>"$OUT"
    rm "$OUT.part"
    docker compose -p "$PROJECT" down -v --remove-orphans >&2
  done
done
echo "$OUT"
