"""Shared helpers for tests and evaluation scripts. Not used by the simulator itself."""

import gzip
import json
from datetime import datetime
from functools import cache
from pathlib import Path
from typing import Any

from watchtower_simulator import scenario as scenarios
from watchtower_simulator.clock import to_ms
from watchtower_simulator.engine import Result, Simulation
from watchtower_simulator.scenario import duration_ms


@cache
def run(name: str) -> Result:
    """Run a named scenario once per process; later calls reuse the result. Treat it as
    read-only: every caller shares it."""
    return Simulation(scenarios.load(name)).run()


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    """JSON lines from a plain or gzip-compressed (.gz) file."""
    text = (
        gzip.decompress(path.read_bytes()).decode("utf-8")
        if path.suffix == ".gz"
        else path.read_text(encoding="utf-8")
    )
    return [json.loads(line) for line in text.splitlines()]


def rows(name: str, vehicle: str) -> list[dict[str, Any]]:
    """One vehicle's fleet-state recording from a named scenario."""
    return [r for r in run(name).recording if r["vehicle_id"] == vehicle]


def _starts_within(truth: list[dict[str, Any]], vehicle: str, kind: str, start: str) -> bool:
    windows = [t for t in truth if t["vehicle_id"] == vehicle and t["kind"] == kind]
    return any(w["start"] <= start and (w["end"] is None or start < w["end"]) for w in windows)


def label_failures(result: Result, labels: dict[str, Any], start_ms: int) -> list[str]:
    """Every way a run disagrees with its ground-truth labels (empty when they all hold).

    An expectation is ``{vehicle, kind, present}``, where ``kind`` may list alternatives as
    ``a|b``, with optional ``shipment``, ``starts_after`` (offset from the start) and
    ``starts_within`` (another kind on the same vehicle). Top-level checks are
    ``expect_duplicates``, ``expect_buffered_readings``, ``expect_boot_ids``, and ``stops``
    (``on_time`` and per-shipment ``in_spec`` at a stop)."""
    failures: list[str] = []
    truth = result.truth

    def offset(stamp: str) -> int:
        return to_ms(datetime.fromisoformat(stamp.replace("Z", "+00:00"))) - start_ms

    for expect in labels.get("expect", []):
        found = [
            t
            for t in truth
            if t["vehicle_id"] == expect["vehicle"]
            and t["kind"] in expect["kind"].split("|")
            and ("shipment" not in expect or t.get("shipment_id") == expect["shipment"])
        ]
        if bool(found) != expect["present"]:
            failures.append(f"presence: {expect}")
            continue
        if not found:
            continue
        first = found[0]
        if "starts_after" in expect and offset(first["start"]) < duration_ms(
            expect["starts_after"]
        ):
            failures.append(f"too early: {expect} at {first['start']}")
        if "starts_within" in expect and not _starts_within(
            truth, expect["vehicle"], expect["starts_within"], first["start"]
        ):
            failures.append(f"not within {expect['starts_within']}: {expect}")

    readings = result.readings
    if labels.get("expect_duplicates") and len({r["event_id"] for r in readings}) == len(readings):
        failures.append("expected duplicate deliveries")
    if labels.get("expect_buffered_readings") and not any(r["link"]["buffered"] for r in readings):
        failures.append("expected buffered readings")
    boots = len({r["boot_id"] for r in readings})
    if "expect_boot_ids" in labels and boots != labels["expect_boot_ids"]:
        failures.append(f"boot ids: {boots} != {labels['expect_boot_ids']}")
    for expect in labels.get("stops", []):
        entry = next((s for s in result.stops if s["stop_id"] == expect["stop"]), None)
        if entry is None:
            failures.append(f"no visit to {expect['stop']}")
            continue
        if "on_time" in expect and entry["on_time"] != expect["on_time"]:
            failures.append(f"on_time at {expect['stop']}")
        for shipment, in_spec in expect.get("in_spec", {}).items():
            got = next((d for d in entry["delivered"] if d["shipment_id"] == shipment), None)
            if got is None or got["in_spec"] != in_spec:
                failures.append(f"in_spec {shipment} at {expect['stop']}")
    return failures
