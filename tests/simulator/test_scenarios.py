"""Every scenario: its ground-truth labels hold, every reading satisfies the Avro contract,
and the same seed produces byte-identical output."""

import hashlib
import json
from datetime import datetime
from pathlib import Path
from typing import Any

import fastavro
import pytest
from watchtower_contracts import event_id, load_schema
from watchtower_simulator import scenario as scenarios
from watchtower_simulator.cli import main
from watchtower_simulator.clock import to_ms
from watchtower_simulator.engine import Result, Simulation, iter_jsonl_ready
from watchtower_simulator.routes import default_data_dir
from watchtower_simulator.scenario import duration_ms
from watchtower_simulator.testing import run

NAMES = sorted(
    p.stem
    for p in (default_data_dir() / "scenarios").glob("*.yaml")
    if not p.stem.endswith(".labels")
)
SCHEMA = fastavro.parse_schema(load_schema("telemetry_event"))


def digest(r: Result) -> str:
    h = hashlib.sha256()
    for row in [*iter_jsonl_ready(r.readings), *r.recording, *r.truth]:
        h.update(json.dumps(row, sort_keys=True).encode())
    return h.hexdigest()


def test_the_required_scenarios_exist() -> None:
    required = {
        "compressor_gradual_degradation", "compressor_hard_fail", "door_open_while_moving",
        "door_open_at_depot", "dead_zone_with_excursion", "defrost_cycle", "duplicate_storm",
    }  # fmt: skip
    assert required <= set(NAMES)


@pytest.mark.parametrize("name", NAMES)
def test_ground_truth_labels_hold(name: str) -> None:
    labels = scenarios.load_labels(name)
    sc = scenarios.load(name)
    truth = run(name).truth
    start = sc.start_ms

    def offset(iso: str) -> int:
        return to_ms(datetime.fromisoformat(iso.replace("Z", "+00:00"))) - start

    for expect in labels["expect"]:
        found = [
            t
            for t in truth
            if t["vehicle_id"] == expect["vehicle"]
            and t["kind"] == expect["kind"]
            and ("shipment" not in expect or t.get("shipment_id") == expect["shipment"])
        ]
        assert bool(found) == expect["present"], expect
        if not found:
            continue
        first = found[0]
        if "starts_after" in expect:
            assert offset(first["start"]) >= duration_ms(expect["starts_after"]), expect
        if "starts_within" in expect:
            windows = [
                t
                for t in truth
                if t["vehicle_id"] == expect["vehicle"] and t["kind"] == expect["starts_within"]
            ]
            assert any(
                w["start"] <= first["start"] and (w["end"] is None or first["start"] < w["end"])
                for w in windows
            ), expect

    readings = run(name).readings
    if labels.get("expect_duplicates"):
        assert len({r["event_id"] for r in readings}) < len(readings)
    if labels.get("expect_buffered_readings"):
        assert any(r["link"]["buffered"] for r in readings)
    stops = run(name).stops
    for expect in labels.get("stops", []):
        entry = next(s for s in stops if s["stop_id"] == expect["stop"])
        for key in ("on_time",):
            if key in expect:
                assert entry[key] == expect[key], (expect, entry)
        for delivered in expect.get("in_spec", {}).items():
            got = next(d for d in entry["delivered"] if d["shipment_id"] == delivered[0])
            assert got["in_spec"] == delivered[1], (expect, got)
    if "expect_boot_ids" in labels:
        assert len({r["boot_id"] for r in readings}) == labels["expect_boot_ids"]


@pytest.mark.parametrize("name", NAMES)
def test_every_reading_matches_the_avro_contract(name: str) -> None:
    # A device with a fast clock can stamp readings "in the future"; everyone else can't.
    skewed = {t["vehicle_id"] for t in run(name).truth if t["kind"] == "fault_clock_skew"}
    for r in run(name).readings:
        assert fastavro.validate(r, SCHEMA, raise_errors=True)
        assert r["event_id"] == str(event_id(r["device_id"], r["boot_id"], r["seq"]))
        if r["vehicle_id"] not in skewed:
            assert r["ingest_time"] > r["event_time"]


@pytest.mark.parametrize("name", NAMES)
def test_readings_arrive_in_gateway_order(name: str) -> None:
    times = [r["ingest_time"] for r in run(name).readings]
    assert times == sorted(times)


def test_same_seed_is_byte_identical_and_a_new_seed_differs() -> None:
    sc = scenarios.load("dead_zone_with_excursion")
    assert digest(Simulation(sc).run()) == digest(Simulation(sc).run())
    other = scenarios.parse({**sc.raw, "seed": sc.seed + 1})
    assert digest(Simulation(other).run()) != digest(Simulation(sc).run())


def test_cli_writes_readings_recording_and_truth(tmp_path: Path) -> None:
    out, rec, truth = tmp_path / "r.jsonl", tmp_path / "f.jsonl", tmp_path / "t.jsonl"
    main(
        [
            "run",
            "defrost_cycle",
            "--out",
            str(out),
            "--recording",
            str(rec),
            "--truth",
            str(truth),
            "--duration",
            "30m",
        ]
    )
    rows: list[dict[str, Any]] = [
        json.loads(line) for line in out.read_text(encoding="utf-8").splitlines()
    ]
    assert len(rows) == 60  # one vehicle, 30 s samples, 30 minutes
    assert rows[0]["event_time"].endswith("Z")
    assert len(rec.read_text(encoding="utf-8").splitlines()) == 120  # every 15 s
    assert truth.exists()
