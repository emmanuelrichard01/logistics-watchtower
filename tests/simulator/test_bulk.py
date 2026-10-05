"""Layer 7: bulk mode. Same physics where kept, contract-valid output, deterministic."""

from datetime import UTC, datetime

import fastavro
import numpy as np
import pytest
from hypothesis import given, settings
from hypothesis import strategies as st
from watchtower_contracts import event_id, load_schema
from watchtower_simulator.bulk import BulkFleet, bench, thermal_step
from watchtower_simulator.geo import haversine_km
from watchtower_simulator.routes import load_routes
from watchtower_simulator.thermal import Inputs, Load, ThermalParams, ThermalState, step

SCHEMA = fastavro.parse_schema(load_schema("telemetry_event"))
START = 1_791_000_000_000


@settings(max_examples=60, deadline=None)
@given(
    air=st.floats(-25, 30),
    cargo=st.floats(-25, 30),
    ambient=st.floats(15, 42),
    health=st.floats(0, 1),
    on=st.booleans(),
    pallets=st.integers(1, 24),
)
def test_vectorised_thermal_matches_the_full_model(
    air: float, cargo: float, ambient: float, health: float, on: bool, pallets: int
) -> None:
    p = ThermalParams()
    load = Load(pallets * 700 * 1.8, pallets * 0.03)
    full = step(ThermalState(air, cargo, on), p, load, Inputs(ambient, -20.0, health), 30.0)
    a, c, o = thermal_step(
        np.array([air]),
        np.array([cargo]),
        np.array([on]),
        np.array([load.ua_kw_per_k]),
        np.array([load.capacity_kj_per_k]),
        np.array([ambient]),
        np.array([-20.0]),
        np.array([health]),
        p,
        30.0,
    )
    assert a[0] == pytest.approx(full.air_c, abs=1e-9)
    assert c[0] == pytest.approx(full.cargo_c, abs=1e-9)
    assert bool(o[0]) == full.compressor_on


def test_same_seed_same_fleet() -> None:
    a, b = BulkFleet(500, 3, START), BulkFleet(500, 3, START)
    ba, bb = a.step(), b.step()
    for _ in range(4):
        ba, bb = a.step(), b.step()
    assert np.array_equal(ba.cargo, bb.cargo)
    assert np.array_equal(ba.sent, bb.sent)
    assert a.records(ba) == b.records(bb)


def test_records_satisfy_the_contract_and_identity() -> None:
    fleet = BulkFleet(300, 4, START)
    batch = fleet.step()
    rows = fleet.records(batch)
    assert len(rows) == batch.count
    for r in rows[:100]:
        record = {
            **r,
            "event_time": datetime.fromisoformat(r["event_time"].replace("Z", "+00:00")),
            "ingest_time": datetime.now(UTC),
        }
        record["event_id"] = __import__("uuid").UUID(r["event_id"])
        assert fastavro.validate(record, SCHEMA, raise_errors=True)
        assert r["event_id"] == str(event_id(r["device_id"], r["boot_id"], r["seq"]))


def test_positions_lie_on_the_corridors_and_outages_drop_readings() -> None:
    routes = [r for r in load_routes().values() if r.kind == "corridor"]
    fleet = BulkFleet(400, 5, START)
    batch = fleet.step()
    sent = batch.count
    for _ in range(29):
        batch = fleet.step()
        sent += batch.count
    on_line = [
        min(
            haversine_km((float(batch.lon[i]), float(batch.lat[i])), p)
            for p in routes[int(fleet.route[i])].points[::5]
        )
        for i in range(0, 400, 40)
    ]
    assert max(on_line) < 5.0
    assert fleet.dropped > 0
    assert sent + fleet.dropped == 400 * 30


def test_bench_reports_every_stage() -> None:
    r = bench(200, 3)
    assert r.readings > 0
    assert r.physics_eps > r.end_to_end_eps > 0
