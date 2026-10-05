"""Shipments as thermal nodes, vehicle classes, delivery rounds and the stop log."""

from dataclasses import replace
from datetime import UTC, datetime
from itertools import pairwise

import pytest
from watchtower_simulator.cargo import PROFILES
from watchtower_simulator.clock import iso, rng, to_ms
from watchtower_simulator.fleet import VEHICLE_CLASSES, Shipment, parse_shipments, sample_drop
from watchtower_simulator.rounds import window_ms
from watchtower_simulator.testing import rows, run
from watchtower_simulator.thermal import Inputs, ThermalParams, step_multi

PHARMA = PROFILES["pharma_2_8"]


def test_cargo_nodes_drift_towards_shared_air_at_their_own_pace() -> None:
    carton = Shipment("A", PHARMA, 0.1, 10.0, packaging="carton")
    boxed = Shipment("B", PHARMA, 0.1, 10.0, packaging="insulated_box")
    i = Inputs(ambient_c=5.0, setpoint_c=5.0, health=0.0)  # no cooling, box at 5 °C ambient
    air, _, temps = step_multi(5.0, False, [10.0, 10.0], [carton.load(), boxed.load()], [0.0, 0.0],
                               ThermalParams(), i, 1800.0)  # fmt: skip
    assert 5.0 < temps[0] < temps[1] < 10.0  # the carton gives up heat faster
    assert air > 5.0


def test_empty_box_is_just_air() -> None:
    air, _, temps = step_multi(20.0, True, [], [], [], ThermalParams(), Inputs(30.0, 5.0), 600.0)
    assert temps == []
    assert air < 20.0


def test_classic_trucks_get_one_shipment_and_vans_many() -> None:
    single = parse_shipments("TRK-1", {}, "frozen", 20, None)
    assert [s.shipment_id for s in single] == ["TRK-1-S1"]
    assert single[0].pallets == 20
    many = parse_shipments(
        "VAN-1",
        {"shipments": [{"profile": "pharma_2_8", "kg": 30, "receiver": "X"}]},
        "frozen",
        0,
        None,
    )
    assert many[0].mass_kg == pytest.approx(30.0)
    assert many[0].receiver == "X"


def test_a_door_opening_hits_a_van_harder_than_a_trailer() -> None:
    i = Inputs(ambient_c=32.0, setpoint_c=5.0, door_open=True)
    trailer = ThermalParams()
    van = replace(ThermalParams(), **VEHICLE_CLASSES["van"].thermal)
    load = [Shipment("A", PHARMA, 1.0, 5.0).load()]
    air_trailer, _, _ = step_multi(5.0, True, [5.0], load, [0.0], trailer, i, 120.0)
    air_van, _, _ = step_multi(5.0, True, [5.0], load, [0.0], van, i, 120.0)
    assert air_van > air_trailer


def test_drop_times_scale_with_customer_type() -> None:
    r = rng(8, "drops")
    pharmacy = [sample_drop("pharmacy", r) for _ in range(400)]
    market = [sample_drop("open_market", r) for _ in range(400)]
    assert sorted(d for d, _ in market)[200] > 2 * sorted(d for d, _ in pharmacy)[200]
    assert all(door <= dwell for dwell, door in pharmacy + market)


def test_windows_are_local_lagos_time() -> None:
    day = to_ms(datetime(2026, 10, 6, 6, 20, tzinfo=UTC))
    start, end = window_ms("07:00-09:30", day)
    assert start is not None
    assert end is not None
    assert iso(start) == "2026-10-06T06:00:00.000Z"  # 07:00 WAT
    assert end - start == 150 * 60_000


def test_round_log_records_plan_actual_window_and_delivered_temperatures() -> None:
    stops = run("abuja_pharma_multidrop").stops
    assert [s["stop_id"] for s in stops] == [f"ABJ-U1-0{n}" for n in range(1, 7)]
    for s in stops:
        assert s["planned_arrival"]
        assert s["actual_arrival"]
        assert s["window_end"]
        assert s["departure"] > s["actual_arrival"]
        assert s["door_open_s"] > 0
        assert len(s["delivered"]) == 1


def test_shipments_leave_the_van_at_their_receiver() -> None:
    van = rows("abuja_pharma_multidrop", "VAN-ABJ1")
    counts = [len(r["shipments"]) for r in van]
    assert counts[0] == 6
    assert counts[-1] == 0
    assert all(b <= a for a, b in pairwise(counts))
    weights = [r["cargo_weight_kg"] for r in van]
    assert weights[0] == 380
    assert weights[-1] == 0


def test_drop_door_opens_after_the_van_stops() -> None:
    truth = run("abuja_pharma_multidrop").truth
    moving = [t for t in truth if t["kind"] == "door_open_moving"]
    assert len(moving) == 1  # only the unlatched door on the move, no arrival blips


def test_parked_van_without_genset_loses_reefer_power() -> None:
    van = rows("abuja_pharma_multidrop", "VAN-ABJ1")
    assert van[-1]["power_source"] == "NONE"
    assert any(t["kind"] == "reefer_power_lost" for t in run("abuja_pharma_multidrop").truth)
