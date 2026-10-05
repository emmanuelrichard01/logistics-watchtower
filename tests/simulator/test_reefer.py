"""Layer 1: cargo profiles and reefer realism."""

import re
import statistics
from functools import cache
from itertools import pairwise
from typing import Any

import pytest
from watchtower_simulator import scenario as scenarios
from watchtower_simulator.cargo import PROFILES
from watchtower_simulator.clock import rng
from watchtower_simulator.engine import Result, Simulation
from watchtower_simulator.reefer import Reefer, ReeferParams
from watchtower_simulator.stops import STOP_TYPES, door_open_seconds
from watchtower_simulator.thermal import Inputs, Load, ThermalParams, ThermalState, step

HOUR_MS = 3_600_000


@cache
def run(name: str) -> Result:
    return Simulation(scenarios.load(name)).run()


def rows(name: str, vehicle: str) -> list[dict[str, Any]]:
    return [r for r in run(name).recording if r["vehicle_id"] == vehicle]


def reefer(**params: float) -> Reefer:
    return Reefer(ReeferParams(**params), humidity_pct=80.0, genset_l=100.0, last_defrost_end_ms=0)


def test_ice_builds_with_runtime_and_scheduled_defrost_restores_capacity() -> None:
    r = reefer(defrost_interval_h=6.0)
    factors: list[float] = []
    defrost_seen = False
    for k in range(int(8 * 3600 / 5)):
        r.update(k * 5000, 5.0, compressor_on=True, door_open=False, forced_defrost=False,
                 outside_rh_pct=80.0, box_rh_pct=80.0)  # fmt: skip
        factors.append(r.capacity_factor())
        defrost_seen |= r.defrosting()
    before = factors[int(5.9 * 720)]
    assert before < 0.9  # hours of running visibly cost capacity
    assert defrost_seen
    assert max(factors[int(6.5 * 720) :]) > 0.99  # the defrost melted the ice


def test_open_door_frosts_the_coil_and_raises_box_humidity() -> None:
    shut, opened = reefer(), reefer()
    for k in range(int(1800 / 5)):
        for unit, door in ((shut, False), (opened, True)):
            unit.update(k * 5000, 5.0, compressor_on=True, door_open=door, forced_defrost=False,
                        outside_rh_pct=90.0, box_rh_pct=60.0)  # fmt: skip
    assert opened.ice_kg > shut.ice_kg + 1.0
    assert opened.humidity_pct > 85.0  # outside air floods in
    assert shut.humidity_pct < 75.0  # relaxing towards the cargo's 60%


def test_genset_burns_fuel_only_when_it_powers_the_unit() -> None:
    engine, genset = reefer(), reefer()
    genset.power_source = "GENSET"
    for k in range(720):
        for unit in (engine, genset):
            unit.update(k * 5000, 5.0, compressor_on=True, door_open=False, forced_defrost=False,
                        outside_rh_pct=60.0, box_rh_pct=60.0)  # fmt: skip
    assert engine.genset_l == 100.0
    assert genset.genset_l == pytest.approx(100.0 - (0.8 + 2.4), abs=0.01)  # one hour at full duty


def test_warm_loaded_cargo_pulls_down_slowly_while_air_looks_normal() -> None:
    trk = rows("warm_loading", "TRK-801")
    after_2h = trk[480]  # 15 s rows
    assert float(after_2h["air_c"]) < 8.0 < float(after_2h["cargo_c"])  # air in range, cargo not
    cargo = [float(r["cargo_c"]) for r in trk[240:]]
    assert cargo[-1] < cargo[0]  # pulling down...
    assert cargo[-1] > PROFILES["fresh_produce"].max_c  # ...but not back in range after hours
    duty_warm = statistics.mean(float(r["duty_cycle_pct"]) for r in trk[240:480])
    duty_ok = statistics.mean(
        float(r["duty_cycle_pct"]) for r in rows("warm_loading", "TRK-802")[240:480]
    )
    assert duty_warm > duty_ok + 30.0  # the warm load keeps the compressor flat out


def test_respiring_produce_warms_faster_than_inert_cargo() -> None:
    bananas = PROFILES["bananas"]
    ld = Load(bananas.capacity_kj_per_k(20), bananas.cargo_ua_kw_per_k(20))
    base = Inputs(ambient_c=30.0, setpoint_c=bananas.setpoint_c, health=0.0)
    inert = live = ThermalState(13.3, 13.3)
    for _ in range(720):
        inert = step(inert, ThermalParams(), ld, base, 5.0)
        heat = bananas.respiration_kw(20, live.cargo_c)
        live = step(
            live,
            ThermalParams(),
            ld,
            Inputs(30.0, bananas.setpoint_c, 0.0, cargo_heat_kw=heat),
            5.0,
        )
    assert live.cargo_c > inert.cargo_c + 0.05
    assert bananas.respiration_kw(20, 23.3) == pytest.approx(2 * bananas.respiration_kw(20, 13.3))


def test_door_open_times_follow_the_stop_type() -> None:
    r = rng(5, "doors")
    depot = [door_open_seconds(STOP_TYPES["depot_loading"], r) for _ in range(3000)]
    drop = [door_open_seconds(STOP_TYPES["delivery_drop"], r) for _ in range(3000)]
    toll = [door_open_seconds(STOP_TYPES["toll_gate"], r) for _ in range(100)]
    assert statistics.median(depot) == pytest.approx(25 * 60, rel=0.1)
    assert statistics.median(drop) == pytest.approx(9 * 60, rel=0.1)
    assert max(depot) > 3 * statistics.median(depot)  # a long tail of slow loadings
    assert set(toll) == {0.0}


def test_power_switches_engine_to_shore_at_the_depot_and_genset_elsewhere() -> None:
    sources = [r["power_source"] for r in rows("warm_loading", "TRK-801")]
    assert sources[0] == "ENGINE"  # engine still running for the first minutes
    assert "SHORE" in sources[40:120]  # plugged in after the engine went off at the depot
    assert sources[200] == "ENGINE"  # driving


def test_reporting_bursts_while_an_alarm_condition_holds() -> None:
    readings = [r for r in run("door_open_while_moving").readings if r["vehicle_id"] == "TRK-301"]
    times = sorted({r["event_time"] for r in readings})
    gaps = [(b - a).total_seconds() for a, b in pairwise(times)]
    assert gaps.count(10.0) >= 40  # 8 minutes of door-open bursts
    assert gaps.count(30.0) > gaps.count(10.0)  # base interval the rest of the time


def test_boot_ids_are_random_64_bit_hex() -> None:
    boots = {r["boot_id"] for r in run("duplicate_storm").readings}
    assert len(boots) == 3
    assert all(re.fullmatch(r"b-[0-9a-f]{16}", b) for b in boots)


def test_one_hertz_reporting_is_available_for_load_tests() -> None:
    sc = scenarios.parse(
        {
            **scenarios.load("defrost_cycle").raw,
            "sample_interval": "1s",
            "burst_interval": "1s",
            "duration": "2m",
        }
    )
    sim = Simulation(sc)
    assert sim.step_ms == 1000
    assert len(sim.run().readings) == 120
