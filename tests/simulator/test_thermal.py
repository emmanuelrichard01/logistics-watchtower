"""Sanity properties of the two-node reefer model (plan section 8)."""

from dataclasses import replace
from itertools import pairwise

from hypothesis import given, settings
from hypothesis import strategies as st
from watchtower_simulator.cargo import PROFILES
from watchtower_simulator.thermal import Inputs, Load, ThermalParams, ThermalState, step

P = ThermalParams()
PHARMA = PROFILES["pharma_2_8"]
FROZEN = PROFILES["frozen"]


def load(profile: str, pallets: int) -> Load:
    p = PROFILES[profile]
    return Load(p.capacity_kj_per_k(pallets), p.cargo_ua_kw_per_k(pallets))


def run(
    state: ThermalState, ld: Load, i: Inputs, seconds: float, dt: float = 5.0
) -> list[ThermalState]:
    states = [state]
    for _ in range(int(seconds / dt)):
        states.append(step(states[-1], P, ld, i, dt))
    return states


def test_healthy_unit_holds_setpoint_by_cycling() -> None:
    states = run(
        ThermalState(5.0, 5.0),
        load("pharma_2_8", 4),
        Inputs(ambient_c=33.0, setpoint_c=5.0),
        4 * 3600,
    )
    tail = states[len(states) // 2 :]
    duty = sum(s.compressor_on for s in tail) / len(tail)
    assert 0.05 < duty < 0.95  # it cycles rather than running flat out or idling
    assert all(abs(s.air_c - 5.0) < 1.5 for s in tail)
    assert abs(tail[-1].cargo_c - 5.0) < 0.3


def test_cargo_lags_air_after_a_failure() -> None:
    states = run(
        ThermalState(5.0, 5.0),
        load("pharma_2_8", 4),
        Inputs(ambient_c=33.0, setpoint_c=5.0, health=0.0),
        3600,
    )
    final = states[-1]
    assert final.air_c - 5.0 > 3 * (final.cargo_c - 5.0) > 0  # air moves first, cargo follows


def test_failed_unit_approaches_ambient_equilibrium() -> None:
    # Wall and cargo surface act in series: frozen cargo takes days (time constant ~4 d).
    states = run(
        ThermalState(-20.0, -20.0),
        load("frozen", 20),
        Inputs(ambient_c=30.0, setpoint_c=-20.0, health=0.0),
        30 * 86_400,
        dt=600.0,
    )
    final = states[-1]
    assert abs(final.air_c - 30.0) < 0.2
    assert abs(final.cargo_c - 30.0) < 0.2
    assert all(b.cargo_c >= a.cargo_c - 1e-9 for a, b in pairwise(states))


def test_defrost_raises_return_air_briefly_without_a_cargo_excursion() -> None:
    ld = load("frozen", 20)
    cold = run(ThermalState(-20.0, -20.0), ld, Inputs(ambient_c=32.0, setpoint_c=-20.0), 1800)[-1]
    during = run(cold, ld, Inputs(ambient_c=32.0, setpoint_c=-20.0, defrost=True), 25 * 60)
    after = run(during[-1], ld, Inputs(ambient_c=32.0, setpoint_c=-20.0), 45 * 60)
    peak = max(s.air_c for s in during)
    assert peak - cold.air_c > 3.0  # a visible return-air rise
    assert max(s.cargo_c for s in during + after) < FROZEN.max_c  # never an excursion
    assert max(s.cargo_c for s in during + after) - cold.cargo_c < 0.5
    assert after[-1].air_c < FROZEN.setpoint_c + 1.5  # recovers once cooling resumes


@settings(max_examples=40, deadline=None)
@given(
    ambient=st.floats(min_value=20.0, max_value=42.0),
    start=st.floats(min_value=-25.0, max_value=10.0),
    pallets=st.integers(min_value=1, max_value=26),
)
def test_failed_unit_with_door_closed_never_cools_the_cargo(
    ambient: float, start: float, pallets: int
) -> None:
    ld = load("frozen", pallets)
    states = run(
        ThermalState(start, start),
        ld,
        Inputs(ambient_c=ambient, setpoint_c=-20.0, health=0.0),
        2 * 3600,
        dt=30.0,
    )
    assert all(b.cargo_c >= a.cargo_c - 1e-9 for a, b in pairwise(states))
    assert all(s.cargo_c <= ambient + 1e-6 for s in states)


def test_open_door_heats_air_faster_than_closed() -> None:
    ld = load("frozen", 20)
    base = Inputs(ambient_c=32.0, setpoint_c=-20.0)
    closed = run(ThermalState(-20.0, -20.0), ld, base, 600)[-1]
    opened = run(ThermalState(-20.0, -20.0), ld, replace(base, door_open=True), 600)[-1]
    assert opened.air_c > closed.air_c + 5.0
