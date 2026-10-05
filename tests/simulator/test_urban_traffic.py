"""Urban traffic: time-of-day congestion and slow corridors."""

import statistics
from datetime import UTC, datetime

from watchtower_simulator.clock import to_ms
from watchtower_simulator.operations import urban_congestion
from watchtower_simulator.testing import rows, run


def at(day: int, hour_utc: int) -> int:
    return to_ms(datetime(2026, 10, day, hour_utc, 0, tzinfo=UTC))


def test_lagos_rush_is_worse_than_abuja_and_worst_on_slow_corridors() -> None:
    monday_8 = at(5, 7)  # 08:00 WAT
    assert urban_congestion("Lagos", "slow_corridor", monday_8) < urban_congestion(
        "Lagos", "urban", monday_8
    )
    assert urban_congestion("Lagos", "urban", monday_8) < urban_congestion(
        "Abuja", "urban", monday_8
    )
    assert urban_congestion("Lagos", "urban", at(5, 12)) > urban_congestion(
        "Lagos", "urban", monday_8
    )
    assert urban_congestion("Lagos", "urban", at(4, 7)) > urban_congestion(
        "Lagos", "urban", monday_8
    )  # Sunday


def test_third_mainland_bridge_crawls_in_the_morning_rush() -> None:
    van = [r for r in rows("lagos_last_mile_morning_rush", "VAN-LAG1") if r["stop_reason"] is None]
    bridge = [r["speed_kmh"] for r in van if 21.0 <= r["km_along"] <= 23.0]
    elsewhere = [r["speed_kmh"] for r in van if r["km_along"] < 20.0 and r["speed_kmh"] > 0]
    assert bridge
    assert statistics.mean(bridge) < statistics.mean(elsewhere)


def test_lateness_accumulates_along_the_round() -> None:
    lag1 = [s for s in run("lagos_last_mile_morning_rush").stops if s["vehicle_id"] == "VAN-LAG1"]
    delays = [s["delay_vs_plan_min"] for s in lag1]
    assert delays[-1] > delays[0]
    assert any(not s["on_time"] for s in lag1)


def test_trike_stops_only_where_it_has_a_delivery() -> None:
    trike = [
        s for s in run("lagos_last_mile_morning_rush").stops if s["vehicle_id"] == "TRIKE-LAG3"
    ]
    assert [s["stop_id"] for s in trike] == ["LAG-U2-02"]
