"""Layer 3: road features, congestion, drivers, policy, fuel and multi-drop deliveries."""

import statistics
from datetime import UTC, datetime
from itertools import pairwise
from typing import Any

from watchtower_simulator.clock import rng, to_ms
from watchtower_simulator.engine import Result
from watchtower_simulator.environment import local_hour
from watchtower_simulator.operations import (
    DRIVERS,
    FleetPolicy,
    Operations,
    congestion_factor,
    road_features,
)
from watchtower_simulator.routes import load_routes
from watchtower_simulator.testing import rows as day_rows
from watchtower_simulator.testing import run

ROUTES = load_routes()
LAG = ROUTES["RT-LAG-ABJ"]


def day() -> Result:
    return run("fleet_operations_day")


def rows(vehicle: str) -> list[dict[str, Any]]:
    return day_rows("fleet_operations_day", vehicle)


def hour_of(row: dict[str, Any]) -> float:
    return local_hour(to_ms(datetime.fromisoformat(row["t"].replace("Z", "+00:00"))))


def test_road_features_are_deterministic_and_plausible() -> None:
    assert road_features(LAG, 1) == road_features(LAG, 1)
    features = road_features(LAG, 1)
    checkpoints = [f for f in features if f.kind == "checkpoint"]
    assert 1.5 < len(checkpoints) / (LAG.length_km / 100) < 5.0
    assert sum(f.kind == "toll_gate" for f in features) == 2
    assert sum(f.kind == "weighbridge" for f in features) == 1
    assert all(LAG.segment(f.km).road_class == "highway" for f in features if f.kind == "poor_road")


def test_lagos_rush_hour_crawls_and_the_open_highway_does_not() -> None:
    monday_8 = to_ms(datetime(2026, 10, 5, 7, 0, tzinfo=UTC))  # 08:00 WAT
    monday_13 = to_ms(datetime(2026, 10, 5, 12, 0, tzinfo=UTC))
    sunday_8 = to_ms(datetime(2026, 10, 4, 7, 0, tzinfo=UTC))
    assert congestion_factor(LAG, 5.0, monday_8) < 0.5
    assert congestion_factor(LAG, 5.0, monday_13) > 0.7
    assert congestion_factor(LAG, 5.0, sunday_8) > congestion_factor(LAG, 5.0, monday_8)
    assert congestion_factor(LAG, 300.0, monday_8) == 1.0


def ops(driver: str) -> Operations:
    return Operations(DRIVERS[driver], FleetPolicy(), [], rng(3, driver))


def test_aggressive_drivers_go_faster_and_brake_harder() -> None:
    noon = to_ms(datetime(2026, 10, 5, 12, 0, tzinfo=UTC))
    speeds = {
        d: statistics.mean(ops(d).target_speed(80.0, LAG, 300.0, noon) for _ in range(500))
        for d in DRIVERS
    }
    assert speeds["cautious"] < speeds["normal"] < speeds["aggressive"]
    brakes = {}
    for d in DRIVERS:
        o = ops(d)
        for _ in range(10_000):
            o.harsh_brake(0.1)  # 1,000 km in 100 m steps
        brakes[d] = o.harsh_brakes
    assert brakes["cautious"] < brakes["normal"] < brakes["aggressive"]


def test_no_night_driving_parks_the_fleet_on_gensets() -> None:
    for vehicle in ("TRK-901", "TRK-902", "TRK-903"):
        night = [r for r in rows(vehicle) if hour_of(r) >= 19.5 or hour_of(r) < 5.5]
        assert night
        assert all(r["speed_kmh"] == 0.0 for r in night)
        # Engine off overnight away from a depot: the reefer runs on its genset.
        assert all(r["power_source"] == "GENSET" for r in night if hour_of(r) < 5.5)


def test_mandated_rest_breaks_up_long_stretches_of_driving() -> None:
    step_s = 15.0
    limit = FleetPolicy().max_drive_h * 3600
    for vehicle in ("TRK-901", "TRK-902", "TRK-903"):
        stretch = 0.0
        for r in rows(vehicle):
            stretch = stretch + step_s if r["stop_reason"] is None else 0.0
            assert stretch <= limit + 60.0


def test_drops_unload_pallets_and_cargo_weight_only_goes_down() -> None:
    trk = rows("TRK-902")
    weights = [r["cargo_weight_kg"] for r in trk]
    assert all(b <= a for a, b in pairwise(weights))
    assert weights[0] == 18 * 700
    assert weights[-1] == 5 * 700


def test_low_fuel_truck_refuels_at_a_station() -> None:
    fuel = [r["fuel_pct"] for r in rows("TRK-903")]
    assert fuel[0] == 22.0
    assert max(fuel) > 85.0


def test_checkpoint_stops_are_short_and_frequent() -> None:
    stops = [t for t in day().truth if t["kind"] == "stop_checkpoint"]
    assert len(stops) >= 15
    durations = [
        (
            datetime.fromisoformat(t["end"].replace("Z", "+00:00"))
            - datetime.fromisoformat(t["start"].replace("Z", "+00:00"))
        ).total_seconds()
        for t in stops
        if t["end"]
    ]
    assert 60 < statistics.median(durations) < 600
