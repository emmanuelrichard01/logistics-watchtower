"""Layer 6: the seeded fleet-day generator."""

from collections import Counter
from functools import cache

import pytest
import yaml
from watchtower_simulator import scenario as scenarios
from watchtower_simulator.clock import rng
from watchtower_simulator.engine import Result, Simulation
from watchtower_simulator.fleetday import INCIDENTS, FleetDay, generate, poisson
from watchtower_simulator.testing import label_failures


@cache
def stressed() -> tuple[FleetDay, Result]:
    day = generate(seed=31, trucks=4, vans=3, hours=6.0, handovers=1, rate_scale=25.0)
    return day, Simulation(scenarios.parse(day.doc)).run()


def test_same_seed_same_day_and_yaml_round_trips() -> None:
    a, b = generate(5, 10, 4), generate(5, 10, 4)
    assert a == b
    assert generate(6, 10, 4) != a
    assert scenarios.parse(yaml.safe_load(yaml.safe_dump(a.doc))) == scenarios.parse(a.doc)


def test_incident_counts_follow_their_rates() -> None:
    days = [generate(seed, trucks=20, vans=0, hours=24.0, handovers=0) for seed in range(60)]
    counts = Counter(i["incident"] for d in days for i in d.labels["injected"])
    truck_days = 60 * 20
    for kind in ("compressor_degradation", "link_outage", "reboot"):
        expected = INCIDENTS[kind][0] * truck_days
        assert counts[kind] == pytest.approx(expected, rel=0.35), kind


def test_poisson_sampler_has_the_right_mean() -> None:
    r = rng(1, "poisson")
    draws = [poisson(r, 2.5) for _ in range(4000)]
    assert sum(draws) / len(draws) == pytest.approx(2.5, rel=0.05)


def test_fleet_mix_covers_trucks_vans_handovers_and_operations() -> None:
    day = generate(9, trucks=8, vans=5, handovers=2)
    fleet = day.doc["fleet"]
    assert day.doc["operations"] is True
    assert sum(v["vehicle_id"].startswith("TRK-") for v in fleet) == 10  # 8 + 2 handover trucks
    assert sum("handover_from" in v for v in fleet) == 2
    assert all(
        v.get("vehicle_class") in ("van", "trike")
        for v in fleet
        if v["vehicle_id"].startswith("VAN-")
    )


def test_every_injected_incident_shows_up_in_ground_truth() -> None:
    day, result = stressed()
    assert len(day.labels["injected"]) >= 10
    assert label_failures(result, day.labels, scenarios.parse(day.doc).start_ms) == []
