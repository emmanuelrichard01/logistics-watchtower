"""Sensor trust against the plan's fault scenarios (sections 8 and 9)."""

import random

import pytest
from watchtower_domain.buckets import ProbeStats, quantise
from watchtower_domain.trust import ProbeStatus, classify, fuse_cargo


def minute(values: list[float]) -> ProbeStats:
    q = [quantise(v) for v in values]
    return ProbeStats(len(q), sum(q), min(q), max(q))


def noisy(start: float, slope_per_min: float, minutes: int, seed: int, sd: float = 0.05):
    rnd = random.Random(seed)
    return [
        minute([start + slope_per_min * m + rnd.gauss(0, sd) for _ in range(4)])
        for m in range(minutes)
    ]


def test_healthy_probes_are_ok() -> None:
    probes = {"cargo": noisy(-19.0, 0.0, 15, 1), "return_air": noisy(-20.0, 0.0, 15, 2)}
    verdicts = classify(probes)
    assert {v.status for v in verdicts.values()} == {ProbeStatus.OK}
    fused = fuse_cargo(probes)
    assert fused.value_c is not None
    assert fused.confidence == 0.95


def test_stuck_cargo_probe_is_faulty_and_never_reported_as_all_clear() -> None:
    # stuck_cargo_probe: cargo flatlines at -19 while the air probes warm.
    probes = {
        "cargo": [minute([-19.0] * 4) for _ in range(15)],
        "return_air": noisy(-20.0, 0.2, 15, 3),
    }
    fused = fuse_cargo(probes)
    assert fused.verdicts["cargo"].status is ProbeStatus.FAULTY
    assert fused.value_c is None  # uncertain, not -19 and not the air probe
    assert fused.reasons[0] == "Cargo temperature uncertain"


def test_defrost_does_not_trip_the_stuck_rule() -> None:
    # defrost_cycle: return air rises briefly; a real cargo probe still shows noise.
    ret = noisy(-20.0, 0.0, 10, 4) + noisy(-20.0, 0.6, 5, 5)
    probes = {"cargo": noisy(-19.0, 0.0, 15, 6), "return_air": ret}
    assert classify(probes)["cargo"].status is ProbeStatus.OK


def test_impossible_rate_is_suspect() -> None:
    cargo = noisy(4.0, 0.0, 15, 7)
    cargo[10] = minute([9.5] * 4)  # +5.5 °C in one minute: thermal mass forbids it
    verdict = classify({"cargo": cargo, "return_air": noisy(4.0, 0.0, 15, 8)})["cargo"]
    assert verdict.status is ProbeStatus.SUSPECT
    assert "thermal mass" in verdict.reasons[0]


def test_dropout_is_suspect() -> None:
    cargo = [s if i % 2 else None for i, s in enumerate(noisy(4.0, 0.0, 15, 9))]
    probes = {"cargo": cargo, "return_air": noisy(4.0, 0.0, 15, 10)}
    assert classify(probes)["cargo"].status is ProbeStatus.SUSPECT
    assert fuse_cargo(probes).confidence == 0.6


def test_drift_against_return_air_is_suspect() -> None:
    # sensor_drift: the cargo probe's offset grows while the air stays put.
    probes = {"cargo": noisy(4.0, 0.12, 15, 11), "return_air": noisy(3.0, 0.0, 15, 12)}
    verdict = classify(probes)["cargo"]
    assert verdict.status is ProbeStatus.SUSPECT
    assert "drifting" in verdict.reasons[0]


def test_real_warming_moves_both_probes_and_stays_ok() -> None:
    # compressor failure: cargo and air warm together; that is a truck problem, not a sensor one.
    probes = {"cargo": noisy(4.0, 0.08, 15, 13), "return_air": noisy(3.0, 0.1, 15, 14)}
    assert classify(probes)["cargo"].status is ProbeStatus.OK


@pytest.mark.parametrize("value", [-55.0, 80.0])
def test_implausible_values_are_faulty(value: float) -> None:
    cargo = noisy(4.0, 0.0, 15, 15)
    cargo[-1] = minute([value])
    assert classify({"cargo": cargo})["cargo"].status is ProbeStatus.FAULTY
