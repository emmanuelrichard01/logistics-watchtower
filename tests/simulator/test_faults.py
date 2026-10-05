"""Layer 4: composable sensor and device fault injectors."""

import copy
from functools import cache
from typing import Any

import pytest
from watchtower_simulator import scenario as scenarios
from watchtower_simulator.clock import rng
from watchtower_simulator.engine import Result, Simulation
from watchtower_simulator.faults import ClockSkew, make_fault
from watchtower_simulator.geo import haversine_km

HOUR = 3_600_000


def reading() -> dict[str, Any]:
    return {
        "reefer": {"supply_air_c": -22.0, "return_air_c": -20.0, "cargo_probe_c": -19.5},
        "position": {
            "lat": 6.5,
            "lon": 3.4,
            "speed_kmh": 30.0,
            "heading_deg": 0.0,
            "gps_fix": "FIX_3D",
            "hdop": 1.0,
        },
    }


@cache
def run(name: str) -> Result:
    return Simulation(scenarios.load(name)).run()


def test_offset_and_drift() -> None:
    r = reading()
    make_fault("offset", 0, None, {"probe": "cargo_probe", "offset_c": 1.5}).apply(r, 0, rng(1))
    assert r["reefer"]["cargo_probe_c"] == -18.0
    r = reading()
    make_fault("drift", 0, None, {"probe": "return_air", "c_per_hour": 0.5}).apply(
        r, 4 * HOUR, rng(1)
    )
    assert r["reefer"]["return_air_c"] == -18.0


def test_flatline_holds_the_first_value_it_saw() -> None:
    f = make_fault("flatline", 0, None, {"probe": "cargo_probe"})
    first, later = reading(), reading()
    later["reefer"]["cargo_probe_c"] = 3.0
    f.apply(first, 0, rng(1))
    f.apply(later, HOUR, rng(1))
    assert later["reefer"]["cargo_probe_c"] == -19.5


def test_faults_compose_in_injection_order() -> None:
    r = reading()
    for f in (
        make_fault("offset", 0, None, {"probe": "cargo_probe", "offset_c": 2.0}),
        make_fault("swap", 0, None, {"probe": "cargo_probe", "other": "return_air"}),
    ):
        f.apply(r, 0, rng(1))
    assert r["reefer"]["cargo_probe_c"] == -20.0
    assert r["reefer"]["return_air_c"] == -17.5


def test_spike_and_dropout_rates() -> None:
    spike = make_fault(
        "spike", 0, None, {"probe": "cargo_probe", "probability": 0.1, "magnitude_c": 20.0}
    )
    drop = make_fault("dropout", 0, None, {"probe": "cargo_probe", "probability": 0.3})
    r1 = rng(4, "spikes")
    spikes = sum(abs(_apply(spike, r1)["reefer"]["cargo_probe_c"] + 19.5) > 10 for _ in range(2000))
    r2 = rng(4, "drops")
    drops = sum(_apply(drop, r2)["reefer"]["cargo_probe_c"] is None for _ in range(2000))
    assert spikes == pytest.approx(200, rel=0.25)
    assert drops == pytest.approx(600, rel=0.15)


def _apply(fault: Any, r: Any) -> dict[str, Any]:
    out = copy.deepcopy(reading())
    fault.apply(out, 0, r)
    return out


def test_gps_multipath_scatters_and_degrades_the_fix() -> None:
    r = rng(2, "gps")
    fault = make_fault("gps_multipath", 0, None, {"sigma_m": 60.0})
    outs = [_apply(fault, r)["position"] for _ in range(500)]
    errors_m = [haversine_km((p["lon"], p["lat"]), (3.4, 6.5)) * 1000 for p in outs]
    assert 40 < sorted(errors_m)[250] < 120  # median error of order sigma*sqrt(2)
    assert {p["gps_fix"] for p in outs} == {"FIX_2D"}
    assert min(p["hdop"] for p in outs) >= 4.0


def test_clock_skew_drifts_then_gps_sync_corrects_it() -> None:
    skew = ClockSkew(
        kind="clock_skew", start_ms=0, offset_s=90.0, drift_ppm=100.0, gps_sync_after_s=7200.0
    )
    assert skew.skew_ms(0) == 90_000
    assert skew.skew_ms(HOUR) == 90_000 + 360  # 100 ppm of an hour
    assert skew.skew_ms(3 * HOUR) == 0  # synced


def test_unknown_faults_and_probes_are_rejected() -> None:
    with pytest.raises(ValueError, match="unknown fault"):
        make_fault("gremlins", 0, None, {})
    with pytest.raises(ValueError, match="probe"):
        make_fault("offset", 0, None, {"probe": "door"})


def test_stuck_probe_hides_a_real_excursion() -> None:
    result = run("stuck_cargo_probe")
    excursion = next(t for t in result.truth if t["kind"] == "cargo_excursion")
    late = [
        r
        for r in result.readings
        if r["event_time"].isoformat() > excursion["start"].replace("Z", "+00:00")
    ]
    assert late
    assert all(r["reefer"]["cargo_probe_c"] < 8.0 for r in late)  # the probe never shows it
    assert max(r["reefer"]["return_air_c"] for r in late) > 8.0  # but the air probe does


def test_drifting_probe_reads_out_of_range_on_a_healthy_load() -> None:
    last = run("sensor_drift").readings[-1]["reefer"]
    assert last["cargo_probe_c"] > -15.0 > last["return_air_c"]


def test_clock_skew_shows_up_as_event_time_ahead_of_ingest() -> None:
    readings = run("clock_skew_and_reboot").readings
    ahead = [(r["event_time"] - r["ingest_time"]).total_seconds() for r in readings]
    assert max(ahead) == pytest.approx(90.0, abs=1.0)
    first_boot = readings[0]["boot_id"]
    after = sorted(r["seq"] for r in readings if r["boot_id"] != first_boot)
    assert after[:3] == [1, 2, 3]  # restarted sequence, not loss


def test_injecting_a_fault_does_not_shift_other_random_streams() -> None:
    raw = scenarios.load("defrost_cycle").raw
    spike = {
        "at": "5m",
        "vehicle": "TRK-601",
        "type": "sensor_fault",
        "fault": "spike",
        "probe": "supply_air",
        "probability": 0.5,
    }
    base = scenarios.parse({**raw, "duration": "30m"})
    faulty = scenarios.parse({**raw, "duration": "30m", "events": [spike]})
    a, b = Simulation(base).run().readings, Simulation(faulty).run().readings
    assert [r["reefer"]["cargo_probe_c"] for r in a] == [r["reefer"]["cargo_probe_c"] for r in b]
