"""Fleet rows carry what the device reported, not just the truth."""

from watchtower_simulator.clock import iso, to_ms
from watchtower_simulator.testing import rows, run


def test_probe_fields_are_the_latest_reading_the_device_produced() -> None:
    readings = {
        (r["vehicle_id"], iso(to_ms(r["event_time"]))): r["reefer"]
        for r in run("dead_zone_with_excursion").readings
    }
    checked = 0
    for row in rows("dead_zone_with_excursion", "TRK-501"):
        key = ("TRK-501", row["probe_t"])
        if row["probe_t"] is None or key not in readings:
            continue  # sampled but still buffered or in flight
        reefer = readings[key]
        assert row["cargo_probe_c"] == reefer["cargo_probe_c"]
        assert row["return_air_probe_c"] == reefer["return_air_c"]
        assert row["supply_air_probe_c"] == reefer["supply_air_c"]
        checked += 1
    assert checked > 100


def test_probes_are_noisy_where_the_truth_is_smooth() -> None:
    steady = rows("defrost_cycle", "TRK-601")[:100]
    assert any(r["cargo_probe_c"] != r["cargo_c"] for r in steady)


def test_a_stuck_probe_shows_in_the_probe_field_but_not_the_truth() -> None:
    late = rows("stuck_cargo_probe", "TRK-A01")[-100:]
    assert len({r["cargo_probe_c"] for r in late}) == 1  # flatlined
    assert late[-1]["cargo_c"] > 8.0 > late[-1]["cargo_probe_c"]
