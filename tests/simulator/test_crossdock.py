"""Cross-dock handover from inter-state trucks to city vans."""

from datetime import datetime

from watchtower_simulator.clock import to_ms
from watchtower_simulator.fleet import parse_shipments
from watchtower_simulator.testing import rows, run

NAME = "cross_dock_handover_delay"


def ms(iso: str) -> int:
    return to_ms(datetime.fromisoformat(iso.replace("Z", "+00:00")))


def test_waiting_vans_start_empty_never_with_a_phantom_load() -> None:
    assert parse_shipments("VAN-1", {"handover_from": "TRK-1"}, "frozen", 20, None) == []
    assert parse_shipments("VAN-2", {"shipments": []}, "frozen", 20, None) == []
    van = rows(NAME, "VAN-ABJ2")
    assert van[0]["shipments"] == []
    assert van[0]["stop_reason"] == "awaiting_handover"


def test_trucks_unload_onto_the_dock_and_park() -> None:
    truck = rows(NAME, "TRK-X01")
    reasons = [r["stop_reason"] for r in truck]
    assert "cross_dock" in reasons
    assert reasons[-1] == "arrived"
    assert truck[-1]["shipments"] == []


def test_dock_time_runs_from_unload_to_the_van_loading() -> None:
    truth = run(NAME).truth
    late = [t for t in truth if t["kind"] == "on_dock" and t["vehicle_id"] == "VAN-ABJ2"]
    routine = [t for t in truth if t["kind"] == "on_dock" and t["vehicle_id"] == "VAN-ABJ3"]
    late_min = (ms(late[0]["end"]) - ms(late[0]["start"])) / 60_000
    routine_min = (ms(routine[0]["end"]) - ms(routine[0]["start"])) / 60_000
    assert late_min > 120
    assert routine_min < 30


def test_open_dock_delay_ruins_the_load_and_the_routine_handover_does_not() -> None:
    stops = {s["stop_id"]: s for s in run(NAME).stops}
    assert all(
        not d["in_spec"]
        for s in stops.values()
        if s["vehicle_id"] == "VAN-ABJ2"
        for d in s["delivered"]
    )
    assert all(
        d["in_spec"]
        for s in stops.values()
        if s["vehicle_id"] == "VAN-ABJ3"
        for d in s["delivered"]
    )


def test_van_round_is_planned_from_its_actual_dispatch() -> None:
    stops = [s for s in run(NAME).stops if s["vehicle_id"] == "VAN-ABJ2"]
    first = stops[0]
    load_end = max(
        ms(t["end"])
        for t in run(NAME).truth
        if t["vehicle_id"] == "VAN-ABJ2" and t["kind"] == "on_dock"
    )
    assert ms(first["planned_arrival"]) > load_end
