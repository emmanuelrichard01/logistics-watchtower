"""Layer 5: hijack, breakdown and tyre blowout."""

import json
from datetime import datetime
from pathlib import Path

from watchtower_simulator.clock import to_ms
from watchtower_simulator.environment import local_hour
from watchtower_simulator.geo import haversine_km
from watchtower_simulator.routes import default_data_dir, load_routes
from watchtower_simulator.testing import read_jsonl, rows, run

ROUTES = load_routes()
CONSOLE = json.loads(
    (default_data_dir().parent / "apps" / "dashboard-fixtures" / "corridors.geojson").read_text(
        encoding="utf-8"
    )
)
DEPOTS: list[tuple[float, float]] = [
    (f["geometry"]["coordinates"][0], f["geometry"]["coordinates"][1])
    for f in CONSOLE["features"]
    if f["geometry"]["type"] == "Point" and f["properties"]["depot"]
]


def ms(iso: str) -> int:
    return to_ms(datetime.fromisoformat(iso.replace("Z", "+00:00")))


def test_hijacked_truck_leaves_the_corridor_while_its_km_post_freezes() -> None:
    trk = rows("theft_hijack", "TRK-B01")
    route = ROUTES["RT-LAG-ABJ"]
    last = trk[-1]
    lat, lon, _ = route.position(last["km_along"])
    assert haversine_km((last["lon"], last["lat"]), (lon, lat)) > 10.0
    deviated = [r for r in trk if r["off_route_km"] > 0]
    assert len({r["km_along"] for r in deviated}) == 1


def test_door_opens_at_night_far_from_any_depot() -> None:
    truth = run("theft_hijack").truth
    door = next(
        t for t in truth if t["vehicle_id"] == "TRK-B01" and t["kind"] == "door_open_stationary"
    )
    hour = local_hour(ms(door["start"]))
    assert hour >= 19 or hour < 6
    at_door = next(r for r in rows("theft_hijack", "TRK-B01") if r["t"] >= door["start"])
    nearest = min(haversine_km((at_door["lon"], at_door["lat"]), d) for d in DEPOTS)
    assert nearest > 20.0


def test_tracker_cut_means_silence_not_buffering() -> None:
    result = run("theft_hijack")
    off = next(
        t for t in result.truth if t["vehicle_id"] == "TRK-B01" and t["kind"] == "tracker_offline"
    )
    after = [
        r
        for r in result.readings
        if r["vehicle_id"] == "TRK-B01" and to_ms(r["event_time"]) >= ms(off["start"])
    ]
    assert after == []
    assert all(
        r["buffer_depth"] == 0 for r in rows("theft_hijack", "TRK-B01") if r["t"] >= off["start"]
    )


def test_breakdown_on_an_empty_genset_loses_reefer_power_then_cargo() -> None:
    truth = [t for t in run("breakdown_and_blowout").truth if t["vehicle_id"] == "TRK-B11"]
    lost = next(t for t in truth if t["kind"] == "reefer_power_lost")
    breach = next(t for t in truth if t["kind"] == "cargo_excursion")
    assert lost["start"] < breach["start"]
    trk = rows("breakdown_and_blowout", "TRK-B11")
    assert min(r["genset_fuel_l"] for r in trk) == 0.0
    assert any(r["power_source"] == "GENSET" for r in trk)


def test_blowout_is_a_sudden_stop_not_a_theft() -> None:
    truth = [t for t in run("breakdown_and_blowout").truth if t["vehicle_id"] == "TRK-B12"]
    blowout = next(t for t in truth if t["kind"] == "tyre_blowout")
    stop = next(t for t in truth if t["kind"] == "stop_unplanned")
    assert stop["start"] == blowout["start"]
    duration_min = (ms(stop["end"]) - ms(stop["start"])) / 60_000
    assert 70 <= duration_min <= 80
    assert not any(t["kind"] == "route_deviation" for t in truth)


def test_breakdown_stop_lasts_its_scheduled_duration() -> None:
    stop = next(
        t
        for t in run("breakdown_and_blowout").truth
        if t["vehicle_id"] == "TRK-B11" and t["kind"] == "stop_breakdown"
    )
    assert (ms(stop["end"]) - ms(stop["start"])) == 6 * 3_600_000


def test_fixture_rows_off_route_are_flagged() -> None:
    for path in Path(default_data_dir().parent / "apps" / "dashboard-fixtures").glob(
        "*.fleet.jsonl.gz"
    ):
        for row in read_jsonl(path)[:5]:
            assert "off_route_km" in row
