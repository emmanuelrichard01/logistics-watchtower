"""The console draws apps/dashboard-fixtures/corridors.geojson and places vehicles from the
fleet-state recording. Both must agree with the geometry the simulator drives on."""

import json
from pathlib import Path
from typing import Any

import pytest
from watchtower_simulator.geo import haversine_km
from watchtower_simulator.routes import default_data_dir, load_routes
from watchtower_simulator.testing import read_jsonl, run

FIXTURES = default_data_dir().parent / "apps" / "dashboard-fixtures"
CONSOLE: dict[str, Any] = json.loads((FIXTURES / "corridors.geojson").read_text(encoding="utf-8"))
ROUTES = load_routes()
CORRIDORS = {k: r for k, r in ROUTES.items() if r.kind == "corridor"}


def lines() -> dict[str, dict[str, Any]]:
    return {
        f["properties"]["id"]: f
        for f in CONSOLE["features"]
        if f["geometry"]["type"] == "LineString"
    }


def test_console_lines_are_the_simulator_geometry() -> None:
    assert sorted(lines()) == sorted(CORRIDORS)
    for route_id, feature in lines().items():
        route = ROUTES[route_id]
        assert [tuple(c) for c in feature["geometry"]["coordinates"]] == route.points
        props = feature["properties"]
        assert props["length_km"] == route.length_km
        assert props["source"] == "osrm"
        assert "OpenStreetMap" in props["attribution"]
        assert props["dead_zones"] == [
            {"name": z.name, "from_km": z.from_km, "to_km": z.to_km} for z in route.dead_zones
        ]


def test_stations_sit_on_their_corridor_at_their_km_post() -> None:
    stations = [f for f in CONSOLE["features"] if f["geometry"]["type"] == "Point"]
    assert {s["properties"]["corridor_id"] for s in stations} == set(CORRIDORS)
    for s in stations:
        p = s["properties"]
        lat, lon, _ = ROUTES[p["corridor_id"]].position(p["km_along"])
        assert haversine_km(tuple(s["geometry"]["coordinates"]), (lon, lat)) < 0.005
    depots = {s["properties"]["name"] for s in stations if s["properties"]["depot"]}
    assert {"Lagos Mainland", "Abuja", "Port Harcourt", "Benin City"} <= depots


def test_file_stays_small() -> None:
    assert (FIXTURES / "corridors.geojson").stat().st_size < 400_000


@pytest.mark.parametrize(
    "path", sorted(FIXTURES.glob("*.fleet.jsonl.gz")), ids=lambda p: Path(p).name
)
def test_recorded_positions_lie_on_the_corridor_line(path: Path) -> None:
    for row in read_jsonl(path)[::50]:
        if row["off_route_km"] > 0:
            continue  # a hijacked truck has left the corridor on purpose
        lat, lon, _ = ROUTES[row["route_id"]].position(row["km_along"])
        assert haversine_km((row["lon"], row["lat"]), (lon, lat)) < 0.001


def test_fixtures_have_exactly_the_columns_the_engine_emits_today() -> None:
    # A stale fixture (recorded before a schema change) fails here instead of in the console.
    current = set(run("defrost_cycle").recording[0])
    for path in sorted(FIXTURES.glob("*.fleet.jsonl.gz")):
        first = read_jsonl(path)[0]
        assert set(first) == current, path.name


def test_every_documented_fleet_field_exists() -> None:
    readme = (FIXTURES / "README.md").read_text(encoding="utf-8")
    for column in run("defrost_cycle").recording[0]:
        assert f"`{column}`" in readme, column
