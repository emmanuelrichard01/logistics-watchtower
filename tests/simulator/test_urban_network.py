"""Urban distribution networks and the console's routes.geojson."""

import json
from typing import Any

import pytest
from watchtower_simulator.build_urban import NETWORKS, slow_corridor_at
from watchtower_simulator.geo import haversine_km
from watchtower_simulator.routes import default_data_dir, load_routes

ROUTES = load_routes()
URBAN = {k: r for k, r in ROUTES.items() if r.kind == "urban"}
CONSOLE: dict[str, Any] = json.loads(
    (default_data_dir().parent / "apps" / "dashboard-fixtures" / "routes.geojson").read_text(
        encoding="utf-8"
    )
)


def test_lagos_abuja_and_port_harcourt_have_hubs_and_rounds() -> None:
    cities = {r.city for r in URBAN.values()}
    assert {"Lagos", "Abuja", "Port Harcourt"} <= cities
    for route in URBAN.values():
        assert route.towns[0].stop_type == "hub"
        assert route.towns[0].depot
        assert len(route.towns) >= 5
        assert route.source in {"osrm", "densified"}


@pytest.mark.parametrize("route_id", sorted(NETWORKS))
def test_stops_sit_on_the_street_route_at_their_km_posts(route_id: str) -> None:
    route = URBAN[route_id]
    for town, spec in zip(route.towns, NETWORKS[route_id]["stops"], strict=True):
        lat, lon, _ = route.position(town.km)
        # OSRM snaps a stop to the nearest street, so allow for the walk from the kerb.
        assert haversine_km((lon, lat), (spec[4], spec[3])) < 0.6
        assert town.stop_id == spec[0]
        assert town.window == spec[5]


def test_customer_types_cover_the_brief() -> None:
    types = {t.stop_type for r in URBAN.values() for t in r.towns}
    assert {"hub", "supermarket", "hospital", "pharmacy", "qsr", "open_market", "hotel"} <= types


def test_lagos_round_crosses_the_slow_corridors() -> None:
    names = {s.road_class for s in URBAN["LAG-U1"].segments}
    assert "slow_corridor" in names
    assert slow_corridor_at((3.3930, 6.5000)) == "Third Mainland Bridge"
    assert slow_corridor_at((3.3700, 6.5500)) == "Ikorodu Road"
    assert slow_corridor_at((7.49, 9.06)) is None


def test_console_routes_cover_both_kinds_on_the_simulator_geometry() -> None:
    lines = {
        f["properties"]["id"]: f
        for f in CONSOLE["features"]
        if f["geometry"]["type"] == "LineString"
    }
    assert set(lines) == set(ROUTES)
    for route_id, f in lines.items():
        assert f["properties"]["kind"] == ROUTES[route_id].kind
        assert [tuple(c) for c in f["geometry"]["coordinates"]] == ROUTES[route_id].points
    stops = [f["properties"] for f in CONSOLE["features"] if f["geometry"]["type"] == "Point"]
    urban = [s for s in stops if s["synthetic"]]
    assert all(s["window"] for s in urban if s["type"] != "hub")
    assert (
        default_data_dir().parent / "apps" / "dashboard-fixtures" / "routes.geojson"
    ).stat().st_size < 400_000
