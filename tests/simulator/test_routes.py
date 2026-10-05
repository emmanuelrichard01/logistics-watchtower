from itertools import pairwise

import pytest
from watchtower_simulator.geo import haversine_km, simplify
from watchtower_simulator.routes import load_routes

ROUTES = load_routes()


def test_three_corridors_on_real_roads() -> None:
    assert sorted(ROUTES) == ["RT-BEN-ABJ", "RT-LAG-ABJ", "RT-PHC-MKD"]
    for route in ROUTES.values():
        # Road distance well exceeds the straight line between the ends.
        straight = haversine_km(route.points[0], route.points[-1])
        assert route.length_km > 1.1 * straight
        assert route.km[-1] == pytest.approx(route.length_km, rel=0.002)


@pytest.mark.parametrize(
    ("route_id", "start", "end"),
    [
        ("RT-LAG-ABJ", (6.455, 3.394), (9.056, 7.499)),
        ("RT-PHC-MKD", (4.816, 7.050), (7.732, 8.522)),
        ("RT-BEN-ABJ", (6.339, 5.618), (9.060, 7.480)),
    ],
)
def test_route_ends_are_near_the_v1_stops(
    route_id: str, start: tuple[float, float], end: tuple[float, float]
) -> None:
    route = ROUTES[route_id]
    lat0, lon0, _ = route.position(0.0)
    lat1, lon1, _ = route.position(route.length_km)
    assert haversine_km((lon0, lat0), (start[1], start[0])) < 3.0
    assert haversine_km((lon1, lat1), (end[1], end[0])) < 3.0


def test_positions_are_continuous_and_headings_in_range() -> None:
    route = ROUTES["RT-LAG-ABJ"]
    previous = route.position(0.0)
    for tenth in range(1, int(route.length_km * 10), 7):
        lat, lon, heading = route.position(tenth / 10)
        assert 0.0 <= heading < 360.0
        assert haversine_km((lon, lat), (previous[1], previous[0])) < 1.0
        previous = (lat, lon, heading)


def test_segments_cover_the_route_and_dead_zones_are_found() -> None:
    for route in ROUTES.values():
        assert route.segments[0].from_km == 0.0
        assert route.segments[-1].to_km == pytest.approx(route.length_km, abs=0.01)
        for a, b in pairwise(route.segments):
            assert a.to_km == pytest.approx(b.from_km)
    zone = ROUTES["RT-LAG-ABJ"].dead_zone(450.0)
    assert zone is not None
    assert zone.name == "Jebba-Mokwa stretch"
    assert ROUTES["RT-LAG-ABJ"].dead_zone(100.0) is None


def test_simplify_keeps_ends_and_drops_collinear_points() -> None:
    line = [(3.0 + i * 0.001, 6.0) for i in range(100)]
    assert simplify(line, 0.01) == [line[0], line[-1]]
