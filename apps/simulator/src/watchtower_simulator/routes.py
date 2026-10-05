"""Corridor routes loaded from data/routes/corridors.geojson: position, heading, road class,
signal profile and dead zones by km along the route."""

import json
from bisect import bisect_right
from dataclasses import dataclass
from itertools import pairwise
from pathlib import Path
from typing import Any

from watchtower_simulator.geo import LonLat, bearing_deg, haversine_km


@dataclass(frozen=True)
class Segment:
    from_km: float
    to_km: float
    road_class: str
    p_drop: float  # per-minute probability the link goes bad (illustrative)
    p_recover: float


@dataclass(frozen=True)
class DeadZone:
    name: str
    from_km: float
    to_km: float


@dataclass(frozen=True)
class Town:
    """A named point along a route: a corridor town, or an urban hub or customer stop."""

    name: str
    km: float
    kind: str
    depot: bool = False
    stop_id: str | None = None
    stop_type: str | None = None  # hub, supermarket, hospital, pharmacy, qsr, open_market, hotel
    window: str | None = None  # delivery window, "HH:MM-HH:MM" local time


class Route:
    def __init__(self, feature: dict[str, Any]) -> None:
        props = feature["properties"]
        self.route_id: str = props["route_id"]
        self.name: str = props["name"]
        self.length_km: float = props["length_km"]
        self.kind: str = props.get("kind", "corridor")
        self.city: str | None = props.get("city")
        self.source: str = props.get("source", "osrm")
        self.points: list[LonLat] = [(c[0], c[1]) for c in feature["geometry"]["coordinates"]]
        scale = props["km_scale"]
        self.km: list[float] = [0.0]
        for a, b in pairwise(self.points):
            self.km.append(self.km[-1] + haversine_km(a, b) * scale)
        self.towns = [
            Town(
                t["name"],
                t["km"],
                t["kind"],
                t.get("depot", False),
                t.get("stop_id"),
                t.get("stop_type"),
                t.get("window"),
            )
            for t in props["towns"]
        ]
        self.segments = [
            Segment(
                s["from_km"],
                s["to_km"],
                s["road_class"],
                s["signal"]["p_drop"],
                s["signal"]["p_recover"],
            )
            for s in props["segments"]
        ]
        self.dead_zones = [
            DeadZone(z["name"], z["from_km"], z["to_km"]) for z in props["dead_zones"]
        ]

    def position(self, km: float) -> tuple[float, float, float]:
        """(lat, lon, heading_deg) at ``km`` along the route, clamped to its ends."""
        km = min(max(km, 0.0), self.km[-1])
        i = min(bisect_right(self.km, km) - 1, len(self.points) - 2)
        a, b = self.points[i], self.points[i + 1]
        span = self.km[i + 1] - self.km[i]
        f = 0.0 if span == 0 else (km - self.km[i]) / span
        lon = a[0] + (b[0] - a[0]) * f
        lat = a[1] + (b[1] - a[1]) * f
        return lat, lon, bearing_deg(a, b)

    def segment(self, km: float) -> Segment:
        for s in self.segments:
            if s.from_km <= km < s.to_km:
                return s
        return self.segments[-1]

    def dead_zone(self, km: float) -> DeadZone | None:
        return next((z for z in self.dead_zones if z.from_km <= km < z.to_km), None)

    def nearest_town(self, km: float) -> Town:
        return min(self.towns, key=lambda t: abs(t.km - km))


def default_data_dir() -> Path:
    """The repo's data/ directory, found by walking up from this file or the cwd."""
    for start in (Path.cwd(), Path(__file__).resolve()):
        for parent in (start, *start.parents):
            if (parent / "data" / "routes" / "corridors.geojson").exists():
                return parent / "data"
    raise FileNotFoundError("could not locate data/routes/corridors.geojson")


ROUTE_FILES = ("corridors.geojson", "urban.geojson")


def load_routes(path: Path | None = None) -> dict[str, Route]:
    """Inter-state corridors and urban delivery rounds, by route id."""
    paths = [path] if path else [default_data_dir() / "routes" / name for name in ROUTE_FILES]
    routes: dict[str, Route] = {}
    for p in paths:
        collection = json.loads(p.read_text(encoding="utf-8"))
        routes |= {r.route_id: r for r in (Route(f) for f in collection["features"])}
    return routes
