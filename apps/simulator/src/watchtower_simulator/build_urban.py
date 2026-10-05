"""Urban distribution networks: cold-store hubs and multi-drop delivery rounds in Lagos, Abuja
and Port Harcourt, routed on real streets.

Usage, from the repo root (network access only for routes not already cached):

    uv run python -m watchtower_simulator.build_urban data/routes/osrm/urban data/routes/urban.geojson

Every stop is **synthetic**: a generic customer of a realistic type (supermarket, hospital,
pharmacy, quick-service restaurant, open market, hotel) at a plausible location in a real
neighbourhood. None names or represents a real business. Delivery windows are illustrative.

Street geometry comes from the public OSRM demo server over OpenStreetMap (ODbL), fetched
once per route through all its stops and cached. If a fetch fails, the route falls back to
densified straight lines between stops and is marked ``source: densified``; it is never
passed off as road geometry. Slow-corridor centrelines (Third Mainland Bridge, Ikorodu
Road) are approximate and illustrative.
"""

import json
import sys
import urllib.request
from itertools import pairwise
from pathlib import Path
from typing import Any

from watchtower_simulator.geo import LonLat, haversine_km, simplify

OSRM = "http://router.project-osrm.org/route/v1/driving/{coords}?overview=full&geometries=geojson"
SIMPLIFY_TOLERANCE_KM = 0.003
SIGNAL = {"p_drop": 0.002, "p_recover": 0.5}  # good urban coverage, illustrative

# (stop_id, name, type, lat, lon, window "HH:MM-HH:MM" local or None)
NETWORKS: dict[str, dict[str, Any]] = {
    "LAG-U1": {
        "name": "Lagos: Ikeja hub to the Island, morning round",
        "city": "Lagos",
        "stops": [
            (
                "HUB-LAG-IKJ",
                "Ikeja cold-store hub (Oregun industrial area)",
                "hub",
                6.5965,
                3.3605,
                None,
            ),
            ("LAG-U1-01", "Ikeja GRA supermarket", "supermarket", 6.5795, 3.3555, "06:30-09:00"),
            ("LAG-U1-02", "Yaba hospital pharmacy", "hospital", 6.5165, 3.3835, "07:00-09:30"),
            ("LAG-U1-03", "Lagos Island open market", "open_market", 6.4555, 3.3925, "06:00-09:00"),
            ("LAG-U1-04", "Victoria Island hotel", "hotel", 6.4305, 3.4215, "07:00-11:00"),
            (
                "LAG-U1-05",
                "Lekki Phase 1 quick-service restaurant",
                "qsr",
                6.4475,
                3.4705,
                "08:00-10:30",
            ),
        ],
    },
    "LAG-U2": {
        "name": "Lagos: Apapa hub along Ikorodu Road",
        "city": "Lagos",
        "stops": [
            ("HUB-LAG-APP", "Apapa cold-store hub", "hub", 6.4485, 3.3615, None),
            ("LAG-U2-01", "Surulere supermarket", "supermarket", 6.4955, 3.3545, "06:30-09:00"),
            ("LAG-U2-02", "Fadeyi quick-service restaurant", "qsr", 6.5285, 3.3705, "07:00-09:00"),
            ("LAG-U2-03", "Maryland hospital", "hospital", 6.5715, 3.3665, "07:30-10:00"),
            ("LAG-U2-04", "Ketu open market", "open_market", 6.5965, 3.3905, "06:00-09:30"),
        ],
    },
    "ABJ-U1": {
        "name": "Abuja: Idu hub pharma round",
        "city": "Abuja",
        "stops": [
            (
                "HUB-ABJ-IDU",
                "Idu cold-store hub (Idu industrial area)",
                "hub",
                9.0415,
                7.3645,
                None,
            ),
            ("ABJ-U1-01", "Jabi supermarket", "supermarket", 9.0705, 7.4255, "07:00-09:30"),
            ("ABJ-U1-02", "Wuse II pharmacy", "pharmacy", 9.0795, 7.4705, "08:00-11:00"),
            ("ABJ-U1-03", "Maitama hospital", "hospital", 9.0885, 7.4945, "07:30-10:30"),
            ("ABJ-U1-04", "Asokoro hospital", "hospital", 9.0425, 7.5235, "08:00-11:00"),
            ("ABJ-U1-05", "Garki pharmacy", "pharmacy", 9.0335, 7.4905, "08:30-12:00"),
            (
                "ABJ-U1-06",
                "Central Business District hotel",
                "hotel",
                9.0565,
                7.4885,
                "09:00-12:00",
            ),
        ],
    },
    "PHC-U1": {
        "name": "Port Harcourt: Trans-Amadi hub retail round",
        "city": "Port Harcourt",
        "stops": [
            ("HUB-PHC-TAM", "Trans-Amadi cold-store hub", "hub", 4.8125, 7.0385, None),
            ("PHC-U1-01", "Old GRA supermarket", "supermarket", 4.7905, 7.0115, "07:00-09:30"),
            ("PHC-U1-02", "D-Line pharmacy", "pharmacy", 4.8005, 7.0035, "08:00-11:00"),
            ("PHC-U1-03", "Mile 1 open market", "open_market", 4.7925, 6.9995, "06:00-09:00"),
            ("PHC-U1-04", "GRA Phase 2 hotel", "hotel", 4.8255, 7.0045, "08:00-11:30"),
        ],
    },
}

# Approximate centrelines of notoriously slow corridors (lon, lat), with a match tolerance.
SLOW_CORRIDORS: dict[str, tuple[list[LonLat], float]] = {
    "Third Mainland Bridge": (
        [(3.3985, 6.5420), (3.3935, 6.5080), (3.3925, 6.4830), (3.3995, 6.4595)],
        0.45,
    ),
    "Ikorodu Road": (
        [(3.3790, 6.5130), (3.3740, 6.5330), (3.3680, 6.5600), (3.3790, 6.5850), (3.3920, 6.5980)],
        0.30,
    ),
}


def fetch(route_id: str, stops: list[tuple[Any, ...]], cache: Path) -> dict[str, Any] | None:
    path = cache / f"{route_id}.json"
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    coords = ";".join(f"{lon},{lat}" for _, _, _, lat, lon, _ in stops)
    try:
        with urllib.request.urlopen(OSRM.format(coords=coords), timeout=60) as resp:
            doc = json.loads(resp.read().decode("utf-8"))
    except OSError as exc:
        print(
            f"{route_id}: OSRM fetch failed ({exc}); falling back to densified lines",
            file=sys.stderr,
        )
        return None
    if doc.get("code") != "Ok":
        return None
    cache.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(doc), encoding="utf-8")
    return doc


def densified(stops: list[tuple[Any, ...]]) -> tuple[list[LonLat], list[float]]:
    points: list[LonLat] = []
    legs: list[float] = []
    for a, b in pairwise(stops):
        pa, pb = (a[4], a[3]), (b[4], b[3])
        n = max(2, int(haversine_km(pa, pb) / 0.1))
        points += [
            (pa[0] + (pb[0] - pa[0]) * k / n, pa[1] + (pb[1] - pa[1]) * k / n) for k in range(n)
        ]
        legs.append(haversine_km(pa, pb))
    points.append((stops[-1][4], stops[-1][3]))
    return points, legs


def point_to_segment_km(p: LonLat, a: LonLat, b: LonLat) -> float:
    ax, ay, bx, by, px, py = a[0], a[1], b[0], b[1], p[0], p[1]
    dx, dy = bx - ax, by - ay
    t = (
        0.0
        if dx == dy == 0
        else max(0.0, min(1.0, ((px - ax) * dx + (py - ay) * dy) / (dx * dx + dy * dy)))
    )
    return haversine_km(p, (ax + t * dx, ay + t * dy))


def slow_corridor_at(p: LonLat) -> str | None:
    for name, (line, tolerance_km) in SLOW_CORRIDORS.items():
        if any(point_to_segment_km(p, a, b) <= tolerance_km for a, b in pairwise(line)):
            return name
    return None


def segments(points: list[LonLat], km: list[float], length_km: float) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for p, at in zip(points, km, strict=True):
        name = slow_corridor_at(p)
        road_class = "slow_corridor" if name else "urban"
        if out and out[-1]["road_class"] == road_class and out[-1].get("name") == name:
            continue
        if out:
            out[-1]["to_km"] = round(at, 3)
        out.append(
            {
                "from_km": round(at, 3),
                "to_km": None,
                "road_class": road_class,
                "signal": SIGNAL,
                **({"name": name} if name else {}),
            }
        )
    out[-1]["to_km"] = round(length_km, 3)
    return [s for s in out if s["to_km"] > s["from_km"]]


def build(cache: Path) -> dict[str, Any]:
    features: list[dict[str, Any]] = []
    for route_id, net in NETWORKS.items():
        stops = net["stops"]
        doc = fetch(route_id, stops, cache)
        if doc is not None:
            route = doc["routes"][0]
            full: list[LonLat] = [(c[0], c[1]) for c in route["geometry"]["coordinates"]]
            legs = [leg["distance"] / 1000 for leg in route["legs"]]
            source = "osrm"
        else:
            full, legs = densified(stops)
            source = "densified"
        points = [
            (round(lon, 5), round(lat, 5)) for lon, lat in simplify(full, SIMPLIFY_TOLERANCE_KM)
        ]
        km = [0.0]
        for a, b in pairwise(points):
            km.append(km[-1] + haversine_km(a, b))
        length = sum(legs)
        scale = length / km[-1]
        stop_km = [0.0]
        for leg in legs:
            stop_km.append(stop_km[-1] + leg)
        towns = [
            {
                "name": name,
                "km": round(at, 3),
                "kind": "urban",
                "depot": stop_type == "hub",
                "stop_id": stop_id,
                "stop_type": stop_type,
                "window": window,
            }
            for (stop_id, name, stop_type, _, _, window), at in zip(stops, stop_km, strict=True)
        ]
        features.append(
            {
                "type": "Feature",
                "geometry": {"type": "LineString", "coordinates": [list(p) for p in points]},
                "properties": {
                    "route_id": route_id,
                    "name": net["name"],
                    "kind": "urban",
                    "city": net["city"],
                    "source": source,
                    "length_km": round(length, 3),
                    "km_scale": round(scale, 6),
                    "towns": towns,
                    "segments": segments(points, [k * scale for k in km], length),
                    "dead_zones": [],
                },
            }
        )
    return {
        "type": "FeatureCollection",
        "attribution": "Street geometry © OpenStreetMap contributors (ODbL), routed by the OSRM demo server. Stops are synthetic.",
        "features": features,
    }


def main() -> None:
    """build_urban <osrm cache dir> <output geojson>"""
    cache, out = Path(sys.argv[1]), Path(sys.argv[2])
    out.write_text(
        json.dumps(build(cache), separators=(",", ":")) + "\n", encoding="utf-8", newline="\n"
    )
    print(f"wrote {out} ({out.stat().st_size} bytes)")


if __name__ == "__main__":
    main()
