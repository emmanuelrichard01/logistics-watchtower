"""Turn raw OSRM route responses into the committed corridors GeoJSON.

Usage, from the repo root after fetching the raw responses (see data/routes/README.md):

    uv run python -m watchtower_simulator.build_routes \
        data/routes/osrm data/routes/corridors.geojson

Road class and signal profiles are derived here, deterministically. Named dead zones are
illustrative placements for testing, not measured network coverage.
"""

import json
import sys
from itertools import pairwise
from pathlib import Path
from typing import Any

from watchtower_simulator.geo import LonLat, haversine_km, simplify
from watchtower_simulator.routes import Route

# Same corridors and stops as v1 (legacy/v1/src/producer.py), now routed on real roads.
# (name, road kind, depot). Depots are the termini plus one cold-storage hub per corridor;
# the hubs are illustrative placements, not real facilities.
CORRIDORS: dict[str, dict[str, Any]] = {
    "RT-LAG-ABJ": {
        "name": "Lagos to Abuja",
        "towns": [
            ("Lagos Mainland", "urban", True),
            ("Ikeja", "urban", False),
            ("Ikorodu", "highway", False),
            ("Sagamu Junction", "highway", False),
            ("Ibadan", "urban", True),
            ("Oshogbo", "highway", False),
            ("Ilorin", "urban", False),
            ("Mokwa", "highway", False),
            ("Abuja", "urban", True),
        ],
        "dead_zones": [("Jebba-Mokwa stretch", 430.0, 470.0), ("Bida approach", 640.0, 662.0)],
    },
    "RT-PHC-MKD": {
        "name": "Port Harcourt to Makurdi",
        "towns": [
            ("Port Harcourt", "urban", True),
            ("Elele", "highway", False),
            ("Owerri", "urban", False),
            ("Okigwe", "highway", False),
            ("Enugu", "urban", True),
            ("Nsukka", "highway", False),
            ("Makurdi", "urban", True),
        ],
        "dead_zones": [("Otukpo approach", 430.0, 465.0)],
    },
    "RT-BEN-ABJ": {
        "name": "Benin to Abuja",
        "towns": [
            ("Benin City", "urban", True),
            ("Ekpoma", "highway", False),
            ("Auchi", "urban", False),
            ("Okene", "highway", False),
            ("Lokoja", "urban", True),
            ("Abaji", "highway", False),
            ("Abuja", "urban", True),
        ],
        "dead_zones": [("Okene hills", 180.0, 205.0), ("Lokoja-Abaji gap", 360.0, 395.0)],
    },
}

URBAN_RADIUS_KM = 12.0  # stretch around an urban stop that drives at urban speeds
SIMPLIFY_TOLERANCE_KM = 0.003  # at most 3 m off the road: faithful at street zoom
# Per-minute Markov transition probabilities for the cellular link (synthetic).
SIGNAL = {
    "urban": {"p_drop": 0.002, "p_recover": 0.5},
    "highway": {"p_drop": 0.01, "p_recover": 0.25},
}


def build(raw_dir: Path) -> dict[str, Any]:
    features: list[dict[str, Any]] = []
    for route_id, spec in CORRIDORS.items():
        raw = json.loads((raw_dir / f"{route_id}.json").read_text(encoding="utf-8"))
        route = raw["routes"][0]
        full: list[LonLat] = [(c[0], c[1]) for c in route["geometry"]["coordinates"]]
        points = [
            (round(lon, 5), round(lat, 5)) for lon, lat in simplify(full, SIMPLIFY_TOLERANCE_KM)
        ]

        km = [0.0]
        for a, b in pairwise(points):
            km.append(km[-1] + haversine_km(a, b))
        scale = route["distance"] / 1000 / km[-1]  # align our km posts with OSRM's leg distances

        town_km = [0.0]
        for leg in route["legs"]:
            town_km.append(town_km[-1] + leg["distance"] / 1000)
        towns = [
            {"name": name, "km": round(at, 2), "kind": kind, "depot": depot}
            for (name, kind, depot), at in zip(spec["towns"], town_km, strict=True)
        ]
        segments = road_class_segments(towns, town_km[-1])
        features.append(
            {
                "type": "Feature",
                "geometry": {"type": "LineString", "coordinates": [list(p) for p in points]},
                "properties": {
                    "route_id": route_id,
                    "name": spec["name"],
                    "length_km": round(town_km[-1], 2),
                    "km_scale": round(scale, 6),
                    "towns": towns,
                    "segments": segments,
                    "dead_zones": [
                        {"name": n, "from_km": a, "to_km": b, "illustrative": True}
                        for n, a, b in spec["dead_zones"]
                    ],
                },
            }
        )
    return {
        "type": "FeatureCollection",
        "attribution": (
            "Road geometry © OpenStreetMap contributors (ODbL), routed by the OSRM demo server."
        ),
        "features": features,
    }


def road_class_segments(towns: list[dict[str, Any]], length_km: float) -> list[dict[str, Any]]:
    urban = sorted(
        (max(0.0, t["km"] - URBAN_RADIUS_KM), min(length_km, t["km"] + URBAN_RADIUS_KM))
        for t in towns
        if t["kind"] == "urban"
    )
    merged: list[tuple[float, float]] = []
    for a, b in urban:
        if merged and a <= merged[-1][1]:
            merged[-1] = (merged[-1][0], max(merged[-1][1], b))
        else:
            merged.append((a, b))
    segments: list[dict[str, Any]] = []
    cursor = 0.0
    for a, b in merged:
        if a > cursor:
            segments.append(segment(cursor, a, "highway"))
        segments.append(segment(a, b, "urban"))
        cursor = b
    if cursor < length_km:
        segments.append(segment(cursor, length_km, "highway"))
    return segments


def segment(a: float, b: float, road_class: str) -> dict[str, Any]:
    return {
        "from_km": round(a, 2),
        "to_km": round(b, 2),
        "road_class": road_class,
        "signal": SIGNAL[road_class],
    }


def console_export(collection: dict[str, Any]) -> dict[str, Any]:
    """The same geometry for the operator console: one LineString per corridor plus station
    points, so vehicle positions in the fleet-state recording sit exactly on the drawn road."""
    features: list[dict[str, Any]] = []
    for feature in collection["features"]:
        props = feature["properties"]
        route = Route(feature)
        features.append(
            {
                "type": "Feature",
                "geometry": feature["geometry"],
                "properties": {
                    "id": props["route_id"],
                    "name": props["name"],
                    "length_km": props["length_km"],
                    "source": "osrm",
                    "attribution": collection["attribution"],
                    "dead_zones": [
                        {"name": z["name"], "from_km": z["from_km"], "to_km": z["to_km"]}
                        for z in props["dead_zones"]
                    ],
                },
            }
        )
        for town in props["towns"]:
            lat, lon, _ = route.position(town["km"])
            features.append(
                {
                    "type": "Feature",
                    "geometry": {"type": "Point", "coordinates": [round(lon, 5), round(lat, 5)]},
                    "properties": {
                        "corridor_id": props["route_id"],
                        "name": town["name"],
                        "km_along": town["km"],
                        "depot": town["depot"],
                    },
                }
            )
    return {
        "type": "FeatureCollection",
        "attribution": collection["attribution"],
        "features": features,
    }


def write(path: Path, doc: dict[str, Any]) -> None:
    path.write_text(json.dumps(doc, separators=(",", ":")) + "\n", encoding="utf-8", newline="\n")
    print(f"wrote {path} ({path.stat().st_size} bytes)")


def main() -> None:
    """build_routes <raw dir> <simulator geojson> [<console geojson>]"""
    raw_dir, out = Path(sys.argv[1]), Path(sys.argv[2])
    collection = build(raw_dir)
    write(out, collection)
    if len(sys.argv) > 3:
        write(Path(sys.argv[3]), console_export(collection))


if __name__ == "__main__":
    main()
