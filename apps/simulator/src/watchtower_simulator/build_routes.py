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

# Same corridors and stops as v1 (legacy/v1/src/producer.py), now routed on real roads.
CORRIDORS: dict[str, dict[str, Any]] = {
    "RT-LAG-ABJ": {
        "name": "Lagos to Abuja",
        "towns": [
            ("Lagos Mainland", "urban"),
            ("Ikeja", "urban"),
            ("Ikorodu", "highway"),
            ("Sagamu Junction", "highway"),
            ("Ibadan", "urban"),
            ("Oshogbo", "highway"),
            ("Ilorin", "urban"),
            ("Mokwa", "highway"),
            ("Abuja", "urban"),
        ],
        "dead_zones": [("Jebba-Mokwa stretch", 430.0, 470.0), ("Bida approach", 640.0, 662.0)],
    },
    "RT-PHC-MKD": {
        "name": "Port Harcourt to Makurdi",
        "towns": [
            ("Port Harcourt", "urban"),
            ("Elele", "highway"),
            ("Owerri", "urban"),
            ("Okigwe", "highway"),
            ("Enugu", "urban"),
            ("Nsukka", "highway"),
            ("Makurdi", "urban"),
        ],
        "dead_zones": [("Otukpo approach", 430.0, 465.0)],
    },
    "RT-BEN-ABJ": {
        "name": "Benin to Abuja",
        "towns": [
            ("Benin City", "urban"),
            ("Ekpoma", "highway"),
            ("Auchi", "urban"),
            ("Okene", "highway"),
            ("Lokoja", "urban"),
            ("Abaji", "highway"),
            ("Abuja", "urban"),
        ],
        "dead_zones": [("Okene hills", 180.0, 205.0), ("Lokoja-Abaji gap", 360.0, 395.0)],
    },
}

URBAN_RADIUS_KM = 12.0  # stretch around an urban stop that drives at urban speeds
SIMPLIFY_TOLERANCE_KM = 0.025
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
            {"name": name, "km": round(at, 2), "kind": kind}
            for (name, kind), at in zip(spec["towns"], town_km, strict=True)
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


def main() -> None:
    raw_dir, out = Path(sys.argv[1]), Path(sys.argv[2])
    out.write_text(json.dumps(build(raw_dir), separators=(",", ":")) + "\n", encoding="utf-8")
    print(f"wrote {out} ({out.stat().st_size} bytes)")


if __name__ == "__main__":
    main()
