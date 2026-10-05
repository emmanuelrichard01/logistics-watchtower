"""Write ``apps/dashboard-fixtures/routes.geojson`` for the operator console: every route the
simulator drives (inter-state corridors and urban rounds), on exactly the same geometry, plus
every stop.

    uv run python -m watchtower_simulator.export_console apps/dashboard-fixtures/routes.geojson
"""

import json
import sys
from pathlib import Path
from typing import Any

from watchtower_simulator.routes import ROUTE_FILES, Route, default_data_dir

ATTRIBUTION = (
    "Geometry © OpenStreetMap contributors (ODbL), routed by the OSRM demo server. "
    "Urban stops are synthetic."
)


def routes_geojson() -> dict[str, Any]:
    features: list[dict[str, Any]] = []
    for name in ROUTE_FILES:
        collection = json.loads((default_data_dir() / "routes" / name).read_text(encoding="utf-8"))
        for feature in collection["features"]:
            props = feature["properties"]
            route = Route(feature)
            features.append(
                {
                    "type": "Feature",
                    "geometry": feature["geometry"],
                    "properties": {
                        "id": route.route_id,
                        "kind": route.kind,
                        "name": route.name,
                        "city": route.city,
                        "length_km": route.length_km,
                        "source": route.source,
                        "slow_corridors": [
                            {"name": s["name"], "from_km": s["from_km"], "to_km": s["to_km"]}
                            for s in props["segments"]
                            if s["road_class"] == "slow_corridor"
                        ],
                        "dead_zones": [
                            {"name": z.name, "from_km": z.from_km, "to_km": z.to_km}
                            for z in route.dead_zones
                        ],
                    },
                }
            )
            for town in route.towns:
                lat, lon, _ = route.position(town.km)
                features.append(
                    {
                        "type": "Feature",
                        "geometry": {
                            "type": "Point",
                            "coordinates": [round(lon, 5), round(lat, 5)],
                        },
                        "properties": {
                            "route_id": route.route_id,
                            "stop_id": town.stop_id or f"{route.route_id}:{town.name}",
                            "name": town.name,
                            "type": town.stop_type or ("depot" if town.depot else "town"),
                            "km_along": town.km,
                            "depot": town.depot,
                            "window": town.window,
                            "synthetic": route.kind == "urban",
                        },
                    }
                )
    return {"type": "FeatureCollection", "attribution": ATTRIBUTION, "features": features}


def main() -> None:
    out = Path(sys.argv[1])
    out.write_text(
        json.dumps(routes_geojson(), separators=(",", ":")) + "\n", encoding="utf-8", newline="\n"
    )
    print(f"wrote {out} ({out.stat().st_size} bytes)")


if __name__ == "__main__":
    main()
