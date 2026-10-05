# Corridor routes

`corridors.geojson` holds the three v1 corridors routed on real roads:

| Route | Name | Length |
| --- | --- | --- |
| RT-LAG-ABJ | Lagos to Abuja | 817.5 km |
| RT-PHC-MKD | Port Harcourt to Makurdi | 564.2 km |
| RT-BEN-ABJ | Benin to Abuja | 616.2 km |

## Source and licence

Road geometry © [OpenStreetMap contributors](https://www.openstreetmap.org/copyright), available under the Open Database License (ODbL). It was routed by the public [OSRM demo server](http://router.project-osrm.org) on 5 Oct 2026, with the v1 stops as via-points. The demo server is fair-use only, so the result is committed instead of fetched at run time.

## What is real and what is synthetic

- **Real:** the road geometry, and the km posts of each stop (from OSRM's leg distances).
- **Derived:** `road_class`. Any stretch within 12 km of an urban stop is `urban`, everything else `highway`. This is an approximation, not OSM road classes.
- **Synthetic (illustrative):** the per-segment `signal` Markov probabilities and the named `dead_zones`. They are placed to exercise the pipeline, not measured network coverage.

## Urban delivery rounds (`urban.geojson`)

City distribution from cold-store hubs, routed on real streets by OSRM through every stop:

| Route | City | Round | Length |
| --- | --- | --- | --- |
| LAG-U1 | Lagos | Ikeja hub to the Island, over Third Mainland Bridge | 39.3 km |
| LAG-U2 | Lagos | Apapa hub along Ikorodu Road | 24.1 km |
| ABJ-U1 | Abuja | Idu hub pharma round | 41.3 km |
| PHC-U1 | Port Harcourt | Trans-Amadi hub retail round | 14.6 km |

- **Stops are synthetic:** generic customers (supermarket, hospital, pharmacy, quick-service restaurant, open market, hotel) at plausible locations in real neighbourhoods. None represents a real business.
- **Illustrative:** delivery windows and the slow-corridor centrelines (Third Mainland Bridge, Ikorodu Road).
- **Fallback:** any route OSRM can't return is built from densified straight lines and marked `source: densified`.

Regenerate with `uv run python -m watchtower_simulator.build_urban data/routes/osrm/urban data/routes/urban.geojson` (cached OSRM responses are reused).

## Regenerate

```bash
mkdir -p data/routes/osrm
curl -o data/routes/osrm/RT-LAG-ABJ.json "http://router.project-osrm.org/route/v1/driving/3.3941,6.4550;3.3683,6.5962;3.6472,6.8256;3.7196,6.8926;3.9398,7.3768;4.4984,7.7027;4.5522,8.4904;5.9667,8.8500;7.4985,9.0563?overview=full&geometries=geojson"
curl -o data/routes/osrm/RT-PHC-MKD.json "http://router.project-osrm.org/route/v1/driving/7.0498,4.8156;7.3678,5.1117;7.0354,5.4851;7.1194,6.0072;7.5464,6.4584;7.3833,6.8833;8.5218,7.7322?overview=full&geometries=geojson"
curl -o data/routes/osrm/RT-BEN-ABJ.json "http://router.project-osrm.org/route/v1/driving/5.6175,6.3392;6.0922,6.7428;6.1360,7.1706;6.2343,7.5629;6.7455,8.0069;7.1500,8.5000;7.4800,9.0600?overview=full&geometries=geojson"
uv run python -m watchtower_simulator.build_routes data/routes/osrm data/routes/corridors.geojson apps/dashboard-fixtures/corridors.geojson
```

The raw responses (`data/routes/osrm/`) are not committed. Lines are simplified with Ramer-Douglas-Peucker at 3 m (no point of the road is more than 3 m from the line), and the simplified length stays within 0.06% of OSRM's distance.
