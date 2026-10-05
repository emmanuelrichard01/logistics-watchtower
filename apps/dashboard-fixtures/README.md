# Dashboard fixtures

Recorded simulator runs for developing the operator console without a running backend (ADR-0010, `docs/design/console.md` section 9). All data is synthetic.

## Files

| File | What it is |
| --- | --- |
| `corridors.geojson` | Corridor road geometry and stations: the same line the simulator drives on |
| `<scenario>.fleet.jsonl` | Fleet-state recording: one row per vehicle every 15 simulated seconds, in time order |
| `<scenario>.truth.jsonl` | Ground-truth intervals (what really happened), for checking what the UI shows against reality |

Current recording: `compressor_gradual_degradation`, the first 2 hours, 4 trucks.

- TRK-101's compressor health falls from 1.0 to 0.2 between 07:30 and 09:00 UTC, so its air temperature climbs while the cargo lags.
- The other trucks run normally, with occasional short link drops.

## Regenerate

```bash
uv run wt-sim run compressor_gradual_degradation --duration 2h \
  --out /tmp/readings.jsonl \
  --recording apps/dashboard-fixtures/compressor_gradual_degradation.fleet.jsonl \
  --truth apps/dashboard-fixtures/compressor_gradual_degradation.truth.jsonl
```

Output is byte-identical for the same scenario and seed.

## Corridors (`corridors.geojson`)

A FeatureCollection, 146 KB:

- **One LineString per corridor.** Properties: `id` (`RT-LAG-ABJ`, `RT-PHC-MKD`, `RT-BEN-ABJ`), `name`, `length_km` (along the road), `source` (`osrm`), `attribution`, and `dead_zones` (`[{name, from_km, to_km}]`, illustrative placements).
- **One Point per station.** Properties: `corridor_id`, `name`, `km_along`, `depot` (bool). Depots are the termini plus one cold-storage hub per corridor; the hubs are illustrative.

The geometry is real roads from OpenStreetMap, routed by OSRM. Attribution is required: "© OpenStreetMap contributors (ODbL)". Simplified with Douglas-Peucker at 3 m, so vertex spacing follows curvature: dense on bends, sparse on straights. Regenerate with the command in `data/routes/README.md`.

## Fleet-state row (`*.fleet.jsonl`)

| Field | Type | Meaning |
| --- | --- | --- |
| `t` | string | Simulated time, ISO-8601 UTC with milliseconds (`2026-03-12T07:00:15.000Z`) |
| `vehicle_id` | string | e.g. `TRK-101` |
| `route_id` | string | `RT-LAG-ABJ`, `RT-PHC-MKD` or `RT-BEN-ABJ` (geometry in `data/routes/corridors.geojson`) |
| `km_along` | number | Distance along the corridor line in `corridors.geojson`; split the line here to draw travelled versus remaining route, or use the station km posts for the lane diagram |
| `lat`, `lon` | number | True position (WGS84), exactly on the corridor line unless `off_route_km` > 0 |
| `off_route_km` | number | How far a hijacked truck has been driven off the corridor (0 normally); `km_along` freezes at the turn-off |
| `speed_kmh`, `heading_deg` | number | Speed, and compass heading of the current road segment |
| `cargo_profile` | string | `frozen`, `pharma_2_8`, `bananas` or `fresh_produce` |
| `setpoint_c`, `min_c`, `max_c` | number | Profile setpoint and allowed cargo range |
| `pallets`, `cargo_weight_kg` | integer | Load on board; falls at each delivery drop |
| `driver` | string or null | `cautious`, `normal` or `aggressive` when the operations model is on |
| `stop_reason` | string or null | Why the truck is stopped: `police_checkpoint`, `toll`, `weighbridge`, `refuel`, `refuel_queue`, `mandated_rest`, `night_parking`, `drop:<pallets>`, `arrived`, or a stop type; null while driving |
| `fuel_pct` | number | Tractor diesel |
| `cargo_c` | number | True cargo (product) temperature, the one that spoils |
| `air_c` | number | Return-air temperature (box air) |
| `supply_air_c` | number | Air leaving the evaporator |
| `ambient_c` | number | Outside air temperature |
| `sun_elevation_deg`, `irradiance_w_m2`, `cloud_cover` | number | Sun height, global horizontal irradiance, cloud fraction 0-1 |
| `storm` | boolean | A rainy-season storm over this truck (cooler air, slower traffic, a flakier link) |
| `solar_heat_kw` | number | Extra heat the sun is driving into the box (higher when parked, sun on a long side) |
| `humidity_pct` | number | Box relative humidity (jumps towards outside RH while the door is open) |
| `door` | string | `OPEN` or `CLOSED` |
| `compressor` | string | `RUNNING`, `OFF` or `FAULT` |
| `compressor_health` | number | 0-1, ground truth only (a real device does not report it) |
| `duty_cycle_pct` | number | Share of the last 30 minutes the compressor ran. Rising duty at a steady temperature is the early sign of a degrading unit |
| `evaporator_ice_kg`, `capacity_factor` | number | Coil ice and the cooling capacity it leaves (ground truth only) |
| `defrost` | boolean | Defrost cycle in progress (scheduled about every 6 h, or on demand) |
| `power_source` | string | `ENGINE`, `GENSET` (engine off away from a depot) or `SHORE` (plugged in at a depot) |
| `genset_fuel_l` | number | Reefer genset diesel left |
| `faults` | string[] | Injected sensor or device faults active now, e.g. `fault_flatline_cargo_probe`, `fault_clock_skew` (ground truth; empty normally) |
| `link_up` | boolean | Cellular link up at this moment |
| `signal_dbm` | integer or null | Null while the link is down |
| `buffered` | boolean | The device is holding unsent readings |
| `buffer_depth` | integer | How many readings it is holding |
| `last_fix_age_s` | integer or null | Seconds since the freshest reading the gateway has received. This is how stale the server's view is; draw estimates, not measurements, when it grows |

## Truth interval (`*.truth.jsonl`)

`{"vehicle_id", "kind", "start", "end"}`, where `end` is null if the interval was still open when the run ended. The kinds are:

- `cargo_excursion`
- `door_open_moving`, `door_open_stationary`
- `link_down`
- `defrost`
- `compressor_fault`, `compressor_degraded`
