# Dashboard fixtures

Recorded simulator runs for developing the operator console without a running backend (ADR-0010, `docs/design/console.md` section 9). All data is synthetic.

## Files

| File | What it is |
| --- | --- |
| `routes.geojson` | Every route (inter-state corridors and city rounds) with stops, types and delivery windows |
| `corridors.geojson` | Inter-state corridors only, with stations (kept for existing consumers) |
| `<scenario>.fleet.jsonl.gz` | Fleet-state recording, **gzip-compressed** JSON lines: one row per vehicle every 15 simulated seconds, in time order |
| `<scenario>.truth.jsonl` | Ground-truth intervals (what really happened), for checking what the UI shows against reality |
| `<scenario>.stops.jsonl` | Urban stop visits: planned versus actual arrival, delivery window, lateness, delivered shipment temperatures |

Current recordings:

- **`console_showcase`**, the main demo fixture: a Thursday morning, 3 hours, 10 vehicles. Trucks run with the operations model on (checkpoints, tolls, congestion); vans and a trike work city rounds; one truck cross-docks to a van. Fleet recording, truth and stop log included. Every story beat lands inside the window:

  | Vehicle | What happens | Look for |
  | --- | --- | --- |
  | TRK-101 | Compressor degrades on Lagos-Abuja; box air climbs, the cargo lags and breaches about 2.4 h in | `compressor_health`, rising `duty_cycle_pct`, `cargo_excursion` |
  | TRK-102 | Compressor fault just before the Otukpo dead zone; the excursion starts offline | `link_down`, `buffered`, `last_fix_age_s`, a late `cargo_excursion` |
  | TRK-103 | Cargo door open at highway speed for 8 minutes | `door_open_moving` |
  | TRK-104 | Cargo probe flatlines; the real cargo is fine | `cargo_probe_c` stuck while `cargo_c` moves; `fault_flatline_cargo_probe` |
  | TRK-105 | Hijacked: leaves the corridor, stops away from any depot, door opened, tracker cut | `off_route_km`, `unexplained_stop`, `tracker_offline` |
  | TRK-106 to VAN-ABJ2 | Cross-dock at the Abuja hub | `stop_cross_dock`, `on_dock`, the van's round planned from its real dispatch |
  | VAN-ABJ1 | Abuja pharma round; door unlatched after Maitama | `door_open_moving` on a van; SHP-ABJ-004 delivered out of spec |
  | VAN-LAG1, TRIKE-LAG3 | Lagos rush over Third Mainland Bridge | late stops in `*.stops.jsonl` |

  Its ground truth is pinned by `data/scenarios/console_showcase.labels.yaml` and checked in CI.
- `compressor_gradual_degradation`: the first 2 hours, 4 inter-state trucks. TRK-101's compressor health falls from 1.0 to 0.2 between 07:30 and 09:00 UTC, so its air temperature climbs while the cargo lags.
- `lagos_last_mile_morning_rush`: the first 3 hours of a Monday morning in Lagos, with two vans and a trike on city rounds (`routes.geojson`, kind `urban`); truth and stop log included.
- `cross_dock_handover_delay`: truth and stop log only. Two trucks hand over to Abuja vans; one handover waits on a hot open dock.

## Reading the recordings

Fleet recordings are gzip-compressed, which takes them from 1.6-2.4 MB to about 90 KB each. In the browser:

```ts
const res = await fetch("/fixtures/lagos_last_mile_morning_rush.fleet.jsonl.gz");
const text = await new Response(res.body!.pipeThrough(new DecompressionStream("gzip"))).text();
const rows = text.trim().split("\n").map((line) => JSON.parse(line));
```

If your dev server already sends `Content-Encoding: gzip` for `.gz`, `res.text()` is enough. Truth and stop logs are small and stay plain JSONL.

## Regenerate

```bash
F=apps/dashboard-fixtures
SIM="uv run python -m watchtower_simulator.cli run"   # same as wt-sim; immune to broken venv shims
$SIM console_showcase --out /tmp/r.jsonl --recording $F/console_showcase.fleet.jsonl.gz \
  --truth $F/console_showcase.truth.jsonl --stops $F/console_showcase.stops.jsonl
$SIM compressor_gradual_degradation --duration 2h --out /tmp/r.jsonl \
  --recording $F/compressor_gradual_degradation.fleet.jsonl.gz --truth $F/compressor_gradual_degradation.truth.jsonl
$SIM lagos_last_mile_morning_rush --duration 3h --out /tmp/r.jsonl \
  --recording $F/lagos_last_mile_morning_rush.fleet.jsonl.gz --truth $F/lagos_last_mile_morning_rush.truth.jsonl \
  --stops $F/lagos_last_mile_morning_rush.stops.jsonl
$SIM cross_dock_handover_delay --out /tmp/r.jsonl --recording /tmp/f.jsonl \
  --truth $F/cross_dock_handover_delay.truth.jsonl --stops $F/cross_dock_handover_delay.stops.jsonl
uv run python -m watchtower_simulator.export_console $F/routes.geojson
```

## Live data instead of recordings

For a running demo, stream the same scenario live and drive faults from the console's chaos panel:

```bash
uv run python -m watchtower_simulator.cli live console_showcase --speed 10 \
  --sink http://127.0.0.1:18090 --control 127.0.0.1:18091
```

The control API is `POST /inject {"vehicle", "fault", "params"}` (fault is any scenario event type, e.g. `compressor_fault`, `door_open`, `sensor_fault` with `params.fault`), `GET /fleet` (each vehicle's latest fleet row, same schema as below) and `GET /health`. CORS allows `http://localhost:5173` by default; add origins with `--cors-origin`.

Output is byte-identical for the same scenario and seed, compressed files included.

## All routes (`routes.geojson`)

Every route the simulator drives, on the same geometry, about 171 KB:

- **One LineString per route.** Properties: `id`; `kind` (`corridor` for inter-state, `urban` for city rounds); `name`; `city` (urban only); `length_km`; `source` (`osrm` or `densified`); `slow_corridors` (`[{name, from_km, to_km}]`, e.g. Third Mainland Bridge); `dead_zones`.
- **One Point per stop.** Properties: `route_id`, `stop_id`, `name`, `type` (`hub`, `supermarket`, `hospital`, `pharmacy`, `qsr`, `open_market`, `hotel` for urban; `depot` or `town` on corridors), `km_along`, `depot`, `window` (delivery window, `"HH:MM-HH:MM"` local time), `synthetic` (true for urban stops: invented customers, not real businesses).

Planned versus actual arrival times depend on the run, so they're in each scenario's `*.stops.jsonl`.

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
| `vehicle_class` | string | `trailer`, `van` or `trike` |
| `cargo_profile` | string | Profile of the shipment at the cargo probe: `frozen`, `pharma_2_8`, `bananas` or `fresh_produce` |
| `shipments` | object[] | Every shipment on board: `{shipment_id, profile, cargo_c, min_c, max_c, receiver}`. `receiver` is the `stop_id` it's going to; each shipment is its own temperature |
| `next_stop` | string or null | Next customer `stop_id` on an urban round |
| `setpoint_c`, `min_c`, `max_c` | number | Profile setpoint and allowed cargo range |
| `pallets`, `cargo_weight_kg` | integer | Load on board; falls at each delivery drop |
| `driver` | string or null | `cautious`, `normal` or `aggressive` when the operations model is on |
| `stop_reason` | string or null | Why the truck is stopped: `police_checkpoint`, `toll`, `weighbridge`, `refuel`, `refuel_queue`, `mandated_rest`, `night_parking`, `drop:<pallets>`, `arrived`, or a stop type; null while driving |
| `fuel_pct` | number | Tractor diesel |
| `cargo_c` | number | True cargo (product) temperature, the one that spoils |
| `air_c` | number | Return-air temperature (box air) |
| `supply_air_c` | number | Air leaving the evaporator |
| `cargo_probe_c`, `return_air_probe_c`, `supply_air_probe_c` | number or null | **What the device reported** in its latest sample: sensor noise, calibration offset and every active fault (flatline, spike, drift, swap) applied; null on probe dropout. Run sensor-trust and alert logic on these, never on the true values |
| `probe_t` | string or null | When that sample was taken (up to one reporting interval old; null before the first sample or while the tracker is cut) |
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
| `power_source` | string | `ENGINE`, `GENSET` (engine off away from a depot), `SHORE` (plugged in at a depot) or `NONE` (a van parked with the engine off: the unit is off) |
| `genset_fuel_l` | number | Reefer genset diesel left |
| `faults` | string[] | Injected sensor or device faults active now, e.g. `fault_flatline_cargo_probe`, `fault_clock_skew` (ground truth; empty normally) |
| `link_up` | boolean | Cellular link up at this moment |
| `signal_dbm` | integer or null | Null while the link is down |
| `buffered` | boolean | The device is holding unsent readings |
| `buffer_depth` | integer | How many readings it is holding |
| `last_fix_age_s` | integer or null | Seconds since the freshest reading the gateway has received. This is how stale the server's view is; draw estimates, not measurements, when it grows |

### Ground truth versus what a device can report

The recording mixes both. A real device can't report these, so a console must never base an alert, a confidence figure or a sensor-trust verdict on them. Use them only to check whether the console got it right:

- the true temperatures: `cargo_c`, `air_c`, `supply_air_c` and each shipment's `cargo_c`;
- `compressor_health`, `capacity_factor`, `evaporator_ice_kg`;
- `faults` (which faults are injected), `off_route_km`, `storm`, `solar_heat_kw`, `irradiance_w_m2`, `cloud_cover`;
- `link_up` from the device's side during an outage (the server only sees silence), `buffer_depth`.

The device-reported fields are the `*_probe_c` set and `probe_t`, position, speed, heading, door, compressor state and fault code, defrost, power source, fuel, battery and signal. The telemetry readings stream carries exactly these.

## Stop visit (`*.stops.jsonl`)

One row per customer drop on an urban round, in arrival order:

| Field | Meaning |
| --- | --- |
| `vehicle_id`, `route_id`, `stop_id`, `name`, `type`, `km_along` | Who stopped where (join to `routes.geojson` stops by `stop_id`) |
| `window_start`, `window_end` | The customer's delivery window, ISO UTC |
| `planned_arrival` | The dispatcher's plan: dispatch time, 22 km/h, median dwell per customer |
| `actual_arrival`, `departure` | What happened |
| `delay_vs_plan_min` | Actual minus planned |
| `on_time`, `late_min` | Arrived before the window closed, and by how much it missed if not |
| `door_open_s` | Cargo door open at this drop |
| `delivered` | `[{shipment_id, profile, cargo_c, in_spec}]`: the temperature each shipment was handed over at |

## Truth interval (`*.truth.jsonl`)

`{"vehicle_id", "kind", "start", "end"}`, where `end` is null if the interval was still open when the run ended. The kinds are:

- `cargo_excursion`
- `door_open_moving`, `door_open_stationary`
- `link_down`
- `defrost`
- `compressor_fault`, `compressor_degraded`
- `on_dock` (a shipment waiting at a cross-dock; recorded under the van that will take it)

Per-shipment intervals (`cargo_excursion`, `on_dock`) carry a `shipment_id` and end when custody passes on (delivered or handed over).
