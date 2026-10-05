# Dashboard fixtures

Recorded simulator runs for developing the operator console without a running backend (ADR-0010, `docs/design/console.md` section 9). All data is synthetic.

## Files

| File | What it is |
| --- | --- |
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

## Fleet-state row (`*.fleet.jsonl`)

| Field | Type | Meaning |
| --- | --- | --- |
| `t` | string | Simulated time, ISO-8601 UTC with milliseconds (`2026-03-12T07:00:15.000Z`) |
| `vehicle_id` | string | e.g. `TRK-101` |
| `route_id` | string | `RT-LAG-ABJ`, `RT-PHC-MKD` or `RT-BEN-ABJ` (geometry in `data/routes/corridors.geojson`) |
| `km` | number | Distance along the route. Use with the route's town km posts to draw the lane diagram |
| `lat`, `lon` | number | True position (WGS84) |
| `speed_kmh`, `heading_deg` | number | Speed, and compass heading of the current road segment |
| `cargo_profile` | string | `frozen` or `pharma_2_8` |
| `setpoint_c`, `min_c`, `max_c` | number | Profile setpoint and allowed cargo range |
| `cargo_c` | number | True cargo (product) temperature, the one that spoils |
| `air_c` | number | Return-air temperature (box air) |
| `supply_air_c` | number | Air leaving the evaporator |
| `ambient_c` | number | Outside air temperature |
| `door` | string | `OPEN` or `CLOSED` |
| `compressor` | string | `RUNNING`, `OFF` or `FAULT` |
| `compressor_health` | number | 0-1, ground truth only (a real device does not report it) |
| `defrost` | boolean | Defrost cycle in progress |
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
