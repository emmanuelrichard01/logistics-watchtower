# Simulator v2 (`wt-sim`)

A deterministic, scenario-driven fleet simulator (rebuild plan, section 8). It is a test instrument: the same scenario and seed always produce byte-identical output, and every run also writes ground truth for evaluation.

```bash
uv run wt-sim run compressor_gradual_degradation --out readings.jsonl \
  --recording fleet.jsonl --truth truth.jsonl [--duration 2h]
```

| Output | Contents |
| --- | --- |
| `--out` | Telemetry readings in gateway arrival order, each matching the `telemetry_event` v1 Avro contract |
| `--recording` | Fleet state every 15 simulated seconds (schema in `apps/dashboard-fixtures/README.md`) |
| `--truth` | What really happened, as intervals, for tests and evaluation only |

## Reporting interval (configurable, decision pending)

The real device reporting rate hasn't been decided. Devices report every **30 s**, and every **10 s** while an alarm condition holds: cargo out of range, box air more than 2 °C outside the range, door open, or a compressor fault. Both rates can be set per scenario (`sample_interval`, `burst_interval`) or per vehicle. **1 Hz** (`1s`) works for load tests. The physics step shrinks automatically to the greatest common divisor of 5 s and the intervals in use.

## Determinism

- Time comes from a virtual clock; nothing reads the wall clock.
- Randomness comes from named streams, `rng(seed, vehicle, purpose)`, never the global `random` module. Adding a new random draw to one purpose doesn't shift any other stream.
- `boot_id` values are seeded random 64-bit hex (`b-9f3a1c2e7d40b812`), not time-shaped, so they can't collide across devices.

## Models

| Module | Model | Status of the numbers |
| --- | --- | --- |
| `routes.py` | Real road geometry (OSRM over OpenStreetMap), derived road class, Markov signal profiles, named dead zones | Geometry real; road class derived; signal and dead zones illustrative |
| `thermal.py` | Two-node air and cargo model with compressor health, thermostat hysteresis, door and defrost heat | Illustrative parameters for a ~13.6 m trailer |
| `cargo.py` | Profiles (frozen, pharma 2-8 °C, bananas, fresh produce) with pallet-scaled thermal mass and respiration heat | Illustrative |
| `reefer.py` | Evaporator icing and defrost, duty cycle, box humidity, power source, genset fuel | Illustrative |
| `stops.py` | Stop types with lognormal door-open durations | Illustrative |
| `ambient.py` | Monthly base and daily cosine by latitude; outside humidity | Illustrative, not a climatology |
| `environment.py` | NOAA sun position, clear-sky irradiance, sol-air solar load by heading, seeded storms and Harmattan haze | Standard equations; illustrative rates |
| `operations.py` | Checkpoints, tolls, weighbridge, fuel and diesel queues, congestion, drivers, rest and night rules, drops | Illustrative |
| `faults.py` | Composable probe, GPS and clock faults | Illustrative |
| `incidents.py` | Hijack detour off the corridor | Scenario-driven |
| `channel.py`, `device.py` | Markov link, dead zones, ring buffer with throttled in-order replay, at-least-once duplicates, reboots | Illustrative rates |

## Scenarios

Scenarios live in `data/scenarios/<name>.yaml`, with ground truth in `<name>.labels.yaml`. Event types:

- `compressor_health` (`to`, `over`)
- `compressor_fault` (`code`)
- `door_open` (`duration`)
- `stop` (`stop_type`, `duration`, optional `door_open`)
- `defrost` (`duration`)
- `link_outage` (`duration`)
- `reboot`
- `breakdown` (`duration`): engine-off stop, reefer on genset
- `tyre_blowout` (`duration`, default 75m): sudden deceleration and a stop for the change
- `hijack` (`deviate_km`, `door_open`, optional `tracker_off_after`, `bearing_offset_deg`): leaves the corridor, unexplained stop, door opened, tracker optionally cut
- `sensor_fault` (`fault` plus its parameters, optional `duration`). The faults are `offset`, `drift`, `flatline`, `spike`, `dropout` and `swap` (probe faults with `probe: supply_air|return_air|cargo_probe`), and `gps_multipath`, `gps_jump` and `clock_skew` (`offset_s`, `drift_ppm`, `gps_sync_after`). They compose in injection order and appear in truth as `fault_<kind>[_<probe>]`.

Scenarios with `operations: true` add the road and operations model: checkpoints, tolls, weighbridges, fuel stops, congestion, driver profiles (`driver:`), `fleet_policy` (`no_night_driving`, `max_drive_h`, `rest_min`) and multi-drop deliveries (`drops: [{km, pallets}]`).
