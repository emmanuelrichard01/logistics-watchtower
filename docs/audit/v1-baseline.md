# v1 Audit and Baseline

Audit of `main` at `abc2a08` (tagged `v1-final`; on the `v2` branch the code lives in `legacy/v1/`), 5 Oct 2026. Rebuild plan, section 19, day 2.

Simulation figures come from running the real `producer.py` and `processor.py` offline on a virtual clock (Kafka stubbed, 0.5 s ticks, fixed seeds). Runtime figures come from the Compose stack (last section).

## Summary

v1 is a clean, readable demo: three services on Redpanda, a sensible event shape, and a dashboard that looks good in a GIF. It does not hold up as a monitoring system. The rule engine is stateless and fires on every tick. The most frequent alert comes from a simulator artifact. Several README claims are unmeasured or wrong. There are no tests, so none of this was visible.

## README claims vs code

| Claim | Reality | Evidence |
| --- | --- | --- |
| End-to-end latency < 200 ms | Never measured. No timer or histogram exists anywhere | `api.py` exports no latency metric |
| 8 alert rules | Code has 8. README table lists 7 (`TIRE_PRESSURE_LOW` missing). The rebuild plan's "7 while claiming 8" has it backwards | `processor.py:207-216`, README "Alert Rules" |
| 15+ sensors per event | 20 fields, 15 of them non-identity. Several are synthetic: heading is derived, `gps_accuracy_m` is pure noise, `engine_status` is constant | `producer.py:54-86` |
| Single alert per truck prevents notification spam | The processor emits an alert **every tick** while a condition holds. Only the UI coalesces them | 120 alert messages in 60 s for one compressor incident |
| TRUCK-101/102 failures enable deterministic testing | The simulation runs in real time. Compressor fault first fires after **~2.0 h**, door fault after **~3.4 h**. Unseeded RNG, so not deterministic | Sim run, 12 h |
| `DEMO_ALERT_DURATION` configures alert length | Read, printed and passed by Compose, but never used | `producer.py:30`, `docker-compose.yaml:52` |
| Route listing | README omits Ikorodu and Oshogbo. "Highways" are straight lines between towns | `producer.py:96-106` |
| Project structure line counts (505/333/550) | 554/333/550 | `wc -l` |
| Structured log event `telemetry_received` | No such event is logged | `api.py` |
| Battery 12.4-14.2 V, drains when engine off | Engine is always `RUNNING`, so the battery never drains and `BATTERY_LOW` can't fire | `producer.py:200`, `:380-386` |

## Defects

Severity reflects impact on a monitoring system, not on the demo.

### Correctness

1. **Alert storm, no state.** `process_telemetry` is stateless: no debounce, hysteresis, dedup or clear event. 3 trucks produced **108,683 alerts in 12 simulated hours** (~2.5/s). The API keeps 100 alerts (`MAX_ALERT_HISTORY`), about 40 seconds of history. `processor.py:237-259`
2. **Severe alerts hidden behind others.** Only the top-severity alert per event is published. Over 12 h, `COMPRESSOR_FAULT` triggered **31,953** times but was published **17** times, always masked by `TEMP_BREACH` at the same severity. `HUMIDITY_BREACH`: 14,001 triggered, 23 published. API metrics count only the published one. `processor.py:247-257`
3. **Tire pressure is an unbounded random walk.** There's no mean reversion, so trucks sit below 85 psi for **12-47% of all ticks**. `TIRE_PRESSURE_LOW` is the most frequent alert (62,721 in 12 h), and it's simulator noise. `producer.py:390-391`
4. **Alert messages wipe truck state in the API.** `update_truck` is called with the alert payload, which has no `speed_kmh`, `temperature_c`, `fuel_level_pct` or `door_status`. `/fleet/status` shows `None`/0 until the next telemetry message. `api.py:334-336`, `:167-180`
5. **False `DOOR_VIOLATION` on every loading stop.** On arrival the truck switches to `LOADING` and opens the door in the same tick, before speed is zeroed, so the event shows door open at ~30 km/h. `producer.py:346-349`
6. **Demo speed alert fires 20% of the time** (105/531 injections). The injected 105-115 km/h is smoothed down by `_update_position` in the same tick. Compressor, door and humidity fire 100%. `producer.py:260-261`, `:311`
7. **Cargo weight only increases.** Each loading stop adds 500-2000 kg and nothing unloads (17.6 t after 72 h). `producer.py:400`
8. **Stale-alert clearing depends on idle polls.** It runs only when `poll()` returns nothing, and it swallows all errors with a bare `except`. At higher message rates alerts never auto-clear. `api.py:310-314`, `:231`
9. **Dashboard alert state flickers.** The truck goes back to "OK" on the next telemetry message (≤0.5 s later). The Alerts metric is effectively "alerts in the last message". `index.html:1311-1312`
10. **Toast keeps first severity.** A repeat alert for the same truck updates the message but not the severity class, icon or type, so a CRITICAL can show as a MEDIUM card. `index.html:1072-1129`

### Reliability and architecture

11. **Blocking Kafka poll on the event loop.** `consumer.poll(0.1)` runs inside an `async` task, stalling REST and WebSocket handling for up to 100 ms per iteration. `api.py:308`
12. **Sequential broadcast.** One slow WebSocket client delays every other client and the Kafka loop. `api.py:271-277`
13. **Fixed consumer group on a fan-out consumer.** A second API replica would split partitions, and each client would see part of the fleet. `api.py:293`
14. **Health check always reports `online`.** It never checks broker connectivity. `api.py:415-430`
15. **No event identity, no event-time handling, no DLQ.** A malformed message is logged and dropped. Duplicates are undetectable.
16. **Process-local state.** All API state is lost on restart, and `latest` offset reset means restarts silently skip data.
17. **Startup race hides alerts for 5 minutes.** If the API subscribes before the processor has created the `alerts` topic, librdkafka only notices the topic on its metadata refresh (`topic.metadata.refresh.interval.ms`, 5 min by default). Observed at 1000 trucks: the API group held only the `telemetry` partition, `alerts` was assigned **297 s** after startup, and it started at the log end (offset ~9,993). About 10,000 alerts never reached the dashboard. Whether this happens depends on container start order. `api.py:304`

### Security

18. **Stored XSS path.** Alert and telemetry fields are interpolated into `innerHTML` and inline `onclick` handlers. Kafka is unauthenticated and exposed on host port 9092, so anyone on the network can inject script into the operator console. `index.html:1137-1154`, `:1223`
19. **CORS `*` with credentials.** Starlette echoes any origin. `api.py:377-383`
20. **No auth** on REST or WebSocket. The hardcoded `ws://localhost:8000` means the dashboard only works on the host machine. `index.html:1289`

### Build and hygiene

21. `confluent_kafka` is imported but not declared (it arrives transitively via quixstreams). `pandas`, `faker`, `python-multipart` and `python-dotenv` are declared but unused. Nothing is pinned or locked.
22. The image installs `build-essential` without needing it and runs as root. Compose builds the same image three times. There's no restart policy, and port 29092 is exposed to the host.
23. Zero tests, no CI, no linting or type checking.
24. Dead code: `demo_alert_active`, `demo_ticks_remaining`, `low_fuel_mode`, the `UNLOADING`/`IDLE`/`MAINTENANCE` states and the `LOW` severity are never used. `datetime.utcnow()` is deprecated.
25. The dashboard isn't served by any container. It's opened from disk.

## Worth carrying into v2

- Route and waypoint data, and the haversine/heading helpers.
- Keying by truck ID on the telemetry topic.
- The failure-injection *concept* (scripted and demo modes), rebuilt as seeded, labelled scenarios.
- Rule metadata shape (`severity`, `action_required`, `threshold`, `actual_value`), which maps onto the v2 evidence and playbook fields.
- The dashboard's map and toast-coalescing ideas (as UI only; the incident queue becomes the record).

## Runtime baseline

Measured 5 Oct 2026 with `legacy/v1/baseline/run.sh` (raw results: `legacy/v1/baseline/results/20261005T091900Z.jsonl`).

**Setup.** Intel i7-10510U (4 cores / 8 threads), 7.8 GB RAM, Windows 11 Pro 10.0.26300, Docker Desktop engine 29.8.1 with a 4 GB / 8 vCPU VM. v1 at `v1-final` with `DEMO_MODE=true` (so the alert path carries traffic) and the default 0.5 s tick. Each fleet size: 20 s warm-up, then a 60 s measurement, one run each.

**Method.** A WebSocket client runs inside the Compose network, so it shares the producer's clock. Latency runs from the producer building the event (its `timestamp` field) to the frame arriving at the client. Before measuring, the driver waits for the `alerts` topic and restarts the API, to work around defect 17. Without that, alerts are invisible for the first 5 minutes.

| Trucks | Intended events/s | Delivered events/s | Telemetry p50 / p95 / p99 (ms) | Alert p50 / p95 / p99 (ms) | CPU: producer / API / processor / Redpanda |
| ---: | ---: | ---: | --- | --- | --- |
| 3 | 6 | 6.0 (100%) | 14 / 19 / 28 | 27 / 38 / 39 (n=3) | 0.3% / 0.8% / 0.3% / 4.4% |
| 100 | 200 | 196 (98%) | 14 / 19 / 22 | 23 / 31 / 38 | 3.5% / 2.7% / 0.8% / 3.1% |
| 300 | 600 | 561 (93%) | 17 / 24 / 30 | 26 / 38 / 42 | 8.2% / 9.3% / 2.3% / 9.7% |
| 1000 | 2000 | 1526 (76%) | 24 / 50 / 82 | 38 / 71 / 105 | 18% / 18% / 5.8% / 8.4% |
| 2000 | 4000 | 2484 (62%) | 28 / 64 / 107 | 48 / 102 / 147 | 58% / 56% / 18% / 24% |

CPU is a single `docker stats` sample taken mid-measurement (100% = one core). Memory stayed flat: about 51 MiB for each Python service and 330-440 MiB for Redpanda.

**Reading the numbers**

- **The producer is the bottleneck, not the pipeline.** It sleeps 0.5 s *after* each tick's work instead of holding a fixed schedule, so the tick stretches as the fleet grows (about 0.8 s at 2000 trucks). Nothing downstream was saturated at 2,484 events/s.
- **Latency hides that backlog.** The clock starts when the producer builds the event, so time spent waiting for a late tick is invisible: a coordinated-omission effect. v2 timers start at gateway ingest and use an open-loop generator (plan section 16).
- **The "< 200 ms" README claim** is roughly consistent with these numbers (alert p99 147 ms, max 198 ms at 2000 trucks), but it was never measured and named no start or stop point. It now has one, with the caveat above.
- Not covered: no repeats (spread unknown), no soak, no failure injection.
