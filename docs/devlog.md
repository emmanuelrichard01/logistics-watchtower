# Devlog

Surprises and measurements, newest first. Raw material for the case study.

## Showcase data in the console, and footage that isn't screen-recorded (Mon 5 Oct 2026)

- The console now replays the simulator's `console_showcase` morning: 10 vehicles on one clock. It reads the **device-reported** probe values instead of noise-free truth. So the stuck-probe rule, disabled on noise-free data, works again: TRK-104's flatline is inferred from the samples alone.
- Listing every incident the console raised found two false breaches. Both came from **empty boxes**: a van waiting for a cross-dock, and a trike after its last drop. The probe reads warm box air when there is no cargo. A third looked loaded but wasn't: the first reading after loading carried a probe sample taken *before* the load (`probe_t` lags `t`). Rule: a sample counts as cargo only if the vehicle was loaded when it was taken.
- **Simulator label gap (for Track B):** VAN-ABJ1 drives at 22-36 km/h with the door open after a police checkpoint at 09:49 UTC, but the truth file has no `door_open_moving` interval for it. The console raises it correctly.
- New incident type `ROUTE_DEVIATION`. Off route, the map shows the recorded position; dead-reckoning a hijacked truck along the road it left would be a lie.
- **Footage.** A CDP screencast captured about 10 fps on the lanes and 1.5 fps on the WebGL map, and slowed the run itself. The clips are now rendered frame by frame on Playwright's fake clock (rAF, timers and `performance.now`), so they are smooth at 30 fps on any machine. CSS animation playback is slowed to match the capture rate.
- The recordings caught two console bugs:
  - **L for live** didn't work after dragging the time handle, the one moment its own hint suggests it. Every `<input>` counted as typing, including the range slider.
  - **Map layers over the panels.** The map container formed no stacking context, so deck.gl's canvas and the new label layer drew over the fleet list. Fixed with `isolation: isolate`.

## The map's first visit froze for 25 seconds (Mon 5 Oct 2026)

- After the console switched to simulator recordings, the map probe dropped from 59.9 to 38-48 fps. A probe sampling 3-second windows showed the steady state was still about 60 fps. The real problem was **the first 23-27 s after opening the map**, which the old probe's 4 s warm-up partly overlapped.
- A CPU profile from navigation put 12 s in luma.gl's `_getLinkStatus`. Per-program link times:
  - deck.gl PathLayer: 3.5 s and 3.0 s (plain and dashed variants).
  - Text: 1.5 s over two programs.
  - Scatterplot and icons: 0.7 s each.
  - MapLibre's own programs: 0.02-0.2 s each.
- Turning on `KHR_parallel_shader_compile` (disabled by default in luma.gl) moved the stall rather than removing it, because luma.gl reflects attributes right after linking.
- **Fix.** deck.gl keeps one IconLayer program for what moves: vehicle discs and arrows are composited into one icon, and the halo and 3D masts reuse it. Lines, stops and dead zones are MapLibre layers, and labels are HTML.
- **Result.** A cold first visit now settles in 5.8-6.7 s and then holds 58.9-59.4 fps. The first 3D toggle went from 1.1-1.3 s of long tasks to zero.
- **Lesson:** a warm-up period in a benchmark hides first-visit cost. The probe now uses a cold profile and reports settle time separately from frame rate.
- **Also fixed on the way:** `map.isStyleLoaded()` waits for tiles too. A guard on it silently skipped adding layers while tiles were in flight, a latent bug in the 3D-buildings toggle as well.

## Simulator: live mode, bulk mode, validation (Mon 5 Oct 2026)

- **Live mode against the real gateway.** The test runs the gateway's own FastAPI app under uvicorn with a broker that fails twice. Every signed reading is accepted after two 503 retries. Signing reuses `watchtower_gateway.validation.sign`, so the two can't drift.
- **Time scale versus the gateway's clock.** At 60x, virtual time outruns the wall clock and the gateway would quarantine readings as FUTURE_EVENT (more than 5 minutes ahead). Live runs to the gateway shift the scenario so it ends at wall-clock now: fast runs replay recent history, never the future.
- **Bulk mode** at 25,000 trucks: physics about 0.5M events/s, end-to-end JSON about 12k/s on one core. One dict and one uuid5 per reading dominate, so physics isn't the bottleneck. 1 Hz load tests at 25k trucks need 2-3 processes (`docs/simulator/bulk-benchmark.md`).
- **The validation report caught two of my own mistakes before they shipped.** First, a sentence claimed box air recovered "within the first hour" without measuring it (it's 0.7 h, now computed). Second, a "sanity property" (cargo never below box air while the compressor runs) failed: right after a defrost, the air is still warmer than the cargo. That's physics; the property was mis-stated, and the report now says so explicitly. Lesson: generated reports state only what the script measures.
- **A real bug surfaced by the showcase scenario.** Operations stops opened the cargo door at the instant the stop began, while the truck was still braking (about 20 s to drop below 5 km/h). Every checkpoint logged a 10-15 s door-open-while-moving, which a processor would raise as CRITICAL. Fixed, with a regression test.
- The OneDrive venv corruption hit three more times (pre-commit, pytest, wt-sim shims). The Makefile now runs tools as `python -m`, which doesn't depend on the shims.

## The processor's core, as pure functions (Mon 5 Oct 2026)

- The domain package now covers stages A-E of plan section 9 without any I/O: dedup and minute buckets, sensor trust, time-to-breach (exponential-approach baseline with a p10-p90 range), MKT and exposure, risk assessment, the alert state machine (ADR-0006) and the vehicle evaluator that composes them over one ordered input log.
- **First lead-time number (one synthetic unit scenario, not the evaluation harness):** vaccines at 5 °C, cooling fails at t+10 min, 8 °C limit. COMPRESSOR_FAULT opens at t+10, BREACH_FORECAST at t+18, CARGO_TEMP_BREACH at t+39: **21 minutes of warning**. Source: `tests/unit/test_vehicle.py::test_forecast_warns_before_the_breach_with_a_measurable_lead_time`. Criterion 4 still needs the labelled scenario suite.
- **Design corrections found by tests:**
  - Escalation now runs from when an alert was *raised*, not from the condition's event-time start (an operator can't act before the alert exists), and never while the condition is recovering.
  - A relapse check read state after resetting it, so relapses were never counted.

## Domain state layout and the console (Mon 5 Oct 2026)

- **Domain refactored to ADR-0016.** `evaluate(view, reading)` returns a `Delta` (only the touched minute bucket, the updated sequence range and the new progress), so a state store can persist one key per bucket. Dedup is keyed by `(device_id, boot_id)`; buckets use integer epoch-minute keys; eviction follows event time, never the wall clock.
- **Rounding surprise.** "Half-up" via `floor(x*100 + 0.5)` turns 0.285 into 28, because 0.285 is 0.28499... in binary. Quantisation now rounds the float's shortest decimal form with `Decimal` (half away from zero), and ADR-0015 is amended. A hand-picked test case caught this; the property tests didn't, because they compare the domain with itself.
- **Console map performance.** On the reference laptop's GPU, the production build holds 59.8-59.9 fps on the map, against 0.2-47.5 fps on the Vite dev server (`apps/dashboard/scripts/perf-map.mjs`, 3 runs each). Two console bugs found by measuring rather than looking:
  - MapLibre's worker was missing from production builds: it locates the worker with a computed URL that Vite can't see. Fixed by bundling it via `?worker&url`.
  - A basemap layer added from a `styledata` event (which fires mid-load) silently aborted the style.
## Week 1, day 6 work: ingest gateway (Mon 5 Oct 2026)

- `input_record` v1 is the envelope for `wt.input.v1`. Avro can't import a schema from another file without registry references, so it embeds `TelemetryEvent` verbatim, and a test fails if the copy ever drifts from `telemetry_event`. Control-record IDs are uuid5 of documented natural keys, pinned by tests.
- Golden payloads use Avro's JSON encoding, the standard text form, so they parse with logical types intact and need no hand-written converter. `fastavro.json_reader` expects one record per line, so the test reads the pretty-printed files through `json.dumps` first.
- The gateway answers only after broker acknowledgement. With no broker, a real librdkafka producer plus `message.timeout.ms` makes the request return 503 in about the configured timeout, instead of 202 for data still queued.
- **Signature before identity.** A tampered reading fails the signature check before any later rule. So the property test re-signs each corruption to reach its specific reason, and separately checks that an unsigned corruption is caught no later than `BAD_SIGNATURE`.
- `make up` failed on a re-run. `objectstore-init` listed buckets before the SeaweedFS filer was ready ("missing address"); the failed listing looked like "no bucket", and the create then failed with "already exists". The objectstore healthcheck covers the master only. The init now retries until listing works and treats "already exists" as success.
- `make up` now passes `--build`. Without it, Compose kept running a stale gateway image after a Dockerfile change.
- Starlette's test client now wants `httpx2`; plain `httpx` raises a deprecation warning.
- The live smoke test against the Compose gateway (127.0.0.1:18090) returned 202, with one accepted and one quarantined (`BAD_SIGNATURE`). `rpk` showed the input record in Confluent framing (schema ID 1), keyed `TRK-101`.

## Week 1, day 4 work: core infrastructure (Mon 5 Oct 2026)

- **MinIO is gone.** Its repository is archived (last push 24 Apr 2026), `minio/minio` no longer exists on Docker Hub, and `quay.io/minio/minio:latest` doesn't resolve. The S3 store is SeaweedFS 4.48 (Apache-2.0), named `objectstore` in Compose so it can be swapped; see ADR-0013.
- **Architecture revision v2.1** (from the coordinator's review), applied the same day:
  - The processor reads one ordered log, `wt.input.v1`, carrying telemetry plus control records (TICK, RULES_ACTIVATED, ASSIGNMENT_CHANGED, OPERATOR_COMMAND). Replaying that single log reproduces every output, which fixes the replay-determinism gap in the original plan (wall-clock ticks).
  - Dropped `telemetry.clean.v1` and the compacted rules topic. Added `telemetry.minutes.v1`. Every topic is keyed by `vehicle_id`; `risk.assessments.v1` and `alerts.events.v1` move from 6 to 12 partitions so they're co-partitioned with the input log.
  - `fleet.state.v1` is compacted with `segment.ms` = 10 minutes, so compaction actually runs at demo volumes.
  - Vehicles use the text natural key (`TRK-101`) everywhere.
  - Alert IDs come from the application (deterministic uuid5). The dedup key is `{vehicle}:{shipment|-}:{type}`, enforced by a CHECK, and is unique per org while live.
  - The audit log hashes stored canonical bytes, never jsonb: Postgres normalises jsonb, so its bytes don't round-trip.
  - `minute_series` is partitioned by UTC day. Partitions are created by a SQL function the projector will call, not by the migration, so the migration doesn't depend on the date it runs.
  - New `packages/platform` with `make_producer`, which pins `murmur2_random` (the Java client's partitioner; librdkafka's default sends the same key elsewhere), idempotence and `acks=all`.
- **`docker compose up --wait` fails when a one-shot init container exits, even with code 0,** unless some service depends on it with `service_completed_successfully`. The first run passed by luck of timing; the second and later runs failed intermittently. Console now waits for both init jobs, which is also the right contract: anything reading topics or buckets starts only after they exist.
- `s3-init` wasn't idempotent: SeaweedFS errors on creating an existing bucket. It now lists first. `topics-init` reports "exists" and moves on, so `make up` is repeatable (checked three times in a row).
- Topic auto-creation is switched off cluster-wide by `topics-init`, so a misconfigured consumer fails loudly instead of silently creating a topic (the v1 defect 17 lesson).
- The PostGIS image installs the US TIGER geocoder and topology extensions into the database at init. Harmless noise in `\dt`; our tables are all in `public`.
- confluent-kafka: an `AdminClient` used as a temporary gets garbage collected before its futures resolve ("Broker handle destroyed"). Keep a reference.
- No Dockerfiles or `.sql` files yet (the DDL lives in the Alembic revision), so hadolint and sqlfluff aren't in pre-commit yet. They arrive with the first service image.
- Integration suite: 7 tests in about 38 s locally on Testcontainers; the default `make test` deselects them.
## Week 1: simulator v2 core (Mon 5 Oct 2026)

- Routes are real roads. The OSRM demo server routed all three v1 corridors through the v1 stops: Lagos-Abuja 817.5 km, Port Harcourt-Makurdi 564.2 km, Benin-Abuja 616.2 km. Simplified at 25 m, they come to 49 KB of GeoJSON. Road class (urban within 12 km of an urban stop) and the signal profiles and named dead zones are derived or illustrative, and labelled so.
- **Thermal surprise:** with plausible trailer and cargo parameters, cargo thermal mass dominates. At compressor health 0.4 (the plan's example), a 2-8 °C pharma load never breaches, and at 0.2 in a standard trailer it takes over 4 hours. The degradation scenario therefore uses an aged trailer (wall UA 0.12 kW/K) and runs 8 hours; the breach lands about 3 h 40 min after health bottoms out. That lead time is exactly what time-to-breach should exploit, and it argues for long virtual-clock scenarios over compressed physics.
- A frozen load's equilibrium time constant with a dead unit is about 4 days, because wall and cargo surface act in series. A test that assumed 10 days was enough was wrong, not the model.
- Defrost: dumping the full 4 kW heater into the air node heated the box by about 30 K. Only about 0.8 kW reaches box air (the rest melts coil ice), which gives the expected brief 6-7 K return-air rise with cargo moving under 0.2 K.
- Same seed means byte-identical readings, recording and truth (hashed in a test). Every emitted reading validates against the `telemetry_event` v1 Avro schema.

## Week 1, day 2 work and console direction (Mon 5 Oct 2026)

- v1 runtime baseline measured, 3 to 2000 trucks (`docs/audit/v1-baseline.md`). Telemetry p99 was 107 ms and alert p99 147 ms at 2000 trucks. The v1 producer, not the pipeline, is the bottleneck: it sleeps *after* each tick, so at 2000 trucks it delivers only 62% of the intended rate.
- **Surprise:** at 1000 trucks the first run delivered zero alerts. The API had subscribed before the `alerts` topic existed, and librdkafka only noticed the topic at the 5-minute metadata refresh (assigned at 297 s), skipping about 10,000 alerts. A start-order race in v1. v2 lesson: create topics explicitly before any consumer starts, and test cold-start ordering.
- **Measurement lesson:** the latency client must run inside the Compose network. A Windows-host client would mix two clocks (host and Docker VM).
- The programme is extended to 14 weeks to fund the operator console (ADR-0010). The console direction is "Signal Box": lanes as track diagrams and time-to-breach as signal aspects (`docs/design/console.md`).

## Week 1, day 3 work (done early, Mon 5 Oct 2026)

- Avro contract `telemetry_event` v1 deviates from the plan's example payload in three ways. `gps_fix` uses `FIX_2D`/`FIX_3D`, because Avro enum symbols can't start with a digit. Every enum has an `UNKNOWN` default, so adding a symbol later doesn't break old readers. `schema_version` starts at 1, not 2.
- The BACKWARD-compatibility check runs as a fastavro test (write with each old version, read with the latest) until the Schema Registry arrives on day 4. Confirmed it rejects a required field added without a default (`SchemaResolutionError`).
- **Property testing found a real bug on its first run.** JSON can carry lone surrogates (`"\ud800"`), and `uuid5` crashed on them instead of rejecting them. Device and boot IDs are now restricted to `[A-Za-z0-9._:-]{1,64}` (ADR-0002).
- **Float sums are order-dependent**, so a replay could differ from the live run in the last bits of a mean. Buckets accumulate probe values as integer hundredths. This is a precondition for "byte-identical replay" (success criterion 3) and input for ADR-0005.
- Domain purity is enforced by an import allowlist (stdlib minus I/O, clocks and randomness) rather than an import-linter denylist: stricter, with no extra tool. The guard has its own test proving it catches violations.

## Week 1, day 1 (Mon 5 Oct 2026)

- Tagged `v1-final` at `abc2a08`, branched `v2`, moved v1 into `legacy/v1/` (still runnable with its own Compose file).
- Offline virtual-clock run of the v1 simulator and rules (details in `docs/audit/v1-baseline.md`):
  - 108,683 alerts in 12 simulated hours from 3 trucks; a single compressor incident emits 120 alerts per minute.
  - `COMPRESSOR_FAULT` triggered 31,953 times but was published 17 times, masked by `TEMP_BREACH`.
  - The most frequent alert, `TIRE_PRESSURE_LOW`, is a simulator artifact: tire pressure is a random walk with no mean reversion.
- Local toolchain: Windows Defender quarantined `uv.exe` at first, and the Python 3.11 and 3.14 installs had lost their `python.exe`. The uv-managed CPython 3.12 still worked. `uv` was restored later the same day.
- ruff 0.16 formats Python code blocks inside Markdown, so the vendored skills under `.claude/` are excluded from ruff.
- The gitleaks pre-commit hook scans only staged changes. Run with `--all-files` in CI, it checks nothing. CI runs the gitleaks container over the full history instead.
- Not yet added to pre-commit: hadolint and sqlfluff, which arrive with the first Dockerfile and migration (day 4).
- Skeleton CI: lint, typecheck, test and secrets jobs. `make check` is green locally.
