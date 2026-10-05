# Devlog

Surprises and measurements, newest first. Raw material for the case study.

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
