# Devlog

Surprises and measurements, newest first. Raw material for the case study.

## Domain state layout and the console (Mon 5 Oct 2026)

- **Domain refactored to ADR-0016.** `evaluate(view, reading)` returns a `Delta` (only the touched minute bucket, the updated sequence range and the new progress), so a state store can persist one key per bucket. Dedup is keyed by `(device_id, boot_id)`; buckets use integer epoch-minute keys; eviction follows event time, never the wall clock.
- **Rounding surprise.** "Half-up" via `floor(x*100 + 0.5)` turns 0.285 into 28, because 0.285 is 0.28499... in binary. Quantisation now rounds the float's shortest decimal form with `Decimal` (half away from zero), and ADR-0015 is amended. A hand-picked test case caught this; the property tests didn't, because they compare the domain with itself.
- **Console map performance.** On the reference laptop's GPU, the production build holds 59.8-59.9 fps on the map, against 0.2-47.5 fps on the Vite dev server (`apps/dashboard/scripts/perf-map.mjs`, 3 runs each). Two console bugs found by measuring rather than looking:
  - MapLibre's worker was missing from production builds: it locates the worker with a computed URL that Vite can't see. Fixed by bundling it via `?worker&url`.
  - A basemap layer added from a `styledata` event (which fires mid-load) silently aborted the style.

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
