# Devlog

Surprises and measurements, newest first. Raw material for the case study.

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
