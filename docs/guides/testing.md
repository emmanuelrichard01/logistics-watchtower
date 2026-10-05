# Testing

Tests are the evidence for every claim the project makes (ADR-0001, honest-claims rule). Each layer exists to prove something specific. The counts below were observed when this guide was written.

| Layer | Count | What it proves | Run |
| --- | --- | --- | --- |
| Unit and property: domain and platform | 82 | Domain logic is idempotent, order-independent and pure; trust, forecast, risk and the alert state machine behave as specified; event identity is stable; the producer factory pins its delivery settings | `make test` |
| Unit: gateway | 51 | Every validation reason code in order, signatures, settings, and the HTTP contract (202 only when every record is acknowledged, 503 otherwise) | `make test` |
| Contract | 14 | For `telemetry_event` and `input_record`: every released version has a golden payload, the newest schema reads every older version (BACKWARD), and golden IDs match the identity function | `make test` |
| Integration | 10 | The migration applies and downgrades cleanly; database invariants hold; an Avro event round-trips through the real Schema Registry, which rejects incompatible changes | `make test-integration` (needs Docker) |
| Console | 7 | The synthetic timeline is deterministic, and each scripted scenario raises exactly the incidents it should | `npm --prefix apps/dashboard test` |

`make check` runs lint, pyright (strict on `packages/domain` and `packages/contracts`) and the fast suite. CI runs the same, plus a full-history secret scan, the integration suite and the console checks (`.github/workflows/ci.yml`).

## Property tests that earn their keep

Written with Hypothesis in `tests/unit`:

- **Applying a reading twice equals applying it once** (`test_buckets.py`).
- **Arrival order and duplicates don't change state.** A duplicate storm plus shuffled, late delivery produces exactly the state of in-order delivery.
- **Processing in batches equals processing whole.**
- **A delta touches only the reading's own minute bucket** (ADR-0016's delta interface), and dedup is scoped to (device, boot), so two devices that share a boot ID never drop each other's readings.
- **Eviction follows event time, not the wall clock**, so a replay evicts exactly what the live run did.
- **Time-to-breach never increases as cargo warms toward the limit**, and a clean exponential curve's crossing time is recovered exactly; noise widens the p10-p90 range around it (`test_forecast.py`).
- **No forecast without a warming trend,** and none when the equilibrium temperature is below the limit.
- **Mean kinetic temperature** of a constant is that constant, matches a golden value, and weights warm periods above the arithmetic mean.
- **Exposure never decreases as data arrives.**
- **Sequence ranges are canonical whatever the arrival order** (`test_seq_ranges.py`).
- **Different (device, boot, seq) triples never share an `event_id`** (`test_event_identity.py`).

On its first run, the identity property found a real bug. JSON can carry a lone surrogate (`"\ud800"`), and `uuid5` crashed with `UnicodeEncodeError` instead of rejecting it. Device and boot IDs are now restricted to `[A-Za-z0-9._:-]{1,64}` (ADR-0002).

Minute buckets accumulate integer hundredths because float addition depends on order: summing the same readings in a different order can change a mean in the last bits, which would break replay determinism (ADR-0015). Quantisation rounds the float's shortest decimal form half away from zero. A test pins cases like 0.285 → 29, which a naive `floor(x * 100 + 0.5)` gets wrong (28, because 0.285 is stored as 0.28499... in binary).

## Guards that test themselves

- **Domain purity** (`test_domain_purity.py`). The domain package may import only the standard library, minus I/O, clocks and randomness. It's an allowlist, so a library nobody thought to ban still fails, and a second test proves the guard catches violations.
- **Schema compatibility.** Before relying on it, the BACKWARD check was confirmed to reject a breaking change (a required field added without a default raises `SchemaResolutionError`).
- **Identity pin.** `test_identity_is_pinned` fails if the `event_id` function's output ever drifts, which would silently break deduplication of replays.

## Integration tests

`tests/integration` uses Testcontainers (Redpanda and PostGIS) and is marked `integration`, so `make test` skips it. It covers:

- the migration upgrading an empty database and downgrading cleanly;
- `alerts_one_live` rejecting a second live alert for the same dedup key, and a malformed dedup key being rejected;
- `interventions` rejecting a duplicate idempotency key;
- a shipment being on only one truck at a time;
- the app role being unable to rewrite append-only tables;
- `minute_series` routing rows to UTC-day partitions;
- an Avro event registered in the Schema Registry, produced and consumed back equal;
- the Schema Registry itself rejecting a BACKWARD-incompatible change;
- a gateway batch travelling end to end through Redpanda and the Schema Registry.

The audit hash chain's constraints exist in the schema, but no test covers them yet.

## Console tests and probes

- `src/data/synthetic.test.ts`: scenario truths for the synthetic timeline (see the [console docs](../console/README.md#data-contract)).
- `scripts/perf-map.mjs`: map frame rate and long-task time. A measurement, not a pass/fail test.
- `scripts/capture.mjs` and `scripts/media.mjs`: screenshots for design review and for these docs.

## Planned

From plan section 15, not built yet: scenario evaluation against ground-truth labels (precision, recall, lead time; v1 rules versus v2), resilience tests for the failure matrix (plan section 10), Playwright end-to-end tests of the incident workflow on desktop and phone (Gate 6), and the benchmark harness (plan section 16).

## Documentation checks

```bash
cd apps/dashboard
node scripts/check-doc-links.mjs                       # every relative link, image and #anchor resolves
npm install --no-save mermaid@11 && node scripts/check-mermaid.mjs ../../README.md ../../docs/architecture/overview.md
node scripts/media.mjs http://localhost:4173           # regenerate docs/media (needs a production preview and ffmpeg)
```
