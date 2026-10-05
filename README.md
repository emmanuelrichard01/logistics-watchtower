# Logistics Watchtower

**A cold-chain risk platform for refrigerated truck fleets on Nigerian roads.** Cargo spoils in the 30-40 °C heat while trucks cross long cellular dead zones. Watchtower's job is to tell an operator *which shipment will breach soonest, how sure the system is, why, and what to do*, before the cargo is lost.

<p align="center"><img src="docs/media/tour-lanes.gif" alt="The lanes board: open a breaching truck's evidence, drag the time handle back to replay the morning, then return to live" width="960"></p>
<p align="center"><sub>The lanes board: open a breaching truck's evidence, replay the morning, return to live. Full quality: <a href="docs/media/tour-lanes.mp4">MP4</a>.</sub></p>

> **Status: v2 rebuild in progress** (week 1 of 14; [plan](plan/Logistics%20Watchtower%202.0%20Rebuild%20Plan.md)). The console replays a **synthetic** simulator recording; the streaming services are designed but not built yet. Every number in this README links to the measurement that produced it. Anything unmeasured is called a target.

## What makes it different

Most fleet dashboards answer "is this truck over the threshold right now?" Watchtower is built to answer harder questions.

- **Forecast, with honest uncertainty.** Time-to-breach is reported as a p10-p90 range with a confidence, never a bare number. The console draws the forecast as a fan, distinct from measurements.
- **Sensors are not trusted blindly.** Every reading carries probe trust, so a stuck cargo probe is reported as "cargo temperature uncertain", not as an all-clear.
- **Dead zones are normal.** Devices buffer and replay. Late data updates its own minute bucket, and an alert learned late is still dated at its true event-time start.
- **Any incident can be replayed exactly.** Everything that can change an output passes through one ordered input log, so replaying the archive reproduces every alert ([ADR-0005](docs/adr/0005-single-ordered-input-log.md), [ADR-0015](docs/adr/0015-determinism-contract.md)).
- **A console designed for decisions.** Lanes are railway track diagrams and time-to-breach is a signal aspect, encoded by lamp position and count, not colour alone. One time handle replays every view at once ([console docs](docs/console/README.md)).

## See it

All footage is the production build replaying the simulator's showcase morning, rendered frame by frame at 30 fps (`apps/dashboard/scripts/media.mjs`). Every vehicle and reading is synthetic.

<p align="center"><img src="docs/media/tour-map.gif" alt="The map: the fleet on real road geometry, the breaching truck selected with its trail and route ahead, 3D signal masts, then a Lagos van at street level" width="960"></p>
<p align="center"><sub>The map: inter-state lanes, the breaching truck with its trail and route ahead, 3D signal masts, and a Lagos city round at street level. <a href="docs/media/tour-map.mp4">MP4</a>.</sub></p>

<table>
  <tr>
    <td width="50%"><img src="docs/media/evidence-dark.png" alt="Evidence layer in the dark theme: verdict, temperature chart with the limit, and why this score"></td>
    <td width="50%"><img src="docs/media/map-3d.png" alt="3D map in the dark theme with a signal mast over the breaching truck"></td>
  </tr>
  <tr>
    <td><sub><b>Evidence.</b> The verdict, minute-mean temperatures against the limit, and why the score is what it is.</sub></td>
    <td><sub><b>3D.</b> Signal masts rise over trucks that need attention; lamp position carries the aspect.</sub></td>
  </tr>
  <tr>
    <td><img src="docs/media/map-city.png" alt="A Lagos van on its morning round at street level, with customer drops and its route ahead"></td>
    <td><img src="docs/media/tour-incidents.gif" alt="Incident workflow from the keyboard: acknowledge, mitigate, move to the next incident and open its evidence"></td>
  </tr>
  <tr>
    <td><sub><b>City logistics.</b> Multi-drop rounds with delivery windows, on real streets.</sub></td>
    <td><sub><b>Incidents.</b> A lifecycle board run from the keyboard. <a href="docs/media/tour-incidents.mp4">MP4</a>.</sub></td>
  </tr>
</table>

<p align="center">
  <img src="docs/media/phone-lanes.png" alt="Phone: compact lanes" width="250">
  <img src="docs/media/phone-map.png" alt="Phone: map with a draggable bottom sheet, dark theme" width="250">
  <img src="docs/media/phone-incidents.png" alt="Phone: incidents" width="250">
</p>
<p align="center"><sub>Full workflow parity on the phone: bottom tabs, compact lanes and draggable sheets.</sub></p>

## Architecture

```mermaid
flowchart LR
  dev["Edge devices<br/>buffer in dead zones"] --> gw["Gateway<br/>validate, quarantine"]
  gw --> log[("wt.input.v1<br/>one ordered log")]
  tick["Ticker"] --> log
  cmd["Operator commands<br/>via outbox"] --> log
  log --> proc["Processor<br/>trust, risk, alerts"]
  proc --> out[("alerts, risk,<br/>fleet state")]
  out --> work["Projector, archiver"]
  log --> work
  work --> pg[("Postgres")]
  work --> s3[("Parquet on S3")]
  pg --> api["API<br/>REST, WebSocket"]
  out --> api
  api --> ui["Operator console"]
```

The processor is the single owner of alert state ([ADR-0006](docs/adr/0006-alert-lifecycle-single-owner.md)). Operator actions travel through the same log as telemetry, so escalation, replay and the audit trail all see one ordered history. See the [architecture overview](docs/architecture/overview.md) for component status, sequence diagrams, topics and the data model.

## Status

| Phase (plan section 18) | What exists | Status |
| --- | --- | --- |
| 0. Foundation | v1 frozen and audited; uv workspace; quality gates; CI workflow (not yet run on GitHub); Avro contracts (telemetry, input record) and event identity; Compose core stack; database schema; stateless ingest gateway with signed readings and quarantine (ADR-0021) | Done |
| 1. Reliable backbone | Device-scoped dedup, idempotent minute buckets, delta-returning `evaluate`, deterministic eviction; sensor trust and cargo fusion (all property-tested) | Started early; DLQ, engine spike and late-data lane planned |
| 2. Durable core | Schema with alerts, interventions, outbox and an append-only audit chain; the alert state machine with a single owner, as a pure function (ADR-0006) | Schema and state machine done; projector, outbox relay and API planned |
| 3. Data layer | Bronze bucket provisioned on SeaweedFS | Planned |
| 4. Decisions | Time-to-breach (exponential fit with a p10-p90 range), mean kinetic temperature, exposure and risk assessment (aspect, confidence, expected loss), composed by a pure vehicle evaluator: the processor's core | Started early; the processor's stream shell and console wiring planned |
| 5. Experience | Operator console: lanes, evidence layer, map (2D/3D), incidents, health, phone layouts, replay | In progress, on simulator recordings |
| 6. Evidence | v1 runtime baseline | Planned |

Simulator v2 is complete: a virtual clock, a two-node reefer thermal model, real road routes, dead zones with edge buffering, sensor and device faults, city last-mile in Lagos and Abuja, seeded fleet days with ground-truth labels, live mode with signed delivery to the gateway and a fault-injection API, and a vectorised bulk mode (about 0.5M physics events/s at 25,000 trucks, [benchmark](docs/simulator/bulk-benchmark.md)). Its physics is checked by a generated [validation report](docs/simulator/validation.md).

## Quick start

Requires [uv](https://docs.astral.sh/uv/), Docker Desktop, Node.js 24+ and GNU Make. Windows notes are in the [local development guide](docs/guides/local-development.md).

```bash
make install    # Python workspace and git hooks
make check      # lint, pyright, fast tests (same as CI)
make up         # Redpanda, Schema Registry, Postgres/PostGIS, SeaweedFS, gateway
make migrate    # apply the database schema
make console    # console production build at http://localhost:4173
```

`make help` lists every target. Run `make test-integration` for the container-backed tests and `make console-check` for the console's lint, types, tests and build.

## Engineering highlights

Real decisions, with their trade-offs:

- **Measure first, then claim.** Before rebuilding anything, v1 was audited against its own README. It produced 108,683 alerts in 12 simulated hours from 3 trucks, masked 31,953 compressor faults behind other alerts, and raised its most frequent alert from a simulator artifact ([v1 audit](docs/audit/v1-baseline.md)). Its runtime baseline (3 to 2,000 trucks) was measured inside the Compose network so producer and client share one clock.
- **A startup race, found by measuring.** At 1,000 trucks, v1's dashboard showed zero alerts. The API had subscribed before the `alerts` topic existed, and only noticed it at librdkafka's 5-minute metadata refresh (297 s), skipping about 10,000 alerts. v2 creates every topic explicitly before any consumer starts, with auto-creation off.
- **Event identity that survives replay.** `event_id` is a UUIDv5 of device, boot and sequence number. Property testing found a crash on its first run: JSON can carry lone surrogates that UTF-8 can't encode. IDs are now restricted to a safe charset ([ADR-0002](docs/adr/0002-event-identity-and-idempotency.md)). The trade-off: the charset is now a contract with device firmware.
- **Order independence by construction.** Minute buckets accumulate integer hundredths, because float sums depend on order and would make a replay differ from the live run in the last bits. Property tests shuffle, duplicate and batch readings and assert identical state.
- **An adversarial architecture review, kept on record.** A fresh-context review found 32 issues in the original plan: two writers to alert state, a non-replayable stream, random alert IDs, and an archived object store (MinIO, verified via the GitHub API). The fixes are recorded in ADRs 0005-0018 ([review](docs/architecture/review-2026-10-05.md)).
- **Performance diagnosed, not guessed.** The console's map felt laggy. The first measurement showed the dev server at 0.2-47.5 fps against 59.8-59.9 fps for the production build. A later profile found the real first-visit problem: **12 s of synchronous shader compilation**, with deck.gl's line shader alone taking 3+ s per variant on ANGLE/D3D11. Lines, stops and labels moved to MapLibre and HTML, leaving deck.gl with one shader. A cold first visit now settles in **about 6 s instead of 23-27 s** ([console docs](docs/console/README.md#performance)). Along the way, MapLibre's worker turned out to be missing from production builds, so the basemap never decoded tiles. Vite couldn't see the worker's computed URL; it's now bundled explicitly.
- **A forecast that never pretends to be exact.** When cooling fails, cargo approaches ambient exponentially, so time-to-breach is a least-squares fit of that curve. Residual scatter widens it into a p10-p90 range, and a property test pins that the estimate never grows as cargo warms. Learned models come only after they beat this transparent baseline (`packages/domain/src/watchtower_domain/forecast.py`).
- **Acknowledge only what the broker acknowledged.** The gateway answers 202 only after every record in a batch is acknowledged by Redpanda. Otherwise it returns 503 with `Retry-After`, and the device resends; deterministic `event_id`s make the repeats harmless. It stays stateless: sequence-reuse detection lives in the processor, because the plan's original "reject sequence regressions" rule would have quarantined every buffered replay ([ADR-0021](docs/adr/0021-gateway-contract-and-quarantine.md)).
- **Domain purity enforced by a test.** The domain package may import only the standard library, minus I/O, clocks and randomness. That makes it deterministic and reusable by the processor, the edge agent and replay.

## Repository

| Path | Contents |
| --- | --- |
| `apps/dashboard` | Operator console (React, Vite, MapLibre, deck.gl) |
| `apps/gateway` | Ingest gateway (FastAPI): validates and signature-checks readings, produces to `wt.input.v1` (ADR-0021) |
| `apps/simulator` | Simulator v2 (`wt-sim`): real OSRM road routes, two-node reefer thermal model, dead zones and edge buffering, sensor and device faults, city last-mile in Lagos and Abuja, seeded fleet days with ground-truth labels |
| `apps/dashboard-fixtures` | Recorded simulator runs for console development (schema in its README) |
| `packages/contracts` | Avro schemas, event identity |
| `packages/domain` | Pure domain logic: dedup, buckets, sensor trust, forecast, risk, alert state machine, vehicle evaluator |
| `packages/platform` | Kafka producer factory, Schema Registry client |
| `infra/compose` | Core stack and topic creation |
| `data/routes` | Corridor and city route geometry from OpenStreetMap via OSRM (ODbL) |
| `data/scenarios` | Seeded scenarios and their ground-truth labels |
| `migrations` | Alembic schema |
| `tests` | Unit, property, contract and integration tests |
| `docs` | Architecture, ADRs, audit, design, guides, devlog |
| `plan` | The rebuild plan |
| `legacy/v1` | Frozen v1, kept for the before-and-after comparison (also tagged `v1-final`) |

## Documentation

- [Architecture overview](docs/architecture/overview.md) and the [architecture review](docs/architecture/review-2026-10-05.md)
- [Architecture decision records](docs/adr/README.md)
- [Operator console](docs/console/README.md) and its [design brief](docs/design/console.md)
- [v1 audit and baseline](docs/audit/v1-baseline.md)
- [Local development](docs/guides/local-development.md) and [testing](docs/guides/testing.md)
- [Devlog](docs/devlog.md): surprises and measurements, day by day
- [Product record](PRODUCT.md)

## Limitations

- **No real fleet.** All data is synthetic, generated by the simulator from seeded scenarios.
- **The stream is only half built.** The gateway is built and produces to `wt.input.v1`, and the processor's core exists as a pure, tested function. But the stream shell that runs that core, the projector, the API and the archiver are still designed, not built, so nothing consumes the input log yet.
- **CI has not run remotely.** The workflow exists, and the same checks pass locally.
- **One laptop, single node.** The performance numbers come from one Windows laptop with integrated graphics and a single-node broker. Nothing here is a production claim.
- **The console replays a recording, not a live stream.** It plays the simulator's showcase morning (10 vehicles, inter-state and city, on real OSRM road and street geometry) using the device-reported probe values. Its time-to-breach is a provisional client-side estimate until the processor's risk assessments are streamed.
- **The plan is the plan.** The 14-week schedule is a target, and the [plan](plan/Logistics%20Watchtower%202.0%20Rebuild%20Plan.md) states its own risks.

## Author

**Emmanuel Richard**, Data Engineer · [GitHub](https://github.com/emmanuelrichard01)
