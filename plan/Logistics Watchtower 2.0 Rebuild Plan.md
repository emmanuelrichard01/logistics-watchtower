# Logistics Watchtower 2.0: Rebuild Plan

Oct 5, 2026 · @Emma

## 1. Mission, success criteria and non-goals

Rebuild Watchtower from a telemetry-and-alerts demo into a cold-chain risk decision platform that stays correct under failure, can replay any incident, and publishes measured numbers. One person can finish the core in 14 weeks if the scope rules in section 2 are held. (Extended from 12 weeks on 5 Oct 2026 to fund the operator console; see ADR-0010.)

**Mission.** Tell an operator which shipment is at risk, how long remains before the cargo is compromised, how confident the system is, what evidence supports that, and what to do next. Do it on a Nigerian reality: 30 to 40 °C ambient, long dead zones between Lagos, Ibadan, Ilorin and Abuja, and sensors that lie.

**What done looks like.** Each criterion needs an artifact a reviewer can check, not a claim.

| # | Success criterion | Proof a reviewer can check |
| --- | --- | --- |
| 1 | One-command start | `make up` brings the full stack healthy in under 5 minutes on a clean laptop |
| 2 | Failure injected, nothing lost | Chaos scenario (compressor fault + 25% packet loss + broker restart) ends with 0 lost events and 0 duplicate alerts, asserted by a test |
| 3 | Deterministic replay | Replaying a stored day of events yields byte-identical alerts and risk scores |
| 4 | Predictive value | Time-to-breach warning fires before the threshold breach on scripted scenarios, with measured lead time |
| 5 | Data trust | Sensor-fault events are separated from real excursions, with precision and recall reported on labelled scenarios |
| 6 | Analytics | Gold dbt models pass all tests; a backfill after a corrected transform shows the changed metrics |
| 7 | Measured performance | Benchmark report with hardware, config, p50/p95/p99 latency and sustained events per second |
| 8 | Operational maturity | ADRs, runbooks, SLO dashboard and a candid limitations section |
| 9 | Operator console | An operator completes the full incident workflow on desktop and phone in Playwright; map frame rate and interaction latency are measured against stated budgets; axe and keyboard-only runs pass |

**Non-goals.** These are deliberate, so they do not creep back in.

- Not a full transport management system: no dispatch, billing or driver apps.
- No real hardware. The simulator stays, but it becomes much more faithful.
- No ML before a deterministic baseline exists and can be measured against.
- No claim of exactly-once delivery. The doc states what is guaranteed and how it is tested.
- No second stream engine, no Kubernetes and no multi-tenancy in the core build. They sit in section 21 as a defined stretch path.

## 2. Scope rules: core, should-have and stretch

The build has three tiers, and the 14-week plan only commits to Tier 0 and Tier 1. Both reviews of the old project agreed on the ideas; the risk now is scope, not direction.

**Admission test for any component.** It enters the build only if all three are true:

1. It answers a specific question or failure mode named in this plan.
2. It can be tested automatically, with a test that fails when it breaks.
3. You can explain, in two sentences, why the simpler alternative was not enough.

| Tier | Commitment | Contents |
| --- | --- | --- |
| 0: Core | Must ship | Event contracts, idempotent processing, DLQ and replay; Postgres domain model and durable alerts; event-time processing with late-data handling; sensor-trust layer; time-to-breach and MKT; edge buffering simulation; Parquet archive; failure-injection tests; benchmark report; operator console with mobile parity (section 13) |
| 1: Should-have | Ship if on schedule | dbt Silver/Gold with backfill demo; OpenTelemetry tracing; SLO dashboard; JWT auth with roles; operator workflow (acknowledge, assign, resolve); loss-weighted prioritisation |
| 2: Stretch | Documented, not built | Flink migration path, Iceberg, multi-tenancy, Kubernetes and Helm, LLM incident summaries, ML anomaly models, per-device mTLS |

**Cut rules.** If a phase runs more than 30% over its time box, drop the lowest-value Tier 1 item in that phase and record the cut in an ADR. Never cut tests, replay or the benchmark: they are the proof that makes the rest credible.

**Honest-claims rule.** Every number in the README comes from a benchmark in the repo. Anything unmeasured is labelled a target.

## 3. Product reframe: from alerts to decisions

The old system answered "is this truck over the threshold right now?" The rebuild answers four harder questions, one per user, and every feature must trace back to one of them.

| User | Decision they make | Question the system must answer |
| --- | --- | --- |
| Control-room operator | Which truck do I call first, and what do I tell the driver? | Which shipment will breach soonest, how sure are we, what is the nearest intervention? |
| Fleet reliability manager | Which trucks, routes and drivers cost us cargo? | Which units show degrading compressors; which lanes have the most excursion minutes? |
| Compliance or QA officer | Can I prove this load stayed in range? | What is the shipment's full temperature history, cumulative exposure and data-gap record, and can it be shown to be untampered? |
| On-call engineer | Is the platform itself healthy and trustworthy? | Is data fresh and complete; where is lag building; did we lose or duplicate anything? |

**Five shifts in thinking**

| From (v1) | To (v2) |
| --- | --- |
| Threshold crossed | Time-to-breach and cumulative thermal exposure |
| Truck is the data model | Shipment, vehicle, sensor and telemetry are separate concepts |
| Every reading is trusted | Every reading carries a quality score; sensor faults are separate from real excursions |
| Connected is assumed | Dead zones are normal; devices buffer and replay, and the system handles late data |
| Alert is a toast notification | Alert is a durable incident with lifecycle, evidence and audit trail |

**Alert lifecycle.** An alert moves through `OPEN` → `ACKNOWLEDGED` → `MITIGATING` → `RESOLVED`, or `OPEN` → `AUTO_CLEARED` when the condition recovers on its own. Escalation raises severity when an open alert is not acknowledged in time. Deduplication groups repeats into one incident with a count; it never hides a more severe alert behind a less severe one, which fixes the v1 single-alert-per-truck behaviour.

## 4. Target architecture

&#91;embedded content: architecture · 13 components around one event bus\]

Telemetry and alert events live on Redpanda, and the archive and alert tables are rebuilt from it by replay. Operator actions are recorded in Postgres first and published through the outbox relay, so no write ever needs to be atomic across a database and a broker.

> **Architecture v2.1 (5 Oct 2026).** An adversarial review (`docs/architecture/review-2026-10-05.md`) changed the target architecture. The changes: one ordered input log (ADR-0005); the processor as the single alert-lifecycle owner (ADR-0006); a determinism contract (ADR-0015); a processor state layout (ADR-0016); vehicle-keyed topics and a pinned partitioner (ADR-0017); a push protocol (ADR-0018); session auth (ADR-0008); and SeaweedFS instead of the archived MinIO (ADR-0013). Where this plan's text disagrees with those ADRs, the ADRs win.

## 5. Technology decisions

Keep Redpanda and Python, add the pieces that answer a named requirement, and keep the processing logic independent of the stream engine so the engine can change later. Each row below becomes an ADR in section 22.

| Area | Choice | Why | Revisit when |
| --- | --- | --- | --- |
| Language and tooling | Python 3.12, `uv` workspace, ruff, pyright, pytest | Fast loop, strict typing, one lockfile for a monorepo | Never for core |
| Device transport | MQTT (Mosquitto) with QoS 1, HTTP batch endpoint as fallback | Real IoT pattern; QoS 1 is at-least-once, so duplicates are realistic and testable | MQTT costs more than a week to wire up |
| Ingest gateway | Async Python service: validate, stamp ingest time, produce | One place to enforce contracts, quarantine bad data, record source metadata | Throughput needs exceed one process per partition set |
| Event broker | Redpanda, single node in Compose, built-in Schema Registry | Kafka API, no ZooKeeper or JVM, already in v1 | Need multi-node durability tests: use 3-node profile |
| Contracts | Avro schemas in a shared package, Pydantic models for in-process use, compatibility mode `BACKWARD` enforced in CI | Evolution rules are well understood; compact on the wire | Consumers outside Python need codegen: consider Protobuf |
| Stream processing | Quix Streams (RocksDB state, changelog topics), with domain logic in a pure Python package | Python-native, keeps v1 investment, enough for per-truck state | Need timers, state over \~50 GB, or SQL-style joins: move to Flink |
| Operational store | PostgreSQL 16+, SQLAlchemy 2, Alembic | Constraints, transactions, audit tables, idempotent upserts | Never for core |
| Geo logic | PostGIS (Tier 1) | Depot proximity, route-corridor deviation, nearest-intervention queries | Skip if time is short; use haversine in Python |
| Raw archive | Parquet on MinIO (S3 API), partitioned by date and hour | Cheap, replayable, same code path as cloud S3 | Query volume needs a warehouse |
| Analytics | dbt Core with DuckDB reading Parquet | Tested, documented transforms with no cluster to run | Data exceeds one machine: Iceberg and a scalable engine |
| API | FastAPI, Pydantic v2, OpenAPI contract tests with Schemathesis | Typed contracts and generated docs | Never for core |
| Real-time push | API consumes the alert and state topics and pushes over WebSocket | No Redis needed while there is one API instance | Several API replicas need fan-out: add Redis or NATS |
| Auth | JWT with roles (viewer, operator, admin), self-issued in dev | Enough to demonstrate RBAC and audit | Real users: add Keycloak or another OIDC provider |
| Dashboard | React, Vite, TypeScript, MapLibre GL | Smooth map rendering with open tiles and no API key | Time-box to 1 week; fall back to upgrading v1 `index.html` |
| Observability | OpenTelemetry SDK and Collector, Prometheus, Tempo, Grafana | Metrics and traces with one trace ID from device event to dashboard push | Logs: add Loki only if time allows |
| Fault injection | Toxiproxy plus scripted container kills | Repeatable latency, loss and partition tests | Never for core |
| CI | GitHub Actions: lint, type check, unit, Compose integration, contract checks | Every claim backed by a green pipeline | Never for core |

**Stream engine decision gate (end of week 2).** Spike the hardest requirements on Quix Streams: out-of-order handling, a staleness check for trucks that stop sending, and recovery after killing a worker. If any needs awkward workarounds, switch the processor shell to Flink while keeping the domain package unchanged. Write the outcome as ADR-003.

## 6. Domain model and PostgreSQL schema

Postgres holds what must be durable and transactional: reference data, current state, risk assessments, alerts, interventions and the audit trail. Raw telemetry never goes there; it goes to the Parquet archive.

| Concept | Table | Notes |
| --- | --- | --- |
| Tenant | `organizations` | Every business table carries `org_id` from day one. Row-level security is enforced later, but the column costs nothing now |
| Vehicle and equipment | `vehicles`, `sensors` | Sensors have a calibration offset and a last-calibrated date, so drift can be modelled |
| Cargo requirements | `cargo_profiles` | Min and max temperature, allowed excursion minutes, MKT limit, value per kg in NGN, reference shelf life. Never hard-code limits |
| Shipment | `shipments`, `shipment_assignments` | A truck can carry several shipments; a shipment can move trucks. Assignment rows have valid-from and valid-to |
| Current state | `vehicle_state` | One row per vehicle, upserted by the processor. Includes last event time and last ingest time |
| Evaluation | `risk_assessments` | Append-only. Stores score, time-to-breach, confidence, exposure, rule version and the evidence event IDs |
| Incident | `alerts` | Durable lifecycle. One open alert per dedup key |
| Human action | `interventions` | Acknowledge, assign, mitigate, resolve. Unique idempotency key per command |
| Rules | `rule_sets` | Versioned rules-as-config with effective dates, so any past alert can be explained by the rules in force |
| Plumbing | `processed_events`, `outbox` | Idempotency and reliable publishing (section 10) |
| Accountability | `audit_log` | Append-only, hash-chained so tampering is detectable |
| Recent series | minute\_series | Minute buckets for the last 72 hours, partitioned by day and dropped by partition. Feeds shipment charts; older ranges come from Gold |

**Core DDL.** These tables carry the correctness guarantees, so write them first and test them hardest.

```sql
CREATE TABLE alerts (
  id              uuid PRIMARY KEY,           -- UUIDv7, generated in app
  org_id          uuid NOT NULL,
  shipment_id     uuid REFERENCES shipments(id),
  vehicle_id      uuid NOT NULL REFERENCES vehicles(id),
  alert_type      text NOT NULL,              -- TEMP_BREACH, DOOR_OPEN_MOVING, ...
  severity        text NOT NULL CHECK (severity IN ('LOW','MEDIUM','HIGH','CRITICAL')),
  state           text NOT NULL CHECK (state IN ('OPEN','ACKNOWLEDGED','MITIGATING','RESOLVED','AUTO_CLEARED')),
  dedup_key       text NOT NULL,              -- e.g. vehicle_id:alert_type
  rule_version    int  NOT NULL,
  opened_at       timestamptz NOT NULL,       -- EVENT time of first trigger
  last_seen_at    timestamptz NOT NULL,
  occurrence_count int NOT NULL DEFAULT 1,
  evidence        jsonb NOT NULL,             -- event ids + computed values
  created_at      timestamptz NOT NULL DEFAULT now()
);
-- one live alert per key; repeats update the row instead of inserting
CREATE UNIQUE INDEX alerts_one_live
  ON alerts (dedup_key) WHERE state IN ('OPEN','ACKNOWLEDGED','MITIGATING');

CREATE TABLE interventions (
  id              uuid PRIMARY KEY,
  alert_id        uuid NOT NULL REFERENCES alerts(id),
  actor_id        uuid NOT NULL,
  action          text NOT NULL,              -- ACK, ASSIGN, MITIGATE, RESOLVE, COMMENT
  note            text,
  idempotency_key text NOT NULL,
  created_at      timestamptz NOT NULL DEFAULT now(),
  UNIQUE (alert_id, idempotency_key)          -- double-click or retry is a no-op
);

CREATE TABLE processed_events (
  consumer_group  text NOT NULL,
  event_id        uuid NOT NULL,
  processed_at    timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (consumer_group, event_id)
);

CREATE TABLE outbox (
  id              bigserial PRIMARY KEY,
  topic           text NOT NULL,
  msg_key         text NOT NULL,
  payload         bytea NOT NULL,
  created_at      timestamptz NOT NULL DEFAULT now(),
  published_at    timestamptz
);
CREATE INDEX outbox_unpublished ON outbox (id) WHERE published_at IS NULL;
```

**Schema rules**

- Store both `event_time` and `ingest_time` everywhere; never mix them.
- All timestamps are `timestamptz` in UTC. Convert to Africa/Lagos only in the UI.
- Migrations are Alembic only, reviewed in PRs, and tested by applying them to an empty database and to a seeded one in CI.
- `risk_assessments` and `audit_log` are append-only, enforced by revoking `UPDATE` and `DELETE` from the application role.

## 7. Event contracts, topics and partitioning

Every event gets a stable identity and two timestamps, and every topic has a stated key, retention and owner. Most correctness problems later trace back to getting these wrong now.

**Event envelope and richer telemetry.** The v1 payload had one temperature and one door flag. Real reefer units report several air and cargo probes, a setpoint and an equipment state, which makes cross-sensor validation possible.

```json
{
  "event_id": "0192f3a1-7c5e-5b1d-9e3a-2f4c8a1d6b70",
  "schema_version": 2,
  "vehicle_id": "TRK-101",
  "device_id": "EDGE-0101",
  "boot_id": "b-20261005-0412",
  "seq": 18273,
  "event_time": "2026-10-05T07:12:03.250Z",
  "ingest_time": "2026-10-05T07:12:41.900Z",
  "position": { "lat": 7.3768, "lon": 3.9398, "speed_kmh": 85.3, "heading_deg": 42.7, "gps_fix": "3D", "hdop": 1.1 },
  "reefer": {
    "setpoint_c": -20.0, "supply_air_c": -21.4, "return_air_c": -19.8, "cargo_probe_c": -19.1,
    "compressor": "RUNNING", "defrost": false, "power_source": "ENGINE", "door": "CLOSED"
  },
  "vehicle": { "fuel_pct": 68.2, "battery_v": 13.8, "ambient_c": 33.5 },
  "link": { "signal_dbm": -87, "buffered": true }
}
```

**Identity rule.** `event_id` is a deterministic UUIDv5 of device ID, boot ID and sequence number. A retransmitted or replayed event therefore carries the same ID, which is what makes idempotent processing possible. `seq` increments per boot, so gaps reveal loss.

**Topics**

| Topic | Key | Partitions | Retention | Purpose |
| --- | --- | --- | --- | --- |
| `telemetry.raw.v1` | vehicle\_id | 12 | 7 days | Everything the gateway accepted, as received |
| `telemetry.quarantine.v1` | vehicle\_id | 3 | 30 days | Failed validation, with reason code and original bytes |
| `telemetry.clean.v1` | vehicle\_id | 12 | 7 days | Validated, deduplicated, quality-scored events |
| `telemetry.late.v1` | vehicle\_id | 3 | 30 days | Events beyond allowed lateness, kept for reconciliation |
| `risk.assessments.v1` | shipment\_id | 6 | 14 days | Scores and time-to-breach, with evidence |
| `alerts.events.v1` | alert dedup key | 6 | 30 days | Alert opened, updated, escalated, cleared |
| `fleet.state.v1` | vehicle\_id | 12 | compacted | Latest state per vehicle, feeds the dashboard |
| `<service>.dlq.v1` | original key | 3 | 30 days | Poison messages that failed after bounded retries |

**Rules**

- Key by `vehicle_id` on all telemetry topics so the same partition and ordering follow a truck through the pipeline.
- Partition counts are hard to change later, so 12 is chosen for headroom, not for the demo load.
- Topic names carry a version. A breaking change creates `v2` and the two run in parallel until consumers move.
- Schema changes may only add optional fields with defaults. The Schema Registry compatibility check runs in CI and blocks the merge on failure.
- Golden-file contract tests pin a sample payload per schema version, so accidental drift fails a test.

**Gateway validation.** Reject to quarantine, never drop silently: unknown device, schema mismatch, event time more than 5 minutes in the future or older than 7 days, physically impossible values (for example a probe below -80 °C), and sequence regressions within one boot. Each rejection carries a reason code that feeds a data-quality metric.

## 8. Simulator v2 and edge gateway

The simulator is a test instrument, not a demo prop: it is deterministic, scenario-driven, physically plausible, and it writes the ground truth the evaluation needs. Its biggest upgrade is connectivity, because dead zones are the central fact of this domain.

**Thermal model.** Replace "temperature drifts to ambient" with a two-node model, because cargo temperature lags air temperature and it is the cargo that spoils. Air probes breach first; the cargo probe breach is what matters, and the gap between them is the lead time the predictor can use.

```latex
C_a \frac{dT_a}{dt} = \frac{T_{amb}-T_a}{R_w} + \frac{T_c-T_a}{R_c} + Q_{door} + Q_{defrost} - h \, Q_{max} \, u(t)


C_c \frac{dT_c}{dt} = \frac{T_a-T_c}{R_c}
```

Here `h` in \[0, 1\] is compressor health, so a failing unit loses capacity gradually instead of switching off. `u(t)` is the on/off duty from a thermostat with hysteresis around the setpoint. Ambient follows a daily curve plus a seasonal offset (Harmattan nights are cooler, dry-season afternoons in Ilorin and Abuja are hotter).

**Connectivity model.** Each route segment gets a signal profile. A two-state Markov channel (good and bad) with segment-specific transition probabilities produces realistic outages, and a few named dead zones can be forced for repeatable tests.

- The device keeps a bounded on-disk ring buffer and, when the link returns, replays in order at a throttled rate while live data continues, so old and new events interleave.
- Replayed events are flagged `buffered: true`, and the gateway records both times so lateness is measurable.
- When the buffer fills, drop the oldest low-priority readings and keep excursion-related ones. Count the drops in a device metric.
- The edge agent runs the same domain-rule package as the processor, so a driver alarm still fires offline. This is the "edge-first" capability, built by reusing code rather than duplicating it.

**Device realism toggles** (each seeded and independently switchable)

- Clock skew and drift, with occasional GPS time correction.
- Reboots that reset `boot_id` and `seq`.
- At-least-once duplicates from MQTT QoS 1 retransmission.
- Sensor faults: flatline, spike, slow drift, noise burst, null dropout, swapped probes.
- GPS jumps and poor fix quality.

**Scenario catalogue.** Scenarios are YAML files with a seed, a fleet, a duration and injected events. Each ships with a ground-truth label file used only by tests and evaluation, never by the pipeline.

| Scenario | What happens | Correct system behaviour |
| --- | --- | --- |
| `compressor_gradual_degradation` | Health `h` falls from 1.0 to 0.4 over 90 minutes | Time-to-breach warning well before any threshold breach |
| `compressor_hard_fail` | `h` drops to 0 | Fast critical alert; time-to-breach counts down |
| `door_open_while_moving` | Door open for 8 minutes at highway speed | Critical alert; humidity and air-temp rise corroborate |
| `door_open_at_depot` | Door open while loading, stationary | Low severity or none, because it is expected |
| `dead_zone_with_excursion` | 35-minute outage during which cargo warms above limit | Edge alarm offline; on replay, alert carries true event-time start and duration |
| `defrost_cycle` | Normal brief return-air rise during defrost | No alert; defrost is recognised |
| `stuck_cargo_probe` | Cargo probe flatlines at -19 °C while air probes warm | Sensor-fault flag and lowered confidence, not a false all-clear |
| `sensor_drift` | Probe offset grows 0.5 °C per hour | Drift detected by cross-probe disagreement |
| `duplicate_storm` | 30% of events retransmitted up to 3 times | Zero duplicate alerts and zero double-counted exposure |
| `clock_skew_and_reboot` | Device clock 90 seconds fast, then reboot | Event times corrected or flagged; no false sequence-loss alarm |

**Faster-than-real-time.** The simulator runs on a virtual clock, so a 24-hour fleet day can be generated in minutes with the same seed and produce identical output. A separate bulk mode skips the full physics and emits pre-generated events from vectorised arrays, which is how the 10,000-truck throughput tests will run.

**Route data.** Replace the 7-waypoint lines with polylines from OpenStreetMap extracts committed as GeoJSON, with road-class tags driving speed and signal profiles.

## 9. Stream processing and risk engine

The processor turns raw readings into a trusted, time-correct picture of each shipment and a forecast of when it will breach. The design principle: all logic lives in a pure Python domain package with no I/O, shaped as `evaluate(state, event, rules) -> (new_state, outputs)`. The stream engine, the edge agent and the replay tool all call the same function, so behaviour is identical everywhere and testable without Kafka.

**Pipeline stages** (all keyed by `vehicle_id`)

| Stage | Responsibility | Output |
| --- | --- | --- |
| A. Normalise | Dedupe by `(boot_id, seq)`, convert units, correct estimated clock skew, detect sequence gaps | `telemetry.clean.v1` |
| B. Sensor trust | Classify each probe OK, SUSPECT or FAULTY; fuse healthy probes into one cargo temperature with a confidence | Quality flags on the event |
| C. Features | Minute-bucket aggregates, rate of change, excursion minutes, degree-minutes, door-open duration, compressor duty cycle, staleness | Per-vehicle state |
| D. Risk | Per-shipment time-to-breach, MKT, shelf-life consumed, expected loss, confidence | `risk.assessments.v1` |
| E. Alerts | State machine with debounce, hysteresis, escalation and dedupe | `alerts.events.v1` |
| F. Publish | Postgres writes through the outbox, state snapshots | `fleet.state.v1`, rows |

**Event time, lateness and order independence.** A stateful accumulator that applies readings in arrival order gives wrong answers when data arrives late. Instead, keep per-vehicle state as **idempotent one-minute buckets** (min, max, mean and count per probe) keyed by event-time minute. Late or replayed events update their own bucket and the affected metrics are recomputed over the bucket range. Results then do not depend on arrival order or duplicates.

- Deduplication uses a per-boot set of contiguous sequence ranges. The same structure exposes gaps, which is how data loss is measured.
- Normal lateness (up to 10 minutes) is absorbed by recomputation. Buffered replays after a dead zone, flagged `buffered: true`, are merged by the same mechanism for up to 48 hours.
- Anything older than the retention window goes to `telemetry.late.v1` and a reconciliation job issues corrected assessments that reference the one they supersede. Corrections are appended, never edited.
- The watermark advances from the live lane only, so a long replay never makes live data look early.

**Staleness needs ticks.** A silent truck produces no events, so nothing triggers processing. A small ticker emits a processing-time `tick` per partition every 15 seconds; on a tick, the processor checks each vehicle's last ingest time. Two cases are kept separate:

- **Link down, device reporting buffered data later:** expected in dead zones. Raise `TELEMETRY_GAP` at low severity, and project temperature forward with the thermal model. Confidence decays with gap length, and the time-to-breach range widens.
- **Link up, sensor silent or flatlined:** a device or sensor problem. Raise `SENSOR_FAULT`.

**Time-to-breach.** Start with a transparent baseline, then upgrade. The baseline fits an exponential approach to equilibrium over the last 20 to 30 minutes of trusted cargo temperature. With equilibrium temperature `T_eq` (ambient-driven when cooling fails), time constant `tau` and a limit between the current temperature and `T_eq`:

```latex
t_{breach} = -\tau \, \ln\!\left(\frac{T_{lim}-T_{eq}}{T_0-T_{eq}}\right)
```

The fit residuals give an uncertainty band, reported as a p10 to p90 range, never a single bare number. A later upgrade estimates the thermal parameters `R`, `C` and compressor health `h` online (recursive least squares or a Kalman filter) so degradation shows up before any breach. Machine-learned models come last, and only after they beat this baseline on the labelled scenarios.

**Cumulative exposure metrics.** Cargo profiles choose which metrics apply, because no single number is right for every product.

```latex
T_{MKT} = \frac{\Delta H / R}{-\ln\!\left(\frac{1}{n}\sum_{i=1}^{n} e^{-\Delta H/(R\,T_i)}\right)}
```

Mean kinetic temperature uses temperatures in kelvin and a default `ΔH/R` of 10,000 K, and is standard for some pharmaceuticals. Frozen food may instead use excursion minutes above the limit and degree-minutes. Shelf-life consumed uses an Arrhenius or Q10 acceleration factor against a reference temperature. Treat all of these as configurable, and have the cargo owner confirm limits; do not present them as universal safety rules.

**Sensor trust rules**

- Flatline: near-zero variance for N minutes while other probes move.
- Impossible rate: a cargo probe changing faster than the thermal mass allows.
- Cross-probe residual: cargo, return-air and supply-air readings that disagree with the thermal model.
- Plausibility: out-of-range values, and GPS speed that contradicts position change.
- Policy: if the cargo probe is FAULTY, report "cargo temperature uncertain" and widen the forecast band. Never silently substitute an air probe or report all-clear.

**Alert rules.** v1 listed 7 rules while claiming 8; v2 defines each with a debounce and a clear condition.

| Alert | Opens when | Severity | Clears when |
| --- | --- | --- | --- |
| `CARGO_TEMP_BREACH` | Trusted cargo temp beyond profile limit for 2 consecutive minutes | CRITICAL | Back in range 5 consecutive minutes |
| `BREACH_FORECAST` | Time-to-breach under 45 min with confidence above 0.6 | HIGH, CRITICAL under 15 min | Forecast above 60 min, or cause resolved |
| `EXPOSURE_BUDGET` | Excursion budget used: 50%, 80%, 100% | MEDIUM, HIGH, CRITICAL | Shipment closes |
| `DOOR_OPEN_MOVING` | Door open over 30 s while speed above 5 km/h | CRITICAL | Door closed 60 s |
| `DOOR_OPEN_PROLONGED` | Door open longer than profile allows while stationary | HIGH | Door closed |
| `COMPRESSOR_FAULT` | Fault code reported | CRITICAL | Code cleared 5 min |
| `COMPRESSOR_DEGRADING` | Duty cycle high and pull-down slower than the vehicle's baseline | HIGH | Baseline restored |
| `SENSOR_FAULT` | Any probe FAULTY; cargo probe FAULTY raises to HIGH after 15 min | MEDIUM or HIGH | Probe OK 10 min |
| `TELEMETRY_GAP` | No data for the stale limit, escalating with projected risk | LOW to HIGH | Data resumes |
| `ROUTE_DEVIATION`, `LOW_FUEL`, `BATTERY_LOW`, `SPEED_VIOLATION` | Geofence, 15%, 11.8 V, speed limits | LOW to MEDIUM | Hysteresis band |
| `SEQUENCE_GAP` | Missing sequence numbers after replay window | LOW | Data-quality only |

**Prioritisation and next action.** Rank open alerts by expected loss, not by label: probability of breach before arrival × cargo value in NGN × expected spoiled fraction, with time-to-breach as the tiebreaker. A deterministic playbook, stored as config, maps alert types and context to a recommended action (for example the nearest depot with cold storage from the PostGIS query). AI summaries, if added later, only phrase this verified output.

**Rules as versioned config.** Rule parameters live in YAML, published to a compacted topic and the `rule_sets` table with an effective date. Every assessment and alert stores the rule version it used, so a past incident can be explained and a replay can run against either the original rules or a what-if set.

## 10. Correctness under failure

The platform guarantees at-least-once delivery with idempotent effects, and it proves that with tests; it does not claim end-to-end exactly-once. Writing to a database and committing a broker offset are separate operations, so the design removes the need for atomicity between them instead of pretending it exists.

**One system of record per fact.** The core rule that avoids dual-write bugs:

| Fact | System of record | Projection | How the projection stays correct |
| --- | --- | --- | --- |
| Telemetry-derived alert transitions | Kafka topic `alerts.events.v1` | `alerts` table in Postgres | Projector applies an event only if its version is newer than the row's; offset committed after the DB commit |
| Operator actions (ack, assign, resolve) | Postgres `interventions` | `alerts.events.v1` for the dashboard | Written in one transaction with an `outbox` row; a relay publishes it; the unique idempotency key makes retries harmless |
| Raw and clean telemetry | Kafka, then Parquet archive | Postgres `vehicle_state` | Upserts keyed by vehicle, guarded by event time |

**Guarantees, hop by hop**

- **Device to gateway:** at-least-once. MQTT QoS 1 may redeliver, and the gateway only acknowledges after a successful produce, so a broker outage causes retries, not loss.
- **Gateway to Redpanda:** idempotent producer with `acks=all`. Duplicates remain possible across restarts and are identified by `event_id`.
- **Processor:** at-least-once input. State updates are idempotent and order-independent (section 9), and every output carries a deterministic ID, for example a hash of dedup key, transition type and event-time minute. A crash and replay therefore reproduces the same outputs rather than new ones.
- **Postgres projection:** idempotent upsert guarded by version, plus `processed_events` where an event has no natural version.

**Retries, dead letters and replay**

- Classify errors as transient (retry with exponential backoff and jitter, at most 3 attempts) or permanent (straight to the dead-letter topic).
- A dead-letter record carries the original bytes, key, error class, stack summary, attempt count and consumer group, so it can be fixed and re-injected.
- DLQ depth is a paged metric; a growing DLQ is an incident, not a log line.
- The replay CLI, `wt replay --from T1 --to T2 --vehicles ... --rules vN`, reads the Parquet archive or Kafka with an isolated consumer group and writes only to `replay.<run_id>.*` topics. It never touches production topics, and a `diff` command compares replayed and original alerts.

**Failure matrix.** Every row is an automated test in `tests/resilience`, named in the last column.

| Failure | Expected behaviour | Test |
| --- | --- | --- |
| Duplicate telemetry (QoS retransmit, redelivery) | No duplicate alerts; exposure not double-counted | `test_duplicate_storm` |
| Events 30 s late, and a 45-minute buffered replay | Metrics recomputed; alert carries true start time | `test_late_and_replayed_events` |
| Processor killed after processing, before offset commit | Reprocessing yields identical outputs, no duplicates downstream | `test_crash_before_commit` |
| Processor loses its worker and local state | State restored from changelog; recovery time measured and recorded | `test_state_recovery` |
| Total state loss | State rebuilt by replaying the archive; result equals the pre-loss state | `test_full_rebuild` |
| Postgres unavailable while stream continues | Processing continues; projector lag grows and is alerted; UI shows a "data delayed" banner; catches up on recovery with no gaps | `test_postgres_outage` |
| Redpanda restart or leader election | Gateway withholds acks and retries; zero lost events | `test_broker_restart` |
| Poison message | 3 retries, then DLQ; partition keeps flowing | `test_poison_message` |
| Operator double-clicks acknowledge | One intervention row, same response both times | `test_idempotent_ack` |
| Schema Registry down | Gateway uses cached schemas; unknown schema IDs fail closed to quarantine | `test_registry_outage` |
| Host clock jumps | Durations use monotonic clocks; event time comes only from data | `test_clock_jump` |
| Invalid rule set published | Rejected at validation; last known good stays active | `test_bad_rules_rejected` |
| Archive store full or unreachable | Archiver backs off and alerts; Kafka retention covers the gap; archive back-filled from Kafka | `test_archive_outage` |

**Invariants checked after every chaos run**

1. Every unique `event_id` accepted by the gateway appears exactly once in the clean archive.
2. No alert transition appears twice for the same dedup key and version.
3. Replaying the archive reproduces the same alerts and risk scores.
4. Every alert's evidence event IDs resolve to real archived events.
5. Exposure recomputed from the archive matches the stored value within a stated tolerance.

## 11. Data platform: Bronze, Silver, Gold

The archive is what makes every claim checkable: it is the input to replay, to analytics and to compliance evidence. Build it as a first-class product with its own tests, not as an export bolted on at the end.

**Archiver service.** A consumer writes `telemetry.raw.v1` to Parquet on MinIO.

- **Bronze is partitioned by ingest time** (`dt=YYYY-MM-DD/hr=HH`), never by event time. Files are then append-only and immutable; late events never force a rewrite of an old partition.
- Each row stores the Kafka coordinates (topic, partition, offset), schema ID, ingest time and the original payload. Coordinates make re-archiving idempotent and give exact lineage.
- Roll files at about 128 MB or 5 minutes. Write to a unique key and register the file, so a crash never leaves a half-visible file.
- A nightly compaction job merges small files; it is also tested, because small-file sprawl is a real failure mode.

**Layers**

| Layer | Contents | Rules |
| --- | --- | --- |
| Bronze | Raw events as received, duplicates included, plus Kafka coordinates and ingest metadata | Immutable. Retention set by policy. Source of truth for replay |
| Silver | Deduplicated by `event_id`, units conformed, quality flags, partitioned by **event** date; `silver_rejects` holds failures with reason codes | Incremental with a 48-hour lookback so late and replayed data is absorbed; merge on `event_id` |
| Gold | Business models below | Tested, documented, contract-enforced |

**Gold models**

| Model | Grain | Question it answers |
| --- | --- | --- |
| `fct_shipment_minutes` | shipment × minute | What was the cargo temperature at any moment, and how trustworthy was it? |
| `fct_excursions` | one excursion episode | When did it start and end, how severe, how many degree-minutes? |
| `fct_shipment_outcomes` | shipment | Delivered in spec? Exposure, MKT, delay, data-gap minutes |
| `fct_alert_response` | alert | Time to acknowledge and resolve; did intervention change the outcome? |
| `dim_vehicle_reliability` | vehicle × week | Which compressors are degrading; failure frequency |
| `fct_lane_performance` | route × day | Excursion minutes per 1,000 km, delays, dead-zone exposure |
| `fct_data_quality_daily` | device × day | Completeness from sequence numbers, late share, duplicate share, quarantine reasons |
| `mart_compliance_report` | shipment | Evidence pack: full history, gaps, rule version, tamper-check result |

**Testing the data.** dbt tests are part of CI, not an afterthought.

- Generic tests: unique and not-null `event_id`, accepted values, relationships between facts and dimensions.
- Custom tests: physical temperature range, exposure never negative, no unexplained minute gaps inside a shipment.
- Source freshness on bronze, with failure thresholds that match the SLOs.
- dbt unit tests on the dedupe and excursion-detection logic using small fixed inputs.
- Model contracts enforced on Gold so a column change breaks the build, not a dashboard.

**Operational data to analytics.** DuckDB attaches to Postgres read-only to bring alerts and interventions into Gold, so response-time analysis needs no second pipeline.

**Data observability.** A small `quality` package computes and exports freshness per layer, row-count anomalies against a 7-day baseline, schema drift and completeness. The dashboard shows a data-trust badge, so a stale or partial number is visibly marked as such.

**Marquee demo: backfill after a bug.** This is the clearest proof of the design.

1. Commit a deliberate transformation bug, such as a wrong unit conversion for one device model.
2. Run the pipeline and record Gold excursion KPIs.
3. Fix the bug and backfill the affected date range from bronze.
4. Show the before and after KPIs, passing tests and the lineage graph from Kafka offsets to the corrected rows.

**Orchestration.** Start with `make pipeline` and a scheduled container that runs `dbt build` every few minutes. Dagster is a clean upgrade path if scheduling and asset lineage become the focus; do not add it before then.

## 12. API, security and tenancy

The API is a thin, typed layer over Postgres and the event streams. It enforces identity, permissions, idempotency and pagination, and it records who did what. Design the contract first (OpenAPI), then implement it, then test the implementation against the contract.

**Endpoints (all under `/api/v1`)**

| Endpoint | Method | Min role | Notes |
| --- | --- | --- | --- |
| `/vehicles`, `/vehicles/{id}` | GET | viewer | Keyset (cursor) pagination; includes data-trust badge |
| `/vehicles/{id}/series` | GET | viewer | Recent minute series from a 72-hour table; older ranges from Gold via DuckDB |
| `/shipments`, `/shipments/{id}` | GET | viewer | Includes current risk, forecast range and confidence |
| `/shipments/{id}/evidence` | GET | viewer | Events, rule version and assessments behind the current risk |
| `/alerts`, `/alerts/{id}` | GET | viewer | Filter by state, severity, vehicle; cursor paging |
| `/alerts/{id}/ack`, `/assign`, `/mitigate`, `/resolve` | POST | operator | Requires `Idempotency-Key` header |
| `/rules`, `/rules/{version}` | GET, POST | viewer, admin | POST validates, versions and activates; invalid sets are rejected |
| `/audit/verify` | GET | admin | Verifies the hash chain over a range |
| `/ws/ticket` | POST | viewer | Issues a 30-second one-time ticket for the WebSocket |
| `/ws/v1/stream` | WS | viewer | Snapshot then deltas, each with a sequence number; client resumes from its last one |
| `/health/live`, `/health/ready` | GET | none | Readiness checks Postgres and broker connectivity |
| `/metrics` | GET | internal | Prometheus; not exposed publicly |

**API conventions**

- Errors use RFC 9457 `application/problem+json` with stable error codes.
- Commands store the idempotency key with a hash of the request. A repeat returns the original response; the same key with a different body returns 422.
- Keyset pagination only; offsets break under concurrent inserts.
- WebSocket tokens never travel in the URL, because URLs end up in logs. The ticket flow avoids that. The server sends heartbeats and drops idle or oversized-message clients.
- Rate limits are token buckets per user and per IP, with a per-user cap on WebSocket connections.

**Authentication and authorisation**

- Short-lived JWT access tokens (15 minutes, asymmetric signing) with `sub`, `org_id` and `roles` claims, plus refresh tokens. Local development issues tokens from a small auth module; a real OIDC provider such as Keycloak is a later swap.
- Permissions are checked in one dependency, not scattered across handlers.

| Action | viewer | operator | admin |
| --- | --- | --- | --- |
| Read fleet, shipments, alerts | yes | yes | yes |
| Acknowledge, assign, mitigate, resolve | no | yes | yes |
| Publish rules, manage cargo profiles | no | no | yes |
| Verify audit chain, manage users | no | no | yes |

- An authorisation test is generated from the OpenAPI document: for every endpoint and role, assert the expected allow or deny.

**Device identity and tamper evidence**

- Core: each device signs its payload with a per-device HMAC key, and the gateway verifies it. Failures go to quarantine with reason `bad_signature`. Sequence and time checks defeat simple replay attacks.
- Stretch: per-device mTLS certificates with rotation.
- `audit_log` rows are hash-chained: each row's hash covers the previous hash and a canonical form of its content. A daily head hash is written to object storage. This makes tampering detectable, not impossible, and the docs say so.

**Tenancy, staged.** Every table, topic key and API query already carries `org_id`, and the API applies a mandatory tenant filter from day one. Postgres row-level security and per-tenant topic prefixes are the Tier 2 hardening step, so the cheap part is done now and the expensive part is deferred.

**Threat model.** Write a one-page STRIDE-style table in `docs/security`: spoofed device, replayed telemetry, tampered history, privilege escalation, WebSocket resource exhaustion, leaked secrets, and personal data exposure. Driver identity and location are personal data, so apply minimisation and retention limits, and check the requirements of the Nigeria Data Protection Act 2023 with someone qualified before any real deployment.

**Secrets.** Never commit `.env` files. Use Docker secrets or SOPS-encrypted files locally, and GitHub OIDC for CI. Run dependency and container scanning in the pipeline.

## 13. Operator dashboard and incident workflows

The dashboard exists to help an operator decide, so it ranks by expected loss, shows confidence and evidence next to every number, and says plainly when data is old or estimated. It is a core deliverable with mobile parity: a first slice in week 9, then a dedicated Experience phase in weeks 10 and 11 (ADR-0010). The visual direction and interaction design live in `docs/design/console.md`. A separate public case-study page carries the scroll-driven storytelling; the console itself uses restrained, state-bearing motion.

| View | Primary user | Key elements |
| --- | --- | --- |
| Fleet board | Operator | Shipments ranked by expected loss, time-to-breach with range, confidence badge, last-heard time |
| Shipment detail | Operator, QA | Cargo and air temperatures against the limit, forecast cone, door and compressor events, shaded data gaps, a "why this score" panel listing contributing factors and the rule version |
| Incident queue | Operator | Alerts with lifecycle actions, response-time clocks, deduplicated counts |
| Map | Operator | MapLibre markers coloured by risk, route corridor, dead-zone overlay, depots |
| Data health | Engineer | Freshness per layer, consumer lag, DLQ depth, quarantine reasons, projector lag |

**Trust behaviours**

- Every risk figure shows a confidence level and the reason when it is low, for example "cargo probe suspect".
- A projected value is labelled "estimated, last heard 12 min ago". The UI never shows a projection as a measurement.
- A banner appears when the WebSocket drops or when projector lag exceeds its threshold. The client reconnects and resumes from its last sequence number.
- Toasts are a convenience: deduplicated with a count, and acknowledgeable in place. The incident queue is the record.
- Colours do not rely on red versus green alone, and motion respects the reduced-motion setting.

**Incident workflow**

1. **Acknowledge** records who and when. An unacknowledged CRITICAL alert escalates after 3 minutes (configurable).
2. **Assign** to a responder.
3. **Mitigate** by choosing a playbook action (call driver, check door seal, switch to shore or genset power, divert to nearest depot, transfer cargo) plus a note.
4. **Resolve** with an outcome: saved, partial loss or total loss, with an estimated loss in NGN.
5. The outcome feeds `fct_alert_response`, so the platform can measure whether interventions help.

**Notifications are transient; incidents are durable.** A separate notifier consumes `alerts.events.v1` and delivers to in-app, webhook, email and SMS (a provider such as Africa's Talking or Termii, stubbed in development). Each channel has its own retry policy and dead-letter handling. A failed delivery never changes the alert, and every attempt is recorded.

**Frontend engineering**

- React, TypeScript and Vite, with TanStack Query for REST and one WebSocket client with reconnection and resume.
- TypeScript types are generated from the OpenAPI document, so an API change breaks the UI build instead of the UI at runtime.
- Batch WebSocket deltas every 250 ms and virtualise long lists, so a busy fleet does not freeze the browser.
- Playwright end-to-end tests drive a real scenario: inject a compressor failure, expect the forecast alert within a stated time, acknowledge it, and see the state change. The same test records the demo video.

## 14. Observability, SLOs and runbooks

Observability turns the "under 200 ms" claim from v1 into a defined, measured, defended number. The question to answer at any moment is whether the platform is healthy and whether its data can be trusted.

**Metrics that matter** (Prometheus, exported by every service)

| Metric | Why it matters |
| --- | --- |
| `wt_gateway_events_total{result}` | Accepted, quarantined and duplicate counts; quarantine spikes reveal a bad device or release |
| `wt_consumer_lag_seconds{group,topic}` | Lag in time, not just offsets, because 10,000 messages means different things at different rates |
| `wt_event_to_alert_seconds` (histogram) | The real end-to-end latency: gateway ingest to alert committed and pushed |
| `wt_late_events_total{lane}`, `wt_seq_gaps_total` | Late and lost data, by lane |
| `wt_dlq_depth{topic}` | Poison messages waiting; any growth is an incident |
| `wt_projector_lag_seconds`, `wt_outbox_unpublished` | Whether Postgres is keeping up with the streams |
| `wt_processor_checkpoint_seconds` | State-store health and recovery cost |
| `wt_data_freshness_seconds{layer}` | Bronze, Silver and Gold staleness |
| `wt_device_buffer_fill_ratio` | Fleet-side backlog during dead zones |
| `wt_ws_connections`, `wt_active_alerts{severity}` | Load and operational state |

**Tracing.** Use OpenTelemetry and propagate W3C `traceparent` in Kafka message headers, so one trace follows a reading from device to gateway, processor, projector, API and WebSocket push. At thousands of events per second, full tracing is unaffordable, so use tail-based sampling in the Collector: keep every trace that contains an alert, an error or an unusually slow span, plus about 1% of the rest. Batch consumption uses span links instead of a single parent.

**Logs.** Structured JSON with `trace_id`, `event_id`, `vehicle_id` and `service`. No personal data, and sampled debug levels.

**Service-level objectives.** These are initial targets only. Replace each with the measured baseline from the Phase 5 benchmark, then tighten.

| SLI | Initial target | Window |
| --- | --- | --- |
| CRITICAL alert visible in the API, measured from gateway ingest | 99% within 5 s | 30 days |
| Valid events accepted by the gateway | 99.9% | 30 days |
| Expected events (by sequence number) present in Silver within 15 min, excluding buffered devices | 99.5% | 30 days |
| Gold freshness under 15 minutes | 99% of the time | 30 days |
| Dead-letter items older than 1 hour | 0 | continuous |

**Burn-rate alerting.** Page on error-budget burn using multiple windows (for example 14.4× over 1 hour with a 5-minute confirmation, and 6× over 6 hours), which catches fast and slow burns without noisy threshold alerts.

**Dashboards as code.** Grafana dashboards are provisioned from JSON committed to the repo: pipeline health, alert-latency SLO, data quality, and fleet device health. Alertmanager routes to a webhook in development.

**Runbooks.** One per alert in `docs/runbooks`, each with symptom, impact, diagnostic queries, mitigation, escalation and follow-up. Start with consumer lag, DLQ growth, Postgres outage, projector lag, quarantine spike, archive outage and schema-compatibility failure.

**Game days.** After each major phase, inject a failure, follow only the runbook, and record time to detect and time to recover. Write a short blameless postmortem for each, because these are strong evidence of operational maturity.

## 15. Testing strategy and failure scenarios

Tests are the evidence for every claim in the README, so each layer has a defined purpose, and the strongest layers are the ones v1 lacked: property-based tests, scenario evaluation against ground truth, and resilience tests.

| Layer | What it proves | Tools | When it runs |
| --- | --- | --- | --- |
| Unit and property | Domain logic is idempotent, order-independent and bounded | pytest, Hypothesis | Every commit |
| Golden numeric | MKT, time-to-breach and exposure match independently computed reference values | pytest with fixed inputs | Every commit |
| Contract | Schemas stay compatible; API matches OpenAPI; golden payloads per schema version | Schema Registry checks, Schemathesis | Every PR |
| Integration | Each service works with real Redpanda, Postgres and MinIO | Testcontainers | Every PR |
| Scenario | The scenario catalogue produces exactly the expected alerts and no others | Virtual-clock simulator, full stack in Compose | PR subset, nightly full |
| Resilience | The failure matrix in section 10 and its invariants | Toxiproxy, container kills | Nightly |
| Data | Silver and Gold correctness | dbt tests and unit tests | Every PR |
| Security | Authorisation matrix, token tampering, ticket reuse, rate limits, dependency and container scans, secret scanning | pytest, pip-audit, Trivy, gitleaks | Every PR |
| Performance | Throughput, latency, soak and reconnect-storm behaviour | Custom load generator, k6 for the API | Nightly smoke, weekly soak |
| End-to-end UI | Operators can see, acknowledge and resolve an incident | Playwright | Nightly |

**Property tests that earn their keep** in the domain package:

- Applying the same event twice gives the same state as applying it once.
- Shuffling events within the lateness window gives the same final buckets and metrics.
- Splitting a stream into batches and processing them in sequence equals processing it whole.
- Time-to-breach never increases when the cargo temperature moves further toward the limit.
- Cumulative exposure never decreases as more data arrives.

**Alert-quality evaluation.** Every scenario ships with ground-truth labels, so quality is measured, not asserted. Report this table per release.

| Metric | Definition |
| --- | --- |
| Precision and recall | Alerts matching labelled incidents versus false and missed ones |
| Detection delay | Alert time minus true onset, in event time |
| Forecast lead time | Time between the first `BREACH_FORECAST` and the actual breach |
| False positives per truck-day | On normal scenarios, including defrost cycles and depot stops |
| Sensor-fault classification | Precision and recall on the fault scenarios |

Run the v1 threshold rules and the v2 engine on the same scenarios and publish both. A visible improvement in lead time and false positives is the strongest single result in the case study.

**Quality rules**

- Every random element is seeded and every test uses the virtual clock; no sleeps for time.
- Flaky tests are quarantined within 24 hours with an issue, and the quarantine list is reviewed weekly.
- Mutation testing (for example mutmut) runs on the risk engine, because tests that pass on mutated logic are not testing anything.
- Chase meaningful assertions, not a coverage percentage.

**CI structure.** A pull request runs lint, type check, unit, contract, integration and the scenario subset, with a 10-minute budget. Nightly adds the full scenario set, the chaos suite and a performance smoke test. Weekly adds a 6-hour soak test.

## 16. Benchmark methodology

No number appears in the README until a repeatable benchmark in the repo produces it. Write each experiment as a hypothesis first, run it, and publish the result even when it disappoints; an explained bottleneck is worth more than a flattering figure.

**Define the timers precisely.** v1's "under 200 ms" never said where the clock started or stopped.

| Timer | Starts | Stops | Use |
| --- | --- | --- | --- |
| T1 gateway | Message received | Broker acknowledges the produce | Ingest cost |
| T2 processing | Broker timestamp of the raw record | Alert event produced | Stream engine cost |
| T3 event-to-alert (headline) | Gateway ingest time | Alert committed in Postgres and WebSocket frame sent | The SLO number |
| T4 visibility | Gateway ingest time | Headless client receives the frame | What an operator experiences |

**Method rules**

- Use an open-loop load generator that sends on a schedule regardless of responses; closed-loop generators hide latency spikes (coordinated omission).
- Record latency in an HDR histogram and report p50, p95, p99 and max, never only an average.
- Warm up before measuring, repeat each run at least 5 times, and report the spread.
- Pin and publish hardware, OS, versions, container limits, partition counts and tuning settings, with the commit hash.
- Raw results are committed as CSV, and the plots regenerate with `make bench`.

**Experiments**

| # | Experiment | Method | Reported |
| --- | --- | --- | --- |
| 1 | Throughput ramp | 100 to 25,000 simulated trucks at 0.2 and 1 event per second | Saturation point, where latency bends and lag starts growing |
| 2 | Steady-state latency | Hold 50% and 80% of the saturation load | T1 to T4 percentiles |
| 3 | Reconnect storm | 5,000 devices reconnect and replay 30 minutes of buffered data at once | Time to drain; effect on live-alert latency |
| 4 | Soak | 6 hours weekly, 24 hours once, at 50% load | Memory, state-store size, file counts, lag stability |
| 5 | Recovery | Kill gateway, processor, projector, Postgres and broker in turn | Time to resume, time to drain backlog, events lost (target 0) |
| 6 | Scale-out | 1, 2 and 4 processor instances | Throughput per instance, rebalance pause, efficiency |
| 7 | Analytics | dbt build, backfill and query time at 1×, 10× and 100× data | Duration, bytes per event, cost per million events |
| 8 | Resource profile | CPU and memory per service at fixed load | Cost per 1,000 events per second |

**Tuning knobs to explore and document:** producer `linger.ms`, batch size and compression, consumer fetch sizes, partition count, RocksDB cache size, checkpoint interval and Parquet row-group size. For each, record the before and after and the reason.

**Baseline.** In Phase 0, benchmark the v1 code on the same machine. v2 is then compared against a measured starting point, not a memory of it.

**Honesty section.** Every report states what it does not show: single-node deployment, simulated devices, no real network jitter, and no production traffic patterns. State the largest known bottleneck and what would be tried next.

## 17. Repository, tooling and engineering workflow

Use one monorepo with a small number of deployable units. The temptation after reading about "platform" architecture is a dozen microservices; resist it. Background consumers share one codebase and image with different entrypoints, and the pure domain package is the part that must stay clean.

```text
logistics-watchtower/
├── apps/
│   ├── simulator/        # Virtual-clock fleet, scenarios, edge agent, bulk load mode
│   ├── gateway/          # MQTT/HTTP ingest, validation, quarantine, producer
│   ├── processor/        # Stream shell: wires domain logic to the engine
│   ├── workers/          # Entrypoints: projector, archiver, notifier, outbox-relay, ticker
│   ├── api/              # FastAPI REST + WebSocket
│   ├── cli/              # wt replay, wt audit verify, wt scenario run
│   └── dashboard/        # React + TypeScript + MapLibre
├── packages/
│   ├── domain/           # PURE logic: buckets, trust, forecast, risk, alert state machine
│   ├── contracts/        # Avro schemas, Pydantic models, golden payloads
│   ├── platform/         # Kafka, Postgres, S3 clients, config, OpenTelemetry helpers
│   └── quality/          # Freshness and completeness checks, metric exporters
├── data/
│   ├── dbt/              # Silver and Gold models, tests, docs
│   └── scenarios/        # YAML scenarios + ground-truth labels
├── infra/
│   ├── compose/          # Profiles: core, obs, analytics, chaos, bench
│   ├── grafana/          # Dashboards as JSON
│   └── otel/             # Collector and sampling config
├── migrations/           # Alembic
├── tests/                # unit, contract, integration, scenario, resilience, perf
├── docs/                 # architecture, adr, runbooks, security, benchmarks, api
├── Makefile
└── README.md
```

**Dependency rule.** `domain` may import nothing from `platform`, Kafka, Postgres or any I/O library. Enforce this with an import-linter contract in CI, so the engine-agnostic design cannot erode quietly.

**Tooling and conventions**

- `uv` workspace, ruff, pyright in strict mode for `domain` and `contracts`, pre-commit with ruff, sqlfluff, hadolint and gitleaks.
- Multi-stage Dockerfiles, non-root users, pinned base image digests, one image per app.
- Compose profiles keep laptops usable: `core` is the minimum pipeline, and `obs`, `analytics`, `chaos` and `bench` layer on top. Run Redpanda in development mode with a single core and a memory cap.
- Configuration through `pydantic-settings`, validated at start-up, with the process refusing to boot on invalid config.
- Makefile targets: `up`, `down`, `seed`, `scenario`, `test`, `resilience`, `bench`, `pipeline`, `docs`.
- Conventional Commits, short-lived branches, required CI checks on `main`, and a PR template asking for tests, metrics, docs and an ADR when a decision changed.
- ADRs in `docs/adr` using the MADR template; contracts versioned and changelogged.
- Docs as a MkDocs site with C4-style diagrams, published from CI.

**Migration from v1**

1. Tag the current repo `v1-final` and keep it reachable, so the before and after are both visible.
2. Create the new layout on a `v2` branch and move across only what earns its place: route and waypoint data, the map and WebSocket ideas from the dashboard, and the failure-injection concept.
3. Rewrite everything else behind the new contracts rather than porting file by file.
4. Replace the monolithic `producer.py`, `processor.py` and `api.py` with the separated apps above, so no file becomes a 500-line catch-all again.

## 18. 14-week roadmap

&#91;embedded content: roadmap · 6 phases, 6 gates, 12 weeks\]

> The embedded roadmap predates the 14-week extension (ADR-0010). The phase list below is current: Experience is a new phase in weeks 10 and 11, and Evidence moves to weeks 12 to 14.

A phase that misses its gate does not hand unfinished work to the next one. The detail below lists each phase's work and the evidence that closes its gate.

### Phase 0: Foundation (week 1)

The day-by-day plan is in section 19.

- Work: tag v1, audit and baseline it, scaffold the monorepo and CI, write the event envelope and identity function, bring up the core Compose stack, build the simulator foundation and a gateway MVP.
- **Gate 1:** CI is green, the v1 baseline is measured and written down, and a simulated event travels from simulator to gateway to the raw topic.

### Phase 1: Reliable backbone (weeks 2 and 3)

- Week 2: finish the Avro contracts with the Schema Registry compatibility gate; gateway validation matrix, quarantine reason codes and metrics; shared retry and dead-letter framework; minute-bucket aggregator and sequence-range deduplication in `domain` with property tests; the stream-engine spike, ending in ADR-003.
- Week 3: processor shell wired to stages A to C; staleness ticker and late-event lane; sensor-trust rules with tests on the fault scenarios; simulator v2 with connectivity model, edge buffer and replay, sensor faults and the first five scenarios; time-to-breach and MKT as pure functions with golden tests, not yet wired in.
- Tests added: duplicate storm, late and replayed events, crash before commit, poison message.
- **Gate 2:** invariants 1 and 2 hold under the duplicate-storm and crash tests; a fixed seed replays deterministically; the engine decision is recorded.

### Phase 2: Durable core (weeks 4 and 5)

- Week 4: Alembic migrations for the full domain model; alert state machine in the processor emitting `alerts.events.v1`; baseline alert rules with debounce, hysteresis and deduplication; projector with version-guarded upserts; `vehicle_state` upserts; outbox table and relay.
- Week 5: API v1 designed from the OpenAPI contract first, with vehicles, shipments, alerts and interventions; idempotency keys; JWT roles; WebSocket ticket and resumable stream; versioned rule sets with validation; hash-chained audit log; authorisation matrix and Schemathesis tests.
- Tests added: Postgres outage, double acknowledge, bad rules rejected, authorisation matrix.
- **Gate 3:** killing Postgres for 60 seconds mid-stream leaves no gaps after recovery; a double-click acknowledge creates one intervention; the authorisation matrix and API contract tests pass.

### Phase 3: Data layer (weeks 6 and 7)

- Week 6: archiver writing Parquet to MinIO with Kafka coordinates and ingest-time partitions; compaction; replay CLI with isolated output topics and a diff command; dbt project, source freshness, Silver models and rejects.
- Week 7: Gold models, dbt tests, unit tests and contracts; DuckDB attached to Postgres for alert-response facts; data-quality metrics; the backfill demonstration; full-rebuild-from-archive resilience test.
- **Gate 4:** replaying a recorded day reproduces identical alerts and risk scores; the backfill demo is documented with before and after KPIs; every dbt test passes in CI.

### Phase 4: Decisions (weeks 8 and 9)

- Week 8: wire time-to-breach and exposure into the processor; `risk.assessments.v1` and its table; expected-loss ranking and playbook actions; forecast, exposure-budget and compressor-degradation alerts; dead-reckoning projection through gaps; reconciliation job for very late data; evaluation harness with precision, recall and lead time, comparing v1 rules with v2.
- Week 9 (time-boxed): dashboard fleet board, shipment detail with forecast range, incident queue, map and data-health view; operator workflow with escalation timers and a notifier with stub channels; Playwright end-to-end test; OpenTelemetry tracing and the SLO dashboard, moving to week 13 if behind.
- **Gate 5:** the compressor-degradation scenario raises a forecast alert before the breach with a measured lead time; the v1 versus v2 quality report is published; an operator completes an incident end to end in the Playwright test.

### Phase 5: Experience (weeks 10 and 11)

- Week 10: design system and tokens (light and dark themes, motion and type scales); app shell with command palette; fleet board and lane view; live map in 2D and 3D with smoothed live tracking; one shared time handle driving live and replay.
- Week 11: shipment detail with the forecast range and the "why this score" panel; incident queue with keyboard triage; data-health view; phone and tablet layouts at full parity; accessibility pass; performance budgets measured.
- **Gate 6:** an operator completes the incident workflow on desktop (1440 px) and phone (390 px) in Playwright; the map holds its frame-rate budget with 1,000 live vehicles on the reference laptop (measured, not claimed); axe and keyboard-only runs pass.

### Phase 6: Evidence (weeks 12 to 14)

- Week 12: benchmark harness with an open-loop generator and the bulk simulator mode; timers instrumented; experiments 1 and 2; the v1 baseline compared; tuning notes.
- Week 13: experiments 3 to 6 (reconnect storm, soak, recovery, scale-out); game days with runbooks and postmortems; security pass with the threat model and scans; deliberate buffer for overruns.
- Week 14: experiments 7 and 8; docs site; ADR clean-up; README rewritten from measured numbers; demo video; case study with limitations and the public case-study page; tag `v2.0`.
- **Gate 7:** every success criterion in section 1 is linked to an artifact a reviewer can open.

## 19. Your first 7 days

By the end of week 1 you have a green CI pipeline, a measured v1 baseline, a versioned event contract, a working core stack, and a first simulator-to-gateway-to-Kafka path. Each day ends with something committed and visible.

| Day | Focus | Concrete tasks | Done when |
| --- | --- | --- | --- |
| 1 (Mon 5 Oct) | Freeze v1, scaffold v2 | Tag `v1-final`; run v1 once and screen-record it; create the `v2` branch with the uv workspace, pre-commit, Makefile and CI skeleton (lint, types, unit); add `docs/adr/0001` capturing the mission and scope tiers from sections 1 and 2 | CI is green on the empty skeleton |
| 2 | Audit and baseline v1 | Check each README claim against the code (7 versus 8 alert rules, the 200 ms figure, the 15+ sensor count, line counts); measure v1 throughput, produce-to-WebSocket latency, CPU and memory with a small script; list defects | `docs/audit/v1-baseline.md` exists, with numbers and a defect list |
| 3 | Contracts and domain skeleton | Write the Avro envelope (section 7); implement the UUIDv5 `event_id` function with tests; create the pure `domain` package with an import-linter contract; first property test: idempotent bucket insert | Schema compat check and property test run in CI |
| 4 | Core infrastructure | Compose `core` profile: Redpanda in dev mode, Schema Registry, Postgres, MinIO, Redpanda Console; first Alembic migration with the core tables from section 6; Testcontainers test that produces and consumes an Avro event | `make up` is healthy; migration applies to an empty DB in CI |
| 5 | Simulator foundation | Virtual clock, seeded RNG, one route as GeoJSON, the two-node thermal model with compressor health, ground-truth label output; unit tests that cargo lags air and that a failure approaches the equilibrium temperature | Same seed gives byte-identical output |
| 6 | Gateway MVP | HTTP batch ingest first (MQTT later); validation, quarantine topic with reason codes, produce to `telemetry.raw.v1`; metrics endpoint; contract tests | Simulator to gateway to Kafka works end to end |
| 7 | Review and gate prep | Write ADR-002 (event identity and idempotency); define the three engine-spike questions for the week-2 gate; turn section 18 tasks into issues on a project board; short written retrospective | Backlog exists; week 2 is planned in detail |

**Habits to start on day 1**

- Commit small and often. A visible daily history is itself portfolio evidence.
- Write the ADR when a decision is made, not afterwards.
- Every new component arrives with a test and a metric in the same PR.
- Keep a running `docs/devlog.md` of surprises and measurements; it becomes the raw material for the case study.

**Week 2 preview.** Complete the contract and DLQ path, build the minute-bucket aggregator and sensor-trust rules in `domain`, run the stream-engine spike, and write ADR-003 with the decision.

## 20. Risks and mitigations

The main risk is not technical: it is trying to build a team's roadmap alone. The mitigations below are mostly rules about scope and evidence.

| Risk | Likelihood | Impact | Mitigation | Early warning |
| --- | --- | --- | --- | --- |
| Scope creep and over-engineering | High | High | Admission test, tiers and cut rules (section 2); Tier 2 stays on paper | A Tier 1 item is blocking a Tier 0 item |
| Stream engine cannot express a requirement cleanly | Medium | High | Week-2 decision gate; engine-agnostic `domain` package so a switch costs the shell only | Spike needs workarounds for staleness or recovery |
| Simulator realism becomes a rabbit hole | High | Medium | Time-box physics to 3 days; validate with three sanity properties; no real-world calibration chase | A fourth day on the thermal model |
| Laptop cannot run the full stack or benchmarks | Medium | Medium | Compose profiles; heavy tests in CI; run the published benchmark once on a rented cloud VM and document it | Containers swapping or OOM-killed |
| Frontend consumes the schedule | High | Medium | Dedicated Experience phase with its own gate (ADR-0010); UI built against recorded simulator streams so it never waits on the backend; API and data quality come first | UI work spills past week 9 |
| Estimates are wrong | High | Medium | 30% overrun rule; slack in weeks 13 and 14; Friday demo and replan | Two consecutive phases late |
| Claims without evidence | Medium | High | Honest-claims rule; every figure from a committed benchmark | A number in the README without a link to a run |
| Cargo tolerances are inaccurate | Medium | Medium | Configurable profiles marked illustrative; cite published guidance per profile; ask a cold-chain practitioner to review | A profile with no source |
| Momentum loss on a long solo build | Medium | High | A deliverable per day, a demo per Friday, public devlog | Two days with no commit |
| Security gaps in the demo | Low | Medium | Threat model, scans in CI, no real secrets or personal data | A scan finding left open over a week |
| Personal data exposure | Low | Medium | Synthetic data only; minimisation and retention rules documented | Any real driver data in a fixture |
| Licence surprises | Low | Medium | Check licences for Redpanda (source-available), MinIO (AGPL-based) and others before any commercial use; record in an ADR | A dependency added without a licence check |

**Weekly checkpoint questions**

1. Is every Tier 0 item still on schedule, and is anything lower-tier blocking one?
2. Which claim in the README is still unproven?
3. What did I measure this week that surprised me, and is it in the devlog?
4. What would I cut if I lost one week tomorrow?

## 21. Future-proofing and the stretch path

Future-proof means each stretch item becomes cheap to add later because of a decision made now, not that every item is built now. Five choices carry most of the weight: engine-agnostic domain logic, versioned contracts, replayable history, open standards (Parquet, OpenTelemetry, OpenAPI, MQTT, SQL), and rules held as versioned data.

| Stretch item | Build it when | Already done to make it cheap | Sketch |
| --- | --- | --- | --- |
| Apache Flink processing | You need timers, large state, joins, or transactional sinks | Pure `domain` package, replay CLI, scenario suite | Keyed process function with timers calling the same domain logic; the migration test is output equality between engines on the scenario suite |
| Apache Iceberg | You need schema evolution, time travel or several query engines | Consistently typed, partitioned Parquet | Iceberg sink with a REST catalog; DuckDB and others read the same tables |
| Multi-tenancy | A second customer or team appears | `org_id` on every table, topic key and API query | Postgres row-level security, topic ACLs and prefixes, per-tenant quotas and rules |
| Kubernetes | You want to demonstrate horizontal scaling | Twelve-factor config, health probes, graceful shutdown | Helm or Kustomize, autoscaling on consumer lag with KEDA, disruption budgets |
| Learned risk models | Labelled history beats the baseline on the scenarios | Minute-level Gold facts, evaluation harness with ground truth | Gradient-boosted time-to-breach and compressor-failure models in shadow mode, versioned artifacts, drift monitoring; advisory only, never overriding deterministic rules |
| LLM incident summaries | Operators want faster triage | A structured evidence endpoint with event IDs | Give the model only verified evidence JSON, require a schema-validated answer that cites event IDs, execute no actions, and treat operator free text as untrusted input |
| Natural-language fleet queries | Analysts want self-service | Tested, documented Gold models and contracts | Constrained semantic layer over Gold with a read-only role, an evaluation set of questions with known answers, and query logging |
| Firmware-grade edge logic | Real devices enter the picture | Edge agent already runs the domain package | Port the rule core to Rust or WebAssembly and prove it against the same golden tests |
| Standards export | A partner or regulator needs traceability data | Evidence pack and audit chain | Export shipment events in the GS1 EPCIS format, after verifying the current standard's sensor-data fields |
| Public demo | You want reviewers to try it | One-command local start, synthetic data | Terraform to a single VM with a read-only dashboard and cost guardrails |

**Architecture fitness checks** keep the design honest as it grows, and cost almost nothing to add to CI:

- Import-linter: `domain` stays free of I/O.
- A replay-determinism test on a fixed archive sample.
- A schema-compatibility gate on every contract change.
- A budget check that fails the build if end-to-end latency regresses beyond a stated margin in the performance smoke test.

## 22. Portfolio deliverables and decision log

The finished project should let a reviewer do four things in under 30 minutes: start it, inject a failure and watch it propagate, inspect the durable alert with its evidence and analytical record, and read the test results, benchmarks and trade-offs. Plan the deliverables from the start so the evidence is collected as you build, not reconstructed at the end.

**Deliverables checklist**

- [ ] README with the problem, a 60-second architecture overview, one-command start and a short scenario walkthrough
- [ ] Architecture diagrams: system context, containers, event flow, and failure-handling sequence
- [ ] Database design with the core DDL and the reasoning behind the invariants
- [ ] Event contract catalogue and the schema evolution policy
- [ ] API contract (OpenAPI) and the authorisation matrix
- [ ] Alert-quality report: v1 versus v2 on the labelled scenarios
- [ ] Benchmark report with timers, hardware, raw CSV and the honesty section
- [ ] Resilience report: failure matrix results and the invariants
- [ ] Game-day postmortems (at least two)
- [ ] Backfill demonstration with before and after KPIs
- [ ] Threat model and security decisions
- [ ] Runbooks and the SLO dashboard
- [ ] ADRs for every decision below
- [ ] A 3-minute demo video showing a failure injected and the incident resolved
- [ ] Public case-study page
- [ ] A candid limitations and next-steps section

**Case study outline.** Problem and constraints; architecture; three deep dives; results; failures and what they taught; what comes next.

- Deep dive 1: late and replayed data, and why minute buckets make results order-independent.
- Deep dive 2: telling a failing sensor from a failing truck.
- Deep dive 3: the backfill after a transformation bug, with lineage from Kafka offset to corrected KPI.

Two short articles come out of these naturally: one on event time and late data in a dead-zone fleet, and one on why "exactly-once" needs careful wording. When you describe the project, state measured results only, for example "measured p99 event-to-alert of X ms at Y events per second on Z hardware", with the real numbers from your benchmark.

**Decision log (ADRs)**

| ADR | Decision | Due | Main options to weigh |
| --- | --- | --- | --- |
| 0001 | Mission, scope tiers and non-goals | Week 1 | Full platform versus focused core |
| 0002 | Event identity and idempotency | Week 1 | Random UUID versus deterministic UUIDv5 from device, boot and sequence |
| 0003 | Stream engine | Week 2 | Quix Streams, Flink, Kafka Streams |
| 0004 | Contract format | Week 2 | Avro, Protobuf, JSON Schema |
| 0005 | Time handling and lateness | Week 3 | Minute buckets with recomputation versus reorder buffer |
| 0006 | One system of record per fact, projector and outbox | Week 4 | Dual write, CDC, event-sourced projection |
| 0007 | Rules as versioned config | Week 5 | Code constants, database table, compacted topic |
| 0008 | Auth model and WebSocket ticket | Week 5 | Session cookies, JWT, OIDC provider |
| 0009 | Bronze partitioning by ingest time | Week 6 | Ingest date versus event date |
| 0010 | Frontend scope and time box | Week 1 (decided early) | Rebuild versus upgrade v1 page; schedule extension versus cuts |
| 0011 | Tracing and sampling strategy | Week 9 | Head, tail, or no tracing |
| 0012 | Benchmark method and timers | Week 10 | Closed-loop versus open-loop load |
| 0013 | Dependency licences | Week 1 | Redpanda, MinIO and alternatives |
