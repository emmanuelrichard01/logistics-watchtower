---
status: accepted
date: 2026-10-05
---

# 0021: Gateway contract and quarantine

## Context and Problem Statement

Devices send telemetry over flaky links and retransmit at least once. The gateway is the one place to enforce the contract before anything reaches `wt.input.v1` (ADR-0005). The architecture review (finding 6) showed that the plan's "reject sequence regressions within a boot" rule would quarantine every buffered replay and retransmission, and would make the gateway stateful. What does the gateway accept, what does it reject, and when may it answer a device?

## Decision Outcome

**Stateless validation**, a pure function in `watchtower_gateway.validation`. Checks run in a fixed order, and the first failure is the reason code:

| Order | Reason | Check |
| --- | --- | --- |
| 1 | `MALFORMED` | Not an object; unknown or missing fields; wrong types or enums (validated against `telemetry_event` v1); naive or unparseable `event_time`; negative `seq`; missing `signature` |
| 2 | `BAD_ID` | `vehicle_id`, `device_id`, `boot_id` outside `[A-Za-z0-9._:-]{1,64}` |
| 3 | `UNKNOWN_DEVICE` | No key registered for `device_id` |
| 4 | `BAD_SIGNATURE` | HMAC-SHA256 mismatch, compared in constant time |
| 5 | `EVENT_ID_MISMATCH` | `event_id` is not `uuid5(device_id, boot_id, seq)` |
| 6 | `OUT_OF_RANGE` | Probe or ambient outside -80..80 °C; position outside Nigeria ±1°; speed outside 0..160 km/h |
| 7 | `FUTURE_EVENT` / `STALE_EVENT` | `event_time` more than 5 min ahead of, or more than 7 days behind, ingest time |

Sequence reuse (the same `(device, boot, seq)` carrying a different reading) is detected by the processor, which keeps per-boot state. The gateway never sees enough history to judge it.

**Signature.** The device sends a `signature` field: hex HMAC-SHA256 over the reading's canonical JSON (every other field, keys sorted, no whitespace, UTF-8), keyed per device. A field rather than a header, because one batch carries readings from many boots, each signed independently. `watchtower_gateway.validation.sign` is the reference implementation. The development key registry is a JSON file mounted read-only; production keys come from a secrets store, and per-device mTLS remains Tier 2.

**Acknowledgement gates the answer.** Accepted readings are stamped with `ingest_time` from an injectable clock and produced as `input_record` (kind `TELEMETRY`, `record_id` = `event_id`) to `wt.input.v1`, keyed by `vehicle_id` through the pinned producer factory (ADR-0017). Rejections go to `telemetry.quarantine.v1`. The response waits for broker acknowledgement of every record:

- **202** when every record landed, with per-item results.
- **503 + `Retry-After`** if any record was not acknowledged, or if the broker or registry is unreachable. The device resends the whole batch; `event_id` makes the repeats harmless downstream.

`message.timeout.ms` bounds every message, so a request never leaves records silently queued behind its answer.

**Quarantine records are JSON**, not Avro: a quarantined reading failed the contract, so its original item travels base64-encoded next to the reason code, detail and ingest time.

### Consequences

- Good: the gateway scales horizontally with no shared state, and every rule is unit-testable as a matrix.
- Good: no silent loss. A 202 means durable in the broker, and anything else asks the device to retry.
- Bad: one slow broker acknowledgement delays the whole batch's answer. Batches are serialised per process to keep acknowledgement per request unambiguous, so throughput per process is bounded. It is measured in the week-10 benchmark.
- Bad: a partially delivered batch is resent in full, creating duplicates that downstream deduplication must absorb (by design, ADR-0002).
- The simulator must sign its readings before it can feed the Compose gateway.

### Confirmation

- `tests/unit/gateway`: the rule matrix; Hypothesis properties (any valid reading is accepted; any single corruption is quarantined with its reason); HTTP behaviour; a real producer with no broker answering 503.
- `tests/integration/test_gateway_pipeline.py`: a mixed batch through Redpanda and the Schema Registry.
