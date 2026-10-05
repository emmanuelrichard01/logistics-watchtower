---
status: accepted
date: 2026-10-05
---

# 0005: Time, ordering and a single ordered input log

## Context and Problem Statement

Success criterion 3 requires replay to reproduce alerts and risk scores exactly. Order-independent minute buckets make the processor's final *state* independent of arrival order, but its *emitted stream* is not. Debounce, escalation and evidence lists depend on when telemetry, staleness ticks, rule changes, shipment assignments and operator commands arrived relative to each other. In the plan, all but telemetry reached the processor out-of-band, and their order was recorded nowhere (review findings 1, 8, 10, 12, 25).

## Decision Outcome

**Every input that can change processor output is a record in one vehicle-keyed topic, `wt.input.v1`.** The record types are:

- telemetry
- `TICK`
- `RULES_ACTIVATED`
- `ASSIGNMENT_CHANGED` (carrying the cargo-profile snapshot)
- `OPERATOR_COMMAND`

Records that apply fleet-wide, such as rule activations, are written to every partition.

- **Lateness** is computed per record as `ingest_time − event_time`. There is no partition watermark, so one device with a fast clock can't poison 2,000 other trucks.
- **Ticks** are produced every 15 s per vehicle by a ticker with an injectable clock. They are ordinary input records, so a replay sees them in the same place.
- **Bronze archives `wt.input.v1`.** Replay re-produces Bronze rows to the same partitions in `(partition, offset)` order, from the run epoch with empty state.
- No Quix `concat`, windows or watermarks. Buckets are custom keyed state.

### Consequences

- Good: any processor output is a pure function of the input-log prefix, which makes criterion 3 provable.
- Good: one topic for the processor to consume and one for the archiver to archive.
- Bad: the gateway, ticker and outbox relay all produce to the same topic, so they must share one producer factory and partitioner (ADR-0017).
- The claim is reworded: buckets make *final state* order-independent; *outputs* are deterministic given the input log.

### Confirmation

A determinism CI job replays a fixed Bronze sample twice under different `PYTHONHASHSEED`s and diffs the canonical output hashes (ADR-0015). This is week-2 spike criterion S5.
