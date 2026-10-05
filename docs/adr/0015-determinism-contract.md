---
status: accepted
date: 2026-10-05
---

# 0015: Determinism contract

## Context and Problem Statement

"Byte-identical replay" (success criterion 3) was undefined. Several things break byte equality without changing meaning:

- Kafka CreateTime and headers;
- the schema-ID prefix;
- `created_at DEFAULT now()`;
- last-bit float differences in exp/ln (MKT, the time-to-breach fit);
- set iteration randomised by `PYTHONHASHSEED`;
- insertion-ordered dict serialisation.

Output IDs derived from (dedup key, transition, event-time minute) also collide when two transitions fall in one minute (review findings 3, 14, 15, 31).

## Decision Outcome

- **What is compared:** the canonical encoding (sorted keys, no whitespace) of output record *values* only. Kafka timestamps, headers, the schema-ID prefix and database `created_at` are excluded.
- **Quantised numbers:** temperatures in integer centi-°C (half-up: `floor(x * 100 + 0.5)`, never Python's banker's `round`), durations in whole seconds, confidence in basis points.
- **Sorted iteration:** every collection that reaches an output is iterated in sorted order.
- **Output IDs:** `uuid5(ns, f"{causing_record_id}/{ordinal}")`. Every output is traceable to the record that caused it, unique, and stable under replay.
- **Scope:** replays start from empty state at the run epoch, on the same container image digest. Matching across platforms is a stretch claim, not a promise. Mid-stream replay needs state snapshots, which are out of scope.

### Confirmation

A CI job replays a fixed input sample under `PYTHONHASHSEED=0` and `=1` and asserts equal canonical hashes.
