---
status: accepted
date: 2026-10-05
---

# 0002: Event identity and idempotency

## Context and Problem Statement

The platform is at-least-once end to end. MQTT QoS 1 retransmits, devices replay their buffers after dead zones, consumers reprocess after crashes, and the replay CLI re-reads the archive. Every stage therefore sees the same reading more than once. How do we recognise "the same reading" so that alerts and exposure are never double-counted?

## Decision Drivers

- A duplicate must be recognisable without coordination: by the gateway, the processor, the archiver and the replay tool, independently.
- Loss must be measurable, not only duplication.
- Identity must survive replays from the archive, possibly years later.

## Considered Options

1. **Random UUID (v4 or v7) assigned by the gateway.** A retransmission gets a new ID, so duplicates are invisible.
2. **Content hash of the payload.** Breaks when a resend differs in any field, for example `link.buffered` or `ingest_time`.
3. **Deterministic UUIDv5 of `(device_id, boot_id, seq)`.**

## Decision Outcome

Chosen option: **3**. `event_id = uuid5(EVENT_NAMESPACE, f"{device_id}/{boot_id}/{seq}")`, implemented in `watchtower_contracts.identity`.

- `EVENT_NAMESPACE` is `6d946293-5eb6-55b9-99e9-709dd0dacd32` and never changes. A pinned-value test fails if the function's output ever drifts.
- `device_id` and `boot_id` must match `[A-Za-z0-9._:-]{1,64}`. This excludes the `/` separator, so two different triples can't produce the same name. It also excludes lone surrogates: JSON can carry them, but UTF-8 can't encode them, and property testing found that they crashed `uuid5`. Events that fail validation go to quarantine.
- `seq` is a non-negative integer that restarts at each boot. Per-boot sorted sequence ranges (`watchtower_domain.seq_ranges`) detect duplicates in O(log n), and the gaps between ranges measure loss.

### Consequences

- Good: any component can deduplicate with no shared state, and replays reproduce the original IDs.
- Good: sequence gaps give a direct completeness metric per device.
- Bad: correctness depends on devices never reusing a `(boot_id, seq)` pair for a different reading. A device that resets `seq` without changing `boot_id` would have new readings silently dropped as duplicates. The gateway rejects sequence regressions within a boot (plan section 7), and the `clock_skew_and_reboot` scenario tests it.
- Bad: the ID charset is a contract with the firmware. Changing it later needs a new schema version.

### Confirmation

`tests/unit/test_event_identity.py` (pinned value, determinism, injectivity, rejection) and `tests/unit/test_buckets.py` (applying a reading twice equals once; duplicate storms don't change state).
