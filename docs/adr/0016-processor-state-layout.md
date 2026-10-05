---
status: accepted
date: 2026-10-05
---

# 0016: Processor state layout

## Context and Problem Statement

`watchtower_domain.buckets.VehicleState` held every bucket in one frozen value, and `apply` copied the whole map on every event. In a RocksDB store with a changelog, that re-serialises hundreds of kilobytes per vehicle per event. Buckets never evicted, and `seen` grew forever. Dedup was keyed by `boot_id` alone, so two devices sharing a boot ID would drop each other's readings (review findings 4, 5).

## Decision Outcome

- **The domain returns deltas** (changed bucket keys, the updated sequence range, emitted outputs) instead of a whole new state. The shell persists one key per `(vehicle, epoch_minute)` bucket.
- **Bucket keys are integer epoch-minutes.**
- **Sequence ranges are keyed by `(device_id, boot_id)`.** boot_id must be random (64-bit), and ranges are garbage-collected 7 days after a boot was last seen.
- **Eviction is deterministic:** buckets older than the vehicle's maximum event_time minus the hot window are dropped. Eviction never uses the wall clock. The hot window is 48 h, pending the owner's answer on trip and dead-zone durations.
- A property test asserts that the delta form equals the existing fold form, so the current tests stay the oracle.

### Confirmation

Week-2 spike criteria S1 (throughput) and S3 (recovery time) are measured on this layout, and bytes per vehicle are published.
