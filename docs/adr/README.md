# Architecture Decision Records

Decisions are recorded when they're made, in [MADR](https://adr.github.io/madr/) format, numbered as in the rebuild plan's decision log (plan section 22). Gaps in the numbering are decisions the plan schedules for later weeks (for example 0003, the stream-engine spike in week 2).

Where an ADR and the plan text disagree, the ADR wins.

| ADR | Decision | Status |
| --- | --- | --- |
| [0001](0001-mission-scope-and-non-goals.md) | Focused core with scope tiers, a cut rule and an honest-claims rule; explicit non-goals | Accepted; timeline amended by 0010 |
| [0002](0002-event-identity-and-idempotency.md) | `event_id` = UUIDv5 of device, boot and sequence; IDs restricted to `[A-Za-z0-9._:-]{1,64}` | Accepted |
| [0005](0005-single-ordered-input-log.md) | Every processor input (telemetry, ticks, rules, assignments, operator commands) goes through one ordered log, `wt.input.v1` | Accepted |
| [0006](0006-alert-lifecycle-single-owner.md) | The processor is the single owner of alert state; operator commands enter through the input log | Accepted |
| [0008](0008-console-auth-sessions.md) | The console uses server-side session cookies, not JWT; WebSocket tickets come from the session | Accepted |
| [0010](0010-frontend-scope-and-time-box.md) | Programme extended to 14 weeks; the console becomes Tier 0 with full mobile parity | Accepted |
| [0013](0013-dependency-licences.md) | Licence review; MinIO dropped (archived) in favour of SeaweedFS | Accepted with caveats |
| [0014](0014-frontend-stack.md) | Vite SPA for the console; Astro Starlight for docs and the case study; deck.gl over MapLibre | Accepted, amended |
| [0015](0015-determinism-contract.md) | "Byte-identical replay" defined: canonical output values, quantised numbers, deterministic output IDs | Accepted |
| [0016](0016-processor-state-layout.md) | Processor state as one key per (vehicle, minute) bucket; deltas; deterministic eviction | Accepted |
| [0017](0017-keys-and-partitioning.md) | `vehicle_id` keys every topic; murmur2 partitioner pinned in one producer factory | Accepted |
| [0018](0018-realtime-push-protocol.md) | WebSocket frames carry `(epoch, seq)` for gap detection; binary columnar tile frames | Accepted |

Most of 0005-0018 came out of the adversarial [architecture review of 5 Oct 2026](../architecture/review-2026-10-05.md).
