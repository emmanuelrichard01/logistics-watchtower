---
status: accepted
date: 2026-10-05
---

# 0001: Mission, scope tiers and non-goals

## Context and Problem Statement

v1 was a telemetry-and-alerts demo. Its README made claims the code could not back (see `docs/audit/v1-baseline.md`), and it had no tests. v2 is a solo 12-week rebuild (5 Oct to 27 Dec 2026). With one builder, the main risk is scope: a team's roadmap attempted alone.

What is v2 for, what must it ship, and what must it explicitly not attempt?

## Decision Drivers

- One person, 12 weeks, laptop-scale infrastructure.
- Every claim must be checkable by a reviewer, not asserted.
- Correctness under failure (duplicates, late data, crashes) matters more than feature count.

## Considered Options

1. **Full platform.** Build everything in the plan, including Flink, Iceberg, Kubernetes, multi-tenancy and ML.
2. **Focused core with tiers.** Commit to a must-ship core and a should-have tier, and keep the stretch path on paper.

## Decision Outcome

Chosen option: **2, focused core with tiers**, because option 1 can't be finished by one person in 12 weeks, and an unfinished platform proves less than a finished core.

**Mission.** Tell an operator which shipment is at risk, how long remains before the cargo is compromised, how confident the system is, what evidence supports that, and what to do next. Design for Nigerian conditions: 30-40 °C ambient, long dead zones, and sensors that lie.

**Tiers.**

| Tier | Commitment | Contents |
| --- | --- | --- |
| 0: Core | Must ship | Event contracts, idempotent processing, DLQ and replay; Postgres domain model and durable alerts; event-time processing with late data; sensor-trust layer; time-to-breach and MKT; edge buffering simulation; Parquet archive; failure-injection tests; benchmark report |
| 1: Should-have | Ship if on schedule | dbt Silver/Gold with backfill demo; OpenTelemetry tracing; SLO dashboard; JWT auth with roles; operator workflow; loss-weighted prioritisation |
| 2: Stretch | Documented, not built | Flink, Iceberg, multi-tenancy, Kubernetes and Helm, LLM summaries, ML anomaly models, per-device mTLS |

**Admission test.** A component enters the build only if it answers a failure mode named in the plan, can be tested automatically, and the simpler alternative can be shown in two sentences to be insufficient.

**Cut rule.** If a phase runs more than 30% over its time box, drop the lowest-value Tier 1 item in that phase and record the cut in a new ADR. Tests, replay and the benchmark are never cut.

**Honest-claims rule.** Every number in the README comes from a benchmark committed to the repo. Anything unmeasured is labelled a target.

**Non-goals.** Not a transport management system (no dispatch, billing or driver apps). No real hardware. No ML before a measured deterministic baseline. No claim of exactly-once delivery. No second stream engine, Kubernetes or multi-tenancy in the core build.

### Consequences

- Good: a finishable scope with a defined fallback at every phase.
- Good: reviewers can verify each success criterion against an artifact.
- Bad: some impressive-sounding items (Flink, Kubernetes) will exist only as documented upgrade paths.

### Confirmation

Gate 6 (end of week 12): every success criterion in plan section 1 links to an artifact a reviewer can open.

## Open Question

Three gates depend on Tier 1 items that the cut rule allows to be dropped:

- Gate 3 requires "auth tests pass" (JWT auth is Tier 1).
- Gate 4 requires "backfill demo works", and success criterion 6 requires the dbt models (dbt is Tier 1).
- Gate 5 requires an operator completing an incident in Playwright (operator workflow is Tier 1).

Either promote these items to Tier 0, or rewrite the gates so a Tier 1 cut doesn't fail the phase. Resolve this before Gate 3 (end of week 5) and record the outcome as an amendment or a superseding ADR.

## More Information

Source: `plan/Logistics Watchtower 2.0 Rebuild Plan.md`, sections 1, 2 and 20.
