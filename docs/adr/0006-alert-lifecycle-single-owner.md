---
status: accepted
date: 2026-10-05
---

# 0006: The processor is the single owner of the alert lifecycle

## Context and Problem Statement

The plan had two writers. The processor emitted OPEN, UPDATE, ESCALATE and AUTO_CLEARED. Operators wrote ACK, ASSIGN and RESOLVE to Postgres, published through the outbox. Version guards can't reconcile two independent counters:

- The processor never learns of a RESOLVE.
- Escalation ("unacknowledged after 3 minutes") needs acknowledgement state inside the processor.
- The `alerts_one_live` index would reject a valid OPEN and send it to the dead-letter queue.

(Review finding 2.)

## Decision Outcome

- **The processor owns every state transition.** Operator commands are written to Postgres `interventions`, which remains the idempotent system of record for the *command*. The outbox relays each command into the input log as an `OPERATOR_COMMAND` record (ADR-0005). The processor applies it, emits the transition, and so learns about it in order.
- **The `alerts` table is a pure projection,** guarded by a per-dedup-key `version` that the processor maintains.
- **Alert ID** = `uuid5(ns, f"{dedup_key}/{opening_record_id}")`, deterministic under replay (ADR-0015).
- **Dedup key** = `{vehicle}:{shipment or -}:{type}`, so two shipments with different limits on one truck never merge.
- **The API returns 202 for commands.** The UI updates optimistically, and the confirmed state arrives after projection through Postgres `LISTEN/NOTIFY`, which also removes the race between the topic and the projection (review finding 16).

### Consequences

- Good: one state machine, replayable, with escalation that knows about acknowledgements.
- Bad: an acknowledgement takes a round trip through the log, roughly 100-500 ms to confirm. The optimistic UI hides this, and undo stays local until confirmation.
