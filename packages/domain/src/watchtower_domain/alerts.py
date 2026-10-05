"""Alert state machine: the processor is its single owner (ADR-0006).

`step` advances one dedup key given whether its condition holds at event time
`now`; `command` applies an operator action. Both are pure and return the new
state plus the events to emit. Event and alert IDs are derived from the record
that caused them (ADR-0015), so a replay of the same input log reproduces the
same events byte for byte.

Lifecycle: OPEN -> ACKNOWLEDGED -> MITIGATING -> RESOLVED, or OPEN -> AUTO_CLEARED
when the condition recovers on its own. Repeats deduplicate into one alert with
an occurrence count; a rise in severity is always emitted, never hidden.
"""

import uuid
from collections.abc import Mapping
from dataclasses import dataclass, replace
from enum import StrEnum

# Fixed forever, like the event namespace: changing it changes every alert ID.
ALERT_NAMESPACE = uuid.UUID("4f6b2c2e-1a52-5c4e-9d61-2b0f7a5d3c11")


class AlertType(StrEnum):
    CARGO_TEMP_BREACH = "CARGO_TEMP_BREACH"
    BREACH_FORECAST = "BREACH_FORECAST"
    DOOR_OPEN_MOVING = "DOOR_OPEN_MOVING"
    COMPRESSOR_FAULT = "COMPRESSOR_FAULT"
    SENSOR_FAULT = "SENSOR_FAULT"
    TELEMETRY_GAP = "TELEMETRY_GAP"


class Severity(StrEnum):
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


SEVERITY_RANK = {Severity.LOW: 0, Severity.MEDIUM: 1, Severity.HIGH: 2, Severity.CRITICAL: 3}


class Lifecycle(StrEnum):
    OPEN = "OPEN"
    ACKNOWLEDGED = "ACKNOWLEDGED"
    MITIGATING = "MITIGATING"
    RESOLVED = "RESOLVED"
    AUTO_CLEARED = "AUTO_CLEARED"


class EventKind(StrEnum):
    OPENED = "OPENED"
    UPDATED = "UPDATED"
    ESCALATED = "ESCALATED"
    ACKNOWLEDGED = "ACKNOWLEDGED"
    MITIGATING = "MITIGATING"
    RESOLVED = "RESOLVED"
    AUTO_CLEARED = "AUTO_CLEARED"


class Action(StrEnum):
    ACK = "ACK"
    MITIGATE = "MITIGATE"
    RESOLVE = "RESOLVE"


@dataclass(frozen=True, slots=True)
class AlertRule:
    """Timings in event-time seconds (plan section 9 alert table)."""

    open_after_s: int  # debounce: the condition must hold this long
    clear_after_s: int  # hysteresis: it must stay false this long
    escalate_after_s: int | None = None  # unacknowledged CRITICAL escalates once


DEFAULT_RULES: Mapping[AlertType, AlertRule] = {
    AlertType.CARGO_TEMP_BREACH: AlertRule(120, 300, escalate_after_s=180),
    AlertType.BREACH_FORECAST: AlertRule(0, 0, escalate_after_s=180),
    AlertType.DOOR_OPEN_MOVING: AlertRule(30, 60, escalate_after_s=180),
    AlertType.COMPRESSOR_FAULT: AlertRule(0, 300, escalate_after_s=180),
    AlertType.SENSOR_FAULT: AlertRule(0, 600),
    AlertType.TELEMETRY_GAP: AlertRule(0, 0),
}


@dataclass(frozen=True, slots=True)
class Condition:
    """What the rules see for one dedup key at one moment."""

    active: bool
    severity: Severity = Severity.MEDIUM
    summary: str = ""


@dataclass(frozen=True, slots=True)
class AlertState:
    """Everything the machine remembers for one dedup key."""

    pending_since: int | None = None  # condition true, debounce running
    alert_id: uuid.UUID | None = None  # a live (or latched-resolved) alert
    lifecycle: Lifecycle | None = None
    severity: Severity | None = None
    opened_at: int | None = None  # event-time start of the condition (for reporting)
    raised_at: int | None = None  # when the alert became visible (for escalation)
    clear_since: int | None = None  # condition false, hysteresis running
    version: int = 0
    occurrences: int = 0
    escalated: bool = False

    @property
    def live(self) -> bool:
        return self.lifecycle in (Lifecycle.OPEN, Lifecycle.ACKNOWLEDGED, Lifecycle.MITIGATING)


IDLE = AlertState()


@dataclass(frozen=True, slots=True)
class AlertEvent:
    event_id: uuid.UUID
    alert_id: uuid.UUID
    dedup_key: str
    kind: EventKind
    lifecycle: Lifecycle
    severity: Severity
    version: int
    at: int  # event time, epoch seconds
    opened_at: int
    occurrences: int
    summary: str
    caused_by: str  # record_id of the input record that caused this event


def dedup_key(vehicle_id: str, shipment_id: str | None, alert_type: AlertType) -> str:
    return f"{vehicle_id}:{shipment_id or '-'}:{alert_type.value}"


class _Emitter:
    """Deterministic event IDs: uuid5(causing record, dedup key, ordinal)."""

    def __init__(self, key: str, record_id: str) -> None:
        self.key = key
        self.record_id = record_id
        self.events: list[AlertEvent] = []

    def emit(self, state: AlertState, kind: EventKind, at: int, summary: str) -> None:
        assert state.alert_id is not None
        assert state.lifecycle is not None
        assert state.severity is not None
        assert state.opened_at is not None
        name = f"{self.record_id}/{self.key}/{len(self.events)}"
        self.events.append(
            AlertEvent(
                event_id=uuid.uuid5(ALERT_NAMESPACE, name),
                alert_id=state.alert_id,
                dedup_key=self.key,
                kind=kind,
                lifecycle=state.lifecycle,
                severity=state.severity,
                version=state.version,
                at=at,
                opened_at=state.opened_at,
                occurrences=state.occurrences,
                summary=summary,
                caused_by=self.record_id,
            )
        )


def step(
    state: AlertState, key: str, condition: Condition, now: int, record_id: str, rule: AlertRule
) -> tuple[AlertState, list[AlertEvent]]:
    """Advance one dedup key to event time `now` (epoch seconds)."""
    out = _Emitter(key, record_id)

    if condition.active:
        relapsed = state.clear_since is not None  # read before resetting it
        state = replace(state, clear_since=None)
        if state.live:
            state = _refresh(state, condition, now, relapsed, out)
        elif state.lifecycle is Lifecycle.RESOLVED:
            pass  # latched: an operator resolved it; a new alert needs the condition to clear first
        else:
            since = state.pending_since if state.pending_since is not None else now
            state = replace(state, pending_since=since)
            if now - since >= rule.open_after_s:
                state = _open(state, key, condition, since, now, record_id, out)
    else:
        state = replace(state, pending_since=None)
        if state.live:
            since = state.clear_since if state.clear_since is not None else now
            state = replace(state, clear_since=since)
            if now - since >= rule.clear_after_s:
                state = replace(state, lifecycle=Lifecycle.AUTO_CLEARED, version=state.version + 1)
                out.emit(state, EventKind.AUTO_CLEARED, now, "Condition recovered")
                state = IDLE
        elif state.lifecycle is Lifecycle.RESOLVED:
            state = IDLE  # condition cleared after a resolve: ready for a new alert

    # Escalation runs from when the alert became visible (an operator cannot act
    # before that) and never fires while the condition is already recovering.
    if (
        state.lifecycle is Lifecycle.OPEN
        and state.severity is Severity.CRITICAL
        and not state.escalated
        and state.clear_since is None
        and rule.escalate_after_s is not None
        and state.raised_at is not None
        and now - state.raised_at >= rule.escalate_after_s
    ):
        state = replace(state, escalated=True, version=state.version + 1)
        out.emit(state, EventKind.ESCALATED, now, "Critical alert unacknowledged")
    return state, out.events


def _open(
    state: AlertState,
    key: str,
    cond: Condition,
    since: int,
    now: int,
    record_id: str,
    out: _Emitter,
) -> AlertState:
    alert_id = uuid.uuid5(ALERT_NAMESPACE, f"{key}/{record_id}")
    state = AlertState(
        alert_id=alert_id,
        lifecycle=Lifecycle.OPEN,
        severity=cond.severity,
        opened_at=since,  # event-time start of the condition, not when we noticed
        raised_at=now,
        version=1,
        occurrences=1,
    )
    out.emit(state, EventKind.OPENED, since, cond.summary)
    return state


def _refresh(
    state: AlertState, cond: Condition, now: int, was_quiet: bool, out: _Emitter
) -> AlertState:
    assert state.severity is not None
    rose = SEVERITY_RANK[cond.severity] > SEVERITY_RANK[state.severity]
    if not rose and not was_quiet:
        return state
    state = replace(
        state,
        severity=cond.severity if rose else state.severity,
        occurrences=state.occurrences + (1 if was_quiet else 0),
        version=state.version + 1,
    )
    out.emit(state, EventKind.UPDATED, now, cond.summary)
    return state


_TRANSITIONS: Mapping[Action, tuple[set[Lifecycle], Lifecycle, EventKind]] = {
    Action.ACK: ({Lifecycle.OPEN}, Lifecycle.ACKNOWLEDGED, EventKind.ACKNOWLEDGED),
    Action.MITIGATE: (
        {Lifecycle.OPEN, Lifecycle.ACKNOWLEDGED},
        Lifecycle.MITIGATING,
        EventKind.MITIGATING,
    ),
    Action.RESOLVE: (
        {Lifecycle.OPEN, Lifecycle.ACKNOWLEDGED, Lifecycle.MITIGATING},
        Lifecycle.RESOLVED,
        EventKind.RESOLVED,
    ),
}


def command(
    state: AlertState, key: str, action: Action, now: int, record_id: str, note: str = ""
) -> tuple[AlertState, list[AlertEvent]]:
    """Apply an operator command. Invalid or repeated commands are no-ops (idempotent)."""
    allowed, target, kind = _TRANSITIONS[action]
    if state.lifecycle not in allowed:
        return state, []
    out = _Emitter(key, record_id)
    state = replace(state, lifecycle=target, version=state.version + 1)
    out.emit(state, kind, now, note or kind.value.capitalize())
    return state, out.events
