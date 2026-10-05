"""The vehicle evaluator: one vehicle's ordered input log in, decisions out.

This is the processor's core (plan section 9 stages A-E) as a pure function:

    handle(head, buckets, record, rules) -> (new_head, delta, outputs)

`buckets` is the bulky per-minute state behind a `StateView` (one key per
bucket, ADR-0016); `head` is the small remainder (assignment, alert states,
last-seen times, last conditions). The stream engine, the edge agent and the
replay tool all call this same function, so behaviour is identical everywhere.

Input records mirror the single ordered input log (ADR-0005). The domain keeps
its own minimal types; the processor shell maps contract records onto them.
"""

from collections.abc import Mapping
from dataclasses import dataclass, field, replace

from watchtower_domain.alerts import (
    DEFAULT_RULES,
    IDLE,
    Action,
    AlertEvent,
    AlertRule,
    AlertState,
    AlertType,
    Condition,
    Severity,
    command,
    dedup_key,
    step,
)
from watchtower_domain.buckets import Delta, ProbeStats, Reading, StateView, evaluate
from watchtower_domain.forecast import time_to_breach
from watchtower_domain.risk import Aspect, Assessment, assess, forecast_alert_due
from watchtower_domain.trust import ProbeStatus, fuse_cargo

PROBES = ("cargo", "return_air", "supply_air")


@dataclass(frozen=True, slots=True)
class CargoProfile:
    name: str
    min_c: float
    max_c: float
    value_ngn: int


@dataclass(frozen=True, slots=True)
class Assignment:
    shipment_id: str
    profile: CargoProfile
    eta_s: int  # planned arrival, epoch seconds


@dataclass(frozen=True, slots=True)
class Telemetry:
    record_id: str
    reading: Reading
    ingest_s: int
    speed_kmh: float = 0.0
    door_open: bool = False
    compressor_fault: bool = False
    defrost: bool = False
    ambient_c: float = 32.0


@dataclass(frozen=True, slots=True)
class Tick:
    record_id: str
    now_s: int


@dataclass(frozen=True, slots=True)
class OperatorCommand:
    record_id: str
    at_s: int
    alert_type: AlertType
    action: Action


@dataclass(frozen=True, slots=True)
class AssignmentChanged:
    record_id: str
    at_s: int
    assignment: Assignment | None


InputRecord = Telemetry | Tick | OperatorCommand | AssignmentChanged


@dataclass(frozen=True, slots=True)
class VehicleRules:
    stale_after_s: int = 300  # TELEMETRY_GAP when nothing is ingested for this long
    forecast_window_min: int = 20
    trust_window_min: int = 15
    alerts: Mapping[AlertType, AlertRule] = field(default_factory=lambda: dict(DEFAULT_RULES))


DEFAULT_VEHICLE_RULES = VehicleRules()


@dataclass(frozen=True, slots=True)
class Head:
    vehicle_id: str
    assignment: Assignment | None = None
    alerts: Mapping[str, AlertState] = field(default_factory=lambda: {})
    conditions: Mapping[str, Condition] = field(default_factory=lambda: {})
    last_ingest_s: int | None = None
    last_event_s: int | None = None


@dataclass(frozen=True, slots=True)
class Outputs:
    assessment: Assessment | None = None
    events: tuple[AlertEvent, ...] = ()
    duplicate: bool = False


class _Overlay:
    """A StateView with one delta applied on top, without copying the store."""

    def __init__(self, base: StateView, delta: Delta) -> None:
        self.base = base
        self.delta = delta

    def seq_ranges(self, key: tuple[str, str]):
        if self.delta.seq_key == key and self.delta.seq_ranges is not None:
            return self.delta.seq_ranges
        return self.base.seq_ranges(key)

    def bucket(self, minute: int) -> Mapping[str, ProbeStats]:
        return self.delta.buckets.get(minute) or self.base.bucket(minute)

    def progress(self) -> int | None:
        return self.delta.progress if self.delta.progress is not None else self.base.progress()


def _series(view: StateView, probe: str, end: int, minutes: int) -> list[ProbeStats | None]:
    return [view.bucket(m).get(probe) for m in range(end - minutes + 1, end + 1)]


def _key(head: Head, alert_type: AlertType) -> str:
    shipment = head.assignment.shipment_id if head.assignment else None
    return dedup_key(head.vehicle_id, shipment, alert_type)


def handle(
    head: Head,
    buckets: StateView,
    record: InputRecord,
    rules: VehicleRules = DEFAULT_VEHICLE_RULES,
) -> tuple[Head, Delta | None, Outputs]:
    """Apply one input record. Deterministic: same head, buckets and record, same result."""
    if isinstance(record, AssignmentChanged):
        return replace(head, assignment=record.assignment), None, Outputs()
    if isinstance(record, OperatorCommand):
        key = _key(head, record.alert_type)
        state, events = command(
            head.alerts.get(key, IDLE), key, record.action, record.at_s, record.record_id
        )
        return (
            replace(head, alerts={**head.alerts, key: state}),
            None,
            Outputs(events=tuple(events)),
        )
    if isinstance(record, Tick):
        return _tick(head, record, rules)
    return _telemetry(head, buckets, record, rules)


def _advance(
    head: Head,
    conditions: Mapping[AlertType, Condition],
    now: int,
    record_id: str,
    rules: VehicleRules,
) -> tuple[Head, list[AlertEvent]]:
    """Step every alert type that has a condition, in a fixed order (ADR-0015)."""
    alerts = dict(head.alerts)
    remembered = dict(head.conditions)
    events: list[AlertEvent] = []
    for alert_type in sorted(conditions, key=lambda a: a.value):
        key = _key(head, alert_type)
        cond = conditions[alert_type]
        alerts[key], out = step(
            alerts.get(key, IDLE), key, cond, now, record_id, rules.alerts[alert_type]
        )
        remembered[key] = cond
        events += out
    return replace(head, alerts=alerts, conditions=remembered), events


def _tick(head: Head, tick: Tick, rules: VehicleRules) -> tuple[Head, Delta | None, Outputs]:
    stale = (
        head.last_ingest_s is not None and tick.now_s - head.last_ingest_s >= rules.stale_after_s
    )
    gap = Condition(stale, Severity.LOW, "No telemetry received") if stale else Condition(False)
    # Re-apply every other remembered condition so escalation timers advance in-band.
    conditions: dict[AlertType, Condition] = {AlertType.TELEMETRY_GAP: gap}
    for key, cond in head.conditions.items():
        alert_type = AlertType(key.rsplit(":", 1)[1])
        if alert_type is not AlertType.TELEMETRY_GAP:
            conditions[alert_type] = cond
    head, events = _advance(head, conditions, tick.now_s, tick.record_id, rules)
    return head, None, Outputs(events=tuple(events))


def _telemetry(
    head: Head, buckets: StateView, t: Telemetry, rules: VehicleRules
) -> tuple[Head, Delta | None, Outputs]:
    delta = evaluate(buckets, t.reading)
    if delta.duplicate:
        return head, None, Outputs(duplicate=True)  # idempotent: a replayed reading changes nothing

    view = _Overlay(buckets, delta)
    now_min = view.progress() or 0
    event_s = int(t.reading.event_time.timestamp())
    head = replace(
        head,
        last_ingest_s=max(head.last_ingest_s or t.ingest_s, t.ingest_s),
        last_event_s=max(head.last_event_s or event_s, event_s),
    )

    fused = fuse_cargo({p: _series(view, p, now_min, rules.trust_window_min) for p in PROBES})
    conditions: dict[AlertType, Condition] = {
        AlertType.DOOR_OPEN_MOVING: Condition(
            t.door_open and t.speed_kmh > 5,
            Severity.CRITICAL,
            f"Door open at {t.speed_kmh:.0f} km/h",
        ),
        AlertType.COMPRESSOR_FAULT: Condition(
            t.compressor_fault, Severity.CRITICAL, "Compressor fault code"
        ),
        AlertType.TELEMETRY_GAP: Condition(False),
    }
    cargo_verdict = fused.verdicts.get("cargo")
    faulty = [n for n, v in fused.verdicts.items() if v.status is ProbeStatus.FAULTY]
    conditions[AlertType.SENSOR_FAULT] = Condition(
        bool(faulty),
        Severity.HIGH
        if cargo_verdict and cargo_verdict.status is ProbeStatus.FAULTY
        else Severity.MEDIUM,
        "; ".join(r for n in faulty for r in fused.verdicts[n].reasons),
    )

    assessment: Assessment | None = None
    if head.assignment:
        profile = head.assignment.profile
        samples = [
            (float(m), s.mean)
            for m in range(now_min - rules.forecast_window_min + 1, now_min + 1)
            if (s := view.bucket(m).get("cargo")) is not None
        ]
        trusted = cargo_verdict is not None and cargo_verdict.status is ProbeStatus.OK
        forecast = (
            time_to_breach(samples, profile.max_c, t.ambient_c)
            if trusted and not t.defrost
            else None
        )
        assessment = assess(
            cargo_c=fused.value_c,
            limit_c=profile.max_c,
            confidence=fused.confidence,
            forecast=forecast,
            minutes_to_arrival=max(0.0, (head.assignment.eta_s - event_s) / 60),
            cargo_value_ngn=profile.value_ngn,
            reasons=fused.reasons,
        )
        due = forecast_alert_due(assessment)
        conditions[AlertType.CARGO_TEMP_BREACH] = Condition(
            assessment.aspect is Aspect.DANGER, Severity.CRITICAL, assessment.reasons[0]
        )
        conditions[AlertType.BREACH_FORECAST] = Condition(
            due,
            Severity.CRITICAL if assessment.aspect is Aspect.CAUTION1 else Severity.HIGH,
            assessment.reasons[0],
        )

    head, events = _advance(head, conditions, event_s, t.record_id, rules)
    return head, delta, Outputs(assessment=assessment, events=tuple(events))
