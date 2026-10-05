"""The vehicle evaluator end to end on synthetic streams: the processor's core
behaviour, without Kafka (plan section 9; success criterion 4 in miniature)."""

import math
import random
from datetime import UTC, datetime, timedelta

from watchtower_domain.alerts import Action, AlertEvent, AlertType, EventKind
from watchtower_domain.buckets import InMemoryState, Reading
from watchtower_domain.risk import Aspect
from watchtower_domain.vehicle import (
    Assignment,
    AssignmentChanged,
    CargoProfile,
    Head,
    InputRecord,
    OperatorCommand,
    Telemetry,
    Tick,
    handle,
)

T0 = datetime(2026, 10, 5, 7, 0, tzinfo=UTC)
VACCINES = CargoProfile("Vaccines", 2.0, 8.0, 62_000_000)


def run(records: list[InputRecord]):
    head = Head("TRK-104")
    store = InMemoryState()
    events: list[AlertEvent] = []
    assessments = []
    for r in records:
        head, delta, out = handle(head, store, r)
        if delta is not None:
            store.commit(delta)
        events += out.events
        if out.assessment:
            assessments.append((r, out.assessment))
    return head, events, assessments


def compressor_failure(minutes: int, fail_at: int, seed: int = 1) -> list[InputRecord]:
    """Readings every 30 s; cooling fails at `fail_at` min and cargo warms toward 32 °C."""
    rnd = random.Random(seed)
    records: list[InputRecord] = [
        AssignmentChanged(
            "assign-1",
            int(T0.timestamp()),
            Assignment("SHP-1", VACCINES, int(T0.timestamp()) + 6 * 3600),
        )
    ]
    for i in range(minutes * 2):
        t = T0 + timedelta(seconds=30 * i)
        m = i / 2
        cargo = 5.0 if m < fail_at else 32 + (5.0 - 32) * math.exp(-(m - fail_at) / 240)
        air = cargo - 1.0
        reading = Reading(
            "EDGE-0104",
            "b-1",
            i,
            t,
            {
                "cargo": cargo + rnd.gauss(0, 0.03),
                "return_air": air + rnd.gauss(0, 0.1),
                "supply_air": air - 1 + rnd.gauss(0, 0.1),
            },
        )
        records.append(
            Telemetry(
                f"tel-{i}",
                reading,
                int(t.timestamp()) + 2,
                speed_kmh=60,
                compressor_fault=m >= fail_at,
            )
        )
    return records


def first(events: list[AlertEvent], alert_type: AlertType, kind: EventKind) -> AlertEvent | None:
    return next(
        (e for e in events if e.dedup_key.endswith(alert_type.value) and e.kind is kind), None
    )


def test_forecast_warns_before_the_breach_with_a_measurable_lead_time() -> None:
    _, events, _ = run(compressor_failure(minutes=120, fail_at=10))
    forecast = first(events, AlertType.BREACH_FORECAST, EventKind.OPENED)
    breach = first(events, AlertType.CARGO_TEMP_BREACH, EventKind.OPENED)
    assert forecast is not None
    assert breach is not None
    lead_min = (breach.opened_at - forecast.opened_at) / 60
    assert lead_min >= 10  # the point of the product: time to act before cargo is lost


def test_replay_reproduces_identical_events() -> None:
    records = compressor_failure(minutes=60, fail_at=10)
    _, a, _ = run(records)
    _, b, _ = run(records)
    assert a == b


def test_duplicate_storm_changes_nothing() -> None:
    records = compressor_failure(minutes=60, fail_at=10)
    rnd = random.Random(7)
    stormed: list[InputRecord] = []
    for r in records:
        stormed.append(r)
        if isinstance(r, Telemetry) and rnd.random() < 0.3:
            stormed += [r] * rnd.randint(1, 3)  # QoS-1 retransmits arrive right behind
    _, clean_events, _ = run(records)
    _, storm_events, _ = run(stormed)
    assert storm_events == clean_events


def test_healthy_truck_raises_nothing() -> None:
    _, events, assessments = run(compressor_failure(minutes=60, fail_at=10_000))
    assert events == []
    assert {a.aspect for _, a in assessments} == {Aspect.CLEAR}


def test_silence_raises_a_telemetry_gap_on_the_tick() -> None:
    records = compressor_failure(minutes=5, fail_at=10_000)
    last = int((T0 + timedelta(minutes=5)).timestamp())
    _, events, _ = run([*records, Tick("tick-1", last + 60), Tick("tick-2", last + 400)])
    gap = first(events, AlertType.TELEMETRY_GAP, EventKind.OPENED)
    assert gap is not None


def test_operator_acknowledgement_travels_through_the_log() -> None:
    records = compressor_failure(minutes=40, fail_at=10)
    at = int((T0 + timedelta(minutes=40)).timestamp())
    _, events, _ = run(
        [*records, OperatorCommand("cmd-1", at, AlertType.COMPRESSOR_FAULT, Action.ACK)]
    )
    acked = first(events, AlertType.COMPRESSOR_FAULT, EventKind.ACKNOWLEDGED)
    assert acked is not None
    assert acked.caused_by == "cmd-1"
