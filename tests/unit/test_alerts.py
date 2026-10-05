"""Alert state machine: lifecycle, debounce, hysteresis, escalation, determinism."""

from hypothesis import given
from hypothesis import strategies as st
from watchtower_domain.alerts import (
    DEFAULT_RULES,
    IDLE,
    Action,
    AlertEvent,
    AlertState,
    AlertType,
    Condition,
    EventKind,
    Lifecycle,
    Severity,
    command,
    dedup_key,
    step,
)

KEY = dedup_key("TRK-101", "SHP-1", AlertType.CARGO_TEMP_BREACH)
RULE = DEFAULT_RULES[AlertType.CARGO_TEMP_BREACH]  # open 120 s, clear 300 s, escalate 180 s
HOT = Condition(True, Severity.CRITICAL, "cargo 9.1 °C")
COOL = Condition(False)


def run(conditions: list[tuple[int, Condition]], state: AlertState = IDLE):
    events: list[AlertEvent] = []
    for i, (t, cond) in enumerate(conditions):
        state, out = step(state, KEY, cond, t, f"rec-{i}", RULE)
        events += out
    return state, events


def kinds(events: list[AlertEvent]) -> list[EventKind]:
    return [e.kind for e in events]


def test_a_blip_shorter_than_the_debounce_never_opens() -> None:
    _, events = run([(0, HOT), (60, HOT), (90, COOL)])
    assert events == []


def test_opens_after_debounce_dated_at_the_true_start() -> None:
    state, events = run([(0, HOT), (60, HOT), (120, HOT)])
    assert kinds(events) == [EventKind.OPENED]
    assert events[0].opened_at == 0  # when the condition began, not when it was confirmed
    assert state.lifecycle is Lifecycle.OPEN


def test_auto_clears_only_after_staying_clear() -> None:
    state, events = run([(0, HOT), (120, HOT), (200, COOL), (400, COOL), (520, COOL)])
    assert kinds(events) == [EventKind.OPENED, EventKind.AUTO_CLEARED]
    assert state == IDLE


def test_recovery_interrupted_by_a_relapse_dedups_into_the_same_alert() -> None:
    _, events = run([(0, HOT), (120, HOT), (200, COOL), (260, HOT)])
    assert kinds(events) == [EventKind.OPENED, EventKind.UPDATED]
    assert events[0].alert_id == events[1].alert_id
    assert events[1].occurrences == 2


def test_unacknowledged_critical_escalates_exactly_once() -> None:
    _, events = run([(0, HOT), (120, HOT), (240, HOT), (300, HOT), (600, HOT)])
    assert kinds(events).count(EventKind.ESCALATED) == 1
    escalated = next(e for e in events if e.kind is EventKind.ESCALATED)
    assert escalated.at == 300  # 3 min after it was raised (120), not after the condition began (0)


def test_a_recovering_alert_does_not_escalate() -> None:
    _, events = run([(0, HOT), (120, HOT), (200, COOL), (320, COOL)])
    assert EventKind.ESCALATED not in kinds(events)


def test_acknowledging_prevents_escalation() -> None:
    state, _ = run([(0, HOT), (120, HOT)])
    state, acked = command(state, KEY, Action.ACK, 130, "cmd-1")
    _, later = run([(200, HOT), (400, HOT)], state)
    assert kinds(acked) == [EventKind.ACKNOWLEDGED]
    assert EventKind.ESCALATED not in kinds(later)


def test_a_rise_in_severity_is_never_hidden() -> None:
    state, _ = step(
        IDLE,
        KEY,
        Condition(True, Severity.HIGH, "forecast 30 min"),
        0,
        "a",
        DEFAULT_RULES[AlertType.BREACH_FORECAST],
    )
    state, events = step(
        state,
        KEY,
        Condition(True, Severity.CRITICAL, "forecast 10 min"),
        60,
        "b",
        DEFAULT_RULES[AlertType.BREACH_FORECAST],
    )
    assert kinds(events) == [EventKind.UPDATED]
    assert events[0].severity is Severity.CRITICAL


def test_double_acknowledge_is_a_no_op() -> None:
    state, _ = run([(0, HOT), (120, HOT)])
    state, first = command(state, KEY, Action.ACK, 130, "cmd-1")
    state, second = command(state, KEY, Action.ACK, 131, "cmd-2")
    assert len(first) == 1
    assert second == []


def test_resolved_stays_latched_until_the_condition_clears() -> None:
    state, _ = run([(0, HOT), (120, HOT)])
    state, _ = command(state, KEY, Action.RESOLVE, 130, "cmd-1")
    state, events = run([(140, HOT), (300, HOT)], state)
    assert events == []  # still hot, but an operator resolved it: no new alert
    state, _ = run([(400, COOL)], state)
    _, reopened = run([(500, HOT), (620, HOT)], state)
    assert kinds(reopened) == [EventKind.OPENED]


@given(st.lists(st.tuples(st.integers(0, 60), st.booleans()), max_size=60))
def test_replay_is_deterministic_and_never_two_live_alerts(steps: list[tuple[int, bool]]) -> None:
    t = 0
    seq: list[tuple[int, Condition]] = []
    for gap, hot in steps:
        t += gap
        seq.append((t, HOT if hot else COOL))
    _, first = run(seq)
    _, second = run(seq)
    assert first == second  # same input log, same events, same IDs

    live: set[str] = set()
    versions: dict[str, int] = {}
    for e in first:
        alert = str(e.alert_id)
        if e.kind is EventKind.OPENED:
            assert not live  # at most one live alert per dedup key
            live.add(alert)
        if e.kind is EventKind.AUTO_CLEARED:
            live.discard(alert)
        assert e.version > versions.get(alert, 0)  # versions strictly increase
        versions[alert] = e.version
