"""Properties the plan requires of per-vehicle state (sections 9 and 15, ADR-0016)."""

import random
from datetime import UTC, datetime, timedelta

import pytest
from hypothesis import given
from hypothesis import strategies as st
from watchtower_domain.buckets import (
    InMemoryState,
    Reading,
    epoch_minute,
    evaluate,
    evictable,
    quantise,
)

T0 = datetime(2026, 10, 5, 7, 0, tzinfo=UTC)
M0 = epoch_minute(T0)

probe_values = st.one_of(st.none(), st.floats(min_value=-40, max_value=40, allow_nan=False))
readings = st.builds(
    Reading,
    device_id=st.sampled_from(["EDGE-1", "EDGE-2"]),
    boot_id=st.sampled_from(["b-1", "b-2"]),
    seq=st.integers(min_value=0, max_value=300),
    event_time=st.integers(min_value=0, max_value=3 * 3600).map(
        lambda s: T0 + timedelta(seconds=s)
    ),
    probes=st.fixed_dictionaries({"cargo": probe_values, "return_air": probe_values}),
)
unique = st.lists(readings, unique_by=lambda r: (r.device_id, r.boot_id, r.seq), max_size=60)


def fold(items: list[Reading]) -> InMemoryState:
    state = InMemoryState()
    for r in items:
        state.apply(r)
    return state


@given(readings)
def test_applying_a_reading_twice_equals_applying_it_once(reading: Reading) -> None:
    once = fold([reading])
    assert fold([reading, reading]).snapshot() == once.snapshot()
    assert evaluate(once, reading).duplicate


@given(unique, st.randoms(use_true_random=False))
def test_arrival_order_and_duplicates_do_not_change_the_state(
    items: list[Reading], rnd: random.Random
) -> None:
    # Models a duplicate storm plus late and replayed delivery.
    arrived = items + rnd.sample(items, k=len(items) // 3)
    rnd.shuffle(arrived)
    assert fold(arrived).snapshot() == fold(items).snapshot()


@given(unique, st.data())
def test_processing_in_batches_equals_processing_whole(
    items: list[Reading], data: st.DataObject
) -> None:
    cut = data.draw(st.integers(min_value=0, max_value=len(items)))
    first = fold(items[:cut])
    for r in items[cut:]:
        first.apply(r)
    assert first.snapshot() == fold(items).snapshot()


@given(readings)
def test_delta_touches_only_the_readings_own_minute(reading: Reading) -> None:
    delta = evaluate(InMemoryState(), reading)
    assert list(delta.buckets) == [epoch_minute(reading.event_time)]


def test_dedup_is_scoped_to_device_and_boot() -> None:
    # Two devices that happen to share a boot ID must not drop each other's readings.
    a = Reading("EDGE-1", "b-1", 7, T0, {"cargo": -19.0})
    b = Reading("EDGE-2", "b-1", 7, T0, {"cargo": -18.0})
    assert fold([a, b]).buckets[M0]["cargo"].count == 2


def test_minute_stats_and_dropouts() -> None:
    state = fold(
        [
            Reading("EDGE-1", "b-1", 1, T0 + timedelta(seconds=5), {"cargo": -19.1}),
            Reading("EDGE-1", "b-1", 2, T0 + timedelta(seconds=35), {"cargo": -18.5}),
            Reading("EDGE-1", "b-1", 3, T0 + timedelta(seconds=50), {"cargo": None}),
        ]
    )
    stats = state.buckets[M0]["cargo"]
    assert (stats.count, stats.low, stats.high) == (2, -1910, -1850)
    assert stats.mean == pytest.approx(-18.8)


@pytest.mark.parametrize(
    ("value", "expected"),
    # 0.285 is 0.28499999... in binary: arithmetic on the float would give 28.
    [(-19.115, -1912), (0.285, 29), (2.5, 250), (0.125, 13), (-0.005, -1), (0.005, 1)],
)
def test_quantise_rounds_half_away_from_zero_as_written(value: float, expected: int) -> None:
    assert quantise(value) == expected


def test_eviction_follows_event_time_not_the_wall_clock() -> None:
    assert evictable([100, 200, 300], progress=300, hot_window=150) == [100]
    state = InMemoryState()
    old = Reading("EDGE-1", "b-1", 1, T0, {"cargo": -19.0})
    new = Reading("EDGE-1", "b-1", 2, T0 + timedelta(hours=49), {"cargo": -19.0})
    state.apply(old).apply(new)
    assert list(state.buckets) == [epoch_minute(new.event_time)]


def test_naive_event_time_is_rejected() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        evaluate(InMemoryState(), Reading("EDGE-1", "b-1", 1, datetime(2026, 10, 5, 7, 0), {}))
