"""Properties the plan requires of per-vehicle state (sections 9 and 15)."""

import random
from datetime import UTC, datetime, timedelta
from functools import reduce

import pytest
from hypothesis import given
from hypothesis import strategies as st
from watchtower_domain.buckets import Reading, VehicleState, apply

T0 = datetime(2026, 10, 5, 7, 0, tzinfo=UTC)

probe_values = st.one_of(st.none(), st.floats(min_value=-40, max_value=40, allow_nan=False))
readings = st.builds(
    Reading,
    boot_id=st.sampled_from(["b-1", "b-2"]),
    seq=st.integers(min_value=0, max_value=300),
    event_time=st.integers(min_value=0, max_value=3 * 3600).map(
        lambda s: T0 + timedelta(seconds=s)
    ),
    probes=st.fixed_dictionaries({"cargo": probe_values, "return_air": probe_values}),
)


def fold(items: list[Reading]) -> VehicleState:
    return reduce(apply, items, VehicleState())


@given(readings)
def test_applying_a_reading_twice_equals_applying_it_once(reading: Reading) -> None:
    once = apply(VehicleState(), reading)
    assert apply(once, reading) == once


@given(
    st.lists(readings, unique_by=lambda r: (r.boot_id, r.seq), max_size=60),
    st.randoms(use_true_random=False),
)
def test_arrival_order_and_duplicates_do_not_change_the_state(
    items: list[Reading], rnd: random.Random
) -> None:
    # Models a duplicate storm plus late and replayed delivery.
    arrived = items + rnd.sample(items, k=len(items) // 3)
    rnd.shuffle(arrived)
    assert fold(arrived) == fold(items)


@given(st.lists(readings, unique_by=lambda r: (r.boot_id, r.seq), max_size=60), st.data())
def test_processing_in_batches_equals_processing_whole(
    items: list[Reading], data: st.DataObject
) -> None:
    cut = data.draw(st.integers(min_value=0, max_value=len(items)))
    assert reduce(apply, items[cut:], fold(items[:cut])) == fold(items)


def test_minute_stats_and_dropouts() -> None:
    state = fold(
        [
            Reading("b-1", 1, T0 + timedelta(seconds=5), {"cargo": -19.1}),
            Reading("b-1", 2, T0 + timedelta(seconds=35), {"cargo": -18.5}),
            Reading("b-1", 3, T0 + timedelta(seconds=50), {"cargo": None}),
        ]
    )
    stats = state.buckets[T0]["cargo"]
    assert (stats.count, stats.low, stats.high) == (2, -1910, -1850)
    assert stats.mean == pytest.approx(-18.8)


def test_naive_event_time_is_rejected() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        apply(VehicleState(), Reading("b-1", 1, datetime(2026, 10, 5, 7, 0), {}))
