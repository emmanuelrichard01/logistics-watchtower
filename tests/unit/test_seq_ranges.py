from functools import reduce
from itertools import pairwise

from hypothesis import given
from hypothesis import strategies as st
from watchtower_domain.seq_ranges import SeqRanges


def build(seqs: list[int]) -> SeqRanges:
    return reduce(SeqRanges.add, seqs, SeqRanges())


def test_contiguous_delivery_is_one_range() -> None:
    assert build([1, 2, 3, 4]).ranges == ((1, 4),)


def test_filling_a_hole_merges_both_neighbours() -> None:
    assert build([1, 2, 5, 6]).ranges == ((1, 2), (5, 6))
    assert build([1, 2, 5, 6, 4, 3]).ranges == ((1, 6),)


def test_gaps_report_missing_sequence_numbers() -> None:
    assert build([1, 2, 5, 9]).gaps() == ((3, 4), (6, 8))
    assert build([7]).gaps() == ()


def test_adding_a_seen_number_returns_the_same_object() -> None:
    ranges = build([1, 2, 3])
    assert ranges.add(2) is ranges


@given(st.lists(st.integers(min_value=0, max_value=200), max_size=100))
def test_ranges_are_canonical_whatever_the_arrival_order(seqs: list[int]) -> None:
    ranges = build(seqs)
    covered = {n for start, end in ranges.ranges for n in range(start, end + 1)}
    assert covered == set(seqs)
    assert all(a[1] + 1 < b[0] for a, b in pairwise(ranges.ranges))
    assert ranges == build(sorted(set(seqs)))
    assert all(ranges.contains(n) == (n in covered) for n in range(-1, 202))
