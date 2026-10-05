"""Per-boot sequence tracking: duplicate detection and loss detection (plan section 9)."""

from bisect import bisect_right
from dataclasses import dataclass
from itertools import pairwise


@dataclass(frozen=True, slots=True)
class SeqRanges:
    """Sequence numbers seen for one device boot, as sorted, disjoint, non-adjacent
    closed ranges. Contiguous delivery keeps this at a single range."""

    ranges: tuple[tuple[int, int], ...] = ()

    def contains(self, seq: int) -> bool:
        i = bisect_right(self.ranges, seq, key=lambda r: r[0]) - 1
        return i >= 0 and self.ranges[i][1] >= seq

    def add(self, seq: int) -> "SeqRanges":
        if self.contains(seq):
            return self
        ranges = list(self.ranges)
        i = bisect_right(ranges, seq, key=lambda r: r[0])  # first range starting after seq
        start = end = seq
        if i > 0 and ranges[i - 1][1] == seq - 1:
            i -= 1
            start = ranges.pop(i)[0]
        if i < len(ranges) and ranges[i][0] == seq + 1:
            end = ranges.pop(i)[1]
        ranges.insert(i, (start, end))
        return SeqRanges(tuple(ranges))

    def gaps(self) -> tuple[tuple[int, int], ...]:
        """Missing ranges between the first and last sequence numbers seen."""
        return tuple((a[1] + 1, b[0] - 1) for a, b in pairwise(self.ranges))
