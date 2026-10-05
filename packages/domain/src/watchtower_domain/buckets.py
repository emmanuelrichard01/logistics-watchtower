"""Idempotent, order-independent per-vehicle state built from event-time minute
buckets (plan section 9, ADR-0016).

`evaluate(view, reading)` is pure: it reads state through a `StateView` and
returns a `Delta` describing only what changed, so a stream engine can persist
one key per bucket instead of re-serialising the whole vehicle state. Late,
replayed and duplicated readings give the same result as in-order delivery.
"""

import math
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from datetime import datetime
from decimal import ROUND_HALF_UP, Decimal
from typing import Protocol

from watchtower_domain.seq_ranges import SeqRanges

# Probe values are accumulated as integer hundredths. Float addition depends on
# order, so float sums would make a replay differ from the live run (ADR-0015).
SCALE = 100

#: Buckets older than the vehicle's newest event minute minus this are evicted.
#: 48 h pending the owner's answer on the longest trip and dead zone (ADR-0016).
HOT_WINDOW_MINUTES = 48 * 60

SeqKey = tuple[str, str]  # (device_id, boot_id)


_HUNDREDTH = Decimal("0.01")


def quantise(value: float) -> int:
    """Round to hundredths, half away from zero (ADR-0015).

    Rounds the float's shortest decimal form, so 0.285 becomes 0.29 as written,
    not 0.28 from its binary value (0.2849999...). Never Python's banker's round.
    """
    return int(Decimal(str(value)).quantize(_HUNDREDTH, rounding=ROUND_HALF_UP) * SCALE)


def epoch_minute(t: datetime) -> int:
    if t.tzinfo is None:
        raise ValueError("event_time must be timezone-aware")
    return math.floor(t.timestamp() // 60)


@dataclass(frozen=True, slots=True)
class Reading:
    device_id: str
    boot_id: str
    seq: int
    event_time: datetime
    probes: Mapping[str, float | None]  # None is a probe dropout, not a value


@dataclass(frozen=True, slots=True)
class ProbeStats:
    count: int
    total: int
    low: int
    high: int

    @classmethod
    def of(cls, value: int) -> "ProbeStats":
        return cls(1, value, value, value)

    def add(self, value: int) -> "ProbeStats":
        return ProbeStats(
            self.count + 1, self.total + value, min(self.low, value), max(self.high, value)
        )

    @property
    def mean(self) -> float:
        return self.total / self.count / SCALE


Bucket = Mapping[str, ProbeStats]


class StateView(Protocol):
    """Read access to one vehicle's state, however the shell stores it."""

    def seq_ranges(self, key: SeqKey) -> SeqRanges: ...

    def bucket(self, minute: int) -> Bucket: ...

    def progress(self) -> int | None:
        """Newest event minute seen for this vehicle, or None."""
        ...


@dataclass(frozen=True, slots=True)
class Delta:
    """Everything one reading changes. Empty when the reading is a duplicate."""

    seq_key: SeqKey | None = None
    seq_ranges: SeqRanges | None = None
    buckets: Mapping[int, Bucket] = field(default_factory=lambda: {})
    progress: int | None = None

    @property
    def duplicate(self) -> bool:
        return self.seq_key is None


def evaluate(view: StateView, reading: Reading) -> Delta:
    """Fold one reading into a vehicle's state, returning only what changed."""
    minute = epoch_minute(reading.event_time)
    key: SeqKey = (reading.device_id, reading.boot_id)
    ranges = view.seq_ranges(key)
    if ranges.contains(reading.seq):
        return Delta()

    stats = dict(view.bucket(minute))
    for probe in sorted(reading.probes):  # sorted: iteration order never reaches outputs
        value = reading.probes[probe]
        if value is None:
            continue
        q = quantise(value)
        previous = stats.get(probe)
        stats[probe] = ProbeStats.of(q) if previous is None else previous.add(q)

    current = view.progress()
    return Delta(
        seq_key=key,
        seq_ranges=ranges.add(reading.seq),
        buckets={minute: stats},
        progress=minute if current is None else max(current, minute),
    )


def evictable(
    minutes: Iterable[int], progress: int, hot_window: int = HOT_WINDOW_MINUTES
) -> list[int]:
    """Bucket minutes that fall out of the hot window. Driven by event time only."""
    return sorted(m for m in minutes if m < progress - hot_window)


@dataclass(slots=True)
class InMemoryState:
    """Reference `StateView` for tests, the edge agent and replay tooling."""

    ranges: dict[SeqKey, SeqRanges] = field(default_factory=lambda: {})
    buckets: dict[int, Bucket] = field(default_factory=lambda: {})
    newest: int | None = None

    def seq_ranges(self, key: SeqKey) -> SeqRanges:
        return self.ranges.get(key, SeqRanges())

    def bucket(self, minute: int) -> Bucket:
        return self.buckets.get(minute, {})

    def progress(self) -> int | None:
        return self.newest

    def commit(self, delta: Delta) -> None:
        if delta.seq_key is None or delta.seq_ranges is None:
            return
        self.ranges[delta.seq_key] = delta.seq_ranges
        self.buckets.update(delta.buckets)
        self.newest = delta.progress
        if self.newest is not None:
            for m in evictable(self.buckets, self.newest):
                del self.buckets[m]

    def apply(self, reading: Reading) -> "InMemoryState":
        self.commit(evaluate(self, reading))
        return self

    def snapshot(self) -> tuple[dict[SeqKey, SeqRanges], dict[int, dict[str, ProbeStats]]]:
        """Comparable value form, for equality checks in tests."""
        return dict(self.ranges), {m: dict(b) for m, b in self.buckets.items()}
