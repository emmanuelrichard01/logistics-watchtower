"""Idempotent, order-independent per-vehicle state built from event-time minute buckets
(plan section 9). Late, replayed and duplicated readings give the same result as
in-order delivery, which is what makes replays reproducible."""

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime

from watchtower_domain.seq_ranges import SeqRanges

# Probe values are accumulated as integer hundredths. Float addition depends on
# order, so float sums would make a replay differ from the live run in the last bits.
SCALE = 100


@dataclass(frozen=True, slots=True)
class Reading:
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


@dataclass(frozen=True, slots=True)
class VehicleState:
    seen: Mapping[str, SeqRanges] = field(default_factory=lambda: {})
    buckets: Mapping[datetime, Mapping[str, ProbeStats]] = field(default_factory=lambda: {})


def apply(state: VehicleState, reading: Reading) -> VehicleState:
    """Fold one reading into the state. A (boot_id, seq) already seen is a no-op."""
    if reading.event_time.tzinfo is None:
        raise ValueError("event_time must be timezone-aware")
    ranges = state.seen.get(reading.boot_id, SeqRanges())
    if ranges.contains(reading.seq):
        return state

    minute = reading.event_time.replace(second=0, microsecond=0)
    stats = dict(state.buckets.get(minute, {}))
    for probe, value in reading.probes.items():
        if value is None:
            continue
        scaled = round(value * SCALE)
        previous = stats.get(probe)
        stats[probe] = ProbeStats.of(scaled) if previous is None else previous.add(scaled)

    return VehicleState(
        seen={**state.seen, reading.boot_id: ranges.add(reading.seq)},
        buckets={**state.buckets, minute: stats},
    )
