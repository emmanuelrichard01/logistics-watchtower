"""Virtual time and seeded randomness. The simulator never reads the wall clock and never
touches the global ``random`` state, so a seed fully determines every run."""

import random
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from functools import lru_cache

EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


def to_ms(moment: datetime) -> int:
    if moment.tzinfo is None:
        raise ValueError("simulation times must be timezone-aware")
    return (moment - EPOCH) // timedelta(milliseconds=1)


@lru_cache(maxsize=4096)  # every vehicle in a tick asks for the same instant
def to_datetime(ms: int) -> datetime:
    return EPOCH + timedelta(milliseconds=ms)


def iso(ms: int) -> str:
    """ISO-8601 UTC with millisecond precision, e.g. 2026-10-05T07:12:03.250Z."""
    return to_datetime(ms).strftime("%Y-%m-%dT%H:%M:%S.") + f"{ms % 1000:03d}Z"


def rng(seed: int, *stream: str) -> random.Random:
    """An independent, reproducible stream. String seeds hash with SHA-512, so the same
    (seed, stream) always yields the same sequence across processes and platforms."""
    return random.Random("/".join((str(seed), *stream)))


@dataclass
class VirtualClock:
    now_ms: int
    step_ms: int

    def tick(self) -> int:
        self.now_ms += self.step_ms
        return self.now_ms
