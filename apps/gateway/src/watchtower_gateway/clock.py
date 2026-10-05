"""Injectable clocks (ADR-0005). The gateway reads time only through one of these, so
tests and virtual-clock scenario runs control ingest_time exactly."""

import time
from typing import Protocol


class Clock(Protocol):
    def now_ms(self) -> int: ...


class SystemClock:
    def now_ms(self) -> int:
        return time.time_ns() // 1_000_000


class FixedClock:
    def __init__(self, now_ms: int) -> None:
        self.t = now_ms

    def now_ms(self) -> int:
        return self.t
