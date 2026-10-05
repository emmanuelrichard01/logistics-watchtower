"""Edge device: per-boot sequence numbers, a bounded ring buffer for outages, throttled
in-order replay when the link returns, and at-least-once duplicate delivery."""

import random
from collections import deque
from dataclasses import dataclass, field
from typing import Any

from watchtower_contracts import event_id

from watchtower_simulator.clock import to_datetime

Reading = dict[str, Any]


@dataclass(frozen=True)
class Delivery:
    """One copy of a reading arriving at the gateway. ``copy`` > 0 marks a retransmission."""

    ingest_ms: int
    reading: Reading
    copy: int = 0


@dataclass
class DeliveryPolicy:
    duplicate_probability: float = 0.0
    max_copies: int = 3
    replay_per_step: int = 1
    buffer_capacity: int = 5000


@dataclass
class Device:
    device_id: str
    boot_date: str  # YYYYMMDD, part of every boot_id
    rng: random.Random
    policy: DeliveryPolicy
    boot_count: int = 1
    seq: int = 0
    buffer: deque[tuple[Reading, bool]] = field(
        default_factory=lambda: deque[tuple[Reading, bool]]()
    )
    dropped: int = 0

    @property
    def boot_id(self) -> str:
        return f"b-{self.boot_date}-{self.boot_count:04d}"

    def reboot(self) -> None:
        self.boot_count += 1
        self.seq = 0

    def stamp(self, reading: Reading, event_ms: int) -> Reading:
        """Give a fresh reading its identity: boot, sequence number and event_id."""
        self.seq += 1
        reading["boot_id"] = self.boot_id
        reading["seq"] = self.seq
        reading["event_id"] = str(event_id(self.device_id, self.boot_id, self.seq))
        reading["event_time"] = to_datetime(event_ms)
        return reading

    def handle(
        self, reading: Reading, now_ms: int, link_up: bool, excursion: bool
    ) -> list[Delivery]:
        """Send live when the link is up (draining the buffer behind it), else buffer."""
        if not link_up:
            self._buffer(reading, excursion)
            return []
        out = self._send(reading, now_ms, buffered=False)
        for _ in range(self.policy.replay_per_step):
            if not self.buffer:
                break
            old, _ = self.buffer.popleft()
            out += self._send(old, now_ms, buffered=True)
        return out

    def drain(self, now_ms: int, link_up: bool) -> list[Delivery]:
        """Replay buffered readings on steps without a fresh sample."""
        out: list[Delivery] = []
        if link_up:
            for _ in range(self.policy.replay_per_step):
                if not self.buffer:
                    break
                old, _ = self.buffer.popleft()
                out += self._send(old, now_ms, buffered=True)
        return out

    def _buffer(self, reading: Reading, excursion: bool) -> None:
        if len(self.buffer) >= self.policy.buffer_capacity:
            # Drop the oldest routine reading; keep excursion evidence if at all possible.
            victim = next((i for i, (_, exc) in enumerate(self.buffer) if not exc), 0)
            del self.buffer[victim]
            self.dropped += 1
        self.buffer.append((reading, excursion))

    def _send(self, reading: Reading, now_ms: int, buffered: bool) -> list[Delivery]:
        reading["link"]["buffered"] = buffered
        arrival = now_ms + self.rng.randint(200, 1500)
        out = [Delivery(arrival, reading)]
        if self.rng.random() < self.policy.duplicate_probability:
            for copy in range(1, self.rng.randint(1, self.policy.max_copies) + 1):
                arrival += self.rng.randint(1000, 5000)
                out.append(Delivery(arrival, reading, copy))
        return out
