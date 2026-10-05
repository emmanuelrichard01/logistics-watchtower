"""Cellular link as a two-state (good/bad) Markov channel with per-segment transition rates,
overridden by forced outages: named dead zones and scenario outages."""

import random
from dataclasses import dataclass


def per_step(p_per_minute: float, dt_s: float) -> float:
    """Convert a per-minute transition probability to one for a step of ``dt_s`` seconds."""
    return 1.0 - (1.0 - p_per_minute) ** (dt_s / 60.0)


@dataclass
class Channel:
    rng: random.Random
    good: bool = True

    def step(self, p_drop: float, p_recover: float, dt_s: float, forced_down: bool) -> bool:
        draw = self.rng.random()  # always draw, so forcing an outage never shifts the stream
        if self.good:
            self.good = draw >= per_step(p_drop, dt_s)
        else:
            self.good = draw < per_step(p_recover, dt_s)
        return self.good and not forced_down

    def signal_dbm(self, up: bool) -> int | None:
        draw = self.rng.randint(-97, -71)
        return draw if up else None
