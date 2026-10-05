"""Stop types and how long the cargo door stays open at each (illustrative distributions).

Durations are lognormal: most stops are short, a few run long, which is what produces
excursions in practice.
"""

import math
import random
from dataclasses import dataclass


@dataclass(frozen=True)
class StopType:
    name: str
    door_probability: float  # chance the cargo door is opened at all
    door_median_min: float
    door_sigma: float  # lognormal shape
    engine_off: bool  # whether the driver usually switches the engine off
    shore_power: bool = False


STOP_TYPES: dict[str, StopType] = {
    s.name: s
    for s in (
        StopType("depot_loading", 1.0, 25.0, 0.5, engine_off=True, shore_power=True),
        StopType("delivery_drop", 1.0, 9.0, 0.6, engine_off=True),
        StopType("checkpoint", 0.25, 3.0, 0.7, engine_off=False),
        StopType("toll_gate", 0.0, 0.0, 0.0, engine_off=False),
        StopType("weighbridge", 0.05, 2.0, 0.5, engine_off=False),
        StopType("fuel", 0.0, 0.0, 0.0, engine_off=True),
        StopType("rest", 0.0, 0.0, 0.0, engine_off=True),
        StopType("breakdown", 0.1, 5.0, 0.5, engine_off=True),
        StopType("unplanned", 0.0, 0.0, 0.0, engine_off=False),
    )
}


def door_open_seconds(stop: StopType, rng: random.Random) -> float:
    """Sampled door-open time for one stop; 0 when the door stays shut."""
    if stop.door_probability == 0.0 or rng.random() >= stop.door_probability:
        return 0.0
    return 60.0 * stop.door_median_min * math.exp(rng.gauss(0.0, stop.door_sigma))
