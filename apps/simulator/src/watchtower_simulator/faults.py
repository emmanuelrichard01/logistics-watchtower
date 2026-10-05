"""Sensor and device faults as composable injectors (plan section 8, "device realism toggles").

A fault edits a reading after the true physics has been sampled, during its active window,
so ground truth stays clean and the pipeline sees what a lying device would send. Faults stack
in injection order: a drifting probe can also drop out.

Probe faults name one of ``supply_air``, ``return_air`` or ``cargo_probe``:

- ``offset``: a fixed calibration error.
- ``drift``: an error growing linearly with time.
- ``flatline``: stuck at the value it read when the fault began.
- ``spike``: occasional large single-reading excursions.
- ``dropout``: some readings come back null.
- ``swap``: two probes wired the wrong way round.

Device faults: ``gps_multipath`` (urban-canyon scatter, 2D fix, poor HDOP), ``gps_jump``
(occasional position teleports), ``clock_skew`` (a fast or slow device clock with drift, which
GPS time sync can correct), plus ``reboot`` handled by the device itself.
"""

import math
import random
from dataclasses import dataclass
from typing import Any

PROBES = ("supply_air", "return_air", "cargo_probe")
M_PER_DEG = 111_320.0


@dataclass
class Fault:
    kind: str
    start_ms: int
    end_ms: int | None = None

    def active(self, now_ms: int) -> bool:
        return self.start_ms <= now_ms and (self.end_ms is None or now_ms < self.end_ms)

    @property
    def truth_kind(self) -> str:
        return f"fault_{self.kind}"

    def apply(self, reading: dict[str, Any], now_ms: int, rng: random.Random) -> None:
        raise NotImplementedError


@dataclass
class ProbeFault(Fault):
    probe: str = "cargo_probe"

    def __post_init__(self) -> None:
        if self.probe not in PROBES:
            raise ValueError(f"probe must be one of {PROBES}, got {self.probe!r}")

    @property
    def field(self) -> str:
        return f"{self.probe}_c"

    @property
    def truth_kind(self) -> str:
        return f"fault_{self.kind}_{self.probe}"


@dataclass
class Offset(ProbeFault):
    offset_c: float = 0.0

    def apply(self, reading: dict[str, Any], now_ms: int, rng: random.Random) -> None:
        r = reading["reefer"]
        if r[self.field] is not None:
            r[self.field] = round(r[self.field] + self.offset_c, 2)


@dataclass
class Drift(ProbeFault):
    c_per_hour: float = 0.5

    def apply(self, reading: dict[str, Any], now_ms: int, rng: random.Random) -> None:
        r = reading["reefer"]
        if r[self.field] is not None:
            r[self.field] = round(
                r[self.field] + self.c_per_hour * (now_ms - self.start_ms) / 3_600_000, 2
            )


@dataclass
class Flatline(ProbeFault):
    stuck_at: float | None = None

    def apply(self, reading: dict[str, Any], now_ms: int, rng: random.Random) -> None:
        r = reading["reefer"]
        if self.stuck_at is None:
            self.stuck_at = r[self.field]
        r[self.field] = self.stuck_at


@dataclass
class Spike(ProbeFault):
    probability: float = 0.05
    magnitude_c: float = 25.0

    def apply(self, reading: dict[str, Any], now_ms: int, rng: random.Random) -> None:
        r = reading["reefer"]
        if r[self.field] is not None and rng.random() < self.probability:
            r[self.field] = round(
                r[self.field] + rng.choice((-1, 1)) * self.magnitude_c * rng.uniform(0.6, 1.0), 2
            )


@dataclass
class Dropout(ProbeFault):
    probability: float = 0.5

    def apply(self, reading: dict[str, Any], now_ms: int, rng: random.Random) -> None:
        if rng.random() < self.probability:
            reading["reefer"][self.field] = None


@dataclass
class Swap(ProbeFault):
    other: str = "return_air"

    @property
    def truth_kind(self) -> str:
        return f"fault_swap_{self.probe}_{self.other}"

    def apply(self, reading: dict[str, Any], now_ms: int, rng: random.Random) -> None:
        r = reading["reefer"]
        a, b = self.field, f"{self.other}_c"
        r[a], r[b] = r[b], r[a]


@dataclass
class GpsMultipath(Fault):
    sigma_m: float = 60.0

    def apply(self, reading: dict[str, Any], now_ms: int, rng: random.Random) -> None:
        pos = reading["position"]
        if pos is None:
            return
        lat = pos["lat"]
        pos["lat"] = round(lat + rng.gauss(0.0, self.sigma_m) / M_PER_DEG, 6)
        pos["lon"] = round(
            pos["lon"] + rng.gauss(0.0, self.sigma_m) / (M_PER_DEG * math.cos(math.radians(lat))), 6
        )
        pos["gps_fix"] = "FIX_2D"
        pos["hdop"] = round(rng.uniform(4.0, 12.0), 1)


@dataclass
class GpsJump(Fault):
    probability: float = 0.03
    distance_km: float = 5.0

    def apply(self, reading: dict[str, Any], now_ms: int, rng: random.Random) -> None:
        pos = reading["position"]
        if pos is None or rng.random() >= self.probability:
            return
        bearing = rng.uniform(0, 2 * math.pi)
        d_m = self.distance_km * 1000 * rng.uniform(0.5, 1.5)
        lat = pos["lat"]
        pos["lat"] = round(lat + d_m * math.cos(bearing) / M_PER_DEG, 6)
        pos["lon"] = round(
            pos["lon"] + d_m * math.sin(bearing) / (M_PER_DEG * math.cos(math.radians(lat))), 6
        )


@dataclass
class ClockSkew(Fault):
    """The device clock runs ``offset_s`` fast (negative: slow) plus ``drift_ppm``. With
    ``gps_sync_after_s`` set, a GPS time sync corrects it that long after the fault began."""

    offset_s: float = 90.0
    drift_ppm: float = 0.0
    gps_sync_after_s: float | None = None

    def active(self, now_ms: int) -> bool:
        if not super().active(now_ms):
            return False
        return (
            self.gps_sync_after_s is None or (now_ms - self.start_ms) / 1000 < self.gps_sync_after_s
        )

    def skew_ms(self, now_ms: int) -> int:
        if not self.active(now_ms):
            return 0
        elapsed_s = (now_ms - self.start_ms) / 1000
        return round((self.offset_s + elapsed_s * self.drift_ppm / 1e6) * 1000)

    def apply(self, reading: dict[str, Any], now_ms: int, rng: random.Random) -> None:
        """Skew acts on the device's timestamp (see ``skew_ms``), not on the reading's values."""


FAULTS: dict[str, type[Fault]] = {
    "offset": Offset,
    "drift": Drift,
    "flatline": Flatline,
    "spike": Spike,
    "dropout": Dropout,
    "swap": Swap,
    "gps_multipath": GpsMultipath,
    "gps_jump": GpsJump,
    "clock_skew": ClockSkew,
}


def make_fault(kind: str, start_ms: int, end_ms: int | None, params: dict[str, Any]) -> Fault:
    if kind not in FAULTS:
        raise ValueError(f"unknown fault {kind!r}; choose from {sorted(FAULTS)}")
    return FAULTS[kind](kind=kind, start_ms=start_ms, end_ms=end_ms, **params)
