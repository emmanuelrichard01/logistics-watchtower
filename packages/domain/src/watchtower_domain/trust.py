"""Sensor trust: tell a failing sensor from a failing truck (plan section 9).

Each probe is classified OK, SUSPECT or FAULTY from its recent minute buckets,
and the cargo temperature is fused with a confidence. Policy: a FAULTY cargo
probe makes the cargo temperature *uncertain*; it is never silently replaced by
an air probe, and never reported as all-clear.
"""

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from enum import StrEnum
from itertools import pairwise

from watchtower_domain.buckets import SCALE, ProbeStats


class ProbeStatus(StrEnum):
    OK = "OK"
    SUSPECT = "SUSPECT"
    FAULTY = "FAULTY"


@dataclass(frozen=True, slots=True)
class TrustRules:
    """Thresholds, versioned with the rule set (plan section 9). Illustrative defaults."""

    window_min: int = 15
    stuck_min: int = 10  # identical readings this long, while another probe moves
    moving_range_c: float = 0.8  # what "another probe moves" means over the window
    max_cargo_rate_c_per_min: float = 1.5  # faster than cargo thermal mass allows
    dropout_share: float = 0.5  # missing in this share of window minutes
    plausible_c: tuple[float, float] = (-40.0, 50.0)
    drift_c: float = 0.75  # cargo-minus-return-air shift between window halves


DEFAULT_RULES = TrustRules()


@dataclass(frozen=True, slots=True)
class ProbeVerdict:
    status: ProbeStatus
    reasons: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class CargoReading:
    """Fused cargo temperature. `value_c` is None when the cargo probe is untrustworthy."""

    value_c: float | None
    confidence: float
    verdicts: Mapping[str, ProbeVerdict] = field(default_factory=lambda: {})
    reasons: tuple[str, ...] = ()


# One minute of one probe; None means no reading that minute.
Series = Sequence[ProbeStats | None]


def _range(series: Series) -> float:
    present = [s for s in series if s is not None]
    if not present:
        return 0.0
    return (max(s.high for s in present) - min(s.low for s in present)) / SCALE


def _is_stuck(series: Series, minutes: int) -> bool:
    tail = list(series[-minutes:])
    if len(tail) < minutes or any(s is None for s in tail):
        return False
    values = {(s.low, s.high) for s in tail if s is not None}
    # Real probes always carry noise; a stuck one repeats one exact value.
    return len(values) == 1 and next(iter(values))[0] == next(iter(values))[1]


def _max_rate(series: Series) -> float:
    means = [s.mean for s in series if s is not None]
    return max((abs(b - a) for a, b in pairwise(means)), default=0.0)


def classify(
    probes: Mapping[str, Series], rules: TrustRules | None = None
) -> dict[str, ProbeVerdict]:
    """Classify every probe from its last `window_min` minute buckets, oldest first."""
    rules = rules or DEFAULT_RULES
    windows = {name: list(series[-rules.window_min :]) for name, series in probes.items()}
    moving = {name for name, s in windows.items() if _range(s) >= rules.moving_range_c}
    verdicts: dict[str, ProbeVerdict] = {}
    for name in sorted(windows):
        series = windows[name]
        faulty: list[str] = []
        suspect: list[str] = []
        present = [s for s in series if s is not None]
        lo, hi = rules.plausible_c
        if any(s.low / SCALE < lo or s.high / SCALE > hi for s in present):
            faulty.append(f"{name}: value outside {lo:g}..{hi:g} °C")
        if (moving - {name}) and _is_stuck(series, rules.stuck_min):
            faulty.append(f"{name}: flat for {rules.stuck_min} min while other probes move")
        if series and (len(series) - len(present)) / len(series) >= rules.dropout_share:
            suspect.append(f"{name}: missing in {len(series) - len(present)} of {len(series)} min")
        if name == "cargo" and _max_rate(series) > rules.max_cargo_rate_c_per_min:
            suspect.append("cargo: changes faster than its thermal mass allows")
        status = (
            ProbeStatus.FAULTY if faulty else ProbeStatus.SUSPECT if suspect else ProbeStatus.OK
        )
        verdicts[name] = ProbeVerdict(status, tuple(faulty + suspect))

    drift = _drift(windows.get("cargo", []), windows.get("return_air", []))
    cargo = verdicts.get("cargo")
    if cargo and cargo.status is ProbeStatus.OK and abs(drift) >= rules.drift_c:
        reason = f"cargo: drifting {drift:+.2f} °C against return air"
        verdicts["cargo"] = ProbeVerdict(ProbeStatus.SUSPECT, (reason,))
    return verdicts


def _drift(cargo: Series, ret: Series) -> float:
    """Change in (cargo - return air) between the first and second half of the window."""
    pairs = [(c.mean, r.mean) for c, r in zip(cargo, ret, strict=False) if c and r]
    if len(pairs) < 6:
        return 0.0
    half = len(pairs) // 2

    def mean_diff(xs: list[tuple[float, float]]) -> float:
        return sum(c - r for c, r in xs) / len(xs)

    return mean_diff(pairs[half:]) - mean_diff(pairs[:half])


def fuse_cargo(probes: Mapping[str, Series], rules: TrustRules | None = None) -> CargoReading:
    """Cargo temperature with a confidence, never substituting an air probe."""
    rules = rules or DEFAULT_RULES
    verdicts = classify(probes, rules)
    cargo = verdicts.get("cargo")
    latest = next((s for s in reversed(list(probes.get("cargo", []))) if s is not None), None)
    if cargo is None or latest is None:
        return CargoReading(None, 0.0, verdicts, ("cargo: no reading",))
    if cargo.status is ProbeStatus.FAULTY:
        return CargoReading(None, 0.1, verdicts, ("Cargo temperature uncertain", *cargo.reasons))
    confidence = 0.95 if cargo.status is ProbeStatus.OK else 0.6
    return CargoReading(latest.mean, confidence, verdicts, cargo.reasons)
