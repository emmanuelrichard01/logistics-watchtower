"""Time-to-breach forecast and cumulative exposure metrics (plan section 9).

The baseline model is deliberately transparent: when cooling is lost, cargo
temperature approaches an equilibrium (ambient) exponentially,

    T(t) = T_eq + (T0 - T_eq) * exp(-t / tau)

so ln(T_eq - T(t)) is linear in t. A least-squares fit of that line gives tau,
and inverting the curve gives the time to cross the limit:

    t_breach = -tau * ln((T_lim - T_eq) / (T0 - T_eq))

Residual scatter widens tau into a p10-p90 range; a bare point estimate is never
reported. Learned models only replace this after beating it on the labelled
scenarios (plan section 15).
"""

import math
from collections.abc import Sequence
from dataclasses import dataclass

#: Activation energy over the gas constant for MKT, in kelvin (plan section 9).
DEFAULT_DH_OVER_R = 10_000.0
_Z_P90 = 1.2816  # standard normal quantile for the 10th/90th percentiles
MIN_POINTS = 6
MAX_HORIZON_MIN = 24 * 60.0


@dataclass(frozen=True, slots=True)
class BreachForecast:
    """Minutes until the cargo crosses its limit, as a range."""

    p10_min: float
    p50_min: float
    p90_min: float
    tau_min: float
    r_squared: float


def time_to_breach(
    samples: Sequence[tuple[float, float]], limit_c: float, equilibrium_c: float
) -> BreachForecast | None:
    """Forecast from (minutes, cargo °C) samples, oldest first.

    Returns None when no breach is forecast: too few points, not warming toward
    the limit, the limit unreachable (equilibrium below it), or beyond 24 h.
    Already above the limit is a breach, not a forecast; callers check first.
    """
    if len(samples) < MIN_POINTS or equilibrium_c <= limit_c:
        return None
    t_now, temp_now = samples[-1]
    if temp_now >= limit_c:
        return None
    gaps = [equilibrium_c - c for _, c in samples]
    if min(gaps) <= 0:
        return None  # at or above equilibrium: the model does not apply

    xs = [t - t_now for t, _ in samples]  # minutes relative to now (<= 0)
    ys = [math.log(g) for g in gaps]
    n = len(xs)
    mx = math.fsum(xs) / n
    my = math.fsum(ys) / n
    sxx = math.fsum((x - mx) ** 2 for x in xs)
    if sxx == 0:
        return None
    slope = math.fsum((x - mx) * (y - my) for x, y in zip(xs, ys, strict=True)) / sxx
    if slope >= 0:
        return None  # gap not closing: not warming toward equilibrium

    intercept = my - slope * mx
    residuals = [y - (intercept + slope * x) for x, y in zip(xs, ys, strict=True)]
    ss_res = math.fsum(r * r for r in residuals)
    ss_tot = math.fsum((y - my) ** 2 for y in ys)
    r_squared = 1.0 if ss_tot == 0 else max(0.0, 1.0 - ss_res / ss_tot)
    slope_se = math.sqrt(ss_res / (n - 2) / sxx) if n > 2 else 0.0

    # Crossing time from now: T_eq - T_lim = (T_eq - T_now) * exp(slope * t).
    ratio = math.log((equilibrium_c - limit_c) / (equilibrium_c - temp_now))  # < 0

    def crossing(s: float) -> float:
        return ratio / s if s < 0 else math.inf

    p50 = crossing(slope)
    fast = crossing(slope - _Z_P90 * slope_se)  # steeper decay of the gap: sooner
    slow = crossing(slope + _Z_P90 * slope_se)  # shallower: later (may be infinite)
    if p50 > MAX_HORIZON_MIN:
        return None
    return BreachForecast(
        p10_min=max(0.0, fast),
        p50_min=p50,
        p90_min=min(MAX_HORIZON_MIN, slow),
        tau_min=-1.0 / slope,
        r_squared=r_squared,
    )


def mean_kinetic_temperature(
    temps_c: Sequence[float], dh_over_r: float = DEFAULT_DH_OVER_R
) -> float:
    """Mean kinetic temperature in °C from equally spaced readings.

    T_mkt = (dH/R) / -ln(mean(exp(-dH / (R * T_i)))), with temperatures in kelvin.
    Weights warm periods more heavily than the arithmetic mean, as degradation does.
    """
    if not temps_c:
        raise ValueError("MKT needs at least one reading")
    kelvin = [c + 273.15 for c in temps_c]
    mean_exp = math.fsum(math.exp(-dh_over_r / k) for k in kelvin) / len(kelvin)
    return dh_over_r / -math.log(mean_exp) - 273.15


@dataclass(frozen=True, slots=True)
class Exposure:
    excursion_minutes: int
    degree_minutes: float


def exposure(minute_means_c: Sequence[float | None], limit_c: float) -> Exposure:
    """Minutes above the limit and the area above it (°C·min), one value per minute.

    Missing minutes (None) contribute nothing: a gap is reported separately as a
    data-quality fact, never silently counted as safe or unsafe.
    """
    over = [c - limit_c for c in minute_means_c if c is not None and c > limit_c]
    return Exposure(excursion_minutes=len(over), degree_minutes=math.fsum(over))
