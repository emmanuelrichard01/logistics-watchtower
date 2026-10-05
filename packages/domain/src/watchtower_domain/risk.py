"""Risk assessment: from forecast and trust to the decision an operator sees.

The aspect (the console's signal lamps), the confidence and the expected loss
are computed here, once, so the console never re-derives risk (review finding 27).

Expected loss uses the whole forecast range, not just its midpoint: the time to
breach is treated as lognormal through the p10/p50/p90 points, so

    P(breach before arrival) = Phi((ln(arrival) - ln(p50)) / sigma)
    sigma = (ln(p90) - ln(p10)) / (2 * 1.2816)

and expected loss = P x cargo value x expected spoiled fraction.
"""

import math
from dataclasses import dataclass
from enum import StrEnum

from watchtower_domain.forecast import BreachForecast

_Z_P90 = 1.2816


class Aspect(StrEnum):
    DANGER = "danger"  # beyond the limit now
    CAUTION1 = "caution1"  # breach within 15 min
    CAUTION2 = "caution2"  # breach within 45 min
    UNKNOWN = "unknown"  # too little trustworthy data to call
    CLEAR = "clear"


@dataclass(frozen=True, slots=True)
class RiskRules:
    """Versioned with the rule set. Illustrative defaults from plan section 9."""

    caution1_min: float = 15.0
    caution2_min: float = 45.0
    min_confidence: float = 0.4  # below this the aspect is UNKNOWN
    alert_confidence: float = 0.6  # BREACH_FORECAST opens only above this
    spoiled_fraction_breach: float = 0.6
    spoiled_fraction_forecast: float = 0.35


DEFAULT_RISK_RULES = RiskRules()


@dataclass(frozen=True, slots=True)
class Assessment:
    aspect: Aspect
    confidence: float
    p_breach_before_arrival: float
    expected_loss_ngn: int
    forecast: BreachForecast | None
    reasons: tuple[str, ...]


def _phi(z: float) -> float:
    return 0.5 * (1.0 + math.erf(z / math.sqrt(2.0)))


def p_breach_before(forecast: BreachForecast, minutes: float) -> float:
    """Probability the breach happens within `minutes`, from the forecast range."""
    if minutes <= 0:
        return 0.0
    p10 = max(forecast.p10_min, 1e-6)
    p50 = max(forecast.p50_min, 1e-6)
    p90 = max(forecast.p90_min, p50)
    sigma = (math.log(p90) - math.log(p10)) / (2 * _Z_P90)
    if sigma <= 1e-9:
        return 1.0 if minutes >= p50 else 0.0
    return _phi((math.log(minutes) - math.log(p50)) / sigma)


def assess(
    *,
    cargo_c: float | None,
    limit_c: float,
    confidence: float,
    forecast: BreachForecast | None,
    minutes_to_arrival: float,
    cargo_value_ngn: int,
    reasons: tuple[str, ...] = (),
    rules: RiskRules = DEFAULT_RISK_RULES,
) -> Assessment:
    """Combine trust (confidence, cargo reading) and forecast into one assessment."""
    if cargo_c is not None and cargo_c > limit_c:
        loss = round(cargo_value_ngn * rules.spoiled_fraction_breach)
        why = (f"Cargo {cargo_c:.1f} °C is above the {limit_c:g} °C limit", *reasons)
        return Assessment(Aspect.DANGER, confidence, 1.0, loss, None, why)
    if cargo_c is None or confidence < rules.min_confidence:
        why = reasons or ("Cargo temperature uncertain",)
        return Assessment(Aspect.UNKNOWN, confidence, 0.0, 0, forecast, why)
    if forecast is None:
        return Assessment(
            Aspect.CLEAR, confidence, 0.0, 0, None, reasons or ("Holding temperature",)
        )

    p = p_breach_before(forecast, minutes_to_arrival)
    loss = round(p * cargo_value_ngn * rules.spoiled_fraction_forecast)
    if forecast.p10_min < rules.caution1_min:
        aspect = Aspect.CAUTION1
    elif forecast.p10_min < rules.caution2_min:
        aspect = Aspect.CAUTION2
    else:
        aspect = Aspect.CLEAR
    lead = f"Breach in {forecast.p10_min:.0f}-{forecast.p90_min:.0f} min"
    return Assessment(aspect, confidence, p, loss, forecast, (lead, *reasons))


def forecast_alert_due(assessment: Assessment, rules: RiskRules = DEFAULT_RISK_RULES) -> bool:
    """BREACH_FORECAST opens only for caution aspects with enough confidence."""
    return (
        assessment.aspect in (Aspect.CAUTION1, Aspect.CAUTION2)
        and assessment.confidence > rules.alert_confidence
    )
