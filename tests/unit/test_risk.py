import pytest
from hypothesis import given
from hypothesis import strategies as st
from watchtower_domain.forecast import BreachForecast
from watchtower_domain.risk import Aspect, assess, forecast_alert_due, p_breach_before


def fc(p10: float, p50: float, p90: float) -> BreachForecast:
    return BreachForecast(p10, p50, p90, tau_min=120.0, r_squared=0.95)


BASE = {"limit_c": 8.0, "minutes_to_arrival": 120.0, "cargo_value_ngn": 62_000_000}


def test_above_limit_is_danger_with_full_probability() -> None:
    a = assess(cargo_c=9.1, confidence=0.9, forecast=None, **BASE)
    assert a.aspect is Aspect.DANGER
    assert a.p_breach_before_arrival == 1.0
    assert a.expected_loss_ngn == round(62_000_000 * 0.6)


def test_uncertain_cargo_is_never_clear() -> None:
    a = assess(cargo_c=None, confidence=0.1, forecast=None, reasons=("Cargo probe faulty",), **BASE)
    assert a.aspect is Aspect.UNKNOWN
    assert a.reasons == ("Cargo probe faulty",)


@pytest.mark.parametrize(
    ("p10", "aspect"), [(10.0, Aspect.CAUTION1), (30.0, Aspect.CAUTION2), (90.0, Aspect.CLEAR)]
)
def test_aspect_follows_the_pessimistic_end_of_the_range(p10: float, aspect: Aspect) -> None:
    a = assess(cargo_c=5.0, confidence=0.9, forecast=fc(p10, p10 * 1.3, p10 * 1.8), **BASE)
    assert a.aspect is aspect


def test_forecast_alert_needs_confidence() -> None:
    sure = assess(cargo_c=5.0, confidence=0.8, forecast=fc(20, 30, 45), **BASE)
    unsure = assess(cargo_c=5.0, confidence=0.5, forecast=fc(20, 30, 45), **BASE)
    assert forecast_alert_due(sure)
    assert not forecast_alert_due(unsure)


def test_probability_matches_the_range_quantiles() -> None:
    f = fc(20.0, 40.0, 80.0)
    assert p_breach_before(f, 40.0) == pytest.approx(0.5)
    assert p_breach_before(f, 20.0) == pytest.approx(0.1, abs=1e-3)
    assert p_breach_before(f, 80.0) == pytest.approx(0.9, abs=1e-3)


@given(
    p10=st.floats(min_value=1, max_value=300),
    spread=st.floats(min_value=1.0, max_value=4.0),
    a=st.floats(min_value=0, max_value=1000),
    b=st.floats(min_value=0, max_value=1000),
)
def test_probability_rises_with_more_time_to_arrival(
    p10: float, spread: float, a: float, b: float
) -> None:
    f = fc(p10, p10 * spread**0.5, p10 * spread)
    lo, hi = sorted((a, b))
    assert 0.0 <= p_breach_before(f, lo) <= p_breach_before(f, hi) <= 1.0
