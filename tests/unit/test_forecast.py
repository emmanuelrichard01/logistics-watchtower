"""Forecast and exposure: golden values from the model's own closed form, plus the
properties plan section 15 requires."""

import math

import pytest
from hypothesis import assume, given
from hypothesis import strategies as st
from watchtower_domain.forecast import exposure, mean_kinetic_temperature, time_to_breach

EQ = 32.0  # ambient equilibrium when cooling has failed


def curve(t0_c: float, tau: float, minutes: range) -> list[tuple[float, float]]:
    """Noise-free samples of T(t) = EQ + (T0 - EQ) e^(-t/tau)."""
    return [(float(t), EQ + (t0_c - EQ) * math.exp(-t / tau)) for t in minutes]


def test_recovers_the_exact_crossing_time_of_a_clean_curve() -> None:
    samples = curve(t0_c=4.0, tau=180.0, minutes=range(0, 21))
    t_now, c_now = samples[-1]
    # Closed form for when the curve crosses 8 °C, measured from now.
    expected = -180.0 * math.log((EQ - 8.0) / (EQ - 4.0)) - t_now
    f = time_to_breach(samples, limit_c=8.0, equilibrium_c=EQ)
    assert f is not None
    assert f.p50_min == pytest.approx(expected, rel=1e-9)
    assert f.tau_min == pytest.approx(180.0, rel=1e-9)
    assert f.p10_min == pytest.approx(f.p50_min) == pytest.approx(f.p90_min)  # no scatter
    assert c_now < 8.0


def test_noise_widens_the_range_around_the_estimate() -> None:
    clean = curve(4.0, 180.0, range(0, 16))  # stays below the 8 °C limit
    noisy = [(t, c + (0.15 if i % 2 else -0.15)) for i, (t, c) in enumerate(clean)]
    f = time_to_breach(noisy, 8.0, EQ)
    assert f is not None
    assert f.p10_min < f.p50_min < f.p90_min
    assert f.r_squared < 1.0


@pytest.mark.parametrize(
    "samples",
    [
        [(float(t), 4.0) for t in range(20)],  # flat: not warming
        [(float(t), 6.0 - 0.05 * t) for t in range(20)],  # cooling
        [(0.0, 4.0), (1.0, 4.2)],  # too few points
    ],
)
def test_no_forecast_without_a_warming_trend(samples: list[tuple[float, float]]) -> None:
    assert time_to_breach(samples, 8.0, EQ) is None


def test_no_forecast_when_equilibrium_is_below_the_limit() -> None:
    assert time_to_breach(curve(4.0, 180.0, range(20)), limit_c=40.0, equilibrium_c=EQ) is None


@given(
    t0=st.floats(min_value=-25, max_value=6),
    tau=st.floats(min_value=30, max_value=600),
    later=st.integers(min_value=1, max_value=30),
)
def test_time_to_breach_never_increases_as_cargo_warms(t0: float, tau: float, later: int) -> None:
    # Plan section 15: moving further toward the limit never buys more time.
    limit = 8.0
    early = time_to_breach(curve(t0, tau, range(0, 20)), limit, EQ)
    late = time_to_breach(curve(t0, tau, range(later, 20 + later)), limit, EQ)
    if early is None or late is None:
        assume(False)
        return
    assert late.p50_min <= early.p50_min + 1e-6


def test_mkt_of_a_constant_temperature_is_that_temperature() -> None:
    assert mean_kinetic_temperature([5.0] * 10) == pytest.approx(5.0, abs=1e-9)


def test_mkt_golden_value() -> None:
    # Two equal halves at 2 °C and 25 °C, computed by hand from the definition:
    # k1 = exp(-10000/275.15), k2 = exp(-10000/298.15); MKT = 10000/-ln((k1+k2)/2) - 273.15
    k1 = math.exp(-10_000 / 275.15)
    k2 = math.exp(-10_000 / 298.15)
    expected = 10_000 / -math.log((k1 + k2) / 2) - 273.15
    assert mean_kinetic_temperature([2.0, 25.0]) == pytest.approx(expected, rel=1e-12)
    # Independently confirmed with 50-digit Decimal exp/ln: 19.4659466054827...
    assert expected == pytest.approx(19.4659466, abs=1e-6)


@given(st.lists(st.floats(min_value=-30, max_value=40), min_size=2, max_size=50))
def test_mkt_weights_warm_periods_above_the_arithmetic_mean(temps: list[float]) -> None:
    assert mean_kinetic_temperature(temps) >= sum(temps) / len(temps) - 1e-9


def test_exposure_counts_minutes_and_area_above_the_limit() -> None:
    e = exposure([7.0, 8.5, None, 10.0, 7.9], limit_c=8.0)
    assert e.excursion_minutes == 2
    assert e.degree_minutes == pytest.approx(0.5 + 2.0)


@given(
    st.lists(st.one_of(st.none(), st.floats(min_value=-30, max_value=40)), max_size=60),
    st.floats(min_value=-30, max_value=40),
)
def test_exposure_never_decreases_as_data_arrives(
    minutes: list[float | None], extra: float
) -> None:
    before = exposure(minutes, 8.0)
    after = exposure([*minutes, extra], 8.0)
    assert after.excursion_minutes >= before.excursion_minutes
    assert after.degree_minutes >= before.degree_minutes
