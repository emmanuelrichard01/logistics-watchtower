"""Layer 2: sun, irradiance, solar load, storms, haze and the latitude gradient."""

from datetime import UTC, datetime

import pytest
from watchtower_simulator.ambient import ambient_c
from watchtower_simulator.clock import rng, to_ms
from watchtower_simulator.environment import (
    Weather,
    clear_sky,
    conditions,
    local_hour,
    sun_position,
)

ABUJA = (9.06, 7.49)
LAGOS = (6.45, 3.39)


def ms(*args: int) -> int:
    return to_ms(datetime(*args, tzinfo=UTC))


def test_sun_is_near_zenith_at_equinox_noon_in_abuja() -> None:
    best = max(
        sun_position(ms(2026, 3, 20, h, m), *ABUJA).elevation_deg
        for h in (11, 12)
        for m in range(0, 60, 2)
    )
    assert best == pytest.approx(90 - 9.06, abs=1.0)  # declination ~0 at equinox


def test_morning_sun_is_in_the_east_and_night_is_dark() -> None:
    morning = sun_position(ms(2026, 3, 20, 7, 30), *ABUJA)  # 08:30 WAT
    assert 80 < morning.azimuth_deg < 100
    assert morning.elevation_deg > 15
    assert sun_position(ms(2026, 3, 20, 23, 0), *ABUJA).elevation_deg < -30
    assert clear_sky(-5.0) == (0.0, 0.0)


def test_clear_sky_noon_irradiance_is_plausible() -> None:
    _, ghi = clear_sky(80.0)
    assert 900 < ghi < 1150


def weather(cloud: float = 0.0) -> Weather:
    return Weather(rng(1, "w"), cloud=cloud)


def test_solar_load_falls_with_speed_cloud_and_a_sun_ahead_heading() -> None:
    noon_morning = ms(2026, 3, 20, 7, 30)  # sun in the east
    parked = conditions(
        weather(), noon_morning, *ABUJA, heading_deg=0.0, speed_kmh=0.0, wall_ua_kw_per_k=0.08
    )
    moving = conditions(
        weather(), noon_morning, *ABUJA, heading_deg=0.0, speed_kmh=80.0, wall_ua_kw_per_k=0.08
    )
    sun_ahead = conditions(
        weather(), noon_morning, *ABUJA, heading_deg=90.0, speed_kmh=0.0, wall_ua_kw_per_k=0.08
    )
    overcast = conditions(
        weather(1.0), noon_morning, *ABUJA, heading_deg=0.0, speed_kmh=0.0, wall_ua_kw_per_k=0.08
    )
    night = conditions(
        weather(),
        ms(2026, 3, 20, 22, 0),
        *ABUJA,
        heading_deg=0.0,
        speed_kmh=0.0,
        wall_ua_kw_per_k=0.08,
    )
    assert parked.solar_heat_kw > 0.2
    assert moving.solar_heat_kw < parked.solar_heat_kw / 2  # airflow strips the skin heat
    assert sun_ahead.solar_heat_kw < parked.solar_heat_kw  # sun on the cab, not a long side
    assert overcast.solar_heat_kw < parked.solar_heat_kw / 3
    assert night.solar_heat_kw == 0.0


def storms(month: int, days: int = 30) -> tuple[int, list[float]]:
    w = Weather(rng(9, "storms", str(month)))
    starts: list[float] = []
    was = False
    t0 = ms(2026, month, 1, 0, 0)
    for k in range(days * 24 * 60):  # 1-minute steps
        now = t0 + k * 60_000
        w.update(now, 60.0)
        if w.storming(now) and not was:
            starts.append(local_hour(now))
        was = w.storming(now)
    return len(starts), starts


def test_storms_come_in_the_rainy_season_mostly_in_the_afternoon() -> None:
    july, hours = storms(7)
    january, _ = storms(1)
    assert july >= 15
    assert january == 0
    afternoon = sum(13 <= h <= 19 for h in hours) / len(hours)
    assert afternoon > 0.5


def test_a_storm_cools_the_air_slows_traffic_and_hurts_the_link() -> None:
    w = weather()
    calm = conditions(
        w, ms(2026, 7, 15, 14, 0), *ABUJA, heading_deg=0.0, speed_kmh=60.0, wall_ua_kw_per_k=0.08
    )
    w.storm_until_ms = ms(2026, 7, 15, 16, 0)
    w.storm_cooling_c = 5.0
    w.cloud = 1.0
    stormy = conditions(
        w, ms(2026, 7, 15, 14, 0), *ABUJA, heading_deg=0.0, speed_kmh=60.0, wall_ua_kw_per_k=0.08
    )
    assert stormy.ambient_c < calm.ambient_c - 5.0
    assert stormy.speed_factor < 1.0 < stormy.link_drop_factor
    assert stormy.storm


def test_harmattan_haze_dims_the_sun() -> None:
    # Same sun elevation in January (haze) and March (none), both cloudless.
    jan = conditions(
        weather(),
        ms(2026, 1, 15, 11, 40),
        *ABUJA,
        heading_deg=0.0,
        speed_kmh=0.0,
        wall_ua_kw_per_k=0.08,
    )
    _, clear = clear_sky(jan.sun.elevation_deg)
    assert jan.haze
    assert jan.ghi_w_m2 == pytest.approx(clear * 0.75, rel=0.01)


def test_middle_belt_afternoons_are_hotter_than_the_coast() -> None:
    afternoon = ms(2026, 3, 12, 14, 0)  # 15:00 WAT
    dawn = ms(2026, 3, 12, 5, 0)
    assert ambient_c(afternoon, ABUJA[0]) > ambient_c(afternoon, LAGOS[0]) + 1.0
    swing_abuja = ambient_c(afternoon, ABUJA[0]) - ambient_c(dawn, ABUJA[0])
    swing_lagos = ambient_c(afternoon, LAGOS[0]) - ambient_c(dawn, LAGOS[0])
    assert swing_abuja > swing_lagos
