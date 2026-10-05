"""Environment along the corridors: sun, cloud, storms, haze and their effects.

- **Sun position.** NOAA's general solar position equations (accurate to about 0.5° here),
  from latitude, longitude and UTC time.
- **Irradiance.** Clear-sky direct beam from the Meinel air-mass approximation, plus a
  diffuse fraction. Cloud attenuates it with the Kasten-Czeplak form, 1 - 0.75 c^3.4.
  Harmattan dust haze attenuates it further.
- **Solar load on the trailer.** A sol-air approach: sunlight raises the effective outside
  temperature of a surface by alpha*I/h_o. The roof sees global horizontal irradiance; the
  sun-facing long side sees the direct beam according to the angle between the sun's
  azimuth and the truck's heading. Moving air raises h_o, so a truck on the highway carries
  less solar load than one parked.
- **Seasons.** Rainy-season (June-September) storms arrive by a Poisson process peaking in
  the afternoon. They cool the air, darken the sky, slow traffic and make the link drop
  more. Harmattan (December-February) brings dust haze; its cool nights and hot afternoons
  are already in the ambient model's wider diurnal swing.

Every rate and coefficient here is illustrative, not a climatology.
"""

import math
import random
from dataclasses import dataclass
from functools import lru_cache

from watchtower_simulator.ambient import WAT_OFFSET_H, ambient_c
from watchtower_simulator.clock import to_datetime

SOLAR_CONSTANT_W_M2 = 1353.0
ALBEDO_ABSORPTANCE = 0.4  # weathered white trailer skin
ROOF_SHARE = 0.30  # share of the envelope's UA in the roof
SUNNY_SIDE_SHARE = 0.25  # one long side
RAINY_MONTHS = frozenset({6, 7, 8, 9})
HARMATTAN_MONTHS = frozenset({12, 1, 2})


@dataclass(frozen=True)
class Sun:
    elevation_deg: float
    azimuth_deg: float  # clockwise from north


def sun_position(t_ms: int, lat: float, lon: float) -> Sun:
    # The sun moves ~0.25° a minute: a minute and 0.01° of resolution is plenty, and lets
    # nearby trucks in the same tick share the calculation.
    return _sun_position(t_ms // 60_000, round(lat, 2), round(lon, 2))


@lru_cache(maxsize=8192)
def _sun_position(minute: int, lat: float, lon: float) -> Sun:
    t = to_datetime(minute * 60_000)
    hour = t.hour + t.minute / 60 + t.second / 3600
    doy = t.timetuple().tm_yday
    g = 2 * math.pi / 365 * (doy - 1 + (hour - 12) / 24)
    eqtime = 229.18 * (
        0.000075 + 0.001868 * math.cos(g) - 0.032077 * math.sin(g)
        - 0.014615 * math.cos(2 * g) - 0.040849 * math.sin(2 * g)
    )  # fmt: skip
    decl = (
        0.006918 - 0.399912 * math.cos(g) + 0.070257 * math.sin(g)
        - 0.006758 * math.cos(2 * g) + 0.000907 * math.sin(2 * g)
        - 0.002697 * math.cos(3 * g) + 0.00148 * math.sin(3 * g)
    )  # fmt: skip
    true_solar_min = hour * 60 + eqtime + 4 * lon
    ha = math.radians(true_solar_min / 4 - 180)
    phi = math.radians(lat)
    cos_zen = math.sin(phi) * math.sin(decl) + math.cos(phi) * math.cos(decl) * math.cos(ha)
    zen = math.acos(max(-1.0, min(1.0, cos_zen)))
    az = math.degrees(
        math.atan2(math.sin(ha), math.cos(ha) * math.sin(phi) - math.tan(decl) * math.cos(phi))
    )
    return Sun(90 - math.degrees(zen), (az + 180) % 360)


def clear_sky(elevation_deg: float) -> tuple[float, float]:
    """(direct normal, global horizontal) irradiance in W/m² under a clear sky."""
    if elevation_deg <= 0:
        return 0.0, 0.0
    cos_zen = math.sin(math.radians(elevation_deg))
    air_mass = 1 / max(cos_zen, 0.05)
    dni = SOLAR_CONSTANT_W_M2 * 0.7 ** (air_mass**0.678)
    return dni, dni * cos_zen * 1.1  # ~10% diffuse on top of the beam


def local_hour(t_ms: int) -> float:
    t = to_datetime(t_ms)
    return (t.hour + WAT_OFFSET_H + t.minute / 60) % 24


@dataclass
class Weather:
    """One vehicle's local weather, evolving on its own seeded stream."""

    rng: random.Random
    cloud: float = 0.3
    storm_until_ms: int = -1
    storm_cooling_c: float = 0.0
    storm_peak_cooling_c: float = 0.0

    def storming(self, now_ms: int) -> bool:
        return now_ms < self.storm_until_ms

    def update(self, now_ms: int, dt_s: float) -> None:
        month = to_datetime(now_ms).month
        rainy = month in RAINY_MONTHS
        mean_cloud = 0.7 if rainy else 0.15 if month in HARMATTAN_MONTHS else 0.35
        target = 1.0 if self.storming(now_ms) else mean_cloud + self.rng.gauss(0.0, 0.25)
        tau_s = 1800.0
        self.cloud = min(1.0, max(0.0, self.cloud + (target - self.cloud) * min(1.0, dt_s / tau_s)))

        if rainy and not self.storming(now_ms):
            hour = local_hour(now_ms)
            per_hour = 0.03 * (4.0 if 13 <= hour <= 19 else 1.0)  # afternoon convective peak
            if self.rng.random() < 1 - math.exp(-per_hour * dt_s / 3600):
                self.storm_until_ms = now_ms + int(self.rng.uniform(30, 100) * 60_000)
                self.storm_peak_cooling_c = self.rng.uniform(3.0, 7.0)
        # Rain-cooled air arrives quickly and recovers slowly after the storm passes.
        target_cooling = self.storm_peak_cooling_c if self.storming(now_ms) else 0.0
        tau = 600.0 if target_cooling > self.storm_cooling_c else 3600.0
        self.storm_cooling_c += (target_cooling - self.storm_cooling_c) * min(1.0, dt_s / tau)


@dataclass(frozen=True)
class Conditions:
    ambient_c: float
    sun: Sun
    ghi_w_m2: float
    dni_w_m2: float
    cloud: float
    storm: bool
    haze: bool
    solar_heat_kw: float
    speed_factor: float
    link_drop_factor: float


def conditions(
    weather: Weather,
    now_ms: int,
    lat: float,
    lon: float,
    heading_deg: float,
    speed_kmh: float,
    wall_ua_kw_per_k: float,
) -> Conditions:
    month = to_datetime(now_ms).month
    haze = month in HARMATTAN_MONTHS
    storm = weather.storming(now_ms)
    sun = sun_position(now_ms, lat, lon)
    dni, ghi = clear_sky(sun.elevation_deg)
    attenuation = (1 - 0.75 * weather.cloud**3.4) * (0.75 if haze else 1.0)
    dni_beam = dni * attenuation * (1 - weather.cloud)  # beam needs a gap in the cloud
    ghi *= attenuation

    # Cloud trims the afternoon peak; rain-cooled air drops the temperature further.
    base = ambient_c(now_ms, lat)
    daytime = max(0.0, math.sin(math.radians(max(sun.elevation_deg, 0.0))))
    air = base - 2.0 * weather.cloud * daytime - weather.storm_cooling_c

    h_o = 17.0 + 3.9 * min(speed_kmh / 3.6, 25.0)  # W/m²K, still air plus forced convection
    roof_excess = ALBEDO_ABSORPTANCE * ghi / h_o
    incidence = abs(math.sin(math.radians(sun.azimuth_deg - heading_deg)))
    side_excess = (
        ALBEDO_ABSORPTANCE
        * dni_beam
        * math.cos(math.radians(max(sun.elevation_deg, 0.0)))
        * incidence
        / h_o
    )
    solar_kw = wall_ua_kw_per_k * (ROOF_SHARE * roof_excess + SUNNY_SIDE_SHARE * side_excess)

    return Conditions(
        ambient_c=air,
        sun=sun,
        ghi_w_m2=ghi,
        dni_w_m2=dni_beam,
        cloud=weather.cloud,
        storm=storm,
        haze=haze,
        solar_heat_kw=solar_kw,
        speed_factor=0.6 if storm else 1.0,
        link_drop_factor=3.0 if storm else 1.0,
    )
