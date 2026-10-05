"""Ambient air temperature along Nigerian corridors.

Illustrative model, not a climatology: a monthly base and diurnal amplitude, a cosine daily
curve peaking mid-afternoon local time (WAT, UTC+1), and a latitude term so the Middle Belt
runs hotter and swings wider than the coast. The environment layer adds sun, cloud and storms.
"""

import math

from watchtower_simulator.clock import to_datetime

WAT_OFFSET_H = 1
PEAK_HOUR_LOCAL = 15.0

# (base °C, diurnal half-amplitude °C) for the coast at ~6.5° N, by month.
# Dry-season heat peaks around March; the rainy season (Jun-Sep) is cooler and flatter;
# Harmattan (Dec-Feb) widens the daily swing.
MONTHLY = {
    1: (27.0, 6.0),
    2: (28.5, 6.0),
    3: (29.5, 5.0),
    4: (29.0, 4.5),
    5: (28.0, 4.0),
    6: (26.5, 3.0),
    7: (25.5, 2.5),
    8: (25.5, 2.5),
    9: (26.0, 3.0),
    10: (27.0, 3.5),
    11: (27.5, 4.5),
    12: (27.0, 5.5),
}
COAST_LAT = 6.5
BASE_PER_DEG_NORTH = 0.35  # inland heat, illustrative
AMPLITUDE_PER_DEG_NORTH = 0.6  # drier air further north swings wider, illustrative


def ambient_c(t_ms: int, lat: float) -> float:
    local = to_datetime(t_ms)
    hour = (local.hour + WAT_OFFSET_H + local.minute / 60 + local.second / 3600) % 24
    base, amplitude = MONTHLY[local.month]
    north = max(0.0, lat - COAST_LAT)
    base += BASE_PER_DEG_NORTH * north
    amplitude += AMPLITUDE_PER_DEG_NORTH * north
    return base + amplitude * math.cos(2 * math.pi * (hour - PEAK_HOUR_LOCAL) / 24)
