"""Security incidents: a hijack that leaves the corridor.

A hijacked truck turns off the route and is driven along a side road (dead-reckoned from the
turn-off point on a fixed bearing), so its position drifts away from the corridor while its
km post along the route freezes. It then stops for a long time somewhere that isn't a depot,
the cargo door is opened (at night, typically), and the thieves may cut the tracker's power.
That is the signature a theft detector has to recognise: route deviation, an unexplained
stop, door open away from any depot, and a tracker going silent rather than buffering.

Distances and timings come from the scenario; nothing here is calibrated to real cases.
"""

import math
from dataclasses import dataclass

KM_PER_DEG_LAT = 111.32


@dataclass
class Detour:
    start_ms: int
    origin_lat: float
    origin_lon: float
    bearing_deg: float
    target_km: float  # how far off-route the truck is driven
    stop_s: float  # the long unexplained stop at the end
    door_open_s: float  # door opened during that stop
    tracker_off_after_s: float | None  # tracker power cut this long after the turn-off
    travelled_km: float = 0.0
    stopped_at_ms: int | None = None

    def position(self) -> tuple[float, float, float]:
        """(lat, lon, heading) after ``travelled_km`` along the side road."""
        b = math.radians(self.bearing_deg)
        lat = self.origin_lat + self.travelled_km * math.cos(b) / KM_PER_DEG_LAT
        lon = self.origin_lon + self.travelled_km * math.sin(b) / (
            KM_PER_DEG_LAT * math.cos(math.radians(self.origin_lat))
        )
        return lat, lon, self.bearing_deg

    def arrived(self) -> bool:
        return self.travelled_km >= self.target_km

    def tracker_off(self, now_ms: int) -> bool:
        if self.tracker_off_after_s is None:
            return False
        return (now_ms - self.start_ms) / 1000 >= self.tracker_off_after_s
