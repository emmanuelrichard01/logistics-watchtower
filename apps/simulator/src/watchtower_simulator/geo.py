"""Small spherical-geometry helpers. Positions are (lon, lat) in degrees, GeoJSON order."""

import math

EARTH_RADIUS_KM = 6371.0088

LonLat = tuple[float, float]


def haversine_km(a: LonLat, b: LonLat) -> float:
    lon1, lat1, lon2, lat2 = map(math.radians, (a[0], a[1], b[0], b[1]))
    h = (
        math.sin((lat2 - lat1) / 2) ** 2
        + math.cos(lat1) * math.cos(lat2) * math.sin((lon2 - lon1) / 2) ** 2
    )
    return 2 * EARTH_RADIUS_KM * math.asin(math.sqrt(h))


def bearing_deg(a: LonLat, b: LonLat) -> float:
    lon1, lat1, lon2, lat2 = map(math.radians, (a[0], a[1], b[0], b[1]))
    x = math.sin(lon2 - lon1) * math.cos(lat2)
    y = math.cos(lat1) * math.sin(lat2) - math.sin(lat1) * math.cos(lat2) * math.cos(lon2 - lon1)
    return (math.degrees(math.atan2(x, y)) + 360) % 360


def simplify(points: list[LonLat], tolerance_km: float) -> list[LonLat]:
    """Ramer-Douglas-Peucker on a local equirectangular projection (fine at corridor scale)."""
    if len(points) < 3:
        return list(points)
    lat0 = math.radians(sum(p[1] for p in points) / len(points))
    km_per_deg = math.pi * EARTH_RADIUS_KM / 180
    xy = [(p[0] * km_per_deg * math.cos(lat0), p[1] * km_per_deg) for p in points]

    keep = [False] * len(points)
    keep[0] = keep[-1] = True
    stack = [(0, len(points) - 1)]
    while stack:
        first, last = stack.pop()
        (x1, y1), (x2, y2) = xy[first], xy[last]
        dx, dy = x2 - x1, y2 - y1
        norm = math.hypot(dx, dy) or 1e-12
        worst, index = 0.0, -1
        for i in range(first + 1, last):
            d = abs(dy * xy[i][0] - dx * xy[i][1] + x2 * y1 - y2 * x1) / norm
            if d > worst:
                worst, index = d, i
        if worst > tolerance_km:
            keep[index] = True
            stack += [(first, index), (index, last)]
    return [p for p, k in zip(points, keep, strict=True) if k]
