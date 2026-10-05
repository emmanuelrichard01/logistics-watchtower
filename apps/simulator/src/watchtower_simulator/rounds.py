"""Urban delivery rounds: the planned schedule for a van working through its stops, and the
log of what actually happened at each one.

The plan is what a dispatcher would publish: leave the hub at the dispatch time, drive at a
planning speed, spend the median dwell at each customer. Each stop also has a delivery
window from its customer. Arriving after the window closes is late, whatever the plan said.
The planning speed (22 km/h door to door in a Nigerian city) is illustrative.
"""

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

from watchtower_simulator.clock import iso, to_datetime, to_ms
from watchtower_simulator.fleet import CUSTOMER_STOPS
from watchtower_simulator.routes import Route

PLANNING_SPEED_KMH = 22.0
WAT = timedelta(hours=1)


@dataclass(frozen=True)
class PlannedStop:
    stop_id: str
    name: str
    stop_type: str
    km: float
    planned_arrival_ms: int
    window_start_ms: int | None
    window_end_ms: int | None


@dataclass
class DeliveryRound:
    stops: list[PlannedStop]
    next_index: int = 0
    log: list[dict[str, Any]] = field(default_factory=lambda: [])
    door_ajar_s: dict[str, float] = field(default_factory=lambda: {})  # stop_id -> seconds

    def next_stop(self) -> PlannedStop | None:
        return self.stops[self.next_index] if self.next_index < len(self.stops) else None


def window_ms(window: str | None, day_ms: int) -> tuple[int | None, int | None]:
    """'07:00-09:30' local (WAT) on the local day containing ``day_ms``, as UTC ms."""
    if not window:
        return None, None
    local_day = (to_datetime(day_ms) + WAT).date()
    start_s, end_s = window.split("-")

    def at(hhmm: str) -> int:
        h, m = (int(x) for x in hhmm.split(":"))
        return to_ms(
            datetime(local_day.year, local_day.month, local_day.day, h, m, tzinfo=UTC) - WAT
        )

    return at(start_s), at(end_s)


def plan_round(route: Route, start_km: float, dispatch_ms: int) -> DeliveryRound:
    stops: list[PlannedStop] = []
    t = float(dispatch_ms)
    km = start_km
    for town in route.towns:
        if town.km <= start_km or town.stop_type in (None, "hub"):
            continue
        t += (town.km - km) / PLANNING_SPEED_KMH * 3_600_000
        km = town.km
        start, end = window_ms(town.window, dispatch_ms)
        stops.append(
            PlannedStop(
                town.stop_id or town.name, town.name, town.stop_type, town.km, int(t), start, end
            )
        )
        t += CUSTOMER_STOPS[town.stop_type].dwell_median_min * 60_000
    return DeliveryRound(stops)


def log_entry(vehicle_id: str, route_id: str, stop: PlannedStop, arrival_ms: int, dwell_s: float,
              door_s: float, delivered: list[dict[str, Any]]) -> dict[str, Any]:  # fmt: skip
    late_min = (
        0.0 if stop.window_end_ms is None else max(0.0, (arrival_ms - stop.window_end_ms) / 60_000)
    )
    return {
        "vehicle_id": vehicle_id,
        "route_id": route_id,
        "stop_id": stop.stop_id,
        "name": stop.name,
        "type": stop.stop_type,
        "km_along": round(stop.km, 3),
        "window_start": iso(stop.window_start_ms) if stop.window_start_ms is not None else None,
        "window_end": iso(stop.window_end_ms) if stop.window_end_ms is not None else None,
        "planned_arrival": iso(stop.planned_arrival_ms),
        "actual_arrival": iso(arrival_ms),
        "departure": iso(arrival_ms + int(dwell_s * 1000)),
        "delay_vs_plan_min": round((arrival_ms - stop.planned_arrival_ms) / 60_000, 1),
        "on_time": late_min == 0.0,
        "late_min": round(late_min, 1),
        "door_open_s": round(door_s),
        "delivered": delivered,
    }
