"""Road and operations: how trucks actually move along Nigerian corridors.

- **Road features**, placed deterministically per route so every truck meets the same ones:
  police checkpoints (frequent), toll gates, a weighbridge, fuel stations, and stretches of
  poor road with potholes that cap speed and make it erratic.
- **Congestion** in the big cities at rush hours, heaviest in Lagos, lighter at weekends.
- **Drivers** (cautious, normal, aggressive) set the pace, its variability and the rate of
  harsh braking.
- **Policy**: a mandated rest after a stretch of continuous driving, and optionally no night
  driving, so the truck parks (engine off, reefer on genset) overnight.
- **Fuel**: trucks refuel below a threshold at the next station, sometimes stuck in a diesel
  queue for hours.
- **Multi-drop deliveries** unload pallets at their km posts, so cargo mass (and weight)
  goes down. v1's cargo weight only ever went up.

Densities, durations and probabilities are illustrative, not survey data.
"""

import math
import random
from dataclasses import dataclass, field
from typing import Any

from watchtower_simulator.clock import rng as make_rng
from watchtower_simulator.clock import to_datetime
from watchtower_simulator.environment import local_hour
from watchtower_simulator.routes import Route

BIG_CITIES = {
    "Lagos Mainland": 1.0,
    "Ikeja": 1.0,
    "Ibadan": 0.7,
    "Abuja": 0.8,
    "Port Harcourt": 0.8,
    "Enugu": 0.6,
    "Benin City": 0.6,
    "Owerri": 0.5,
}
CITY_RADIUS_KM = 15.0


@dataclass(frozen=True)
class DriverProfile:
    name: str
    speed_factor: float
    speed_sd: float  # relative spread of chosen speeds
    harsh_brakes_per_100km: float


DRIVERS = {
    d.name: d
    for d in (
        DriverProfile("cautious", 0.88, 0.05, 0.15),
        DriverProfile("normal", 1.0, 0.08, 0.6),
        DriverProfile("aggressive", 1.12, 0.12, 2.5),
    )
}


@dataclass(frozen=True)
class FleetPolicy:
    no_night_driving: bool = False
    night_start_h: float = 19.0  # local time
    night_end_h: float = 6.0
    max_drive_h: float = 4.0  # continuous driving before a mandated rest
    rest_min: float = 30.0
    refuel_below_pct: float = 25.0

    @classmethod
    def parse(cls, doc: dict[str, Any] | None) -> "FleetPolicy":
        return cls(**(doc or {}))


@dataclass(frozen=True)
class RoadFeature:
    kind: str  # checkpoint, toll_gate, weighbridge, fuel_station, poor_road
    km: float
    to_km: float = 0.0  # end of a poor_road stretch


def road_features(route: Route, seed: int = 0) -> list[RoadFeature]:
    """Deterministic per route (and seed), shared by every truck on it."""
    r = make_rng(seed, "roads", route.route_id)
    length = route.length_km
    out: list[RoadFeature] = []
    km = r.uniform(8, 30)
    while km < length - 5:
        out.append(RoadFeature("checkpoint", km))
        km += r.uniform(20, 55)
    out += [RoadFeature("toll_gate", length * f) for f in (0.12, 0.58)]
    out.append(RoadFeature("weighbridge", length * 0.4))
    km = r.uniform(15, 40)
    while km < length - 5:
        out.append(RoadFeature("fuel_station", km))
        km += r.uniform(25, 60)
    km = r.uniform(30, 80)
    while km < length - 10:
        stretch = r.uniform(1.0, 6.0)
        if route.segment(km).road_class == "highway":
            out.append(RoadFeature("poor_road", km, km + stretch))
        km += stretch + r.uniform(20, 90)
    return sorted(out, key=lambda f: (f.km, f.kind))


def congestion_factor(route: Route, km: float, t_ms: int) -> float:
    """Speed multiplier from city traffic: 1.0 is free-flowing."""
    hour = local_hour(t_ms)
    weekday = to_datetime(t_ms).weekday() < 5
    severity = max(
        (w for name, w in BIG_CITIES.items() for t in route.towns
         if t.name == name and abs(t.km - km) < CITY_RADIUS_KM),
        default=0.0,
    )  # fmt: skip
    if severity == 0.0:
        return 1.0
    rush = (7 <= hour < 10) or (16 <= hour < 20)
    load = (0.65 if rush else 0.25) * severity * (1.0 if weekday else 0.5)
    return max(0.2, 1.0 - load)


@dataclass(frozen=True)
class StopRequest:
    stop_type: str
    duration_s: float
    reason: str


@dataclass
class Drop:
    km: float
    pallets: int
    done: bool = False


@dataclass
class Operations:
    driver: DriverProfile
    policy: FleetPolicy
    features: list[RoadFeature]
    rng: random.Random
    drops: list[Drop] = field(default_factory=lambda: [])
    next_feature: int = 0
    drive_s: float = 0.0  # continuous driving since the last rest
    harsh_brakes: int = 0

    def skip_behind(self, km: float) -> None:
        while self.next_feature < len(self.features) and self.features[self.next_feature].km < km:
            self.next_feature += 1

    def road_condition(self, km: float) -> tuple[float, float] | None:
        """(speed cap km/h, extra relative spread) on poor road, else None."""
        for f in self.features:
            if f.kind == "poor_road" and f.km <= km < f.to_km:
                return 35.0, 0.3
        return None

    def target_speed(self, base_kmh: float, route: Route, km: float, t_ms: int) -> float:
        d = self.driver
        speed = base_kmh * d.speed_factor * (1 + self.rng.gauss(0.0, d.speed_sd))
        speed *= congestion_factor(route, km, t_ms)
        rough = self.road_condition(km)
        if rough is not None:
            cap, spread = rough
            speed = min(speed, cap * (1 + self.rng.gauss(0.0, spread)))
        return max(5.0, speed)

    def harsh_brake(self, distance_km: float) -> bool:
        p = 1 - math.exp(-self.driver.harsh_brakes_per_100km * distance_km / 100)
        if self.rng.random() < p:
            self.harsh_brakes += 1
            return True
        return False

    def night(self, t_ms: int) -> bool:
        h = local_hour(t_ms)
        return h >= self.policy.night_start_h or h < self.policy.night_end_h

    def seconds_until_morning(self, t_ms: int) -> float:
        h = local_hour(t_ms)
        return ((self.policy.night_end_h - h) % 24) * 3600

    def next_stop(self, prev_km: float, km: float, t_ms: int, fuel_pct: float, moving: bool,
                  dt_s: float) -> StopRequest | None:  # fmt: skip
        """Decide whether the truck stops now; called once per step while driving."""
        if moving:
            self.drive_s += dt_s
        if self.policy.no_night_driving and self.night(t_ms):
            self.drive_s = 0.0
            return StopRequest("rest", self.seconds_until_morning(t_ms), "night_parking")
        if self.drive_s >= self.policy.max_drive_h * 3600:
            self.drive_s = 0.0
            return StopRequest(
                "rest", self.rng.uniform(0.9, 1.4) * self.policy.rest_min * 60, "mandated_rest"
            )
        for drop in self.drops:
            if not drop.done and prev_km < drop.km <= km:
                drop.done = True
                return StopRequest(
                    "delivery_drop",
                    60 * self.rng.lognormvariate(math.log(25), 0.4),
                    f"drop:{drop.pallets}",
                )
        while self.next_feature < len(self.features) and self.features[self.next_feature].km <= km:
            f = self.features[self.next_feature]
            self.next_feature += 1
            if f.km <= prev_km:
                continue
            request = self.feature_stop(f, fuel_pct)
            if request is not None:
                return request
        return None

    def feature_stop(self, f: RoadFeature, fuel_pct: float) -> StopRequest | None:
        r = self.rng
        match f.kind:
            case "checkpoint" if r.random() < 0.55:
                return StopRequest(
                    "checkpoint", 60 * r.lognormvariate(math.log(3), 0.7), "police_checkpoint"
                )
            case "toll_gate":
                return StopRequest("toll_gate", r.uniform(40, 200), "toll")
            case "weighbridge" if r.random() < 0.3:
                return StopRequest(
                    "weighbridge", 60 * r.lognormvariate(math.log(10), 0.5), "weighbridge"
                )
            case "fuel_station" if fuel_pct < self.policy.refuel_below_pct:
                queue = 60 * r.uniform(30, 180) if r.random() < 0.15 else 0.0  # diesel queue
                return StopRequest(
                    "fuel", 60 * r.uniform(10, 20) + queue, "refuel" + ("_queue" if queue else "")
                )
            case _:
                return None
