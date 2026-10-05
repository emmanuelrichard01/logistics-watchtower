"""What a vehicle is and what it carries: vehicle classes, shipments, and how long a drop takes
at each kind of customer. All figures are illustrative.

- **Vehicle classes.** An inter-state 13.6 m reefer trailer, a 3.5 t refrigerated city van, and
  an insulated cargo tricycle with a small unit. Smaller boxes hold far less air and cargo, so
  every door opening hits them harder, and their units have far less capacity to recover.
- **Shipments.** A vehicle carries one or more shipments, each with its own cargo profile,
  mass and receiver (the stop where it is delivered). Each is its own thermal node sharing the
  box air (``thermal.step_multi``).
- **Customer drops.** Dwell and door-open times by customer type: a supermarket's goods-in bay
  is slow, a pharmacy hand-over quick, an open market slow and chaotic.
"""

import math
import random
from dataclasses import dataclass
from typing import Any

from watchtower_simulator.cargo import PROFILES, CargoProfile
from watchtower_simulator.thermal import Load


@dataclass(frozen=True)
class VehicleClass:
    name: str
    thermal: dict[str, float]  # ThermalParams overrides
    speed_bands: dict[str, tuple[float, float]]  # km/h by road class
    tank_l: float
    l_per_km: float
    standby_genset: bool = True  # runs the unit with the engine off
    idle_at_drops: bool = False  # driver keeps the engine running at customer drops
    loading_door_median_min: float = 25.0
    reefer: dict[str, float] | None = None  # ReeferParams overrides: a small coil ices faster


VEHICLE_CLASSES: dict[str, VehicleClass] = {
    c.name: c
    for c in (
        VehicleClass(
            "trailer",
            {},
            {"highway": (65.0, 90.0), "urban": (20.0, 45.0), "slow_corridor": (15.0, 40.0)},
            400.0,
            0.35,
        ),
        VehicleClass(
            "van",
            {
                "air_capacity_kj_per_k": 60.0,
                "wall_ua_kw_per_k": 0.035,
                "door_ua_kw_per_k": 0.6,
                "q_max_kw": 3.0,
                "defrost_kw": 0.3,
            },
            {"highway": (70.0, 95.0), "urban": (20.0, 50.0), "slow_corridor": (15.0, 45.0)},
            80.0,
            0.12,
            standby_genset=False,
            idle_at_drops=True,
            loading_door_median_min=6.0,
            reefer={
                "frost_kg_per_run_h": 0.15,
                "door_frost_kg_per_h": 1.2,
                "ice_loss_per_kg": 0.15,
            },
        ),
        VehicleClass(
            "trike",
            {
                "air_capacity_kj_per_k": 15.0,
                "wall_ua_kw_per_k": 0.012,
                "door_ua_kw_per_k": 0.25,
                "q_max_kw": 0.8,
                "defrost_kw": 0.08,
            },
            {"highway": (35.0, 45.0), "urban": (15.0, 35.0), "slow_corridor": (10.0, 30.0)},
            12.0,
            0.03,
            standby_genset=False,
            idle_at_drops=True,
            loading_door_median_min=3.0,
            reefer={"frost_kg_per_run_h": 0.05, "door_frost_kg_per_h": 0.4, "ice_loss_per_kg": 0.4},
        ),
    )
}


# Air-to-cargo exchange relative to a stretch-wrapped pallet: loose cartons expose far more
# surface per kilo; an insulated shipper (the usual vaccine box) far less. Illustrative.
PACKAGING_EXCHANGE = {"pallet": 1.0, "carton": 4.0, "insulated_box": 0.25}


@dataclass
class Shipment:
    shipment_id: str
    profile: CargoProfile
    pallets: float  # in pallet-equivalents of the profile; vans carry fractions
    cargo_c: float
    receiver: str | None = None  # stop_id it is delivered to; None stays on board
    delivered_ms: int | None = None
    packaging: str = "pallet"

    @property
    def mass_kg(self) -> float:
        return self.pallets * self.profile.kg_per_pallet

    def load(self) -> Load:
        return Load(
            max(self.profile.capacity_kj_per_k(1) * self.pallets, 1.0),
            self.profile.cargo_ua_kw_per_k(1) * self.pallets * PACKAGING_EXCHANGE[self.packaging],
        )

    def in_range(self) -> bool:
        return self.profile.in_range(self.cargo_c)


def parse_shipments(vehicle_id: str, spec: dict[str, Any], default_profile: str, pallets: float,
                    initial_c: float | None) -> list[Shipment]:  # fmt: skip
    """Shipments from a vehicle spec's ``shipments`` list, or one shipment for the classic
    single-load truck (``cargo_profile`` and ``pallets``)."""
    rows = spec.get("shipments")
    if not rows:
        profile = PROFILES[default_profile]
        start = initial_c if initial_c is not None else profile.setpoint_c
        return [Shipment(f"{vehicle_id}-S1", profile, pallets, start)]
    out: list[Shipment] = []
    for n, row in enumerate(rows, start=1):
        profile = PROFILES[row["profile"]]
        amount = (
            float(row["kg"]) / profile.kg_per_pallet
            if "kg" in row
            else float(row.get("pallets", 1))
        )
        start = float(row["initial_c"]) if "initial_c" in row else profile.setpoint_c
        shipment_id = str(row.get("id", f"{vehicle_id}-S{n}"))
        packaging = str(row.get("packaging", "pallet"))
        out.append(
            Shipment(shipment_id, profile, amount, start, row.get("receiver"), None, packaging)
        )
    return out


@dataclass(frozen=True)
class CustomerStop:
    dwell_median_min: float  # whole stop, kerb to kerb
    door_median_min: float  # cargo door open within it
    sigma: float


CUSTOMER_STOPS: dict[str, CustomerStop] = {
    "supermarket": CustomerStop(25.0, 14.0, 0.45),
    "hospital": CustomerStop(15.0, 6.0, 0.4),
    "pharmacy": CustomerStop(10.0, 4.0, 0.4),
    "qsr": CustomerStop(12.0, 7.0, 0.4),
    "open_market": CustomerStop(30.0, 18.0, 0.55),
    "hotel": CustomerStop(18.0, 9.0, 0.4),
}


def sample_drop(stop_type: str, rng: random.Random) -> tuple[float, float]:
    """(dwell seconds, door-open seconds) for one drop; the door can't outlast the stop."""
    c = CUSTOMER_STOPS[stop_type]
    shared = rng.gauss(0.0, c.sigma)  # a slow drop is slow throughout
    dwell = 60 * c.dwell_median_min * math.exp(shared)
    door = 60 * c.door_median_min * math.exp(0.6 * shared + 0.4 * rng.gauss(0.0, c.sigma))
    return dwell, min(door, 0.9 * dwell)
