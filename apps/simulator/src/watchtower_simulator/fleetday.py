"""Seeded "fleet day" generator: a realistic mixed day for evaluation and soak tests.

It composes inter-state trucks on the corridors and vans on city rounds, with mixed cargo,
packaging and drivers and the operations model switched on, and a few trucks handing over to
vans at a hub. Incidents are drawn per vehicle from Poisson rates per vehicle-day. Every
injected incident is written to the labels, so evaluation can score precision, recall and lead
time against ground truth. The output is an ordinary scenario document, so a generated day
can be saved, rerun and diffed like any hand-written scenario.

Rates are illustrative, chosen to give a handful of incidents in a 20-truck day, not
measured fleet statistics.
"""

import math
import random
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from watchtower_simulator.clock import iso, rng, to_ms
from watchtower_simulator.routes import Route, load_routes

# Incident -> (rate per vehicle-day, truth kind it guarantees, applies to)
INCIDENTS: dict[str, tuple[float, str, str]] = {
    "compressor_degradation": (0.05, "compressor_degraded", "any"),
    "compressor_fault": (0.02, "compressor_fault", "any"),
    # A door opened while parked is stationary, so either kind satisfies the label.
    "door_open": (0.03, "door_open_moving|door_open_stationary", "truck"),
    "sensor_flatline": (0.03, "fault_flatline_cargo_probe", "any"),
    "sensor_drift": (0.03, "fault_drift_cargo_probe", "any"),
    "gps_jump": (0.03, "fault_gps_jump", "any"),
    "reboot": (0.05, "", "any"),
    "breakdown": (0.02, "stop_breakdown", "truck"),
    "tyre_blowout": (0.02, "tyre_blowout", "truck"),
    "hijack": (0.004, "route_deviation", "truck"),
    "link_outage": (0.10, "link_down", "any"),
}
TRUCK_PROFILES = [("frozen", 0.45), ("pharma_2_8", 0.25), ("fresh_produce", 0.2), ("bananas", 0.1)]
VAN_PROFILES = [("fresh_produce", 0.5), ("pharma_2_8", 0.35), ("frozen", 0.15)]
DRIVERS = [("cautious", 0.3), ("normal", 0.5), ("aggressive", 0.2)]
PACKAGING = [("carton", 0.5), ("insulated_box", 0.3), ("pallet", 0.2)]


@dataclass(frozen=True)
class FleetDay:
    doc: dict[str, Any]  # scenario document, ready for scenario.parse or YAML
    labels: dict[str, Any]


def pick(r: random.Random, weighted: list[tuple[str, float]]) -> str:
    return r.choices([k for k, _ in weighted], weights=[w for _, w in weighted])[0]


def poisson(r: random.Random, mean: float) -> int:
    """Knuth's method; fine for the small means used here."""
    limit, k, p = math.exp(-mean), 0, 1.0
    while True:
        p *= r.random()
        if p <= limit:
            return k
        k += 1


def incident_event(kind: str, vehicle: str, at_s: float, r: random.Random) -> dict[str, Any]:
    at = f"{round(at_s)}s"
    match kind:
        case "compressor_degradation":
            return {
                "at": at,
                "vehicle": vehicle,
                "type": "compressor_health",
                "to": round(r.uniform(0.1, 0.4), 2),
                "over": f"{r.randint(45, 150)}m",
            }
        case "compressor_fault":
            return {"at": at, "vehicle": vehicle, "type": "compressor_fault", "code": "E-COMP-01"}
        case "door_open":
            return {
                "at": at,
                "vehicle": vehicle,
                "type": "door_open",
                "duration": f"{r.randint(3, 12)}m",
            }
        case "sensor_flatline":
            return {
                "at": at,
                "vehicle": vehicle,
                "type": "sensor_fault",
                "fault": "flatline",
                "probe": "cargo_probe",
            }
        case "sensor_drift":
            return {
                "at": at,
                "vehicle": vehicle,
                "type": "sensor_fault",
                "fault": "drift",
                "probe": "cargo_probe",
                "c_per_hour": round(r.uniform(0.3, 1.0), 2),
            }
        case "gps_jump":
            return {
                "at": at,
                "vehicle": vehicle,
                "type": "sensor_fault",
                "fault": "gps_jump",
                "probability": 0.05,
                "duration": f"{r.randint(20, 90)}m",
            }
        case "reboot":
            return {"at": at, "vehicle": vehicle, "type": "reboot"}
        case "breakdown":
            return {
                "at": at,
                "vehicle": vehicle,
                "type": "breakdown",
                "duration": f"{r.randint(60, 360)}m",
            }
        case "tyre_blowout":
            return {
                "at": at,
                "vehicle": vehicle,
                "type": "tyre_blowout",
                "duration": f"{r.randint(45, 120)}m",
            }
        case "hijack":
            return {
                "at": at,
                "vehicle": vehicle,
                "type": "hijack",
                "deviate_km": r.randint(6, 20),
                "door_open": f"{r.randint(20, 60)}m",
                "tracker_off_after": f"{r.randint(10, 40)}m",
            }
        case "link_outage":
            return {
                "at": at,
                "vehicle": vehicle,
                "type": "link_outage",
                "duration": f"{r.randint(10, 60)}m",
            }
        case other:
            raise ValueError(other)


def generate(seed: int, trucks: int = 20, vans: int = 6, hours: float = 24.0,
             start: str = "2026-10-06T04:00:00Z", handovers: int = 2,
             rate_scale: float = 1.0) -> FleetDay:  # fmt: skip
    """A fleet day. ``rate_scale`` multiplies every incident rate (tests and stress runs)."""
    r = rng(seed, "fleet-day")
    routes = load_routes()
    corridors = sorted(k for k, v in routes.items() if v.kind == "corridor")
    rounds = sorted(k for k, v in routes.items() if v.kind == "urban")
    duration_s = hours * 3600
    fleet: list[dict[str, Any]] = []
    events: list[dict[str, Any]] = []
    expect: list[dict[str, Any]] = []
    injected: list[dict[str, Any]] = []

    def incidents(vehicle: str, role: str) -> None:
        for kind, (rate, truth_kind, applies) in INCIDENTS.items():
            if applies not in ("any", role):
                continue
            for _ in range(poisson(r, rate * rate_scale * hours / 24)):
                at_s = r.uniform(0.1, 0.85) * duration_s
                events.append(incident_event(kind, vehicle, at_s, r))
                injected.append(
                    {"vehicle": vehicle, "incident": kind, "at": iso_offset(start, at_s)}
                )
                if truth_kind:
                    expect.append({"vehicle": vehicle, "kind": truth_kind, "present": True})

    vans_ids = [f"VAN-{n:03d}" for n in range(1, vans + 1)]
    abuja_rounds = [k for k in rounds if routes[k].city == "Abuja"] or rounds
    handover_vans = vans_ids[: min(handovers, vans)]

    for n in range(1, trucks + 1):
        vid = f"TRK-{n:03d}"
        corridor = r.choice(corridors)
        route: Route = routes[corridor]
        truck: dict[str, Any] = {
            "vehicle_id": vid,
            "route": corridor,
            "start_km": round(r.uniform(0.0, 0.6) * route.length_km, 1),
            "cargo_profile": pick(r, TRUCK_PROFILES),
            "pallets": r.randint(10, 24),
            "driver": pick(r, DRIVERS),
        }
        if r.random() < 0.08:  # loaded with field heat
            truck["initial_cargo_c"] = round(r.uniform(8.0, 16.0), 1)
        fleet.append(truck)
        incidents(vid, "truck")

    # A few Abuja-bound trucks finish their run at the hub and hand over to a waiting van.
    for k, van in enumerate(handover_vans):
        vid = f"TRK-H{k + 1:02d}"
        corridor = r.choice([c for c in corridors if c.endswith("ABJ")])
        route = routes[corridor]
        round_id = r.choice(abuja_rounds)
        stops = [t.stop_id for t in routes[round_id].towns if t.stop_type not in (None, "hub")]
        shipments = [
            {
                "id": f"SHP-{vid}-{i}",
                "profile": pick(r, VAN_PROFILES),
                "kg": r.randint(40, 300),
                "receiver": stop,
                "handover_to": van,
                "packaging": pick(r, PACKAGING),
            }
            for i, stop in enumerate(r.sample(stops, k=min(3, len(stops))), start=1)
        ]
        fleet.append(
            {
                "vehicle_id": vid,
                "route": corridor,
                "start_km": round(route.length_km - r.uniform(20, 60), 1),
                "driver": pick(r, DRIVERS),
                "cross_dock": {
                    "unload": f"{r.randint(15, 30)}m",
                    "dock_air_c": 12.0 if r.random() < 0.8 else 28.0,
                },
                "shipments": shipments,
            }
        )
        fleet.append(
            {
                "vehicle_id": van,
                "route": round_id,
                "vehicle_class": "van",
                "setpoint_c": 5.0,
                "handover_from": vid,
                "available_at": f"{r.randint(60, 180)}m",
                "load": "10m",
            }
        )
        expect.append({"vehicle": van, "kind": "on_dock", "present": True})

    for van in vans_ids[len(handover_vans) :]:
        round_id = r.choice(rounds)
        stops = [t.stop_id for t in routes[round_id].towns if t.stop_type not in (None, "hub")]
        cls = "trike" if r.random() < 0.2 else "van"
        chosen = r.sample(stops, k=1 if cls == "trike" else len(stops))
        spec: dict[str, Any] = {
            "vehicle_id": van,
            "route": round_id,
            "vehicle_class": cls,
            "setpoint_c": 4.0,
            "dispatch_after": f"{r.randint(5, 40)}m",
            "shipments": [
                {
                    "id": f"SHP-{van}-{i}",
                    "profile": pick(r, VAN_PROFILES),
                    "kg": r.randint(15, 60 if cls == "trike" else 250),
                    "receiver": stop,
                    "packaging": pick(r, PACKAGING),
                }
                for i, stop in enumerate(chosen, start=1)
            ],
        }
        # An unlatched door only swings open on the move if there's a next stop to drive to.
        ordered = [s for s in stops if s in chosen]
        if cls == "van" and len(ordered) > 1 and r.random() < 0.15:
            spec["door_ajar"] = {r.choice(ordered[:-1]): f"{r.randint(10, 30)}m"}
            expect.append({"vehicle": van, "kind": "door_open_moving", "present": True})
        fleet.append(spec)
        incidents(van, "van")

    doc = {
        "name": f"fleet_day_{seed}",
        "seed": seed,
        "start": start,
        "duration": f"{round(duration_s)}s",
        "operations": True,
        "fleet_policy": {"no_night_driving": r.random() < 0.5, "max_drive_h": 4.0, "rest_min": 30},
        "fleet": fleet,
        "events": events,
    }
    labels = {"scenario": doc["name"], "expect": expect, "injected": injected}
    return FleetDay(doc, labels)


def iso_offset(start: str, seconds: float) -> str:
    base = to_ms(datetime.fromisoformat(start.replace("Z", "+00:00")))
    return iso(base + int(seconds * 1000))
