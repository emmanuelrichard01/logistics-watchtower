"""The simulation loop: a fixed 5 s physics step on a virtual clock.

Each step applies due scenario events, then for every vehicle: samples readings (at the
scenario's interval) and hands them to the device, which delivers or buffers them; records
ground truth and the fleet-state recording; and finally advances motion and thermal physics.
"""

import copy
import random
from collections.abc import Iterator
from dataclasses import dataclass, field, replace
from typing import Any

from watchtower_simulator.ambient import ambient_c
from watchtower_simulator.cargo import PROFILES, CargoProfile
from watchtower_simulator.channel import Channel
from watchtower_simulator.clock import iso, rng, to_datetime, to_ms
from watchtower_simulator.device import Delivery, DeliveryPolicy, Device, Reading
from watchtower_simulator.routes import Route, load_routes
from watchtower_simulator.scenario import Event, Scenario, VehicleSpec, duration_ms
from watchtower_simulator.thermal import (
    Inputs,
    Load,
    ThermalParams,
    ThermalState,
    step,
    supply_air_c,
)

STEP_MS = 5_000
RECORDING_INTERVAL_MS = 15_000
SCHEMA_VERSION = 1
SPEED_BANDS = {"highway": (65.0, 90.0), "urban": (20.0, 45.0)}  # km/h, illustrative
TANK_LITRES = 400.0
LITRES_PER_KM = 0.35


@dataclass
class Ramp:
    start_ms: int
    end_ms: int
    start: float
    end: float

    def value(self, t_ms: int) -> float:
        if t_ms >= self.end_ms:
            return self.end
        f = (t_ms - self.start_ms) / max(1, self.end_ms - self.start_ms)
        return self.start + (self.end - self.start) * f


@dataclass
class VehicleSim:
    spec: VehicleSpec
    route: Route
    profile: CargoProfile
    load: Load
    params: ThermalParams
    device: Device
    channel: Channel
    motion: random.Random
    sensor: random.Random
    thermal: ThermalState
    km: float
    speed_kmh: float = 0.0
    target_kmh: float = 0.0
    fuel_pct: float = 80.0
    health: float = 1.0
    health_ramp: Ramp | None = None
    fault_code: str | None = None
    door_until_ms: int = -1
    stop_until_ms: int = -1
    defrost_until_ms: int = -1
    outage_until_ms: int = -1
    link_up: bool = True
    signal_dbm: int | None = None
    last_event_ms: int | None = None  # freshest reading the gateway has received
    open_truth: dict[str, int] = field(default_factory=lambda: {})

    def moving(self) -> bool:
        return self.speed_kmh > 5.0


@dataclass
class Result:
    readings: list[dict[str, Any]]
    recording: list[dict[str, Any]]
    truth: list[dict[str, Any]]
    dropped: dict[str, int]


class Simulation:
    def __init__(
        self,
        scenario: Scenario,
        routes: dict[str, Route] | None = None,
        params: ThermalParams | None = None,
    ) -> None:
        self.scenario = scenario
        self.routes = routes or load_routes()
        self.params = params or ThermalParams()
        policy = DeliveryPolicy(scenario.duplicate_probability, scenario.max_copies)
        boot_date = to_datetime(scenario.start_ms).strftime("%Y%m%d")
        self.vehicles: dict[str, VehicleSim] = {}
        for spec in scenario.fleet:
            route = self.routes[spec.route]
            profile = PROFILES[spec.cargo_profile]
            seed = scenario.seed
            air = spec.initial_air_c if spec.initial_air_c is not None else profile.setpoint_c
            cargo = spec.initial_cargo_c if spec.initial_cargo_c is not None else profile.setpoint_c
            self.vehicles[spec.vehicle_id] = VehicleSim(
                spec=spec,
                route=route,
                profile=profile,
                load=Load(
                    profile.capacity_kj_per_k(spec.pallets), profile.cargo_ua_kw_per_k(spec.pallets)
                ),
                params=replace(self.params, **spec.extra.get("thermal", {})),
                device=Device(
                    spec.device_id, boot_date, rng(seed, spec.vehicle_id, "delivery"), policy
                ),
                channel=Channel(rng(seed, spec.vehicle_id, "channel")),
                motion=rng(seed, spec.vehicle_id, "motion"),
                sensor=rng(seed, spec.vehicle_id, "sensor"),
                thermal=ThermalState(air_c=air, cargo_c=cargo),
                km=spec.start_km,
                stop_until_ms=scenario.start_ms + scenario.duration_ms + 1 if spec.stopped else -1,
                fuel_pct=rng(seed, spec.vehicle_id, "fuel").uniform(55.0, 95.0),
            )
        self.deliveries: list[tuple[Delivery, str]] = []
        self.recording: list[dict[str, Any]] = []
        self.truth: list[dict[str, Any]] = []

    # -- events ---------------------------------------------------------------------------

    def apply(self, event: Event, now_ms: int) -> None:
        v = self.vehicles[event.vehicle]
        p = event.params
        until = now_ms + duration_ms(p.get("duration", "0s"))
        match event.type:
            case "compressor_health":
                over = duration_ms(p.get("over", "0s"))
                v.health_ramp = Ramp(now_ms, now_ms + over, v.health, float(p["to"]))
            case "compressor_fault":
                v.health_ramp = Ramp(now_ms, now_ms, v.health, 0.0)
                v.fault_code = str(p.get("code", "E-COMP-01"))
            case "door_open":
                v.door_until_ms = until
            case "stop":
                v.stop_until_ms = until
                if p.get("door_open"):
                    v.door_until_ms = until
            case "defrost":
                v.defrost_until_ms = until
            case "link_outage":
                v.outage_until_ms = until
            case "reboot":
                v.device.reboot()
            case other:
                raise ValueError(f"unknown event type {other!r}")

    # -- per-step behaviour --------------------------------------------------------------

    def run(self) -> Result:
        s = self.scenario
        pending = list(s.events)
        end = s.start_ms + s.duration_ms
        for now in range(s.start_ms, end, STEP_MS):
            elapsed = now - s.start_ms
            while pending and pending[0].at_ms <= elapsed:
                self.apply(pending.pop(0), now)
            for vid in sorted(self.vehicles):
                v = self.vehicles[vid]
                self.observe(
                    v,
                    now,
                    sample=elapsed % s.sample_interval_ms == 0,
                    record=elapsed % RECORDING_INTERVAL_MS == 0,
                )
                self.advance(v, now)
        for v in self.vehicles.values():
            for kind, start in v.open_truth.items():
                self.truth.append(
                    {
                        "vehicle_id": v.spec.vehicle_id,
                        "kind": kind,
                        "start": iso(start),
                        "end": None,
                    }
                )
        self.truth.sort(key=lambda r: (r["start"], r["vehicle_id"], r["kind"]))
        self.deliveries.sort(key=lambda d: (d[0].ingest_ms, d[1], d[0].reading["seq"], d[0].copy))
        readings = [self.materialise(d) for d, _ in self.deliveries]
        return Result(
            readings,
            self.recording,
            self.truth,
            {vid: v.device.dropped for vid, v in sorted(self.vehicles.items())},
        )

    def inputs(self, v: VehicleSim, now: int, lat: float) -> Inputs:
        return Inputs(
            ambient_c=ambient_c(now, lat),
            setpoint_c=v.profile.setpoint_c,
            health=v.health,
            door_open=now < v.door_until_ms,
            defrost=now < v.defrost_until_ms,
        )

    def observe(self, v: VehicleSim, now: int, *, sample: bool, record: bool) -> None:
        if v.health_ramp is not None:
            v.health = v.health_ramp.value(now)
        lat, lon, heading = v.route.position(v.km)
        segment = v.route.segment(v.km)
        zone = v.route.dead_zone(v.km) if self.scenario.named_dead_zones else None
        forced = zone is not None or now < v.outage_until_ms
        v.link_up = v.channel.step(segment.p_drop, segment.p_recover, STEP_MS / 1000, forced)
        v.signal_dbm = v.channel.signal_dbm(v.link_up)
        i = self.inputs(v, now, lat)
        excursion = not (v.profile.min_c <= v.thermal.cargo_c <= v.profile.max_c)

        if sample:
            reading = v.device.stamp(self.reading(v, i, lat, lon, heading), now)
            out = v.device.handle(reading, now, v.link_up, excursion)
        else:
            out = v.device.drain(now, v.link_up)
        for d in out:
            self.deliveries.append((d, v.spec.vehicle_id))
            event_ms = to_ms(d.reading["event_time"])
            v.last_event_ms = max(v.last_event_ms or event_ms, event_ms)

        self.track(
            v,
            now,
            {
                "cargo_excursion": excursion,
                "door_open_moving": i.door_open and v.moving(),
                "door_open_stationary": i.door_open and not v.moving(),
                "link_down": not v.link_up,
                "defrost": i.defrost,
                "compressor_fault": v.fault_code is not None,
                "compressor_degraded": v.health < 0.95,
            },
        )
        if record:
            self.recording.append(self.snapshot(v, now, i, lat, lon, heading))

    def advance(self, v: VehicleSim, now: int) -> None:
        dt_s = STEP_MS / 1000
        lat, _, _ = v.route.position(v.km)
        v.thermal = step(v.thermal, v.params, v.load, self.inputs(v, now, lat), dt_s)
        stopped = now < v.stop_until_ms or v.km >= v.route.length_km
        if stopped:
            v.target_kmh = 0.0
        elif now % 60_000 == 0 or v.target_kmh == 0.0:
            low, high = SPEED_BANDS[v.route.segment(v.km).road_class]
            v.target_kmh = v.motion.uniform(low, high)
        tau_s = 8.0 if stopped else 30.0
        v.speed_kmh += (v.target_kmh - v.speed_kmh) * min(1.0, dt_s / tau_s)
        if v.speed_kmh < 0.5 and stopped:
            v.speed_kmh = 0.0
        distance = v.speed_kmh * dt_s / 3600
        v.km = min(v.route.length_km, v.km + distance)
        v.fuel_pct = max(0.0, v.fuel_pct - distance * LITRES_PER_KM / TANK_LITRES * 100)

    # -- records -------------------------------------------------------------------------

    def probe(self, v: VehicleSim, value: float) -> float:
        return round(value + v.sensor.gauss(0.0, 0.05), 2)

    def compressor_state(self, v: VehicleSim, i: Inputs) -> str:
        if v.fault_code is not None:
            return "FAULT"
        return "RUNNING" if v.thermal.compressor_on and not i.defrost else "OFF"

    def reading(self, v: VehicleSim, i: Inputs, lat: float, lon: float, heading: float) -> Reading:
        t = v.thermal
        return {
            "schema_version": SCHEMA_VERSION,
            "vehicle_id": v.spec.vehicle_id,
            "device_id": v.spec.device_id,
            "position": {
                "lat": round(lat + v.sensor.gauss(0.0, 0.00002), 6),
                "lon": round(lon + v.sensor.gauss(0.0, 0.00002), 6),
                "speed_kmh": round(v.speed_kmh, 1),
                "heading_deg": round(heading, 1),
                "gps_fix": "FIX_3D",
                "hdop": round(v.sensor.uniform(0.7, 1.4), 1),
            },
            "reefer": {
                "setpoint_c": v.profile.setpoint_c,
                "supply_air_c": self.probe(v, supply_air_c(t, v.params, i)),
                "return_air_c": self.probe(v, t.return_air_c),
                "cargo_probe_c": self.probe(v, t.cargo_c),
                "compressor": self.compressor_state(v, i),
                "fault_code": v.fault_code,
                "defrost": i.defrost,
                "power_source": "ENGINE",
                "door": "OPEN" if i.door_open else "CLOSED",
            },
            "vehicle": {
                "fuel_pct": round(v.fuel_pct, 1),
                "battery_v": round(13.8 + v.sensor.gauss(0.0, 0.08), 2),
                "ambient_c": round(i.ambient_c, 1),
            },
            "link": {"signal_dbm": v.signal_dbm, "buffered": False},
        }

    def snapshot(
        self, v: VehicleSim, now: int, i: Inputs, lat: float, lon: float, heading: float
    ) -> dict[str, Any]:
        t = v.thermal
        return {
            "t": iso(now),
            "vehicle_id": v.spec.vehicle_id,
            "route_id": v.route.route_id,
            "km": round(v.km, 3),
            "lat": round(lat, 6),
            "lon": round(lon, 6),
            "speed_kmh": round(v.speed_kmh, 1),
            "heading_deg": round(heading, 1),
            "cargo_profile": v.profile.name,
            "setpoint_c": v.profile.setpoint_c,
            "min_c": v.profile.min_c,
            "max_c": v.profile.max_c,
            "cargo_c": round(t.cargo_c, 2),
            "air_c": round(t.return_air_c, 2),
            "supply_air_c": round(supply_air_c(t, v.params, i), 2),
            "ambient_c": round(i.ambient_c, 1),
            "door": "OPEN" if i.door_open else "CLOSED",
            "compressor": self.compressor_state(v, i),
            "compressor_health": round(v.health, 3),
            "defrost": i.defrost,
            "link_up": v.link_up,
            "signal_dbm": v.signal_dbm,
            "buffered": bool(v.device.buffer),
            "buffer_depth": len(v.device.buffer),
            "last_fix_age_s": None
            if v.last_event_ms is None
            else round((now - v.last_event_ms) / 1000),
        }

    def track(self, v: VehicleSim, now: int, flags: dict[str, bool]) -> None:
        for kind, active in flags.items():
            if active and kind not in v.open_truth:
                v.open_truth[kind] = now
            elif not active and kind in v.open_truth:
                start = v.open_truth.pop(kind)
                self.truth.append(
                    {
                        "vehicle_id": v.spec.vehicle_id,
                        "kind": kind,
                        "start": iso(start),
                        "end": iso(now),
                    }
                )

    @staticmethod
    def materialise(d: Delivery) -> dict[str, Any]:
        record = copy.deepcopy(d.reading)
        record["ingest_time"] = to_datetime(d.ingest_ms)
        order = [
            "event_id",
            "schema_version",
            "vehicle_id",
            "device_id",
            "boot_id",
            "seq",
            "event_time",
            "ingest_time",
            "position",
            "reefer",
            "vehicle",
            "link",
        ]
        return {k: record[k] for k in order}


def iter_jsonl_ready(records: list[dict[str, Any]]) -> Iterator[dict[str, Any]]:
    """Records with datetimes rendered as ISO-8601 UTC milliseconds, for JSON output."""
    for r in records:
        out = dict(r)
        for key in ("event_time", "ingest_time"):
            if key in out:
                out[key] = iso(to_ms(out[key]))
        yield out
