"""The simulation loop on a virtual clock.

The step is the greatest common divisor of 5 s, the 15 s recording interval and every
reporting interval in use (so a 1 Hz load test steps at 1 s). Each ``tick``:

1. applies due scenario events (and anything injected live);
2. for every vehicle, observes the world: link state, sampling at the base interval or the
   burst interval while an alarm condition holds, delivery or buffering, ground truth and
   the fleet-state recording;
3. advances physics: thermal, reefer (icing, defrost, humidity, power) and motion.

``run`` ticks through a whole scenario faster than real time; the live runner calls ``tick``
on a paced clock.
"""

import math
import random
from collections.abc import Iterator
from dataclasses import dataclass, field, replace
from typing import Any

from watchtower_simulator.ambient import ambient_rh_pct
from watchtower_simulator.cargo import PROFILES, CargoProfile
from watchtower_simulator.channel import Channel
from watchtower_simulator.clock import iso, rng, to_datetime, to_ms
from watchtower_simulator.device import Delivery, DeliveryPolicy, Device, Reading
from watchtower_simulator.environment import Conditions, Weather, conditions
from watchtower_simulator.faults import ClockSkew, Fault, make_fault
from watchtower_simulator.operations import (
    DRIVERS,
    Drop,
    FleetPolicy,
    Operations,
    road_features,
)
from watchtower_simulator.reefer import Reefer, ReeferParams
from watchtower_simulator.routes import Route, load_routes
from watchtower_simulator.scenario import Event, Scenario, VehicleSpec, duration_ms
from watchtower_simulator.stops import STOP_TYPES, StopType, door_open_seconds
from watchtower_simulator.thermal import (
    Inputs,
    Load,
    ThermalParams,
    ThermalState,
    step,
    supply_air_c,
)

MAX_STEP_MS = 5_000
RECORDING_INTERVAL_MS = 15_000
SCHEMA_VERSION = 1
SPEED_BANDS = {"highway": (65.0, 90.0), "urban": (20.0, 45.0)}  # km/h, illustrative
TANK_LITRES = 400.0
LITRES_PER_KM = 0.35
ENGINE_OFF_AFTER_S = 600.0  # drivers switch the engine off on longer stops


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
class Stop:
    kind: StopType
    started_ms: int
    until_ms: int
    reason: str = ""


@dataclass
class VehicleSim:
    spec: VehicleSpec
    route: Route
    profile: CargoProfile
    load: Load
    params: ThermalParams
    reefer: Reefer
    device: Device
    channel: Channel
    motion: random.Random
    sensor: random.Random
    ops: random.Random
    fault_rng: random.Random
    thermal: ThermalState
    weather: Weather
    km: float
    pallets: int
    base_interval_ms: int
    burst_interval_ms: int
    speed_kmh: float = 0.0
    target_kmh: float = 0.0
    fuel_pct: float = 80.0
    health: float = 1.0
    health_ramp: Ramp | None = None
    fault_code: str | None = None
    door_until_ms: int = -1
    stop: Stop | None = None
    forced_defrost_until_ms: int = -1
    outage_until_ms: int = -1
    link_up: bool = True
    signal_dbm: int | None = None
    last_sample_ms: int | None = None
    last_event_ms: int | None = None  # freshest reading the gateway has received
    cond: Conditions | None = None  # this tick's environment
    operations: Operations | None = None
    faults: list[Fault] = field(default_factory=lambda: [])
    open_truth: dict[str, int] = field(default_factory=lambda: {})

    _pos_km: float = field(default=-1.0, repr=False)
    _pos: tuple[float, float, float] = field(default=(0.0, 0.0, 0.0), repr=False)

    def position(self) -> tuple[float, float, float]:
        """(lat, lon, heading) at the current km, computed once per move."""
        if self.km != self._pos_km:
            self._pos, self._pos_km = self.route.position(self.km), self.km
        return self._pos

    def moving(self) -> bool:
        return self.speed_kmh > 5.0

    def active_stop(self, now: int) -> Stop | None:
        if self.stop is not None and now < self.stop.until_ms:
            return self.stop
        return None

    def stopped(self, now: int) -> bool:
        return self.active_stop(now) is not None

    def engine_running(self, now: int) -> bool:
        stop = self.active_stop(now)
        if stop is None:
            return True
        idle_s = (now - stop.started_ms) / 1000
        return not stop.kind.engine_off or idle_s < ENGINE_OFF_AFTER_S


@dataclass
class Result:
    readings: list[dict[str, Any]]
    recording: list[dict[str, Any]]
    truth: list[dict[str, Any]]
    dropped: dict[str, int]


def step_for(scenario: Scenario) -> int:
    intervals = [
        MAX_STEP_MS,
        RECORDING_INTERVAL_MS,
        scenario.sample_interval_ms,
        scenario.burst_interval_ms,
    ]
    for v in scenario.fleet:
        intervals += [i for i in (v.sample_interval_ms, v.burst_interval_ms) if i]
    return math.gcd(*intervals)


class Simulation:
    def __init__(
        self,
        scenario: Scenario,
        routes: dict[str, Route] | None = None,
        params: ThermalParams | None = None,
        reefer_params: ReeferParams | None = None,
    ) -> None:
        self.scenario = scenario
        self.routes = routes or load_routes()
        self.params = params or ThermalParams()
        self.reefer_params = reefer_params or ReeferParams()
        self.step_ms = step_for(scenario)
        self.policy = DeliveryPolicy(scenario.duplicate_probability, scenario.max_copies)
        self.vehicles: dict[str, VehicleSim] = {}
        for spec in scenario.fleet:
            self.add_vehicle(spec)
        self.pending: list[Event] = list(scenario.events)
        self.deliveries: list[tuple[Delivery, str]] = []
        self.recording: list[dict[str, Any]] = []
        self.truth: list[dict[str, Any]] = []

    def add_vehicle(self, spec: VehicleSpec) -> VehicleSim:
        s = self.scenario
        route = self.routes[spec.route]
        profile = PROFILES[spec.cargo_profile]
        air = spec.initial_air_c if spec.initial_air_c is not None else profile.setpoint_c
        cargo = spec.initial_cargo_c if spec.initial_cargo_c is not None else profile.setpoint_c
        stream = rng(s.seed, spec.vehicle_id, "setup")
        v = VehicleSim(
            spec=spec,
            route=route,
            profile=profile,
            load=Load(
                profile.capacity_kj_per_k(spec.pallets), profile.cargo_ua_kw_per_k(spec.pallets)
            ),
            params=replace(self.params, **spec.extra.get("thermal", {})),
            reefer=Reefer(
                params=replace(self.reefer_params, **spec.extra.get("reefer", {})),
                humidity_pct=profile.box_humidity_pct,
                genset_l=stream.uniform(60.0, 170.0),
                # Stagger the defrost schedule so a fleet doesn't defrost in lockstep.
                last_defrost_end_ms=s.start_ms - int(stream.uniform(0, 6) * 3_600_000),
            ),
            device=Device(
                spec.device_id,
                rng(s.seed, spec.vehicle_id, "delivery"),
                rng(s.seed, spec.vehicle_id, "boot"),
                self.policy,
            ),
            channel=Channel(rng(s.seed, spec.vehicle_id, "channel")),
            motion=rng(s.seed, spec.vehicle_id, "motion"),
            sensor=rng(s.seed, spec.vehicle_id, "sensor"),
            ops=rng(s.seed, spec.vehicle_id, "ops"),
            fault_rng=rng(s.seed, spec.vehicle_id, "faults"),
            thermal=ThermalState(air_c=air, cargo_c=cargo),
            weather=Weather(rng(s.seed, spec.vehicle_id, "weather")),
            km=spec.start_km,
            pallets=spec.pallets,
            base_interval_ms=spec.sample_interval_ms or s.sample_interval_ms,
            burst_interval_ms=spec.burst_interval_ms or s.burst_interval_ms,
            fuel_pct=float(spec.extra.get("fuel_pct", stream.uniform(55.0, 95.0))),
        )
        if spec.stopped:
            v.stop = Stop(STOP_TYPES["depot_loading"], s.start_ms, s.start_ms + s.duration_ms + 1)
        if s.operations:
            v.operations = Operations(
                driver=DRIVERS[spec.extra.get("driver", "normal")],
                policy=FleetPolicy.parse(s.fleet_policy),
                features=road_features(route, s.seed),
                rng=rng(s.seed, spec.vehicle_id, "operations"),
                drops=[
                    Drop(float(d["km"]), int(d["pallets"])) for d in spec.extra.get("drops", [])
                ],
            )
            v.operations.skip_behind(spec.start_km)
        self.vehicles[spec.vehicle_id] = v
        return v

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
                kind = STOP_TYPES[p.get("stop_type", "unplanned")]
                v.stop = Stop(kind, now_ms, until)
                if "door_open" in p:
                    door_s = duration_ms(p["door_open"]) / 1000 if p["door_open"] else 0.0
                else:
                    door_s = min(door_open_seconds(kind, v.ops), (until - now_ms) / 1000)
                if door_s > 0:
                    v.door_until_ms = now_ms + int(door_s * 1000)
            case "defrost":
                v.forced_defrost_until_ms = until
            case "link_outage":
                v.outage_until_ms = until
            case "reboot":
                v.device.reboot()
            case "sensor_fault":
                params = {k: x for k, x in p.items() if k not in ("fault", "duration")}
                if "gps_sync_after" in params:
                    params["gps_sync_after_s"] = duration_ms(params.pop("gps_sync_after")) / 1000
                end = until if "duration" in p else None
                v.faults.append(make_fault(str(p["fault"]), now_ms, end, params))
            case other:
                raise ValueError(f"unknown event type {other!r}")

    def inject(self, vehicle: str, type: str, params: dict[str, Any], now_ms: int) -> None:
        """Apply an event immediately (live mode's fault injection)."""
        self.apply(Event(now_ms - self.scenario.start_ms, vehicle, type, params), now_ms)

    # -- the loop ------------------------------------------------------------------------

    def tick(self, now: int) -> list[tuple[Delivery, str]]:
        """Advance one step at ``now``; return the deliveries it produced."""
        elapsed = now - self.scenario.start_ms
        while self.pending and self.pending[0].at_ms <= elapsed:
            self.apply(self.pending.pop(0), now)
        produced: list[tuple[Delivery, str]] = []
        for vid in sorted(self.vehicles):
            v = self.vehicles[vid]
            produced += self.observe(v, now, record=elapsed % RECORDING_INTERVAL_MS == 0)
            self.advance(v, now)
        self.deliveries += produced
        return produced

    def run(self) -> Result:
        s = self.scenario
        for now in range(s.start_ms, s.start_ms + s.duration_ms, self.step_ms):
            self.tick(now)
        return self.finish()

    def finish(self) -> Result:
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
            v.open_truth.clear()
        self.truth.sort(key=lambda r: (r["start"], r["vehicle_id"], r["kind"]))
        self.deliveries.sort(key=lambda d: (d[0].ingest_ms, d[1], d[0].reading["seq"], d[0].copy))
        readings = [self.materialise(d) for d, _ in self.deliveries]
        return Result(
            readings,
            self.recording,
            self.truth,
            {vid: v.device.dropped for vid, v in sorted(self.vehicles.items())},
        )

    # -- per-vehicle behaviour -----------------------------------------------------------

    def defrosting(self, v: VehicleSim, now: int) -> bool:
        return now < v.forced_defrost_until_ms or v.reefer.defrosting()

    def power_available(self, v: VehicleSim) -> bool:
        return not (v.reefer.power_source == "GENSET" and v.reefer.genset_l <= 0.0)

    def environment(self, v: VehicleSim, now: int) -> Conditions:
        lat, lon, heading = v.position()
        v.cond = conditions(
            v.weather, now, lat, lon, heading, v.speed_kmh, v.params.wall_ua_kw_per_k
        )
        return v.cond

    def inputs(self, v: VehicleSim, now: int) -> Inputs:
        c = v.cond or self.environment(v, now)
        return Inputs(
            ambient_c=c.ambient_c,
            extra_heat_kw=c.solar_heat_kw,
            setpoint_c=v.profile.setpoint_c,
            health=v.health,
            door_open=now < v.door_until_ms,
            defrost=self.defrosting(v, now),
            capacity_factor=v.reefer.capacity_factor() if self.power_available(v) else 0.0,
            cargo_heat_kw=v.profile.respiration_kw(v.pallets, v.thermal.cargo_c),
        )

    def alarm(self, v: VehicleSim, i: Inputs) -> bool:
        """Conditions under which the device reports at its burst interval."""
        t = v.thermal
        air_out = not (v.profile.min_c - 2.0 <= t.air_c <= v.profile.max_c + 2.0)
        return (
            not v.profile.in_range(t.cargo_c) or air_out or i.door_open or v.fault_code is not None
        )

    def observe(self, v: VehicleSim, now: int, *, record: bool) -> list[tuple[Delivery, str]]:
        if v.health_ramp is not None:
            v.health = v.health_ramp.value(now)
        lat, lon, heading = v.position()
        c = self.environment(v, now)
        segment = v.route.segment(v.km)
        zone = v.route.dead_zone(v.km) if self.scenario.named_dead_zones else None
        forced = zone is not None or now < v.outage_until_ms
        p_drop = min(1.0, segment.p_drop * c.link_drop_factor)
        v.link_up = v.channel.step(p_drop, segment.p_recover, self.step_ms / 1000, forced)
        v.signal_dbm = v.channel.signal_dbm(v.link_up)
        i = self.inputs(v, now)
        excursion = not v.profile.in_range(v.thermal.cargo_c)

        interval = v.burst_interval_ms if self.alarm(v, i) else v.base_interval_ms
        if v.last_sample_ms is None or now - v.last_sample_ms >= interval:
            v.last_sample_ms = now
            reading = self.reading(v, i, lat, lon, heading)
            for fault in v.faults:
                if fault.active(now):
                    fault.apply(reading, now, v.fault_rng)
            reading = v.device.stamp(reading, now + self.clock_skew_ms(v, now))
            out = v.device.handle(reading, now, v.link_up, excursion)
        else:
            out = v.device.drain(now, v.link_up)
        for d in out:
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
                "reefer_power_lost": not self.power_available(v),
                "storm": c.storm,
                **self.stop_flags(v, now),
                **self.fault_flags(v, now),
            },
        )
        if record:
            self.recording.append(self.snapshot(v, now, i, lat, lon, heading))
        return [(d, v.spec.vehicle_id) for d in out]

    def advance(self, v: VehicleSim, now: int) -> None:
        dt_s = self.step_ms / 1000
        lat, _, _ = v.position()

        if v.km >= v.route.length_km and not v.stopped(now):
            # Arrived: park with the engine off until the run ends.
            end = self.scenario.start_ms + self.scenario.duration_ms + 1
            v.stop = Stop(STOP_TYPES["rest"], now, end, "arrived")
        stop = v.active_stop(now)
        if v.engine_running(now):
            v.reefer.power_source = "ENGINE"
        elif stop is not None and stop.kind.shore_power:
            v.reefer.power_source = "SHORE"
        else:
            v.reefer.power_source = "GENSET"

        i = self.inputs(v, now)
        v.thermal = step(v.thermal, v.params, v.load, i, dt_s)
        v.weather.update(now, dt_s)
        v.reefer.update(
            now,
            dt_s,
            compressor_on=v.thermal.compressor_on and i.capacity_factor > 0.0,
            door_open=i.door_open,
            forced_defrost=now < v.forced_defrost_until_ms,
            outside_rh_pct=96.0
            if v.cond is not None and v.cond.storm
            else ambient_rh_pct(now, lat),
            box_rh_pct=v.profile.box_humidity_pct,
        )

        stopped = v.stopped(now)
        ops = v.operations
        if stopped:
            v.target_kmh = 0.0
        elif now % 60_000 == 0 or v.target_kmh == 0.0:
            low, high = SPEED_BANDS[v.route.segment(v.km).road_class]
            target = v.motion.uniform(low, high)
            if ops is not None:
                target = ops.target_speed(target, v.route, v.km, now)
            v.target_kmh = target * (v.cond.speed_factor if v.cond else 1.0)
        tau_s = 8.0 if stopped else 30.0
        v.speed_kmh += (v.target_kmh - v.speed_kmh) * min(1.0, dt_s / tau_s)
        if v.speed_kmh < 0.5 and stopped:
            v.speed_kmh = 0.0
        distance = v.speed_kmh * dt_s / 3600
        prev_km = v.km
        v.km = min(v.route.length_km, v.km + distance)
        v.fuel_pct = max(0.0, v.fuel_pct - distance * LITRES_PER_KM / TANK_LITRES * 100)
        if ops is not None and not stopped:
            if ops.harsh_brake(distance):
                v.speed_kmh *= 0.4
                self.point_truth(v, now, "harsh_brake")
            request = ops.next_stop(prev_km, v.km, now, v.fuel_pct, v.moving(), dt_s)
            if request is not None:
                self.begin_stop(v, now, request.stop_type, request.duration_s, request.reason)

    def begin_stop(
        self, v: VehicleSim, now: int, stop_type: str, duration_s: float, reason: str
    ) -> None:
        kind = STOP_TYPES[stop_type]
        v.stop = Stop(kind, now, now + int(duration_s * 1000), reason)
        door_s = min(door_open_seconds(kind, v.ops), duration_s)
        if door_s > 0:
            v.door_until_ms = now + int(door_s * 1000)
        if stop_type == "fuel":
            v.fuel_pct = max(v.fuel_pct, v.ops.uniform(90.0, 98.0))
        if reason.startswith("drop:"):
            v.pallets = max(0, v.pallets - int(reason.split(":")[1]))
            capacity = v.profile.capacity_kj_per_k(v.pallets)
            v.load = Load(max(capacity, 1.0), v.profile.cargo_ua_kw_per_k(v.pallets))

    def stop_flags(self, v: VehicleSim, now: int) -> dict[str, bool]:
        active = v.active_stop(now)
        name = active.kind.name if active is not None else None
        return {f"stop_{t}": name == t for t in STOP_TYPES}

    def clock_skew_ms(self, v: VehicleSim, now: int) -> int:
        return sum(f.skew_ms(now) for f in v.faults if isinstance(f, ClockSkew))

    def fault_flags(self, v: VehicleSim, now: int) -> dict[str, bool]:
        flags: dict[str, bool] = {}
        for f in v.faults:
            flags[f.truth_kind] = flags.get(f.truth_kind, False) or f.active(now)
        return flags

    def point_truth(self, v: VehicleSim, now: int, kind: str) -> None:
        self.truth.append(
            {"vehicle_id": v.spec.vehicle_id, "kind": kind, "start": iso(now), "end": iso(now)}
        )

    # -- records -------------------------------------------------------------------------

    def probe(self, v: VehicleSim, value: float) -> float:
        return round(value + v.sensor.gauss(0.0, 0.05), 2)

    def compressor_state(self, v: VehicleSim, i: Inputs) -> str:
        if v.fault_code is not None:
            return "FAULT"
        running = v.thermal.compressor_on and not i.defrost and i.capacity_factor > 0.0
        return "RUNNING" if running else "OFF"

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
                "power_source": v.reefer.power_source,
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
        t, r = v.thermal, v.reefer
        active = v.active_stop(now)
        return {
            "t": iso(now),
            "vehicle_id": v.spec.vehicle_id,
            "route_id": v.route.route_id,
            "km_along": round(v.km, 3),
            "lat": round(lat, 6),
            "lon": round(lon, 6),
            "speed_kmh": round(v.speed_kmh, 1),
            "heading_deg": round(heading, 1),
            "cargo_profile": v.profile.name,
            "pallets": v.pallets,
            "cargo_weight_kg": round(v.pallets * v.profile.kg_per_pallet),
            "driver": v.operations.driver.name if v.operations else None,
            "stop_reason": (active.reason or active.kind.name) if active else None,
            "fuel_pct": round(v.fuel_pct, 1),
            "setpoint_c": v.profile.setpoint_c,
            "min_c": v.profile.min_c,
            "max_c": v.profile.max_c,
            "cargo_c": round(t.cargo_c, 2),
            "air_c": round(t.return_air_c, 2),
            "supply_air_c": round(supply_air_c(t, v.params, i), 2),
            "ambient_c": round(i.ambient_c, 1),
            "sun_elevation_deg": round(v.cond.sun.elevation_deg, 1) if v.cond else None,
            "irradiance_w_m2": round(v.cond.ghi_w_m2) if v.cond else None,
            "cloud_cover": round(v.cond.cloud, 2) if v.cond else None,
            "storm": bool(v.cond and v.cond.storm),
            "solar_heat_kw": round(i.extra_heat_kw, 3),
            "humidity_pct": round(r.humidity_pct, 1),
            "door": "OPEN" if i.door_open else "CLOSED",
            "compressor": self.compressor_state(v, i),
            "compressor_health": round(v.health, 3),
            "duty_cycle_pct": round(100 * r.duty_cycle(), 1),
            "evaporator_ice_kg": round(r.ice_kg, 2),
            "capacity_factor": round(i.capacity_factor, 3),
            "defrost": i.defrost,
            "power_source": r.power_source,
            "genset_fuel_l": round(r.genset_l, 1),
            "faults": sorted({f.truth_kind for f in v.faults if f.active(now)}),
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
        # Shallow on purpose: a reading is final once delivered, and retransmitted copies
        # differ only in ingest_time, so the nested sections can be shared read-only.
        record = {**d.reading, "ingest_time": to_datetime(d.ingest_ms)}
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
