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
from bisect import insort
from collections.abc import Iterator
from dataclasses import dataclass, field, replace
from typing import Any

from watchtower_simulator.ambient import ambient_rh_pct
from watchtower_simulator.cargo import PROFILES, CargoProfile
from watchtower_simulator.channel import Channel
from watchtower_simulator.clock import iso, rng, to_datetime, to_ms
from watchtower_simulator.crossdock import DockedShipment
from watchtower_simulator.device import Delivery, DeliveryPolicy, Device, Reading
from watchtower_simulator.environment import Conditions, Weather, conditions
from watchtower_simulator.faults import ClockSkew, Fault, make_fault
from watchtower_simulator.fleet import (
    VEHICLE_CLASSES,
    Shipment,
    VehicleClass,
    parse_shipments,
    sample_drop,
)
from watchtower_simulator.incidents import Detour
from watchtower_simulator.operations import (
    DRIVERS,
    Drop,
    FleetPolicy,
    Operations,
    road_features,
    urban_congestion,
)
from watchtower_simulator.reefer import Reefer, ReeferParams
from watchtower_simulator.rounds import DeliveryRound, log_entry, plan_round
from watchtower_simulator.routes import Route, load_routes
from watchtower_simulator.scenario import Event, Scenario, VehicleSpec, duration_ms
from watchtower_simulator.stops import STOP_TYPES, StopType, door_open_seconds
from watchtower_simulator.thermal import (
    Inputs,
    ThermalParams,
    ThermalState,
    step_multi,
    supply_air_c,
)

MAX_STEP_MS = 5_000
RECORDING_INTERVAL_MS = 15_000
SCHEMA_VERSION = 1
ENGINE_OFF_AFTER_S = 600.0  # drivers switch the engine off on longer stops
DOCK_AIR_C = 12.0  # chilled loading dock at a cold-store hub (illustrative)
DOOR_AFTER_STOP_MS = 30_000  # drivers pull up, then open the cargo door


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
    vclass: VehicleClass
    shipments: list[Shipment]
    empty_profile: CargoProfile  # what the box is set up for when nothing is on board
    setpoint_c: float
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
    base_interval_ms: int
    burst_interval_ms: int
    speed_kmh: float = 0.0
    target_kmh: float = 0.0
    fuel_pct: float = 80.0
    health: float = 1.0
    health_ramp: Ramp | None = None
    fault_code: str | None = None
    door_from_ms: int = -1
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
    detour: Detour | None = None  # hijacked off the corridor
    round: DeliveryRound | None = None  # urban multi-drop round
    last_probes: tuple[int, dict[str, Any]] | None = None  # (sample time, reefer as reported)
    open_truth: dict[str, int] = field(default_factory=lambda: {})

    _pos_km: float = field(default=-1.0, repr=False)
    _pos: tuple[float, float, float] = field(default=(0.0, 0.0, 0.0), repr=False)

    def position(self) -> tuple[float, float, float]:
        """(lat, lon, heading) at the current km (or on the detour), cached per move."""
        if self.detour is not None:
            return self.detour.position()
        if self.km != self._pos_km:
            self._pos, self._pos_km = self.route.position(self.km), self.km
        return self._pos

    _onboard: list[Shipment] | None = field(default=None, repr=False)

    @property
    def onboard(self) -> list[Shipment]:
        """Shipments still on board; cached, refreshed whenever custody changes."""
        if self._onboard is None:
            self._onboard = [x for x in self.shipments if x.delivered_ms is None]
        return self._onboard

    def custody_changed(self) -> None:
        self._onboard = None

    @property
    def profile(self) -> CargoProfile:
        """The cargo profile at the probe: the first shipment still on board."""
        onboard = self.onboard
        return onboard[0].profile if onboard else self.empty_profile

    @property
    def cargo_c(self) -> float:
        """What the cargo probe touches; in an empty box it reads air."""
        onboard = self.onboard
        return onboard[0].cargo_c if onboard else self.thermal.air_c

    @property
    def pallets(self) -> float:
        return sum(x.pallets for x in self.onboard)

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
        if stop.kind.name == "delivery_drop" and self.vclass.idle_at_drops:
            return True
        idle_s = (now - stop.started_ms) / 1000
        return not stop.kind.engine_off or idle_s < ENGINE_OFF_AFTER_S


@dataclass
class Result:
    readings: list[dict[str, Any]]
    recording: list[dict[str, Any]]
    truth: list[dict[str, Any]]
    dropped: dict[str, int]
    stops: list[dict[str, Any]] = field(default_factory=lambda: [])


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
        self.dock: list[DockedShipment] = []
        self.pending: list[Event] = list(scenario.events)
        self.deliveries: list[tuple[Delivery, str]] = []
        self.recording: list[dict[str, Any]] = []
        self.truth: list[dict[str, Any]] = []

    def add_vehicle(self, spec: VehicleSpec) -> VehicleSim:
        s = self.scenario
        route = self.routes[spec.route]
        vclass = VEHICLE_CLASSES[spec.extra.get("vehicle_class", "trailer")]
        shipments = parse_shipments(
            spec.vehicle_id, spec.extra, spec.cargo_profile, spec.pallets, spec.initial_cargo_c
        )
        profile = shipments[0].profile if shipments else PROFILES[spec.cargo_profile]
        setpoint = float(spec.extra.get("setpoint_c", profile.setpoint_c))
        air = spec.initial_air_c if spec.initial_air_c is not None else setpoint
        stream = rng(s.seed, spec.vehicle_id, "setup")
        v = VehicleSim(
            spec=spec,
            route=route,
            vclass=vclass,
            shipments=shipments,
            empty_profile=profile,
            setpoint_c=setpoint,
            params=replace(self.params, **{**vclass.thermal, **spec.extra.get("thermal", {})}),
            reefer=Reefer(
                params=replace(
                    self.reefer_params, **{**(vclass.reefer or {}), **spec.extra.get("reefer", {})}
                ),
                humidity_pct=profile.box_humidity_pct,
                genset_l=float(spec.extra.get("genset_l", stream.uniform(60.0, 170.0)))
                if vclass.standby_genset
                else 0.0,
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
            thermal=ThermalState(air_c=air, cargo_c=shipments[0].cargo_c if shipments else air),
            weather=Weather(rng(s.seed, spec.vehicle_id, "weather")),
            km=spec.start_km,
            base_interval_ms=spec.sample_interval_ms or s.sample_interval_ms,
            burst_interval_ms=spec.burst_interval_ms or s.burst_interval_ms,
            fuel_pct=float(spec.extra.get("fuel_pct", stream.uniform(55.0, 95.0))),
        )
        if spec.stopped:
            v.stop = Stop(STOP_TYPES["depot_loading"], s.start_ms, s.start_ms + s.duration_ms + 1)
        if "handover_from" in spec.extra:
            end = s.start_ms + s.duration_ms + 1
            v.stop = Stop(STOP_TYPES["depot_loading"], s.start_ms, end, "awaiting_handover")
        elif route.kind == "urban":
            dispatch_ms = s.start_ms + duration_ms(spec.extra.get("dispatch_after", "0s"))
            v.round = plan_round(route, spec.start_km, dispatch_ms)
            v.round.door_ajar_s = {
                k: duration_ms(x) / 1000 for k, x in spec.extra.get("door_ajar", {}).items()
            }
            if dispatch_ms > s.start_ms:  # loading at the hub on shore power
                v.stop = Stop(STOP_TYPES["depot_loading"], s.start_ms, dispatch_ms, "loading")
                median_s = 60 * vclass.loading_door_median_min
                door_s = min(
                    median_s * math.exp(v.ops.gauss(0.0, 0.4)), (dispatch_ms - s.start_ms) / 1000
                )
                v.door_until_ms = s.start_ms + int(door_s * 1000)
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
            case "breakdown":
                self.begin_stop(v, now_ms, "breakdown", (until - now_ms) / 1000, "breakdown")
            case "tyre_blowout":
                v.speed_kmh *= 0.25
                self.point_truth(v, now_ms, "tyre_blowout")
                self.point_truth(v, now_ms, "harsh_brake")
                dur = duration_ms(p.get("duration", "75m")) / 1000
                self.begin_stop(v, now_ms, "unplanned", dur, "tyre_change")
            case "hijack":
                lat, lon, heading = v.position()
                off = p.get("tracker_off_after")
                v.detour = Detour(
                    start_ms=now_ms,
                    origin_lat=lat,
                    origin_lon=lon,
                    bearing_deg=(heading + float(p.get("bearing_offset_deg", 90.0))) % 360,
                    target_km=float(p.get("deviate_km", 12.0)),
                    stop_s=0.0,
                    door_open_s=duration_ms(p.get("door_open", "40m")) / 1000,
                    tracker_off_after_s=duration_ms(off) / 1000 if off else None,
                )
            case "unload_complete":
                self.unload(v, now_ms)
            case "dispatch":
                self.dispatch(v, now_ms)
            case "sensor_fault":
                params = {k: x for k, x in p.items() if k not in ("fault", "duration")}
                if "gps_sync_after" in params:
                    params["gps_sync_after_s"] = duration_ms(params.pop("gps_sync_after")) / 1000
                end = until if "duration" in p else None
                v.faults.append(make_fault(str(p["fault"]), now_ms, end, params))
            case other:
                raise ValueError(f"unknown event type {other!r}")

    def schedule(self, event: Event) -> None:
        """Queue an event for later in the run, keeping the queue in time order."""
        insort(self.pending, event, key=lambda e: e.at_ms)

    def inject(self, vehicle: str, type: str, params: dict[str, Any], now_ms: int) -> None:
        """Apply an event immediately (live mode's fault injection)."""
        self.apply(Event(now_ms - self.scenario.start_ms, vehicle, type, params), now_ms)

    # -- the loop ------------------------------------------------------------------------

    def tick(self, now: int) -> list[tuple[Delivery, str]]:
        """Advance one step at ``now``; return the deliveries it produced."""
        elapsed = now - self.scenario.start_ms
        while self.pending and self.pending[0].at_ms <= elapsed:
            self.apply(self.pending.pop(0), now)
        for docked in self.dock:
            docked.step(self.step_ms / 1000)
        for vid in sorted(self.vehicles):
            self.maybe_load(self.vehicles[vid], now)
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
                self.truth.append(truth_row(v.spec.vehicle_id, kind, start, None))
            v.open_truth.clear()
        self.truth.sort(key=lambda r: (r["start"], r["vehicle_id"], r["kind"]))
        self.deliveries.sort(key=lambda d: (d[0].ingest_ms, d[1], d[0].reading["seq"], d[0].copy))
        readings = [self.materialise(d) for d, _ in self.deliveries]
        stops = sorted(
            (entry for v in self.vehicles.values() if v.round for entry in v.round.log),
            key=lambda e: (e["actual_arrival"], e["vehicle_id"]),
        )
        return Result(
            readings,
            self.recording,
            self.truth,
            {vid: v.device.dropped for vid, v in sorted(self.vehicles.items())},
            stops,
        )

    # -- per-vehicle behaviour -----------------------------------------------------------

    def defrosting(self, v: VehicleSim, now: int) -> bool:
        return now < v.forced_defrost_until_ms or v.reefer.defrosting()

    def power_available(self, v: VehicleSim) -> bool:
        source = v.reefer.power_source
        return source != "NONE" and not (source == "GENSET" and v.reefer.genset_l <= 0.0)

    def door_open(self, v: VehicleSim, now: int) -> bool:
        return v.door_from_ms <= now < v.door_until_ms

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
            setpoint_c=v.setpoint_c,
            health=v.health,
            door_open=self.door_open(v, now),
            defrost=self.defrosting(v, now),
            capacity_factor=v.reefer.capacity_factor() if self.power_available(v) else 0.0,
            door_air_c=DOCK_AIR_C if (st := v.active_stop(now)) and st.kind.shore_power else None,
        )

    def alarm(self, v: VehicleSim, i: Inputs) -> bool:
        """Conditions under which the device reports at its burst interval."""
        air = v.thermal.air_c
        air_out = not (v.profile.min_c - 2.0 <= air <= v.profile.max_c + 2.0)
        cargo_out = any(not x.in_range() for x in v.onboard)
        return cargo_out or air_out or i.door_open or v.fault_code is not None

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
        excursion = any(not x.in_range() for x in v.onboard)

        interval = v.burst_interval_ms if self.alarm(v, i) else v.base_interval_ms
        tracker_off = v.detour is not None and v.detour.tracker_off(now)
        if tracker_off:
            v.link_up, v.signal_dbm = False, None
            out = []  # power cut: nothing sampled, nothing buffered, nothing sent
        elif v.last_sample_ms is None or now - v.last_sample_ms >= interval:
            v.last_sample_ms = now
            reading = self.reading(v, i, lat, lon, heading)
            for fault in v.faults:
                if fault.active(now):
                    fault.apply(reading, now, v.fault_rng)
            v.last_probes = (now, dict(reading["reefer"]))
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
                **{f"cargo_excursion@{x.shipment_id}": not x.in_range() for x in v.onboard},
                **{
                    f"cargo_excursion@{d.shipment.shipment_id}": not d.shipment.in_range()
                    for d in self.dock
                    if d.to_vehicle == v.spec.vehicle_id
                },
                **{
                    f"on_dock@{d.shipment.shipment_id}": True
                    for d in self.dock
                    if d.to_vehicle == v.spec.vehicle_id
                },
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
                "route_deviation": v.detour is not None,
                "unexplained_stop": self.stop_reason(v, now) == "unexplained",
                "tracker_offline": tracker_off,
            },
        )
        if record:
            self.recording.append(self.snapshot(v, now, i, lat, lon, heading))
        return [(d, v.spec.vehicle_id) for d in out]

    def advance(self, v: VehicleSim, now: int) -> None:
        dt_s = self.step_ms / 1000
        lat, _, _ = v.position()

        if v.km >= v.route.length_km and not v.stopped(now):
            end = self.scenario.start_ms + self.scenario.duration_ms + 1
            plan = v.spec.extra.get("cross_dock")
            if plan and any(x.handover_to for x in v.onboard):
                # Arrived at the hub: unload onto the dock, door open throughout.
                unload_ms = duration_ms(plan.get("unload", "20m"))
                v.stop = Stop(STOP_TYPES["cross_dock"], now, now + unload_ms, "cross_dock")
                v.door_from_ms, v.door_until_ms = now + DOOR_AFTER_STOP_MS, now + unload_ms
                at = now + unload_ms - self.scenario.start_ms
                self.schedule(Event(at, v.spec.vehicle_id, "unload_complete", {}))
            else:
                # Arrived: park with the engine off until the run ends.
                v.stop = Stop(STOP_TYPES["rest"], now, end, "arrived")
        stop = v.active_stop(now)
        if v.engine_running(now):
            v.reefer.power_source = "ENGINE"
        elif stop is not None and stop.kind.shore_power:
            v.reefer.power_source = "SHORE"
        elif v.vclass.standby_genset:
            v.reefer.power_source = "GENSET"
        else:
            v.reefer.power_source = "NONE"  # engine off, no genset: the unit is off

        i = self.inputs(v, now)
        onboard = v.onboard
        air, on, temps = step_multi(
            v.thermal.air_c,
            v.thermal.compressor_on,
            [x.cargo_c for x in onboard],
            [x.load() for x in onboard],
            [x.profile.respiration_kw(x.pallets, x.cargo_c) for x in onboard],
            v.params,
            i,
            dt_s,
        )
        for x, t in zip(onboard, temps, strict=True):
            x.cargo_c = t
        v.thermal = ThermalState(air, v.cargo_c, on)
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
            road_class = v.route.segment(v.km).road_class
            low, high = v.vclass.speed_bands[road_class]
            target = v.motion.uniform(low, high)
            if v.route.kind == "urban":
                target *= urban_congestion(v.route.city, road_class, now)
            if ops is not None:
                target = ops.target_speed(target, v.route, v.km, now)
            v.target_kmh = target * (v.cond.speed_factor if v.cond else 1.0)
        tau_s = 8.0 if stopped else 30.0
        v.speed_kmh += (v.target_kmh - v.speed_kmh) * min(1.0, dt_s / tau_s)
        if v.speed_kmh < 0.5 and stopped:
            v.speed_kmh = 0.0
        distance = v.speed_kmh * dt_s / 3600
        prev_km = v.km
        if v.detour is not None:
            self.drive_detour(v, now, distance)
            return
        v.km = min(v.route.length_km, v.km + distance)
        v.fuel_pct = max(0.0, v.fuel_pct - distance * v.vclass.l_per_km / v.vclass.tank_l * 100)
        if v.round is not None and not stopped:
            self.maybe_drop(v, now)
        if ops is not None and not stopped:
            if ops.harsh_brake(distance):
                v.speed_kmh *= 0.4
                self.point_truth(v, now, "harsh_brake")
            request = ops.next_stop(prev_km, v.km, now, v.fuel_pct, v.moving(), dt_s)
            if request is not None:
                self.begin_stop(v, now, request.stop_type, request.duration_s, request.reason)

    def stop_reason(self, v: VehicleSim, now: int) -> str | None:
        stop = v.active_stop(now)
        return stop.reason if stop is not None else None

    def drive_detour(self, v: VehicleSim, now: int, distance_km: float) -> None:
        """Off the corridor: drive the side road, then park there for good."""
        d = v.detour
        assert d is not None
        v.fuel_pct = max(0.0, v.fuel_pct - distance_km * v.vclass.l_per_km / v.vclass.tank_l * 100)
        if d.stopped_at_ms is not None:
            return
        d.travelled_km = min(d.target_km, d.travelled_km + distance_km)
        v.target_kmh = min(v.target_kmh, 40.0)  # side roads
        if d.arrived():
            d.stopped_at_ms = now
            end = self.scenario.start_ms + self.scenario.duration_ms + 1
            v.stop = Stop(STOP_TYPES["unplanned"], now, end, "unexplained")
            # They pull up first, then open the cargo door a few minutes later.
            door = {"duration": f"{d.door_open_s}s"}
            self.schedule(
                Event(now + 300_000 - self.scenario.start_ms, v.spec.vehicle_id, "door_open", door)
            )

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
        if reason.startswith("drop:") and v.onboard:
            first = v.onboard[0]
            first.pallets = max(0.0, first.pallets - int(reason.split(":")[1]))

    def unload(self, v: VehicleSim, now: int) -> None:
        """The truck's handover shipments leave it for the dock; the truck parks."""
        dock_air = float(v.spec.extra.get("cross_dock", {}).get("dock_air_c", DOCK_AIR_C))
        for x in v.onboard:
            if x.handover_to:
                x.delivered_ms = now
                v.custody_changed()
                self.dock.append(DockedShipment(x, v.spec.vehicle_id, x.handover_to, now, dock_air))
        end = self.scenario.start_ms + self.scenario.duration_ms + 1
        v.stop = Stop(STOP_TYPES["rest"], now, end, "arrived")

    def expected_handover(self, van_id: str) -> list[Shipment]:
        return [x for v in self.vehicles.values() for x in v.shipments if x.handover_to == van_id]

    def maybe_load(self, v: VehicleSim, now: int) -> None:
        """A waiting van starts loading once it's available and everything it expects is on
        the dock."""
        stop = v.active_stop(now)
        if stop is None or stop.reason != "awaiting_handover":
            return
        available = self.scenario.start_ms + duration_ms(v.spec.extra.get("available_at", "0s"))
        expected = self.expected_handover(v.spec.vehicle_id)
        on_dock = {d.shipment.shipment_id for d in self.dock if d.to_vehicle == v.spec.vehicle_id}
        if now < available or not expected or any(x.shipment_id not in on_dock for x in expected):
            return
        load_ms = duration_ms(v.spec.extra.get("load", "10m"))
        v.stop = Stop(STOP_TYPES["depot_loading"], now, now + load_ms, "loading")
        v.door_from_ms, v.door_until_ms = now, now + load_ms
        self.schedule(
            Event(now + load_ms - self.scenario.start_ms, v.spec.vehicle_id, "dispatch", {})
        )

    def dispatch(self, v: VehicleSim, now: int) -> None:
        """Shipments come off the dock into the van, which plans its round from now."""
        mine = [d for d in self.dock if d.to_vehicle == v.spec.vehicle_id]
        self.dock = [d for d in self.dock if d.to_vehicle != v.spec.vehicle_id]
        for d in mine:
            x = d.shipment
            v.shipments.append(
                Shipment(
                    x.shipment_id, x.profile, x.pallets, x.cargo_c, x.receiver, None, x.packaging
                )
            )
        v.custody_changed()
        v.round = plan_round(v.route, v.km, now)
        v.stop = None

    def maybe_drop(self, v: VehicleSim, now: int) -> None:
        """On an urban round: stop at the next customer once reached, deliver its shipments,
        log arrival against the plan and window."""
        r = v.round
        assert r is not None
        stop = r.next_stop()
        if stop is None or v.km < stop.km:
            return
        r.next_index += 1
        receivers = {x.receiver for x in v.shipments if x.receiver}
        if receivers and stop.stop_id not in receivers:
            return  # nothing for this customer on this vehicle: drive on
        v.km = stop.km
        dwell_s, door_s = sample_drop(stop.stop_type, v.ops)
        v.stop = Stop(
            STOP_TYPES["delivery_drop"], now, now + int(dwell_s * 1000), f"drop:{stop.stop_id}"
        )
        v.door_from_ms = now + DOOR_AFTER_STOP_MS
        v.door_until_ms = v.door_from_ms + int(door_s * 1000)
        if stop.stop_id in r.door_ajar_s:
            # Not latched on leaving: the door swings open on the move.
            v.door_until_ms = now + int((dwell_s + r.door_ajar_s[stop.stop_id]) * 1000)
        delivered = []
        for x in v.onboard:
            if x.receiver == stop.stop_id:
                x.delivered_ms = now
                v.custody_changed()
                delivered.append(
                    {
                        "shipment_id": x.shipment_id,
                        "profile": x.profile.name,
                        "cargo_c": round(x.cargo_c, 2),
                        "in_spec": x.in_range(),
                    }
                )
        r.log.append(
            log_entry(v.spec.vehicle_id, v.route.route_id, stop, now, dwell_s, door_s, delivered)
        )

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
                "setpoint_c": v.setpoint_c,
                "supply_air_c": self.probe(v, supply_air_c(t, v.params, i)),
                "return_air_c": self.probe(v, t.return_air_c),
                "cargo_probe_c": self.probe(v, v.cargo_c),
                "compressor": self.compressor_state(v, i),
                "fault_code": v.fault_code,
                "defrost": i.defrost,
                "power_source": "UNKNOWN"
                if v.reefer.power_source == "NONE"
                else v.reefer.power_source,
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
            "vehicle_class": v.vclass.name,
            "cargo_profile": v.profile.name,
            "pallets": round(v.pallets, 2),
            "cargo_weight_kg": round(sum(x.mass_kg for x in v.onboard)),
            "shipments": [
                {
                    "shipment_id": x.shipment_id,
                    "profile": x.profile.name,
                    "cargo_c": round(x.cargo_c, 2),
                    "min_c": x.profile.min_c,
                    "max_c": x.profile.max_c,
                    "receiver": x.receiver,
                }
                for x in v.onboard
            ],
            "next_stop": (n.stop_id if (n := v.round.next_stop()) else None) if v.round else None,
            "driver": v.operations.driver.name if v.operations else None,
            "stop_reason": (active.reason or active.kind.name) if active else None,
            "fuel_pct": round(v.fuel_pct, 1),
            "setpoint_c": v.setpoint_c,
            "min_c": v.profile.min_c,
            "max_c": v.profile.max_c,
            "cargo_c": round(v.cargo_c, 2),
            "air_c": round(t.return_air_c, 2),
            "supply_air_c": round(supply_air_c(t, v.params, i), 2),
            **self.probe_fields(v),
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
            "off_route_km": round(v.detour.travelled_km, 2) if v.detour else 0.0,
            "link_up": v.link_up,
            "signal_dbm": v.signal_dbm,
            "buffered": bool(v.device.buffer),
            "buffer_depth": len(v.device.buffer),
            "last_fix_age_s": None
            if v.last_event_ms is None
            else round((now - v.last_event_ms) / 1000),
        }

    @staticmethod
    def probe_fields(v: VehicleSim) -> dict[str, Any]:
        """The device's latest sample as reported: noise, calibration and active faults
        applied, null on dropout. Unlike the true temperatures, this is all a console sees."""
        if v.last_probes is None:
            return {
                "probe_t": None,
                "cargo_probe_c": None,
                "return_air_probe_c": None,
                "supply_air_probe_c": None,
            }
        at, reefer = v.last_probes
        return {
            "probe_t": iso(at),
            "cargo_probe_c": reefer["cargo_probe_c"],
            "return_air_probe_c": reefer["return_air_c"],
            "supply_air_probe_c": reefer["supply_air_c"],
        }

    def track(self, v: VehicleSim, now: int, flags: dict[str, bool]) -> None:
        for gone in [k for k in v.open_truth if k not in flags]:
            # A shipment delivered or handed over: its intervals end when custody passes.
            self.truth.append(truth_row(v.spec.vehicle_id, gone, v.open_truth.pop(gone), now))
        for kind, active in flags.items():
            if active and kind not in v.open_truth:
                v.open_truth[kind] = now
            elif not active and kind in v.open_truth:
                self.truth.append(truth_row(v.spec.vehicle_id, kind, v.open_truth.pop(kind), now))

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


def truth_row(vehicle_id: str, key: str, start: int, end: int | None) -> dict[str, Any]:
    """A truth interval. Per-shipment keys look like ``cargo_excursion@SHP-1``."""
    kind, _, shipment = key.partition("@")
    row: dict[str, Any] = {
        "vehicle_id": vehicle_id,
        "kind": kind,
        "start": iso(start),
        "end": iso(end) if end is not None else None,
    }
    if shipment:
        row["shipment_id"] = shipment
    return row


def iter_jsonl_ready(records: list[dict[str, Any]]) -> Iterator[dict[str, Any]]:
    """Records with datetimes rendered as ISO-8601 UTC milliseconds, for JSON output."""
    for r in records:
        out = dict(r)
        for key in ("event_time", "ingest_time"):
            if key in out:
                out[key] = iso(to_ms(out[key]))
        yield out
