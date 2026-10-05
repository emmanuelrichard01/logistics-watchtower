"""Bulk mode: 10,000-25,000 trucks with vectorised NumPy, for throughput and soak tests.

Same equations as the full engine where it matters, simplified where per-truck Python logic
can't vectorise. Bulk output is for load, not for alert-quality evaluation; use the full engine
for that.

Kept, identical to the full engine:

- the two-node thermal model with compressor health and thermostat hysteresis
  (``thermal_step``, pinned to ``thermal.step`` by a test);
- real corridor geometry, km posts and road-class speed bands;
- the ambient model (monthly base, diurnal cosine, latitude gradient);
- the per-segment Markov link;
- the telemetry contract, with event_id = uuid5(device, boot, seq).

Simplified:

- one cargo node per truck (no multi-shipment vans, no cross-dock);
- no weather, sun, operations, stops, doors, defrost or icing;
- a fixed share of trucks degrading;
- readings are produced on a fixed reporting interval and sent at once (no link latency);
- readings taken while the link is down are **dropped and counted**, not buffered and
  replayed. A reconnect-storm test needs the full engine or a dedicated replay generator.

Randomness comes from one seeded ``numpy.random.Generator``, so a seed reproduces a run.
"""

import math
import time
import uuid
from dataclasses import dataclass
from itertools import pairwise
from typing import Any

import numpy as np
import numpy.typing as npt
from watchtower_contracts.identity import EVENT_NAMESPACE

from watchtower_simulator.ambient import (
    AMPLITUDE_PER_DEG_NORTH,
    BASE_PER_DEG_NORTH,
    COAST_LAT,
    MONTHLY,
    PEAK_HOUR_LOCAL,
    WAT_OFFSET_H,
)
from watchtower_simulator.cargo import PROFILES
from watchtower_simulator.clock import iso, to_datetime
from watchtower_simulator.fleet import VEHICLE_CLASSES
from watchtower_simulator.geo import bearing_deg
from watchtower_simulator.routes import Route, load_routes
from watchtower_simulator.thermal import MAX_SUBSTEP_S, ThermalParams

F = npt.NDArray[np.float64]
B = npt.NDArray[np.bool_]
I = npt.NDArray[np.int64]  # noqa: E741


def thermal_step(
    air: F,
    cargo: F,
    on: B,
    ua: F,
    cap: F,
    ambient: F,
    setpoint: F,
    health: F,
    p: ThermalParams,
    dt_s: float,
) -> tuple[F, F, B]:
    """Vectorised ``thermal.step`` with the door shut and no defrost, solar or respiration."""
    remaining = dt_s
    while remaining > 1e-9:
        dt = min(MAX_SUBSTEP_S, remaining)
        remaining -= dt
        on = np.where(
            air > setpoint + p.hysteresis_k,
            True,
            np.where(air < setpoint - p.hysteresis_k, False, on),
        )
        q_cargo = ua * (cargo - air)
        q_net = (
            p.wall_ua_kw_per_k * (ambient - air) + q_cargo - np.where(on, p.q_max_kw * health, 0.0)
        )
        air, cargo = air + dt * q_net / p.air_capacity_kj_per_k, cargo - dt * q_cargo / cap
    return air, cargo, on


def ambient_vec(t_ms: int, lat: F) -> F:
    local = to_datetime(t_ms)
    hour = (local.hour + WAT_OFFSET_H + local.minute / 60 + local.second / 3600) % 24
    base, amplitude = MONTHLY[local.month]
    north = np.maximum(0.0, lat - COAST_LAT)
    return (base + BASE_PER_DEG_NORTH * north) + (
        amplitude + AMPLITUDE_PER_DEG_NORTH * north
    ) * math.cos(2 * math.pi * (hour - PEAK_HOUR_LOCAL) / 24)


@dataclass(frozen=True)
class RouteArrays:
    km: F
    lon: F
    lat: F
    heading: F  # per vertex, towards the next
    seg_from: F
    seg_highway: B
    seg_p_drop: F
    seg_p_recover: F
    length_km: float


def route_arrays(route: Route) -> RouteArrays:
    pts = route.points
    headings = [bearing_deg(a, b) for a, b in pairwise(pts)]
    return RouteArrays(
        km=np.asarray(route.km),
        lon=np.asarray([p[0] for p in pts]),
        lat=np.asarray([p[1] for p in pts]),
        heading=np.asarray([*headings, headings[-1]]),
        seg_from=np.asarray([s.from_km for s in route.segments]),
        seg_highway=np.asarray([s.road_class == "highway" for s in route.segments], dtype=np.bool_),
        seg_p_drop=np.asarray([s.p_drop for s in route.segments]),
        seg_p_recover=np.asarray([s.p_recover for s in route.segments]),
        length_km=route.length_km,
    )


@dataclass
class Batch:
    """One reporting interval's readings, column-wise. ``sent`` masks out dropped readings."""

    t_ms: int
    sent: B
    seq: I
    lat: F
    lon: F
    speed: F
    heading: F
    air: F
    cargo: F
    on: B
    setpoint: F

    @property
    def count(self) -> int:
        return int(self.sent.sum())


class BulkFleet:
    def __init__(
        self,
        trucks: int,
        seed: int,
        start_ms: int,
        *,
        interval_s: float = 30.0,
        degrading_share: float = 0.02,
    ) -> None:
        self.n = trucks
        self.rng = np.random.default_rng(seed)
        self.interval_s = interval_s
        self.t_ms = start_ms
        corridors = [r for r in load_routes().values() if r.kind == "corridor"]
        self.routes = [route_arrays(r) for r in corridors]
        n, rng = trucks, self.rng
        self.route = rng.integers(len(self.routes), size=n)
        lengths = np.asarray([r.length_km for r in self.routes])[self.route]
        self.km = rng.uniform(0.0, 0.9, size=n) * lengths
        frozen = rng.random(n) < 0.6
        fz, ph = PROFILES["frozen"], PROFILES["pharma_2_8"]
        self.setpoint = np.where(frozen, fz.setpoint_c, ph.setpoint_c)
        pallets = np.where(frozen, rng.integers(10, 25, size=n), rng.integers(2, 9, size=n)).astype(
            np.float64
        )
        self.cap = pallets * np.where(
            frozen,
            fz.kg_per_pallet * fz.specific_heat_kj_per_kg_k,
            ph.kg_per_pallet * ph.specific_heat_kj_per_kg_k,
        )
        self.ua = pallets * np.where(frozen, fz.ua_per_pallet_kw_per_k, ph.ua_per_pallet_kw_per_k)
        self.air = self.setpoint.copy()
        self.cargo = self.setpoint.copy()
        self.on = np.zeros(n, dtype=np.bool_)
        self.health = np.ones(n)
        self.decay_per_s = np.where(
            rng.random(n) < degrading_share, rng.uniform(0.5, 0.9, size=n) / (4 * 3600), 0.0
        )
        self.speed = np.zeros(n)
        self.link_up = np.ones(n, dtype=np.bool_)
        self.seq = np.zeros(n, dtype=np.int64)
        self.devices = [f"BULK-{i:05d}" for i in range(n)]
        self.boots = [f"b-{int(b):016x}" for b in rng.integers(0, 2**63, size=n)]
        self.params = ThermalParams()
        bands = VEHICLE_CLASSES["trailer"].speed_bands
        self.highway_band, self.urban_band = bands["highway"], bands["urban"]
        self.dropped = 0

    def _geometry(self) -> tuple[F, F, F, B, F, F]:
        lat, lon, heading = np.empty(self.n), np.empty(self.n), np.empty(self.n)
        highway, p_drop, p_rec = (
            np.empty(self.n, dtype=np.bool_),
            np.empty(self.n),
            np.empty(self.n),
        )
        for r_id, r in enumerate(self.routes):
            m = self.route == r_id
            km = self.km[m]
            lon[m] = np.interp(km, r.km, r.lon)
            lat[m] = np.interp(km, r.km, r.lat)
            heading[m] = r.heading[
                np.clip(np.searchsorted(r.km, km, side="right") - 1, 0, len(r.km) - 1)
            ]
            seg = np.clip(np.searchsorted(r.seg_from, km, side="right") - 1, 0, len(r.seg_from) - 1)
            highway[m], p_drop[m], p_rec[m] = (
                r.seg_highway[seg],
                r.seg_p_drop[seg],
                r.seg_p_recover[seg],
            )
        return lat, lon, heading, highway, p_drop, p_rec

    def step(self) -> Batch:
        dt = self.interval_s
        rng = self.rng
        lat, lon, heading, highway, p_drop, p_rec = self._geometry()

        # Link: per-minute Markov rates converted to this step.
        drop = 1 - (1 - p_drop) ** (dt / 60)
        recover = 1 - (1 - p_rec) ** (dt / 60)
        draw = rng.random(self.n)
        self.link_up = np.where(self.link_up, draw >= drop, draw < recover)

        self.seq += 1
        batch = Batch(
            self.t_ms,
            self.link_up.copy(),
            self.seq.copy(),
            lat,
            lon,
            self.speed.copy(),
            heading,
            self.air.copy(),
            self.cargo.copy(),
            self.on.copy(),
            self.setpoint,
        )
        self.dropped += int((~self.link_up).sum())

        ambient = ambient_vec(self.t_ms, lat)
        self.air, self.cargo, self.on = thermal_step(
            self.air,
            self.cargo,
            self.on,
            self.ua,
            self.cap,
            ambient,
            self.setpoint,
            self.health,
            self.params,
            dt,
        )
        self.health = np.maximum(0.1, self.health - self.decay_per_s * dt)
        lo = np.where(highway, self.highway_band[0], self.urban_band[0])
        hi = np.where(highway, self.highway_band[1], self.urban_band[1])
        target = rng.uniform(lo, hi)
        self.speed += (target - self.speed) * min(1.0, dt / 30.0)
        lengths = np.asarray([r.length_km for r in self.routes])[self.route]
        self.km = self.km + self.speed * dt / 3600
        self.km = np.where(self.km >= lengths, 0.0, self.km)  # loop back to the start
        self.t_ms += int(dt * 1000)
        return batch

    def records(self, batch: Batch) -> list[dict[str, Any]]:
        """Contract-shaped readings (device form, no ingest_time) for the sent rows.

        Columns are rounded and converted to Python lists in one go: per-element NumPy scalar
        conversion would cost more than the physics."""
        idx = np.flatnonzero(batch.sent)
        event_time = iso(batch.t_ms)
        cols = zip(
            idx.tolist(),
            batch.seq[idx].tolist(),
            np.round(batch.lat[idx], 6).tolist(),
            np.round(batch.lon[idx], 6).tolist(),
            np.round(batch.speed[idx], 1).tolist(),
            np.round(batch.heading[idx], 1).tolist(),
            np.round(batch.air[idx], 2).tolist(),
            np.round(batch.cargo[idx], 2).tolist(),
            batch.on[idx].tolist(),
            batch.setpoint[idx].tolist(),
            strict=True,
        )
        out: list[dict[str, Any]] = []
        for i, seq, lat, lon, speed, heading, air, cargo, on, setpoint in cols:
            device, boot = self.devices[i], self.boots[i]
            out.append(
                {
                    "event_id": str(uuid.uuid5(EVENT_NAMESPACE, f"{device}/{boot}/{seq}")),
                    "schema_version": 1,
                    "vehicle_id": f"TRK-B{i:05d}",
                    "device_id": device,
                    "boot_id": boot,
                    "seq": seq,
                    "event_time": event_time,
                    "position": {
                        "lat": lat,
                        "lon": lon,
                        "speed_kmh": speed,
                        "heading_deg": heading,
                        "gps_fix": "FIX_3D",
                        "hdop": 1.0,
                    },
                    "reefer": {
                        "setpoint_c": setpoint,
                        "supply_air_c": None,
                        "return_air_c": air,
                        "cargo_probe_c": cargo,
                        "compressor": "RUNNING" if on else "OFF",
                        "fault_code": None,
                        "defrost": False,
                        "power_source": "ENGINE",
                        "door": "CLOSED",
                    },
                    "vehicle": {"fuel_pct": None, "battery_v": None, "ambient_c": None},
                    "link": {"signal_dbm": None, "buffered": False},
                }
            )
        return out


@dataclass(frozen=True)
class BenchResult:
    trucks: int
    steps: int
    readings: int
    physics_s: float
    records_s: float
    json_s: float

    @property
    def physics_eps(self) -> float:
        return self.readings / self.physics_s

    @property
    def end_to_end_eps(self) -> float:
        return self.readings / (self.physics_s + self.records_s + self.json_s)


def bench(trucks: int, steps: int, seed: int = 1, start_ms: int = 1_791_000_000_000) -> BenchResult:
    """Time the three stages separately: vectorised physics, building contract dicts (event_id
    included), and JSON encoding. Wall time (perf_counter): Windows process time ticks in
    15.6 ms steps, too coarse for one stage of one step, so run benchmarks on a quiet host."""
    import json

    fleet = BulkFleet(trucks, seed, start_ms)
    physics = records = encode = 0.0
    readings = 0
    for _ in range(steps):
        t0 = time.perf_counter()
        batch = fleet.step()
        t1 = time.perf_counter()
        rows = fleet.records(batch)
        t2 = time.perf_counter()
        for r in rows:
            json.dumps(r, separators=(",", ":"))
        t3 = time.perf_counter()
        physics, records, encode = physics + t1 - t0, records + t2 - t1, encode + t3 - t2
        readings += batch.count
    return BenchResult(trucks, steps, readings, physics, records, encode)
