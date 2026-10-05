"""Two-node reefer thermal model (plan section 8).

    C_a dT_a/dt = (T_amb - T_a)/R_w + (T_c - T_a)/R_c + Q_door + Q_defrost - h Q_max u(t)
    C_c dT_c/dt = (T_a - T_c)/R_c + Q_resp

Conductances are used instead of resistances (UA = 1/R). ``h`` in [0, 1] is compressor health,
so a failing unit loses capacity gradually; ``u`` is the thermostat duty with hysteresis on
return air. Cargo lags air, and the cargo temperature is what spoils.

Parameter values are illustrative for a ~13.6 m refrigerated trailer, not calibrated to a
particular unit. Integrated with explicit Euler in sub-steps of at most 5 s, well inside the
air node's time constant (~25 min).
"""

from dataclasses import dataclass, replace

MAX_SUBSTEP_S = 5.0


@dataclass(frozen=True)
class ThermalParams:
    air_capacity_kj_per_k: float = 400.0
    wall_ua_kw_per_k: float = 0.08
    door_ua_kw_per_k: float = 1.5
    q_max_kw: float = 9.0
    defrost_kw: float = 0.8  # share of heater power reaching box air; the rest melts coil ice
    hysteresis_k: float = 1.0
    supply_drop_k: float = 6.0  # supply air below return air at full capacity


@dataclass(frozen=True)
class ThermalState:
    air_c: float
    cargo_c: float
    compressor_on: bool = False

    @property
    def return_air_c(self) -> float:
        return self.air_c


@dataclass(frozen=True)
class Load:
    capacity_kj_per_k: float
    ua_kw_per_k: float


@dataclass(frozen=True)
class Inputs:
    ambient_c: float
    setpoint_c: float
    health: float = 1.0
    door_open: bool = False
    defrost: bool = False
    extra_heat_kw: float = 0.0  # into box air: solar load and other environment terms
    capacity_factor: float = 1.0  # evaporator icing etc. from the reefer layer
    cargo_heat_kw: float = 0.0  # generated inside the cargo: produce respiration
    door_air_c: float | None = None  # air outside the open door, if not ambient (a chilled dock)


def thermostat(air_c: float, on: bool, p: ThermalParams, i: Inputs) -> bool:
    """Hysteresis on return air: on above setpoint + band, off below setpoint - band."""
    if i.defrost:
        return False
    if air_c > i.setpoint_c + p.hysteresis_k:
        return True
    if air_c < i.setpoint_c - p.hysteresis_k:
        return False
    return on


def cooling_kw(state: ThermalState, p: ThermalParams, i: Inputs) -> float:
    return p.q_max_kw * i.health * i.capacity_factor if state.compressor_on else 0.0


def supply_air_c(state: ThermalState, p: ThermalParams, i: Inputs) -> float:
    if i.defrost:
        return state.air_c + 2.0  # heater on, fans off: coil air reads warm
    return state.air_c - p.supply_drop_k * cooling_kw(state, p, i) / p.q_max_kw


def step_multi(
    air: float,
    on: bool,
    cargos: list[float],
    loads: list[Load],
    heats_kw: list[float],
    p: ThermalParams,
    i: Inputs,
    dt_s: float,
) -> tuple[float, bool, list[float]]:
    """One air node and N cargo nodes (one per shipment) sharing it.

    A van carrying vaccines, insulin and dairy has three thermal masses at different
    temperatures, each exchanging heat with the same box air. ``heats_kw`` is heat generated
    inside each cargo (respiration). With no cargo the box is just air."""
    cargos = list(cargos)
    door_ua = p.door_ua_kw_per_k if i.door_open else 0.0
    fixed_kw = (p.defrost_kw if i.defrost else 0.0) + i.extra_heat_kw
    full_kw = p.q_max_kw * i.health * i.capacity_factor
    remaining = dt_s
    while remaining > 1e-9:
        dt = min(MAX_SUBSTEP_S, remaining)
        remaining -= dt
        on = thermostat(air, on, p, i)
        flows = [ld.ua_kw_per_k * (c - air) for c, ld in zip(cargos, loads, strict=True)]
        door_air = i.ambient_c if i.door_air_c is None else i.door_air_c
        q_out = p.wall_ua_kw_per_k * (i.ambient_c - air) + door_ua * (door_air - air)
        q_net = q_out + sum(flows) + fixed_kw - (full_kw if on else 0.0)
        air += dt * q_net / p.air_capacity_kj_per_k
        cargos = [
            c + dt * (h - q) / ld.capacity_kj_per_k
            for c, q, h, ld in zip(cargos, flows, heats_kw, loads, strict=True)
        ]
    return air, on, cargos


def step(state: ThermalState, p: ThermalParams, load: Load, i: Inputs, dt_s: float) -> ThermalState:
    """Single cargo node: the common inter-state trailer case."""
    air, on, (cargo,) = step_multi(
        state.air_c, state.compressor_on, [state.cargo_c], [load], [i.cargo_heat_kw], p, i, dt_s
    )
    return replace(state, air_c=air, cargo_c=cargo, compressor_on=on)
