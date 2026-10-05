"""Two-node reefer thermal model (plan section 8).

    C_a dT_a/dt = (T_amb - T_a)/R_w + (T_c - T_a)/R_c + Q_door + Q_defrost - h Q_max u(t)
    C_c dT_c/dt = (T_a - T_c)/R_c

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
    extra_heat_kw: float = 0.0  # solar and other loads from the environment layer
    capacity_factor: float = 1.0  # evaporator icing etc. from the reefer layer


def thermostat(state: ThermalState, p: ThermalParams, i: Inputs) -> bool:
    if i.defrost:
        return False
    if state.air_c > i.setpoint_c + p.hysteresis_k:
        return True
    if state.air_c < i.setpoint_c - p.hysteresis_k:
        return False
    return state.compressor_on


def cooling_kw(state: ThermalState, p: ThermalParams, i: Inputs) -> float:
    return p.q_max_kw * i.health * i.capacity_factor if state.compressor_on else 0.0


def supply_air_c(state: ThermalState, p: ThermalParams, i: Inputs) -> float:
    if i.defrost:
        return state.air_c + 2.0  # heater on, fans off: coil air reads warm
    return state.air_c - p.supply_drop_k * cooling_kw(state, p, i) / p.q_max_kw


def step(state: ThermalState, p: ThermalParams, load: Load, i: Inputs, dt_s: float) -> ThermalState:
    remaining = dt_s
    while remaining > 1e-9:
        dt = min(MAX_SUBSTEP_S, remaining)
        remaining -= dt
        state = replace(state, compressor_on=thermostat(state, p, i))
        q_wall = p.wall_ua_kw_per_k * (i.ambient_c - state.air_c)
        q_cargo = load.ua_kw_per_k * (state.cargo_c - state.air_c)
        q_door = p.door_ua_kw_per_k * (i.ambient_c - state.air_c) if i.door_open else 0.0
        q_defrost = p.defrost_kw if i.defrost else 0.0
        q_net = q_wall + q_cargo + q_door + q_defrost + i.extra_heat_kw - cooling_kw(state, p, i)
        air = state.air_c + dt * q_net / p.air_capacity_kj_per_k
        cargo = state.cargo_c - dt * q_cargo / load.capacity_kj_per_k
        state = replace(state, air_c=air, cargo_c=cargo)
    return state
