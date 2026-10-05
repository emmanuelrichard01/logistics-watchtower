"""The reefer unit around the thermal core: evaporator icing and defrost, compressor duty
cycle, box humidity, power source and genset fuel.

All parameters are illustrative engineering approximations, not a particular unit's data.

- **Icing.** Moisture freezes on the evaporator coil while the compressor runs, faster when
  the door lets humid outside air in. Ice insulates the coil, so capacity falls in proportion
  to the ice load until a defrost melts it.
- **Defrost.** Scheduled every few hours, or on demand when capacity drops below a
  threshold. It runs until the ice is gone, within a minimum and maximum duration.
- **Humidity.** Box RH relaxes towards the cargo's equilibrium with the door shut, and towards
  outside RH (fast) with it open.
- **Power.** ENGINE while the tractor runs, GENSET when the engine is off away from a depot,
  SHORE when plugged in at a depot. The genset burns diesel in proportion to compressor duty.
"""

from collections import deque
from dataclasses import dataclass, field


@dataclass(frozen=True)
class ReeferParams:
    ice_loss_per_kg: float = 0.06  # capacity fraction lost per kg of coil ice
    min_capacity_factor: float = 0.4
    frost_kg_per_run_h: float = 0.4
    door_frost_kg_per_h: float = 3.0
    defrost_interval_h: float = 6.0
    demand_defrost_below: float = 0.7
    melt_kg_per_min: float = 0.5
    defrost_min_s: float = 600.0
    defrost_max_s: float = 2400.0
    duty_window_s: float = 1800.0
    genset_base_lph: float = 0.8
    genset_full_lph: float = 2.4
    genset_tank_l: float = 180.0
    humidity_relax_per_h: float = 1.5
    door_humidity_tau_s: float = 120.0


@dataclass
class Reefer:
    params: ReeferParams
    humidity_pct: float
    genset_l: float
    last_defrost_end_ms: int
    ice_kg: float = 0.0
    defrost_started_ms: int = -1  # automatic defrost in progress since this time, or -1
    power_source: str = "ENGINE"
    _duty: deque[bool] = field(default_factory=lambda: deque[bool]())

    def capacity_factor(self) -> float:
        p = self.params
        return max(p.min_capacity_factor, 1.0 - p.ice_loss_per_kg * self.ice_kg)

    def defrosting(self) -> bool:
        return self.defrost_started_ms >= 0

    def duty_cycle(self) -> float:
        """Fraction of the recent window the compressor ran (0-1)."""
        return sum(self._duty) / len(self._duty) if self._duty else 0.0

    def update(
        self,
        now_ms: int,
        dt_s: float,
        *,
        compressor_on: bool,
        door_open: bool,
        forced_defrost: bool,
        outside_rh_pct: float,
        box_rh_pct: float,
    ) -> None:
        p = self.params
        hours = dt_s / 3600

        if compressor_on and not self.defrosting():
            self.ice_kg += p.frost_kg_per_run_h * hours * (self.humidity_pct / 70.0)
        if door_open:
            self.ice_kg += p.door_frost_kg_per_h * hours * (outside_rh_pct / 70.0)
            self.humidity_pct += (outside_rh_pct - self.humidity_pct) * min(
                1.0, dt_s / p.door_humidity_tau_s
            )
        else:
            self.humidity_pct += (box_rh_pct - self.humidity_pct) * min(
                1.0, p.humidity_relax_per_h * hours
            )

        if self.defrosting() or forced_defrost:
            self.ice_kg = max(0.0, self.ice_kg - p.melt_kg_per_min * dt_s / 60)
        if self.defrosting():
            elapsed_s = (now_ms - self.defrost_started_ms) / 1000
            if (
                self.ice_kg == 0.0 and elapsed_s >= p.defrost_min_s
            ) or elapsed_s >= p.defrost_max_s:
                self.defrost_started_ms = -1
                self.last_defrost_end_ms = now_ms
        elif not forced_defrost:
            due = (now_ms - self.last_defrost_end_ms) / 3_600_000 >= p.defrost_interval_h
            if due or self.capacity_factor() < p.demand_defrost_below:
                self.defrost_started_ms = now_ms
        if forced_defrost:
            self.last_defrost_end_ms = now_ms  # a manual defrost resets the schedule

        window = max(1, int(p.duty_window_s / dt_s))
        self._duty.append(compressor_on and not self.defrosting())
        while len(self._duty) > window:
            self._duty.popleft()

        if self.power_source == "GENSET":
            lph = p.genset_base_lph + p.genset_full_lph * (1.0 if compressor_on else 0.0)
            self.genset_l = max(0.0, self.genset_l - lph * hours)
