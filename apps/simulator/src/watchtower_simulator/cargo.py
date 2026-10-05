"""Cargo profiles. All figures are illustrative engineering approximations for simulation,
not product guidance; real limits come from the cargo owner (plan section 9).

Thermal mass and air-to-cargo exchange scale with pallet count. Living produce respires, adding
heat that rises with temperature (Q10 of about 2, illustrative).
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class CargoProfile:
    name: str
    setpoint_c: float
    min_c: float
    max_c: float
    kg_per_pallet: float
    specific_heat_kj_per_kg_k: float
    ua_per_pallet_kw_per_k: float  # air-to-cargo heat exchange per pallet
    respiration_kw_per_pallet: float = 0.0  # at setpoint
    box_humidity_pct: float = 60.0  # equilibrium box RH with the door shut
    q10: float = 2.0

    def capacity_kj_per_k(self, pallets: int) -> float:
        return pallets * self.kg_per_pallet * self.specific_heat_kj_per_kg_k

    def cargo_ua_kw_per_k(self, pallets: int) -> float:
        return pallets * self.ua_per_pallet_kw_per_k

    def respiration_kw(self, pallets: int, cargo_c: float) -> float:
        if self.respiration_kw_per_pallet == 0.0:
            return 0.0
        rate = self.q10 ** ((cargo_c - self.setpoint_c) / 10.0)
        return pallets * self.respiration_kw_per_pallet * rate

    def in_range(self, cargo_c: float) -> bool:
        return self.min_c <= cargo_c <= self.max_c


PROFILES: dict[str, CargoProfile] = {
    p.name: p
    for p in (
        CargoProfile("frozen", -20.0, -30.0, -15.0, 700.0, 1.8, 0.03, box_humidity_pct=75.0),
        CargoProfile("pharma_2_8", 5.0, 2.0, 8.0, 300.0, 3.5, 0.1, box_humidity_pct=50.0),
        # Bananas: carried around 13 °C; below ~12 °C they suffer chilling injury.
        CargoProfile("bananas", 13.3, 12.0, 14.5, 1000.0, 3.35, 0.05, 0.06, box_humidity_pct=90.0),
        CargoProfile("fresh_produce", 4.0, 1.0, 8.0, 700.0, 3.8, 0.06, 0.03, box_humidity_pct=90.0),
    )
}
