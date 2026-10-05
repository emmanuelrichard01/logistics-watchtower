"""Cargo profiles. All figures are illustrative engineering approximations for simulation,
not product guidance; real limits come from the cargo owner (plan section 9)."""

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

    def capacity_kj_per_k(self, pallets: int) -> float:
        return pallets * self.kg_per_pallet * self.specific_heat_kj_per_kg_k

    def cargo_ua_kw_per_k(self, pallets: int) -> float:
        return pallets * self.ua_per_pallet_kw_per_k


PROFILES: dict[str, CargoProfile] = {
    p.name: p
    for p in (
        CargoProfile("frozen", -20.0, -30.0, -15.0, 700.0, 1.8, 0.03),
        CargoProfile("pharma_2_8", 5.0, 2.0, 8.0, 300.0, 3.5, 0.1),
    )
}
