"""Cross-docking at a city hub: inter-state trucks hand over to city vans.

The truck arrives and unloads; its shipments wait on the dock until the van that will deliver
them is available and loads them. While they wait, each shipment exchanges heat with the dock
air, as its own thermal node with the same packaging exposure it has on a vehicle. A chilled
dock (about 12 °C) is forgiving; an unrefrigerated one in Abuja heat is not. A late van turns
a routine handover into the cold-chain break, which is why the handover is its own event.
"""

from dataclasses import dataclass

from watchtower_simulator.fleet import Shipment


@dataclass
class DockedShipment:
    shipment: Shipment
    from_vehicle: str
    to_vehicle: str
    since_ms: int
    dock_air_c: float

    def step(self, dt_s: float) -> None:
        load = self.shipment.load()
        s = self.shipment
        s.cargo_c += (
            dt_s * load.ua_kw_per_k * (self.dock_air_c - s.cargo_c) / load.capacity_kj_per_k
        )
