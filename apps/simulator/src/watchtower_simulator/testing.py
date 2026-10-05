"""Shared helpers for tests and evaluation scripts. Not used by the simulator itself."""

from functools import cache
from typing import Any

from watchtower_simulator import scenario as scenarios
from watchtower_simulator.engine import Result, Simulation


@cache
def run(name: str) -> Result:
    """Run a named scenario once per process; later calls reuse the result. Treat it as
    read-only: every caller shares it."""
    return Simulation(scenarios.load(name)).run()


def rows(name: str, vehicle: str) -> list[dict[str, Any]]:
    """One vehicle's fleet-state recording from a named scenario."""
    return [r for r in run(name).recording if r["vehicle_id"] == vehicle]
