"""Shared helpers for tests and evaluation scripts. Not used by the simulator itself."""

import gzip
import json
from functools import cache
from pathlib import Path
from typing import Any

from watchtower_simulator import scenario as scenarios
from watchtower_simulator.engine import Result, Simulation


@cache
def run(name: str) -> Result:
    """Run a named scenario once per process; later calls reuse the result. Treat it as
    read-only: every caller shares it."""
    return Simulation(scenarios.load(name)).run()


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    """JSON lines from a plain or gzip-compressed (.gz) file."""
    text = (
        gzip.decompress(path.read_bytes()).decode("utf-8")
        if path.suffix == ".gz"
        else path.read_text(encoding="utf-8")
    )
    return [json.loads(line) for line in text.splitlines()]


def rows(name: str, vehicle: str) -> list[dict[str, Any]]:
    """One vehicle's fleet-state recording from a named scenario."""
    return [r for r in run(name).recording if r["vehicle_id"] == vehicle]
