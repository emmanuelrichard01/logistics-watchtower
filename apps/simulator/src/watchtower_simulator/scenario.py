"""Scenario files: YAML with a seed, a start time, a fleet, a duration and timed events.
Ground-truth labels live beside each scenario in ``<name>.labels.yaml`` and are read only by
tests and evaluation, never by the pipeline."""

import re
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

import yaml

from watchtower_simulator.clock import to_ms
from watchtower_simulator.routes import default_data_dir

_DURATION = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*(ms|s|m|h|d)\s*$")
_UNIT_MS = {"ms": 1, "s": 1000, "m": 60_000, "h": 3_600_000, "d": 86_400_000}


def duration_ms(value: str | int | float) -> int:
    """'90s', '35m', '2h' or a number of seconds."""
    if isinstance(value, int | float):
        return int(value * 1000)
    match = _DURATION.match(value)
    if not match:
        raise ValueError(f"bad duration {value!r}; use e.g. 90s, 35m, 2h")
    return int(float(match.group(1)) * _UNIT_MS[match.group(2)])


@dataclass(frozen=True)
class VehicleSpec:
    vehicle_id: str
    device_id: str
    route: str
    start_km: float = 0.0
    cargo_profile: str = "frozen"
    pallets: int = 20
    initial_air_c: float | None = None
    initial_cargo_c: float | None = None
    stopped: bool = False  # parked at start (e.g. loading at a depot)
    sample_interval_ms: int | None = None  # overrides the scenario's reporting intervals
    burst_interval_ms: int | None = None
    extra: dict[str, Any] = field(default_factory=lambda: {})


@dataclass(frozen=True)
class Event:
    at_ms: int  # offset from scenario start
    vehicle: str
    type: str
    params: dict[str, Any]


@dataclass(frozen=True)
class Scenario:
    name: str
    seed: int
    start_ms: int
    duration_ms: int
    sample_interval_ms: int  # base reporting interval
    fleet: tuple[VehicleSpec, ...]
    events: tuple[Event, ...]
    duplicate_probability: float = 0.0
    max_copies: int = 3
    named_dead_zones: bool = True
    burst_interval_ms: int = 10_000  # reporting interval while an alarm condition holds
    operations: bool = False  # checkpoints, congestion, fuel, rest rules, drops (layer 3)
    fleet_policy: dict[str, Any] = field(default_factory=lambda: {})
    raw: dict[str, Any] = field(default_factory=lambda: {}, compare=False)

    def with_duration(self, duration: int) -> "Scenario":
        return Scenario(**{**self.__dict__, "duration_ms": duration})


def parse(doc: dict[str, Any]) -> Scenario:
    start = doc["start"]
    start_dt = (
        start
        if isinstance(start, datetime)
        else datetime.fromisoformat(str(start).replace("Z", "+00:00"))
    )
    fleet = tuple(
        VehicleSpec(
            vehicle_id=v["vehicle_id"],
            device_id=v.get("device_id", "EDGE-" + v["vehicle_id"].split("-")[-1].zfill(4)),
            route=v["route"],
            start_km=float(v.get("start_km", 0.0)),
            cargo_profile=v.get("cargo_profile", "frozen"),
            pallets=int(v.get("pallets", 20)),
            initial_air_c=v.get("initial_air_c"),
            initial_cargo_c=v.get("initial_cargo_c"),
            stopped=bool(v.get("stopped", False)),
            sample_interval_ms=duration_ms(v["sample_interval"])
            if "sample_interval" in v
            else None,
            burst_interval_ms=duration_ms(v["burst_interval"]) if "burst_interval" in v else None,
            extra={k: x for k, x in v.items() if k not in VehicleSpec.__dataclass_fields__},
        )
        for v in doc["fleet"]
    )
    events = tuple(
        sorted(
            (
                Event(
                    at_ms=duration_ms(e["at"]),
                    vehicle=e["vehicle"],
                    type=e["type"],
                    params={k: x for k, x in e.items() if k not in ("at", "vehicle", "type")},
                )
                for e in doc.get("events", [])
            ),
            key=lambda e: (e.at_ms, e.vehicle, e.type),
        )
    )
    delivery = doc.get("delivery", {})
    return Scenario(
        name=doc["name"],
        seed=int(doc["seed"]),
        start_ms=to_ms(start_dt),
        duration_ms=duration_ms(doc["duration"]),
        sample_interval_ms=duration_ms(doc.get("sample_interval", "30s")),
        fleet=fleet,
        events=events,
        duplicate_probability=float(delivery.get("duplicate_probability", 0.0)),
        max_copies=int(delivery.get("max_copies", 3)),
        named_dead_zones=bool(doc.get("named_dead_zones", True)),
        burst_interval_ms=duration_ms(doc.get("burst_interval", "10s")),
        operations=bool(doc.get("operations", False)),
        fleet_policy=dict(doc.get("fleet_policy") or {}),
        raw=doc,
    )


def scenario_path(name_or_path: str) -> Path:
    candidate = Path(name_or_path)
    if candidate.suffix in (".yaml", ".yml") and candidate.exists():
        return candidate
    return default_data_dir() / "scenarios" / f"{name_or_path}.yaml"


def load(name_or_path: str) -> Scenario:
    return parse(yaml.safe_load(scenario_path(name_or_path).read_text(encoding="utf-8")))


def load_labels(name_or_path: str) -> dict[str, Any]:
    path = scenario_path(name_or_path)
    labels: dict[str, Any] = yaml.safe_load(
        path.with_name(path.stem + ".labels.yaml").read_text(encoding="utf-8")
    )
    return labels
