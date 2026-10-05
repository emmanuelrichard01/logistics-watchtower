"""Shared builders for gateway tests: a valid, signed device reading."""

import copy
from datetime import UTC, datetime, timedelta
from typing import Any

from watchtower_contracts import event_id
from watchtower_gateway.validation import sign

NOW = datetime(2026, 10, 5, 7, 12, 45, tzinfo=UTC)
NOW_MS = round(NOW.timestamp() * 1000)
KEYS = {"EDGE-0101": b"dev-only-key-EDGE-0101", "EDGE-0102": b"dev-only-key-EDGE-0102"}

_BASE: dict[str, Any] = {
    "schema_version": 1,
    "vehicle_id": "TRK-101",
    "device_id": "EDGE-0101",
    "boot_id": "b-9f3a1c2e7d40b812",
    "seq": 18273,
    "position": {
        "lat": 7.3768,
        "lon": 3.9398,
        "speed_kmh": 85.3,
        "heading_deg": 42.7,
        "gps_fix": "FIX_3D",
        "hdop": 1.1,
    },
    "reefer": {
        "setpoint_c": -20.0,
        "supply_air_c": -21.4,
        "return_air_c": -19.8,
        "cargo_probe_c": -19.1,
        "compressor": "RUNNING",
        "fault_code": None,
        "defrost": False,
        "power_source": "ENGINE",
        "door": "CLOSED",
    },
    "vehicle": {"fuel_pct": 68.2, "battery_v": 13.8, "ambient_c": 33.5},
    "link": {"signal_dbm": -87, "buffered": False},
}


def reading(
    *,
    seq: int = 18273,
    device_id: str = "EDGE-0101",
    age: timedelta = timedelta(seconds=30),
    **overrides: Any,
) -> dict[str, Any]:
    r = copy.deepcopy(_BASE)
    r.update(seq=seq, device_id=device_id)
    r["event_time"] = (NOW - age).isoformat()
    r.update(overrides)
    r["event_id"] = str(event_id(r["device_id"], r["boot_id"], r["seq"]))
    return resign(r)


def resign(r: dict[str, Any]) -> dict[str, Any]:
    key = KEYS.get(r.get("device_id", ""), b"unregistered-device-key")
    r["signature"] = sign(r, key)
    return r
