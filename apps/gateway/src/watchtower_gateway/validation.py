"""Stateless reading validation (plan section 7, ADR-0021). Pure: no clock, network or
file access, so every rule is a plain function of its inputs and testable as a matrix.

A device reading is the telemetry_event v1 record without ``ingest_time`` plus a
``signature`` field: hex HMAC-SHA256, keyed by the device's key, over the reading's
canonical JSON (every field except ``signature``, keys sorted, no whitespace, UTF-8).
``event_time`` is an ISO-8601 string with a UTC offset, or integer epoch milliseconds.

Checks run in a fixed order and the first failure decides the reason, so each
reading has exactly one quarantine reason.
"""

import hashlib
import hmac
import json
import math
import uuid
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

from fastavro import parse_schema
from fastavro.validation import ValidationError, validate
from watchtower_contracts import event_id, load_schema
from watchtower_contracts.identity import ID_PATTERN

_SCHEMA = load_schema("telemetry_event", 1)
_PARSED = parse_schema(_SCHEMA)
_DEVICE_FIELDS = {f["name"] for f in _SCHEMA["fields"]} - {"ingest_time"}
SIGNATURE_FIELD = "signature"


class Reason(StrEnum):
    MALFORMED = "MALFORMED"
    BAD_ID = "BAD_ID"
    UNKNOWN_DEVICE = "UNKNOWN_DEVICE"
    BAD_SIGNATURE = "BAD_SIGNATURE"
    EVENT_ID_MISMATCH = "EVENT_ID_MISMATCH"
    OUT_OF_RANGE = "OUT_OF_RANGE"
    FUTURE_EVENT = "FUTURE_EVENT"
    STALE_EVENT = "STALE_EVENT"


@dataclass(frozen=True)
class Limits:
    future_ms: int = 5 * 60_000
    max_age_ms: int = 7 * 86_400_000
    probe_c: tuple[float, float] = (-80.0, 80.0)
    # Nigeria's bounding box widened by 1 degree on every side.
    lat: tuple[float, float] = (3.27, 14.89)
    lon: tuple[float, float] = (1.67, 15.68)
    speed_kmh: tuple[float, float] = (0.0, 160.0)


@dataclass(frozen=True)
class Accepted:
    vehicle_id: str
    telemetry: dict[str, Any]  # typed TelemetryEvent, ingest_time stamped


@dataclass(frozen=True)
class Rejected:
    reason: Reason
    detail: str
    key: str = field(default="unknown")  # quarantine partition key


def canonical_bytes(reading: Mapping[str, Any]) -> bytes:
    body = {k: v for k, v in reading.items() if k != SIGNATURE_FIELD}
    return json.dumps(
        body, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    ).encode("utf-8")


def sign(reading: Mapping[str, Any], key: bytes) -> str:
    """The signature a device attaches. Exposed for the simulator and tests."""
    return hmac.new(key, canonical_bytes(reading), hashlib.sha256).hexdigest()


def _event_time(value: Any) -> datetime:
    if isinstance(value, bool):
        raise ValueError("event_time must be a timestamp")
    if isinstance(value, int):
        return datetime.fromtimestamp(value / 1000, UTC)
    if isinstance(value, str):
        parsed = datetime.fromisoformat(value)
        if parsed.tzinfo is None:
            raise ValueError("event_time must carry a UTC offset")
        return parsed.astimezone(UTC)
    raise ValueError("event_time must be an ISO-8601 string or epoch milliseconds")


def _finite_within(value: Any, lo: float, hi: float) -> bool:
    return (
        isinstance(value, int | float)
        and not isinstance(value, bool)
        and math.isfinite(value)
        and lo <= value <= hi
    )


def _range_problem(r: Mapping[str, Any], limits: Limits) -> str | None:
    reefer = r["reefer"]
    probes = {
        "reefer.setpoint_c": reefer["setpoint_c"],
        "reefer.supply_air_c": reefer["supply_air_c"],
        "reefer.return_air_c": reefer["return_air_c"],
        "reefer.cargo_probe_c": reefer["cargo_probe_c"],
        "vehicle.ambient_c": r["vehicle"]["ambient_c"],
    }
    for name, value in probes.items():
        if value is not None and not _finite_within(value, *limits.probe_c):
            return f"{name}={value} outside {limits.probe_c}"
    pos = r["position"]
    if pos is not None:
        if not _finite_within(pos["lat"], *limits.lat) or not _finite_within(
            pos["lon"], *limits.lon
        ):
            return f"position ({pos['lat']}, {pos['lon']}) outside the service area"
        if not _finite_within(pos["speed_kmh"], *limits.speed_kmh):
            return f"speed_kmh={pos['speed_kmh']} outside {limits.speed_kmh}"
    return None


def _key(raw: Mapping[str, Any]) -> str:
    for name in ("vehicle_id", "device_id"):
        value = raw.get(name)
        if isinstance(value, str) and ID_PATTERN.fullmatch(value):
            return value
    return "unknown"


DEFAULT_LIMITS = Limits()


def validate_reading(
    raw: Any, *, ingest_ms: int, keys: Mapping[str, bytes], limits: Limits = DEFAULT_LIMITS
) -> Accepted | Rejected:
    if not isinstance(raw, dict):
        return Rejected(Reason.MALFORMED, "reading must be a JSON object")
    key = _key(raw)

    unknown = set(raw) - _DEVICE_FIELDS - {SIGNATURE_FIELD}
    if unknown:
        return Rejected(Reason.MALFORMED, f"unknown fields: {sorted(unknown)}", key)
    if not isinstance(raw.get(SIGNATURE_FIELD), str):
        return Rejected(Reason.MALFORMED, "signature missing", key)
    try:
        record = {k: v for k, v in raw.items() if k != SIGNATURE_FIELD}
        record["event_id"] = uuid.UUID(str(raw.get("event_id")))
        record["event_time"] = _event_time(raw.get("event_time"))
        record["ingest_time"] = datetime.fromtimestamp(ingest_ms / 1000, UTC)
        validate(record, _PARSED, raise_errors=True)
    except (ValueError, TypeError, KeyError, ValidationError) as exc:
        return Rejected(Reason.MALFORMED, str(exc)[:300], key)

    for name in ("vehicle_id", "device_id", "boot_id"):
        if not ID_PATTERN.fullmatch(record[name]):
            return Rejected(Reason.BAD_ID, f"{name} must match {ID_PATTERN.pattern}", key)
    if isinstance(record["seq"], bool) or record["seq"] < 0:
        return Rejected(Reason.MALFORMED, "seq must be a non-negative integer", key)

    device_key = keys.get(record["device_id"])
    if device_key is None:
        return Rejected(Reason.UNKNOWN_DEVICE, f"no key registered for {record['device_id']}", key)
    try:
        expected = sign(raw, device_key)
    except ValueError as exc:  # NaN or infinity cannot be canonicalised
        return Rejected(Reason.MALFORMED, str(exc), key)
    if not hmac.compare_digest(expected, raw[SIGNATURE_FIELD]):
        return Rejected(Reason.BAD_SIGNATURE, "signature does not match the reading", key)

    if record["event_id"] != event_id(record["device_id"], record["boot_id"], record["seq"]):
        return Rejected(
            Reason.EVENT_ID_MISMATCH, "event_id is not uuid5(device_id, boot_id, seq)", key
        )

    problem = _range_problem(record, limits)
    if problem:
        return Rejected(Reason.OUT_OF_RANGE, problem, key)

    event_ms = round(record["event_time"].timestamp() * 1000)
    if event_ms > ingest_ms + limits.future_ms:
        return Rejected(
            Reason.FUTURE_EVENT,
            f"event_time is {(event_ms - ingest_ms) // 1000} s in the future",
            key,
        )
    if event_ms < ingest_ms - limits.max_age_ms:
        return Rejected(
            Reason.STALE_EVENT,
            f"event_time is {(ingest_ms - event_ms) // 86_400_000} days old",
            key,
        )

    return Accepted(record["vehicle_id"], record)
