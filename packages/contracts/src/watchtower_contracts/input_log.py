"""Record identity for the processor's single ordered input log, wt.input.v1 (ADR-0005).

Every record carries a deterministic ``record_id`` so that a record produced twice
(a retry, a replay, a relay restart) is recognisably the same record. Telemetry
reuses its ``event_id`` (ADR-0002); every other kind is a UUIDv5 of a documented
natural key, built here and nowhere else.
"""

import uuid
from datetime import datetime

from watchtower_contracts.identity import ID_PATTERN

# Fixed forever, like EVENT_NAMESPACE: changing it changes every control record's ID.
# Derived once as uuid5(NAMESPACE_URL, "https://github.com/emmanuelrichard01/logistics-watchtower/input").
INPUT_NAMESPACE = uuid.UUID("f07e0521-c5bc-50e7-8a24-ecb40a06acbb")

INPUT_TOPIC = "wt.input.v1"

# Records that apply to the whole fleet (rule activations) are written to every
# partition with this key, so one copy lands in each partition's order.
BROADCAST_VEHICLE = "*"


def _epoch_ms(t: datetime) -> int:
    if t.tzinfo is None:
        raise ValueError("timestamps must be timezone-aware")
    return round(t.timestamp() * 1000)


def _key_part(name: str, value: str) -> str:
    if not ID_PATTERN.fullmatch(value):
        raise ValueError(f"{name} must match {ID_PATTERN.pattern}, got {value!r}")
    return value


def _derive(*parts: str) -> uuid.UUID:
    return uuid.uuid5(INPUT_NAMESPACE, "/".join(parts))


def telemetry_record_id(event_id: uuid.UUID) -> uuid.UUID:
    """Telemetry records are identified by the reading's own event_id."""
    return event_id


def tick_record_id(vehicle_id: str, tick_time: datetime) -> uuid.UUID:
    """Natural key: ``tick/{vehicle_id}/{tick_time epoch ms}``."""
    return _derive("tick", _key_part("vehicle_id", vehicle_id), str(_epoch_ms(tick_time)))


def rules_record_id(rule_version: int, partition: int) -> uuid.UUID:
    """Natural key: ``rules/{rule_version}/p{partition}``; one copy per partition."""
    if rule_version < 0 or partition < 0:
        raise ValueError("rule_version and partition must be non-negative")
    return _derive("rules", str(rule_version), f"p{partition}")


def assignment_record_id(
    shipment_id: str, vehicle_id: str, change: str, valid_from: datetime
) -> uuid.UUID:
    """Natural key: ``assignment/{shipment_id}/{vehicle_id}/{change}/{valid_from epoch ms}``."""
    return _derive(
        "assignment",
        _key_part("shipment_id", shipment_id),
        _key_part("vehicle_id", vehicle_id),
        _key_part("change", change),
        str(_epoch_ms(valid_from)),
    )


def command_record_id(intervention_id: uuid.UUID) -> uuid.UUID:
    """Natural key: ``command/{intervention_id}``. The interventions row is the
    system of record for the command and is itself idempotent (ADR-0006)."""
    return _derive("command", str(intervention_id))
