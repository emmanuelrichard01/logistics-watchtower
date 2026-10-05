"""Deterministic event identity (ADR-0002)."""

import re
import uuid

# Fixed forever. Changing it changes every event_id, and replayed or retransmitted
# events would no longer deduplicate against the originals.
# Derived once as uuid5(NAMESPACE_URL, "https://github.com/emmanuelrichard01/logistics-watchtower/events").
EVENT_NAMESPACE = uuid.UUID("6d946293-5eb6-55b9-99e9-709dd0dacd32")

# Excludes the "/" separator, so ("a/b", "c") and ("a", "b/c") can't collide, and
# lone surrogates, which JSON can carry but UTF-8 can't encode.
ID_PATTERN = re.compile(r"[A-Za-z0-9._:-]{1,64}")


def event_id(device_id: str, boot_id: str, seq: int) -> uuid.UUID:
    """Return the UUIDv5 identifying one reading from one device boot.

    A retransmission or replay of the same reading carries the same ID, which is
    what makes idempotent processing possible.
    """
    for name, value in (("device_id", device_id), ("boot_id", boot_id)):
        if not ID_PATTERN.fullmatch(value):
            raise ValueError(f"{name} must match {ID_PATTERN.pattern}, got {value!r}")
    if seq < 0:
        raise ValueError(f"seq must be non-negative, got {seq}")
    return uuid.uuid5(EVENT_NAMESPACE, f"{device_id}/{boot_id}/{seq}")
