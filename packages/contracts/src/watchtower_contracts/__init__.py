"""Watchtower event contracts: Avro schemas and event identity."""

import json
from importlib.resources import files
from typing import Any

from watchtower_contracts.identity import EVENT_NAMESPACE, event_id

__all__ = ["EVENT_NAMESPACE", "event_id", "load_schema", "schema_versions"]


def schema_versions(subject: str) -> list[int]:
    """Released versions of a schema subject, oldest first."""
    directory = files(__package__) / "schemas" / subject
    return sorted(int(p.name[1:-5]) for p in directory.iterdir() if p.name.endswith(".avsc"))


def load_schema(subject: str, version: int | None = None) -> dict[str, Any]:
    """Load a schema as parsed JSON; the latest version when ``version`` is None."""
    version = version if version is not None else schema_versions(subject)[-1]
    path = files(__package__) / "schemas" / subject / f"v{version}.avsc"
    schema: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
    return schema
