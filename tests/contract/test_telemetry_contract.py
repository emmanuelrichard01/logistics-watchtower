"""Golden-payload and compatibility tests for the telemetry contract (plan section 7)."""

import io
import json
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

import fastavro
import pytest
from watchtower_contracts import event_id, load_schema, schema_versions

SUBJECT = "telemetry_event"
GOLDEN = Path(__file__).parent / "golden" / SUBJECT


def golden(version: int) -> dict[str, Any]:
    record: dict[str, Any] = json.loads((GOLDEN / f"v{version}.json").read_text(encoding="utf-8"))
    record["event_id"] = uuid.UUID(record["event_id"])
    for key in ("event_time", "ingest_time"):
        record[key] = datetime.fromisoformat(record[key])
    return record


def roundtrip(record: dict[str, Any], writer: dict[str, Any], reader: dict[str, Any]) -> Any:
    buffer = io.BytesIO()
    fastavro.schemaless_writer(buffer, fastavro.parse_schema(writer), record)
    buffer.seek(0)
    return fastavro.schemaless_reader(
        buffer, fastavro.parse_schema(writer), fastavro.parse_schema(reader)
    )


def test_every_released_version_has_a_golden_payload() -> None:
    assert [int(p.stem[1:]) for p in sorted(GOLDEN.glob("v*.json"))] == schema_versions(SUBJECT)


@pytest.mark.parametrize("version", schema_versions(SUBJECT))
def test_golden_payload_roundtrips_through_its_own_schema(version: int) -> None:
    record = golden(version)
    schema = load_schema(SUBJECT, version)
    assert roundtrip(record, schema, schema) == record


@pytest.mark.parametrize("version", schema_versions(SUBJECT))
def test_latest_schema_reads_data_written_by_every_older_version(version: int) -> None:
    # BACKWARD compatibility: consumers upgrade first, so the newest schema must
    # read anything already sitting in a topic.
    roundtrip(golden(version), load_schema(SUBJECT, version), load_schema(SUBJECT))


@pytest.mark.parametrize("version", schema_versions(SUBJECT))
def test_golden_event_id_matches_the_identity_function(version: int) -> None:
    record = golden(version)
    assert record["event_id"] == event_id(record["device_id"], record["boot_id"], record["seq"])
