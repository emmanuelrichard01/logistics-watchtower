"""Golden payloads, identity and compatibility for the input log contract (ADR-0005)."""

import io
import json
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import fastavro
import pytest
from watchtower_contracts import event_id, load_schema, schema_versions
from watchtower_contracts.input_log import (
    INPUT_NAMESPACE,
    assignment_record_id,
    command_record_id,
    rules_record_id,
    telemetry_record_id,
    tick_record_id,
)

SUBJECT = "input_record"
GOLDEN = Path(__file__).parent / "golden" / SUBJECT

BRANCH_FOR_KIND = {
    "TELEMETRY": "io.watchtower.telemetry.TelemetryEvent",
    "TICK": "io.watchtower.input.Tick",
    "RULES_ACTIVATED": "io.watchtower.input.RulesActivated",
    "ASSIGNMENT_CHANGED": "io.watchtower.input.AssignmentChanged",
    "OPERATOR_COMMAND": "io.watchtower.input.OperatorCommand",
}


def goldens(version: int) -> dict[str, dict[str, Any]]:
    schema = fastavro.parse_schema(load_schema(SUBJECT, version))
    out: dict[str, dict[str, Any]] = {}
    for path in sorted((GOLDEN / f"v{version}").glob("*.json")):
        # Goldens are pretty-printed for review; Avro JSON reading wants one record per line.
        line = json.dumps(json.loads(path.read_text(encoding="utf-8")))
        out[path.stem] = next(iter(fastavro.json_reader(io.StringIO(line), schema)))
    return out


def binary(
    record: dict[str, Any], writer: dict[str, Any], reader: dict[str, Any], *, named: bool = False
) -> Any:
    buf = io.BytesIO()
    fastavro.schemaless_writer(buf, fastavro.parse_schema(writer), record)
    buf.seek(0)
    return fastavro.schemaless_reader(
        buf, fastavro.parse_schema(writer), fastavro.parse_schema(reader), return_record_name=named
    )


def test_every_version_has_a_golden_for_every_kind() -> None:
    assert [int(p.name[1:]) for p in sorted(GOLDEN.glob("v*"))] == schema_versions(SUBJECT)
    for version in schema_versions(SUBJECT):
        kinds = {g["kind"] for g in goldens(version).values()}
        assert kinds == set(BRANCH_FOR_KIND)


@pytest.mark.parametrize("version", schema_versions(SUBJECT))
def test_goldens_roundtrip_and_kind_matches_payload_branch(version: int) -> None:
    schema = load_schema(SUBJECT, version)
    for name, record in goldens(version).items():
        assert binary(record, schema, schema) == record, name
        branch, _ = binary(record, schema, schema, named=True)["payload"]
        assert branch == BRANCH_FOR_KIND[record["kind"]], name


@pytest.mark.parametrize("version", schema_versions(SUBJECT))
def test_latest_schema_reads_every_older_version(version: int) -> None:
    for record in goldens(version).values():
        binary(record, load_schema(SUBJECT, version), load_schema(SUBJECT))


def test_embedded_telemetry_event_is_the_telemetry_contract() -> None:
    # Avro can't import across files without registry references, so the input
    # log embeds TelemetryEvent. This keeps the copy from drifting.
    payload = next(f for f in load_schema(SUBJECT)["fields"] if f["name"] == "payload")
    embedded = next(b for b in payload["type"] if b.get("name") == "TelemetryEvent")
    assert embedded == load_schema("telemetry_event")


def test_golden_record_ids_follow_the_documented_derivation() -> None:
    g = goldens(1)
    tel = g["telemetry"]["payload"]
    assert g["telemetry"]["record_id"] == telemetry_record_id(tel["event_id"])
    assert tel["event_id"] == event_id(tel["device_id"], tel["boot_id"], tel["seq"])
    tick = g["tick"]
    assert tick["record_id"] == tick_record_id(tick["vehicle_id"], tick["payload"]["tick_time"])
    rules = g["rules_activated"]["payload"]
    assert g["rules_activated"]["record_id"] == rules_record_id(
        rules["rule_version"], rules["partition"]
    )
    a = g["assignment_changed"]
    p = a["payload"]
    assert a["record_id"] == assignment_record_id(
        p["shipment_id"], a["vehicle_id"], p["change"], p["valid_from"]
    )
    c = g["operator_command"]
    assert c["record_id"] == command_record_id(c["payload"]["intervention_id"])


def test_identities_are_pinned() -> None:
    # If these change, every stored control record stops deduplicating on replay.
    assert (
        uuid.uuid5(
            uuid.NAMESPACE_URL, "https://github.com/emmanuelrichard01/logistics-watchtower/input"
        )
        == INPUT_NAMESPACE
    )
    t0 = datetime(2026, 10, 5, 7, 12, 45, tzinfo=UTC)
    assert tick_record_id("TRK-101", t0) == uuid.UUID("1af78463-552d-5cc9-9eb1-0540a158d728")
    assert rules_record_id(3, 0) != rules_record_id(3, 1)


@pytest.mark.parametrize(
    "call",
    [
        lambda: tick_record_id("TRK/101", datetime(2026, 1, 1, tzinfo=UTC)),
        lambda: tick_record_id("TRK-101", datetime(2026, 1, 1)),
        lambda: rules_record_id(-1, 0),
        lambda: assignment_record_id(
            "SHP 1", "TRK-101", "ASSIGNED", datetime(2026, 1, 1, tzinfo=UTC)
        ),
    ],
)
def test_ambiguous_or_naive_keys_are_rejected(call: Any) -> None:
    with pytest.raises(ValueError, match=r"must"):
        call()
