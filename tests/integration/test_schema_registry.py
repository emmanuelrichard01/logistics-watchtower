"""The telemetry contract round-trips through a real Redpanda Schema Registry and broker."""

import io
import json
import struct
import urllib.request
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

import fastavro
import pytest
from confluent_kafka import Consumer
from confluent_kafka.admin import AdminClient
from confluent_kafka.cimpl import NewTopic
from testcontainers.community.kafka import RedpandaContainer
from watchtower_contracts import load_schema
from watchtower_platform import make_producer

pytestmark = pytest.mark.integration

TOPIC = "telemetry.raw.v1"
SUBJECT = f"{TOPIC}-value"
GOLDEN = Path(__file__).parents[1] / "contract" / "golden" / "telemetry_event" / "v1.json"


def registry(method: str, url: str, body: dict[str, Any] | None = None) -> Any:
    request = urllib.request.Request(
        url,
        data=json.dumps(body).encode() if body is not None else None,
        method=method,
        headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
    )
    with urllib.request.urlopen(request, timeout=15) as response:
        return json.load(response)


def golden() -> dict[str, Any]:
    record: dict[str, Any] = json.loads(GOLDEN.read_text(encoding="utf-8"))
    record["event_id"] = uuid.UUID(record["event_id"])
    for key in ("event_time", "ingest_time"):
        record[key] = datetime.fromisoformat(record[key])
    return record


def encode(schema_id: int, schema: dict[str, Any], record: dict[str, Any]) -> bytes:
    """Confluent wire format: magic byte 0, 4-byte schema ID, Avro body."""
    body = io.BytesIO()
    fastavro.schemaless_writer(body, fastavro.parse_schema(schema), record)
    return b"\x00" + struct.pack(">I", schema_id) + body.getvalue()


def test_golden_event_roundtrips_through_registry_and_broker(redpanda: RedpandaContainer) -> None:
    url = redpanda.get_schema_registry_address()
    bootstrap = redpanda.get_bootstrap_server()
    schema = load_schema("telemetry_event")

    registry("PUT", f"{url}/config/{SUBJECT}", {"compatibility": "BACKWARD"})
    schema_id = registry(
        "POST", f"{url}/subjects/{SUBJECT}/versions", {"schema": json.dumps(schema)}
    )["id"]

    # Topics are created explicitly, never by a producer or consumer (v1 defect 17).
    admin = AdminClient({"bootstrap.servers": bootstrap})  # must outlive the future
    admin.create_topics([NewTopic(TOPIC, 3, 1)])[TOPIC].result(30)

    producer = make_producer(bootstrap)
    record = golden()
    producer.produce(
        TOPIC, key=record["vehicle_id"].encode(), value=encode(schema_id, schema, record)
    )
    assert producer.flush(30) == 0

    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": f"it-{uuid.uuid4()}",
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([TOPIC])
    try:
        message = None
        for _ in range(60):
            message = consumer.poll(1.0)
            if message is not None and message.error() is None:
                break
        assert message is not None, "no message within 60 s"
        assert message.error() is None
        raw = message.value()
        assert raw is not None
    finally:
        consumer.close()

    magic, wire_id = raw[0], struct.unpack(">I", raw[1:5])[0]
    assert (magic, wire_id) == (0, schema_id)
    writer = json.loads(registry("GET", f"{url}/schemas/ids/{wire_id}")["schema"])
    decoded = fastavro.schemaless_reader(io.BytesIO(raw[5:]), fastavro.parse_schema(writer), None)
    assert decoded == record


def test_registry_rejects_a_backward_incompatible_change(redpanda: RedpandaContainer) -> None:
    url = redpanda.get_schema_registry_address()
    subject = f"compat-check-{uuid.uuid4().hex[:8]}-value"
    schema = load_schema("telemetry_event")
    registry("PUT", f"{url}/config/{subject}", {"compatibility": "BACKWARD"})
    registry("POST", f"{url}/subjects/{subject}/versions", {"schema": json.dumps(schema)})

    breaking = {**schema, "fields": [*schema["fields"], {"name": "shipment_id", "type": "string"}]}
    compatible = {
        **schema,
        "fields": [
            *schema["fields"],
            {"name": "shipment_id", "type": ["null", "string"], "default": None},
        ],
    }
    check = f"{url}/compatibility/subjects/{subject}/versions/latest"
    assert registry("POST", check, {"schema": json.dumps(breaking)})["is_compatible"] is False
    assert registry("POST", check, {"schema": json.dumps(compatible)})["is_compatible"] is True
