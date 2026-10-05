"""Gateway end to end: HTTP batch in, Avro input records and quarantine records out, on a
real Redpanda broker and Schema Registry."""

import base64
import json
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from confluent_kafka import Consumer
from confluent_kafka.admin import AdminClient
from confluent_kafka.cimpl import NewTopic
from fastapi.testclient import TestClient
from testcontainers.community.kafka import RedpandaContainer
from watchtower_contracts import event_id
from watchtower_gateway.app import create_app
from watchtower_gateway.clock import FixedClock
from watchtower_gateway.publisher import KafkaPublisher
from watchtower_gateway.validation import sign
from watchtower_platform import SchemaRegistry, decode

pytestmark = pytest.mark.integration

INPUT = "wt.input.v1"
QUARANTINE = "telemetry.quarantine.v1"
NOW = datetime(2026, 10, 5, 7, 12, 45, tzinfo=UTC)
NOW_MS = round(NOW.timestamp() * 1000)
KEYS = {"EDGE-0101": b"dev-only-key-EDGE-0101", "EDGE-0202": b"dev-only-key-EDGE-0202"}


def reading(device: str, vehicle: str, seq: int, **reefer: Any) -> dict[str, Any]:
    r: dict[str, Any] = {
        "event_id": str(event_id(device, "b-9f3a1c2e7d40b812", seq)),
        "schema_version": 1,
        "vehicle_id": vehicle,
        "device_id": device,
        "boot_id": "b-9f3a1c2e7d40b812",
        "seq": seq,
        "event_time": (NOW - timedelta(seconds=60 - seq)).isoformat(),
        "position": {
            "lat": 7.38,
            "lon": 3.94,
            "speed_kmh": 70.0,
            "heading_deg": None,
            "gps_fix": "FIX_3D",
            "hdop": None,
        },
        "reefer": {
            "setpoint_c": -20.0,
            "supply_air_c": -21.0,
            "return_air_c": -19.5,
            "cargo_probe_c": -19.0,
            "compressor": "RUNNING",
            "fault_code": None,
            "defrost": False,
            "power_source": "ENGINE",
            "door": "CLOSED",
            **reefer,
        },
        "vehicle": {"fuel_pct": 60.0, "battery_v": 13.6, "ambient_c": 31.0},
        "link": {"signal_dbm": -90, "buffered": False},
    }
    r["signature"] = sign(r, KEYS[device])
    return r


def drain(bootstrap: str, topic: str, expected: int) -> list[Any]:
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": f"it-{uuid.uuid4()}",
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([topic])
    out: list[Any] = []
    try:
        for _ in range(60):
            msg = consumer.poll(1.0)
            if msg is not None and msg.error() is None:
                out.append(msg)
            if len(out) >= expected and msg is None:
                break
    finally:
        consumer.close()
    return out


def test_batch_lands_as_input_records_and_quarantine(redpanda: RedpandaContainer) -> None:
    bootstrap = redpanda.get_bootstrap_server()
    admin = AdminClient({"bootstrap.servers": bootstrap})  # must outlive the futures
    for future in admin.create_topics([NewTopic(INPUT, 3, 1), NewTopic(QUARANTINE, 1, 1)]).values():
        future.result(30)

    registry = SchemaRegistry(redpanda.get_schema_registry_address())
    publisher = KafkaPublisher(
        bootstrap_servers=bootstrap,
        registry=registry,
        input_topic=INPUT,
        quarantine_topic=QUARANTINE,
        org_id="00000000-0000-4000-8000-000000000001",
        delivery_timeout_s=15,
    )
    client = TestClient(create_app(publisher=publisher, keys=KEYS, clock=FixedClock(NOW_MS)))

    good = [reading("EDGE-0101", "TRK-101", s) for s in (1, 2, 3)] + [
        reading("EDGE-0202", "TRK-202", s) for s in (1, 2)
    ]
    tampered = reading("EDGE-0101", "TRK-101", 4)
    tampered["reefer"]["cargo_probe_c"] = -30.0  # changed after signing
    impossible = reading("EDGE-0202", "TRK-202", 3, cargo_probe_c=120.0)
    batch = [*good, tampered, impossible]

    res = client.post("/v1/telemetry:batch", json=batch)
    assert res.status_code == 202, res.text
    assert (res.json()["accepted"], res.json()["quarantined"]) == (5, 2)
    assert client.get("/health/ready").status_code == 200

    records = drain(bootstrap, INPUT, 5)
    assert len(records) == 5
    by_key: dict[str, list[int]] = {}
    for msg in records:
        _, record = decode(msg.value(), registry, return_record_name=True)
        branch, payload = record["payload"]
        assert record["kind"] == "TELEMETRY"
        assert branch == "io.watchtower.telemetry.TelemetryEvent"
        assert (
            record["record_id"]
            == payload["event_id"]
            == event_id(payload["device_id"], payload["boot_id"], payload["seq"])
        )
        assert msg.key().decode() == record["vehicle_id"] == payload["vehicle_id"]
        assert payload["ingest_time"] == NOW
        by_key.setdefault(record["vehicle_id"], []).append(payload["seq"])
    # One vehicle, one partition: its records keep their order.
    assert by_key == {"TRK-101": [1, 2, 3], "TRK-202": [1, 2]}

    quarantined = [json.loads(m.value()) for m in drain(bootstrap, QUARANTINE, 2)]
    assert sorted(q["reason"] for q in quarantined) == ["BAD_SIGNATURE", "OUT_OF_RANGE"]
    originals = sorted(json.loads(base64.b64decode(q["original_b64"]))["seq"] for q in quarantined)
    assert originals == [3, 4]

    # At-least-once: a resent batch is accepted again with identical record IDs,
    # so downstream deduplication by event_id absorbs it.
    again = client.post("/v1/telemetry:batch", json=good)
    assert again.status_code == 202
    first_ids = [r["event_id"] for r in res.json()["results"][:5]]
    assert [r["event_id"] for r in again.json()["results"]] == first_ids
