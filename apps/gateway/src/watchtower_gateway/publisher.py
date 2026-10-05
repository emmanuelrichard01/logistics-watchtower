"""Producing accepted readings to wt.input.v1 and rejections to quarantine.

A batch is answered only after the broker has acknowledged every record in it
(at-least-once, ADR-0021). Anything unacknowledged makes the whole batch
retryable: the device resends, and event_id makes the repeats harmless.
"""

import base64
import json
import threading
from collections.abc import Sequence
from typing import Any, Protocol

from confluent_kafka import KafkaError, Message, Producer
from watchtower_contracts import load_schema
from watchtower_contracts.input_log import telemetry_record_id
from watchtower_platform import RegistryUnavailableError, SchemaRegistry, encode, make_producer

from watchtower_gateway.validation import Accepted, Rejected

_INPUT_SCHEMA = load_schema("input_record", 1)
_TELEMETRY_BRANCH = "io.watchtower.telemetry.TelemetryEvent"


class UnavailableError(Exception):
    """Broker or registry could not take the batch; the caller should retry."""


class Publisher(Protocol):
    def publish(
        self, accepted: Sequence[Accepted], rejected: Sequence[tuple[Rejected, bytes, int]]
    ) -> list[bool]:
        """Return one delivered flag per record: accepted first, then rejected."""
        ...

    def ready(self) -> tuple[bool, str]: ...


def input_record(accepted: Accepted, org_id: str) -> dict[str, Any]:
    t = accepted.telemetry
    return {
        "record_id": telemetry_record_id(t["event_id"]),
        "schema_version": 1,
        "vehicle_id": accepted.vehicle_id,
        "org_id": org_id,
        "kind": "TELEMETRY",
        "payload": (_TELEMETRY_BRANCH, t),
    }


def quarantine_value(rejected: Rejected, original: bytes, ingest_ms: int) -> bytes:
    # JSON, not Avro: a quarantined reading failed the contract, so the original
    # bytes travel opaquely next to the reason code (ADR-0021).
    return json.dumps(
        {
            "reason": rejected.reason.value,
            "detail": rejected.detail,
            "ingest_time_ms": ingest_ms,
            "original_b64": base64.b64encode(original).decode("ascii"),
        },
        separators=(",", ":"),
    ).encode()


class KafkaPublisher:
    def __init__(
        self,
        *,
        bootstrap_servers: str,
        registry: SchemaRegistry,
        input_topic: str,
        quarantine_topic: str,
        org_id: str,
        delivery_timeout_s: float,
        producer: Producer | None = None,
    ) -> None:
        self._registry = registry
        self._input_topic = input_topic
        self._quarantine_topic = quarantine_topic
        self._org_id = org_id
        self._timeout = delivery_timeout_s
        timeout_ms = str(int(delivery_timeout_s * 1000))
        self._producer = producer or make_producer(
            bootstrap_servers, **{"message.timeout.ms": timeout_ms, "linger.ms": "5"}
        )
        # One batch in flight at a time keeps "acknowledged" unambiguous per request.
        self._lock = threading.Lock()

    def _schema_id(self) -> tuple[int, Any]:
        subject = f"{self._input_topic}-value"
        schema_id = self._registry.register(subject, _INPUT_SCHEMA)
        return schema_id, self._registry.parsed(schema_id)

    def publish(
        self, accepted: Sequence[Accepted], rejected: Sequence[tuple[Rejected, bytes, int]]
    ) -> list[bool]:
        try:
            schema_id, parsed = self._schema_id() if accepted else (0, None)
        except RegistryUnavailableError as exc:
            raise UnavailableError(str(exc)) from exc

        delivered = [False] * (len(accepted) + len(rejected))

        def on_delivery(index: int):
            def callback(err: KafkaError | None, _msg: Message) -> None:
                delivered[index] = err is None

            return callback

        with self._lock:
            try:
                for i, a in enumerate(accepted):
                    value = encode(schema_id, parsed, input_record(a, self._org_id))
                    self._producer.produce(
                        self._input_topic,
                        key=a.vehicle_id.encode(),
                        value=value,
                        on_delivery=on_delivery(i),
                    )
                for j, (r, original, ingest_ms) in enumerate(rejected):
                    self._producer.produce(
                        self._quarantine_topic,
                        key=r.key.encode(),
                        value=quarantine_value(r, original, ingest_ms),
                        on_delivery=on_delivery(len(accepted) + j),
                    )
            except BufferError as exc:
                raise UnavailableError(f"producer queue full: {exc}") from exc
            # message.timeout.ms bounds every message, so flush returns with each
            # one either acknowledged or failed, never silently queued.
            self._producer.flush(self._timeout + 2)
        return delivered

    def ready(self) -> tuple[bool, str]:
        try:
            meta = self._producer.list_topics(self._input_topic, timeout=2)
            topic = meta.topics.get(self._input_topic)
            if topic is None or topic.error is not None or not topic.partitions:
                return False, f"topic {self._input_topic} unavailable"
        except Exception as exc:  # librdkafka raises KafkaException subclasses
            return False, f"broker unreachable: {exc}"
        if not self._registry.ping():
            return False, "schema registry unreachable"
        return True, "ok"
