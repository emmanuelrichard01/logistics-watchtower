"""HTTP behaviour: acknowledgement-gated 202, quarantine, retryable 503, metrics, readiness."""

import json
import time
from collections.abc import Sequence
from typing import Any

import pytest
from fastapi.testclient import TestClient
from watchtower_gateway.app import create_app
from watchtower_gateway.clock import FixedClock
from watchtower_gateway.publisher import KafkaPublisher, UnavailableError
from watchtower_gateway.validation import Accepted, Rejected

from .conftest import KEYS, NOW_MS, reading

URL = "/v1/telemetry:batch"


class FakePublisher:
    def __init__(self, fail: set[int] | None = None, unavailable: bool = False) -> None:
        self.fail = fail or set()
        self.unavailable = unavailable
        self.accepted: list[Accepted] = []
        self.rejected: list[tuple[Rejected, bytes, int]] = []

    def publish(
        self, accepted: Sequence[Accepted], rejected: Sequence[tuple[Rejected, bytes, int]]
    ) -> list[bool]:
        if self.unavailable:
            raise UnavailableError("broker down")
        self.accepted += accepted
        self.rejected += rejected
        return [i not in self.fail for i in range(len(accepted) + len(rejected))]

    def ready(self) -> tuple[bool, str]:
        return (not self.unavailable, "ok" if not self.unavailable else "down")


def client(publisher: Any, **kw: Any) -> TestClient:
    return TestClient(create_app(publisher=publisher, keys=KEYS, clock=FixedClock(NOW_MS), **kw))


def test_mixed_batch_is_202_with_per_item_results() -> None:
    pub = FakePublisher()
    bad = reading(seq=2)
    bad["reefer"]["cargo_probe_c"] = 999.0  # tampered: signature no longer matches
    res = client(pub).post(URL, json=[reading(seq=1), bad, reading(seq=3)])
    assert res.status_code == 202
    body = res.json()
    assert (body["accepted"], body["quarantined"]) == (2, 1)
    assert [r["status"] for r in body["results"]] == ["accepted", "quarantined", "accepted"]
    assert body["results"][1]["reason"] == "BAD_SIGNATURE"
    assert len(pub.accepted) == 2
    _, original, ingest_ms = pub.rejected[0]
    assert ingest_ms == NOW_MS
    assert json.loads(original)["seq"] == 2  # original item travels with the reason


def test_unacknowledged_record_makes_the_batch_retryable() -> None:
    res = client(FakePublisher(fail={1})).post(URL, json=[reading(seq=1), reading(seq=2)])
    assert res.status_code == 503
    assert res.headers["retry-after"] == "1"
    assert [r["status"] for r in res.json()["results"]] == ["accepted", "retryable"]


def test_broker_or_registry_unavailable_is_503_not_silent_loss() -> None:
    res = client(FakePublisher(unavailable=True)).post(URL, json=[reading()])
    assert res.status_code == 503
    assert res.json()["results"][0]["status"] == "retryable"


@pytest.mark.parametrize("body", [b"{not json", b'{"a": 1}', b'"text"'])
def test_non_array_bodies_are_400(body: bytes) -> None:
    assert client(FakePublisher()).post(URL, content=body).status_code == 400


def test_oversized_batch_is_413() -> None:
    assert (
        client(FakePublisher(), max_batch=2)
        .post(URL, json=[reading(seq=i) for i in range(3)])
        .status_code
        == 413
    )


def test_metrics_count_outcomes_by_reason() -> None:
    c = client(FakePublisher())
    bad = reading(seq=9)
    bad["position"]["speed_kmh"] = 500.0
    c.post(URL, json=[reading(seq=1), bad])
    text = c.get("/metrics").text
    assert 'wt_gateway_events_total{reason="",result="accepted"} 1.0' in text
    assert 'wt_gateway_events_total{reason="BAD_SIGNATURE",result="quarantined"} 1.0' in text
    assert "wt_gateway_request_seconds_count 1.0" in text


def test_health_endpoints() -> None:
    assert client(FakePublisher()).get("/health/live").json() == {"status": "ok"}
    assert client(FakePublisher()).get("/health/ready").status_code == 200
    assert client(FakePublisher(unavailable=True)).get("/health/ready").status_code == 503


class StubRegistry:
    """Registry stand-in: the outage under test is the broker's."""

    def register(self, subject: str, schema: dict[str, Any]) -> int:
        return 1

    def parsed(self, schema_id: int) -> Any:
        import fastavro
        from watchtower_contracts import load_schema

        return fastavro.parse_schema(load_schema("input_record", 1))

    def ping(self) -> bool:
        return True


def test_real_producer_with_no_broker_answers_503_within_the_timeout() -> None:
    # Nothing listens on port 1: librdkafka can never deliver. The gateway must
    # say so (503, device retries) rather than return 202 for data it lost.
    publisher = KafkaPublisher(
        bootstrap_servers="127.0.0.1:1",
        registry=StubRegistry(),  # type: ignore[arg-type]
        input_topic="wt.input.v1",
        quarantine_topic="telemetry.quarantine.v1",
        org_id="00000000-0000-4000-8000-000000000001",
        delivery_timeout_s=1.5,
    )
    started = time.monotonic()
    res = client(publisher).post(URL, json=[reading()])
    assert res.status_code == 503
    assert res.json()["results"][0]["status"] == "retryable"
    assert time.monotonic() - started < 10
    assert publisher.ready()[0] is False
