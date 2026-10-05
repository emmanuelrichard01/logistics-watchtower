"""HTTP surface of the ingest gateway (plan section 7, ADR-0021)."""

import json
import time
from collections.abc import Mapping
from typing import Any

from fastapi import FastAPI, Request
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import JSONResponse, PlainTextResponse
from prometheus_client import (
    CONTENT_TYPE_LATEST,
    CollectorRegistry,
    Counter,
    Histogram,
    generate_latest,
)

from watchtower_gateway.clock import Clock, SystemClock
from watchtower_gateway.publisher import Publisher, UnavailableError
from watchtower_gateway.validation import (
    DEFAULT_LIMITS,
    Accepted,
    Limits,
    Rejected,
    validate_reading,
)


def create_app(
    *,
    publisher: Publisher,
    keys: Mapping[str, bytes],
    max_batch: int = 500,
    clock: Clock | None = None,
    limits: Limits = DEFAULT_LIMITS,
) -> FastAPI:
    clock = clock or SystemClock()
    registry = CollectorRegistry()
    events = Counter(
        "wt_gateway_events", "Readings by outcome", ["result", "reason"], registry=registry
    )
    latency = Histogram(
        "wt_gateway_request_seconds",
        "Batch request latency, from receipt to broker acknowledgement",
        registry=registry,
        buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10),
    )
    app = FastAPI(title="Watchtower ingest gateway", version="2.0.0.dev0")

    @app.post("/v1/telemetry:batch")
    async def ingest(request: Request) -> JSONResponse:
        started = time.perf_counter()
        try:
            items = json.loads(await request.body())
        except (json.JSONDecodeError, UnicodeDecodeError):
            return JSONResponse({"error": "body must be a JSON array of readings"}, status_code=400)
        if not isinstance(items, list):
            return JSONResponse({"error": "body must be a JSON array of readings"}, status_code=400)
        if len(items) > max_batch:
            return JSONResponse(
                {"error": f"batch larger than {max_batch} readings"}, status_code=413
            )

        ingest_ms = clock.now_ms()
        outcomes = [
            validate_reading(item, ingest_ms=ingest_ms, keys=keys, limits=limits) for item in items
        ]
        accepted = [(i, o) for i, o in enumerate(outcomes) if isinstance(o, Accepted)]
        rejected = [(i, o) for i, o in enumerate(outcomes) if isinstance(o, Rejected)]
        originals = [
            json.dumps(items[i], separators=(",", ":"), ensure_ascii=False).encode()
            for i, _ in rejected
        ]

        try:
            delivered = await run_in_threadpool(
                publisher.publish,
                [o for _, o in accepted],
                [
                    (o, original, ingest_ms)
                    for (_, o), original in zip(rejected, originals, strict=True)
                ],
            )
        except UnavailableError as exc:
            delivered = [False] * len(items)
            detail = str(exc)
        else:
            detail = "broker did not acknowledge every record"

        results: list[dict[str, Any]] = [{} for _ in items]
        for slot, (i, o) in enumerate(accepted):
            ok = delivered[slot]
            results[i] = {
                "index": i,
                "status": "accepted" if ok else "retryable",
                "event_id": str(o.telemetry["event_id"]),
            }
            events.labels("accepted" if ok else "retryable", "").inc()
        for slot, (i, o) in enumerate(rejected, start=len(accepted)):
            ok = delivered[slot]
            results[i] = {
                "index": i,
                "status": "quarantined" if ok else "retryable",
                "reason": o.reason.value,
                "detail": o.detail,
            }
            events.labels("quarantined" if ok else "retryable", o.reason.value).inc()

        latency.observe(time.perf_counter() - started)
        all_delivered = all(delivered)
        body = {
            "accepted": sum(r["status"] == "accepted" for r in results),
            "quarantined": sum(r["status"] == "quarantined" for r in results),
            "results": results,
        }
        if all_delivered:
            return JSONResponse(body, status_code=202)
        # At-least-once: tell the device to resend the whole batch. Repeats of
        # anything that did land are harmless because event_id dedups downstream.
        return JSONResponse(
            {**body, "error": detail}, status_code=503, headers={"Retry-After": "1"}
        )

    @app.get("/health/live")
    def live() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/health/ready")
    async def ready() -> JSONResponse:
        ok, detail = await run_in_threadpool(publisher.ready)
        return JSONResponse(
            {"status": "ok" if ok else "unavailable", "detail": detail},
            status_code=200 if ok else 503,
        )

    @app.get("/metrics")
    def metrics() -> PlainTextResponse:
        return PlainTextResponse(generate_latest(registry).decode(), media_type=CONTENT_TYPE_LATEST)

    return app
