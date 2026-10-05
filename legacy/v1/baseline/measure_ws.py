"""Measure v1 delivered throughput and producer-to-WebSocket latency.

Runs inside the Compose network so the producer's timestamps and this client's
clock come from the same kernel. Latency starts when the producer builds the
event (its `timestamp` field) and stops when the frame arrives here.
"""

import asyncio
import json
import statistics
import sys
import time
from datetime import UTC, datetime

import websockets


async def connect(url: str):
    for _ in range(120):
        try:
            return await websockets.connect(url, max_size=None)
        except OSError:
            await asyncio.sleep(1)
    raise SystemExit(f"could not connect to {url}")


def summary(values: list[float]) -> dict[str, float | int]:
    if len(values) < 2:
        return {"n": len(values)}
    q = statistics.quantiles(values, n=100, method="inclusive")
    return {"n": len(values), "p50": q[49], "p95": q[94], "p99": q[98], "max": max(values)}


async def main(url: str, fleet: int, warmup: float, duration: float) -> None:
    latency: dict[str, list[float]] = {"telemetry": [], "alerts": []}
    ws = await connect(url)
    deadline = time.monotonic() + warmup
    while time.monotonic() < deadline:
        await ws.recv()
    start = time.monotonic()
    closed = None
    while time.monotonic() - start < duration:
        try:
            msg = json.loads(await ws.recv())
        except websockets.ConnectionClosed as exc:
            closed = f"{exc.rcvd.code if exc.rcvd else 'none'} {exc}"
            break
        received = datetime.now(UTC)
        sent = datetime.fromisoformat(msg["timestamp"].replace("Z", "+00:00"))
        if sent.tzinfo is None:
            sent = sent.replace(tzinfo=UTC)
        latency[msg.get("topic", "unknown")].append((received - sent).total_seconds() * 1000)
    elapsed = time.monotonic() - start
    await ws.close()
    print(
        json.dumps(
            {
                "fleet_size": fleet,
                "duration_s": round(elapsed, 1),
                "connection_closed": closed,
                "expected_telemetry_per_s": fleet / 0.5,
                "delivered_telemetry_per_s": len(latency["telemetry"]) / elapsed,
                "latency_ms": {topic: summary(v) for topic, v in latency.items()},
            }
        )
    )


if __name__ == "__main__":
    url, fleet, warmup, duration = sys.argv[1], int(sys.argv[2]), float(sys.argv[3]), float(sys.argv[4])
    asyncio.run(main(url, fleet, warmup, duration))
