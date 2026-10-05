"""Live mode: run a scenario paced against the wall clock and stream readings to a sink.

- **Pacing.** One simulated second takes ``1 / speed`` wall seconds (1x, 10x, 60x...). If the
  host falls behind, the loop runs flat out until it catches up; it never skips ticks, so
  output stays identical to a batch run of the same scenario.
- **Delivery timing.** A reading leaves for the sink when the virtual clock passes the moment
  it would reach the gateway (``ingest_ms``), so link latency, dead-zone buffering and replay
  bursts arrive in the order and rhythm a real fleet would produce.
- **Anchoring.** The gateway rejects event times more than 5 minutes ahead of its clock or
  older than 7 days. At 60x, virtual time outruns the wall clock, so with ``anchor="now"`` the
  scenario is shifted to *end* at the wall-clock moment the run will finish. Earlier
  readings are history replayed fast, never the future. The calendar date shifts with it
  (season and sun angle follow the new date). ``anchor="scenario"`` keeps the scenario's own
  dates, which is right for file and stdout sinks.
- **Control API** (optional): ``POST /inject`` queues a fault or event for a vehicle, applied on
  the simulation thread at the next tick; ``GET /fleet`` returns each vehicle's latest state.
  It powers the console's demo chaos panel. It binds to localhost and allows CORS only from
  the listed origins.
"""

import json
import math
import queue
import random
import sys
import threading
import time
import urllib.error
import urllib.request
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field, replace
from heapq import heappop, heappush
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Protocol, TextIO

from watchtower_simulator.clock import iso, to_ms
from watchtower_simulator.device import Delivery
from watchtower_simulator.engine import Simulation
from watchtower_simulator.scenario import Scenario

INJECTABLE = frozenset(
    {
        "compressor_health",
        "compressor_fault",
        "door_open",
        "stop",
        "defrost",
        "link_outage",
        "reboot",
        "breakdown",
        "tyre_blowout",
        "hijack",
        "sensor_fault",
    }
)


# -- wire format -------------------------------------------------------------------------


def device_reading(reading: Mapping[str, Any]) -> dict[str, Any]:
    """The reading as a device sends it: the telemetry contract without ``ingest_time``
    (the gateway stamps that), with ``event_time`` as ISO-8601 UTC."""
    out = {k: v for k, v in reading.items() if k != "ingest_time"}
    out["event_time"] = iso(to_ms(out["event_time"]))
    return out


def load_keys(path: Path) -> dict[str, bytes]:
    raw: dict[str, str] = json.loads(path.read_text(encoding="utf-8"))
    return {device: key.encode("utf-8") for device, key in raw.items()}


def assign_devices(scenario: Scenario, keys: Mapping[str, bytes]) -> Scenario:
    """Give every vehicle a device that has a registered key. Vehicles whose device already has
    one keep it; the rest take the unused keys in sorted order."""
    spare = sorted(set(keys) - {v.device_id for v in scenario.fleet})
    fleet = []
    for v in scenario.fleet:
        if v.device_id in keys:
            fleet.append(v)
            continue
        if not spare:
            raise ValueError(
                f"no device key left for {v.vehicle_id}: add one to the gateway's device-keys file"
            )
        fleet.append(replace(v, device_id=spare.pop(0)))
    return replace(scenario, fleet=tuple(fleet))


# -- sinks -------------------------------------------------------------------------------


class Sink(Protocol):
    def send(self, readings: list[dict[str, Any]]) -> None: ...
    def close(self) -> None: ...


@dataclass
class StreamSink:
    """JSON lines to a text stream (stdout, or a file)."""

    stream: TextIO

    def send(self, readings: list[dict[str, Any]]) -> None:
        for r in readings:
            self.stream.write(json.dumps(r, separators=(",", ":"), ensure_ascii=False) + "\n")
        self.stream.flush()

    def close(self) -> None:
        if self.stream is not sys.stdout:
            self.stream.close()


@dataclass
class SinkStats:
    sent_batches: int = 0
    accepted: int = 0
    quarantined: int = 0
    retries: int = 0
    dropped: int = 0  # discarded because the outbound queue was full
    quarantine_reasons: dict[str, int] = field(default_factory=lambda: {})


class HttpSink:
    """Signed batches to the ingest gateway's ``POST /v1/telemetry:batch``.

    Readings queue in memory and a background thread posts them in batches, so a slow or
    unavailable gateway never stalls the simulation. A 503 or a connection failure retries the
    whole batch with exponential backoff and full jitter (at-least-once; the gateway and
    processor deduplicate by event_id). Quarantined readings are counted, not retried: the
    gateway has judged them, and resending would not change the verdict. When the queue is
    full, the oldest readings are dropped and counted, like a device's ring buffer.
    """

    def __init__(
        self,
        url: str,
        keys: Mapping[str, bytes],
        *,
        batch: int = 200,
        queue_limit: int = 50_000,
        base_backoff_s: float = 0.5,
        max_backoff_s: float = 30.0,
        timeout_s: float = 10.0,
        rng: random.Random | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        from watchtower_gateway.validation import sign  # the gateway's own reference signer

        self._sign = sign
        self.url = url.rstrip("/") + "/v1/telemetry:batch"
        self.keys = keys
        self.batch = batch
        self.base_backoff_s = base_backoff_s
        self.max_backoff_s = max_backoff_s
        self.timeout_s = timeout_s
        self.rng = rng or random.Random(0)
        self.sleep = sleep
        self.stats = SinkStats()
        self._queue: queue.Queue[dict[str, Any] | None] = queue.Queue()
        self._limit = queue_limit
        self._thread = threading.Thread(target=self._worker, name="wt-sim-http-sink", daemon=True)
        self._thread.start()

    def send(self, readings: list[dict[str, Any]]) -> None:
        for r in readings:
            if self._queue.qsize() >= self._limit:
                try:
                    self._queue.get_nowait()
                    self.stats.dropped += 1
                except queue.Empty:
                    pass
            signed = {**r, "signature": self._sign(r, self.keys[r["device_id"]])}
            self._queue.put(signed)

    def close(self) -> None:
        """Flush everything queued, then stop."""
        self._queue.put(None)
        self._thread.join()

    def _worker(self) -> None:
        done = False
        while not done:
            first = self._queue.get()
            if first is None:
                break
            batch = [first]
            while len(batch) < self.batch:
                try:
                    item = self._queue.get_nowait()
                except queue.Empty:
                    break
                if item is None:
                    done = True
                    break
                batch.append(item)
            self._post_until_delivered(batch)

    def _post_until_delivered(self, batch: list[dict[str, Any]]) -> None:
        body = json.dumps(batch, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
        attempt = 0
        while True:
            status, payload = self._post(body)
            if status == 202:
                self.stats.sent_batches += 1
                self.stats.accepted += payload.get("accepted", 0)
                self.stats.quarantined += payload.get("quarantined", 0)
                for item in payload.get("results", []):
                    if item.get("status") == "quarantined":
                        reason = item.get("reason", "UNKNOWN")
                        self.stats.quarantine_reasons[reason] = (
                            self.stats.quarantine_reasons.get(reason, 0) + 1
                        )
                return
            if status is not None and status not in (429, 503) and status < 500:
                # 400 or 413 is a client bug; retrying the same bytes cannot succeed.
                raise RuntimeError(f"gateway rejected the batch with {status}: {payload}")
            attempt += 1
            self.stats.retries += 1
            cap = min(self.max_backoff_s, self.base_backoff_s * 2**attempt)
            self.sleep(self.rng.uniform(0, cap))  # full jitter

    def _post(self, body: bytes) -> tuple[int | None, dict[str, Any]]:
        request = urllib.request.Request(
            self.url, data=body, method="POST", headers={"Content-Type": "application/json"}
        )
        try:
            with urllib.request.urlopen(request, timeout=self.timeout_s) as resp:
                return resp.status, json.loads(resp.read() or b"{}")
        except urllib.error.HTTPError as exc:
            try:
                payload = json.loads(exc.read() or b"{}")
            except json.JSONDecodeError:
                payload = {}
            return exc.code, payload
        except (urllib.error.URLError, TimeoutError, ConnectionError):
            return None, {}


# -- the paced runner --------------------------------------------------------------------


class Clock(Protocol):
    def monotonic(self) -> float: ...
    def sleep(self, seconds: float) -> None: ...
    def now_ms(self) -> int: ...


class WallClock:
    def monotonic(self) -> float:
        return time.monotonic()

    def sleep(self, seconds: float) -> None:
        time.sleep(seconds)

    def now_ms(self) -> int:
        return time.time_ns() // 1_000_000


def anchored(scenario: Scenario, speed: float, now_ms: int) -> Scenario:
    """Shift the scenario so its last simulated moment lands when the paced run finishes."""
    wall_ms = scenario.duration_ms / speed
    start = math.floor(now_ms + wall_ms - scenario.duration_ms)
    return replace(scenario, start_ms=start - start % 1000)


@dataclass
class Injection:
    vehicle: str
    type: str
    params: dict[str, Any]


class LiveRunner:
    def __init__(
        self, scenario: Scenario, sink: Sink, *, speed: float = 1.0, clock: Clock | None = None
    ) -> None:
        if speed <= 0:
            raise ValueError("speed must be positive")
        self.sim = Simulation(scenario)
        self.sink = sink
        self.speed = speed
        self.clock = clock or WallClock()
        self.now_ms = scenario.start_ms
        self.injections: queue.Queue[Injection] = queue.Queue()
        self._pending: list[tuple[int, int, str, Delivery]] = []
        self._counter = 0
        self._lock = threading.Lock()
        self._latest: dict[str, dict[str, Any]] = {}
        self.stop_event = threading.Event()
        self.errors: list[str] = []

    # Thread-safe views for the control API.
    def fleet(self) -> dict[str, Any]:
        with self._lock:
            return {
                "t": iso(self.now_ms),
                "speed": self.speed,
                "vehicles": list(self._latest.values()),
                "errors": self.errors[-20:],
            }

    def queue_injection(self, injection: Injection) -> None:
        if injection.vehicle not in self.sim.vehicles:
            raise ValueError(f"unknown vehicle {injection.vehicle!r}")
        if injection.type not in INJECTABLE:
            raise ValueError(f"cannot inject {injection.type!r}; choose from {sorted(INJECTABLE)}")
        self.injections.put(injection)

    def run(self) -> None:
        s = self.sim.scenario
        step = self.sim.step_ms
        wall0 = self.clock.monotonic()
        for now in range(s.start_ms, s.start_ms + s.duration_ms, step):
            if self.stop_event.is_set():
                break
            due = wall0 + (now - s.start_ms) / 1000 / self.speed
            ahead = due - self.clock.monotonic()
            if ahead > 0:
                self.clock.sleep(ahead)
            self.now_ms = now
            while not self.injections.empty():
                inj = self.injections.get_nowait()
                try:
                    self.sim.inject(inj.vehicle, inj.type, inj.params, now)
                except (ValueError, KeyError, TypeError) as exc:
                    # A bad parameter must not kill a running demo; report it instead.
                    with self._lock:
                        self.errors.append(f"{iso(now)} {inj.type} on {inj.vehicle}: {exc}")
            for delivery, vehicle in self.sim.tick(now):
                self._counter += 1
                heappush(self._pending, (delivery.ingest_ms, self._counter, vehicle, delivery))
            self._flush(now)
            self._publish_latest()
        self._flush(None)
        self.sink.close()

    def _flush(self, upto: int | None) -> None:
        out: list[dict[str, Any]] = []
        while self._pending and (upto is None or self._pending[0][0] <= upto):
            _, _, _, delivery = heappop(self._pending)
            out.append(device_reading(delivery.reading))
        # Live runs can be long: nothing below is needed after the tick, so free it.
        self.sim.deliveries.clear()
        if out:
            self.sink.send(out)

    def _publish_latest(self) -> None:
        if not self.sim.recording:
            return
        with self._lock:
            for row in self.sim.recording:
                self._latest[row["vehicle_id"]] = row
        self.sim.recording.clear()


# -- control API -------------------------------------------------------------------------


def control_server(
    runner: LiveRunner, host: str, port: int, origins: frozenset[str]
) -> ThreadingHTTPServer:
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, format: str, *args: Any) -> None:
            pass  # keep stdout for readings

        def _cors(self) -> None:
            origin = self.headers.get("Origin")
            if origin in origins:
                self.send_header("Access-Control-Allow-Origin", origin)
                self.send_header("Vary", "Origin")
                self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
                self.send_header("Access-Control-Allow-Headers", "Content-Type")

        def _reply(self, status: int, body: dict[str, Any]) -> None:
            data = json.dumps(body).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self._cors()
            self.end_headers()
            self.wfile.write(data)

        def do_OPTIONS(self) -> None:
            self.send_response(204)
            self._cors()
            self.end_headers()

        def do_GET(self) -> None:
            if self.path == "/fleet":
                self._reply(200, runner.fleet())
            elif self.path == "/health":
                self._reply(200, {"status": "ok", "t": iso(runner.now_ms)})
            else:
                self._reply(404, {"error": "not found"})

        def do_POST(self) -> None:
            if self.path != "/inject":
                self._reply(404, {"error": "not found"})
                return
            try:
                length = int(self.headers.get("Content-Length", "0"))
                if length > 64_000:
                    raise ValueError("body too large")
                body = json.loads(self.rfile.read(length) or b"{}")
                params = body.get("params") or {}
                if not isinstance(params, dict):
                    raise ValueError("params must be an object")
                runner.queue_injection(Injection(str(body["vehicle"]), str(body["fault"]), params))
            except (ValueError, KeyError, json.JSONDecodeError) as exc:
                self._reply(400, {"error": str(exc)})
                return
            self._reply(202, {"queued": True, "at": iso(runner.now_ms)})

    server = ThreadingHTTPServer((host, port), Handler)
    threading.Thread(target=server.serve_forever, name="wt-sim-control", daemon=True).start()
    return server
