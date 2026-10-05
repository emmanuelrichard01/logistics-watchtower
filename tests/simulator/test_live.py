"""Layer 8: live mode. Pacing, the signed HTTP sink against the real gateway app, and the
control API."""

import io
import json
import socket
import threading
import time
import urllib.error
import urllib.request
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import pytest
import uvicorn
from watchtower_gateway.app import create_app
from watchtower_gateway.publisher import UnavailableError
from watchtower_gateway.validation import Accepted, Rejected
from watchtower_simulator import scenario as scenarios
from watchtower_simulator.clock import rng
from watchtower_simulator.engine import Simulation, iter_jsonl_ready
from watchtower_simulator.live import (
    HttpSink,
    LiveRunner,
    StreamSink,
    WallClock,
    anchored,
    assign_devices,
    control_server,
    load_keys,
)
from watchtower_simulator.routes import default_data_dir

KEYS = load_keys(default_data_dir().parent / "infra" / "compose" / "gateway" / "device-keys.json")


class FakeClock:
    """Advances only when the runner sleeps: tests pacing without waiting."""

    def __init__(self) -> None:
        self.t = 0.0
        self.slept = 0.0

    def monotonic(self) -> float:
        return self.t

    def sleep(self, seconds: float) -> None:
        self.t += seconds
        self.slept += seconds

    def now_ms(self) -> int:
        return int(self.t * 1000)


def scenario(minutes: int = 30) -> scenarios.Scenario:
    return scenarios.load("dead_zone_with_excursion").with_duration(minutes * 60_000)


def run_to_lines(sc: scenarios.Scenario, speed: float) -> tuple[list[dict[str, Any]], FakeClock]:
    out, clock = io.StringIO(), FakeClock()
    runner = LiveRunner(sc, StreamSink(out), speed=speed, clock=clock)
    runner.sink.close = lambda: None  # type: ignore[method-assign]
    runner.run()
    return [json.loads(line) for line in out.getvalue().splitlines()], clock


def test_pacing_follows_the_time_scale() -> None:
    sc = scenario(30)
    _, clock = run_to_lines(sc, speed=60.0)
    last_tick_s = (sc.duration_ms - Simulation(sc).step_ms) / 1000
    assert clock.slept == pytest.approx(last_tick_s / 60.0, rel=0.01)


def test_live_output_is_the_batch_run_minus_ingest_time() -> None:
    sc = scenario(30)
    lines, _ = run_to_lines(sc, speed=1000.0)
    batch = [
        {k: v for k, v in r.items() if k != "ingest_time"}
        for r in iter_jsonl_ready(Simulation(sc).run().readings)
    ]
    assert sorted(lines, key=lambda r: (r["event_id"], r["event_time"])) == sorted(
        batch, key=lambda r: (r["event_id"], r["event_time"])
    )
    assert all("ingest_time" not in r for r in lines)


def test_anchoring_ends_the_run_at_wall_clock_now() -> None:
    sc = scenario(60)
    now = 1_791_300_000_000
    shifted = anchored(sc, 60.0, now)
    end = shifted.start_ms + shifted.duration_ms
    assert end == pytest.approx(now + sc.duration_ms / 60, abs=1000)


def test_devices_are_mapped_onto_registered_keys() -> None:
    sc = scenarios.load("abuja_pharma_multidrop")
    mapped = assign_devices(sc, KEYS)
    assert all(v.device_id in KEYS for v in mapped.fleet)
    with pytest.raises(ValueError, match="no device key"):
        assign_devices(sc, {})


class FlakyPublisher:
    """Broker down for the first ``failures`` batches, then accepts everything."""

    def __init__(self, failures: int) -> None:
        self.failures = failures
        self.accepted: list[Accepted] = []
        self.rejected: list[Rejected] = []

    def publish(
        self, accepted: Sequence[Accepted], rejected: Sequence[tuple[Rejected, bytes, int]]
    ) -> list[bool]:
        if self.failures > 0:
            self.failures -= 1
            raise UnavailableError("broker down")
        self.accepted += accepted
        self.rejected += [r for r, _, _ in rejected]
        return [True] * (len(accepted) + len(rejected))

    def ready(self) -> tuple[bool, str]:
        return True, "ok"


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


@pytest.fixture
def gateway() -> Any:
    publisher = FlakyPublisher(failures=2)
    port = free_port()
    server = uvicorn.Server(
        uvicorn.Config(create_app(publisher=publisher, keys=KEYS), port=port, log_level="error")
    )
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    deadline = time.monotonic() + 10
    while not server.started and time.monotonic() < deadline:
        time.sleep(0.05)
    yield f"http://127.0.0.1:{port}", publisher
    server.should_exit = True
    thread.join(timeout=10)


def test_signed_readings_pass_the_real_gateway_and_503s_are_retried(gateway: Any) -> None:
    url, publisher = gateway
    sc = assign_devices(anchored(scenario(20), 2400.0, WallClock().now_ms()), KEYS)
    sink = HttpSink(url, KEYS, batch=50, rng=rng(1, "jitter"), sleep=lambda _: None)
    LiveRunner(sc, sink, speed=2400.0, clock=FakeClock()).run()
    expected = len(Simulation(sc).run().readings)
    assert sink.stats.retries >= 2
    assert sink.stats.quarantined == 0, sink.stats.quarantine_reasons
    assert sink.stats.accepted == expected
    assert len(publisher.accepted) == expected
    assert {a.vehicle_id for a in publisher.accepted} == {"TRK-501", "TRK-502"}


def test_control_api_injects_faults_and_reports_the_fleet() -> None:
    sc = scenarios.load("defrost_cycle").with_duration(3_600_000)
    runner = LiveRunner(sc, StreamSink(io.StringIO()), speed=900.0)  # 4 s of wall time
    port = free_port()
    server = control_server(runner, "127.0.0.1", port, frozenset({"http://localhost:5173"}))
    base = f"http://127.0.0.1:{port}"
    thread = threading.Thread(target=runner.run, daemon=True)
    thread.start()
    try:
        body = json.dumps(
            {"vehicle": "TRK-601", "fault": "compressor_fault", "params": {"code": "E-COMP-07"}}
        ).encode()
        req = urllib.request.Request(
            f"{base}/inject", data=body, method="POST", headers={"Origin": "http://localhost:5173"}
        )
        with urllib.request.urlopen(req, timeout=5) as resp:
            assert resp.status == 202
            assert resp.headers["Access-Control-Allow-Origin"] == "http://localhost:5173"
        bad = urllib.request.Request(
            f"{base}/inject", data=b'{"vehicle": "NOPE", "fault": "reboot"}', method="POST"
        )
        with pytest.raises(urllib.error.HTTPError) as err:
            urllib.request.urlopen(bad, timeout=5)
        assert err.value.code == 400
        time.sleep(0.5)
        with urllib.request.urlopen(f"{base}/fleet", timeout=5) as resp:
            fleet = json.loads(resp.read())
        assert fleet["vehicles"][0]["compressor"] == "FAULT"
    finally:
        runner.stop_event.set()
        thread.join(timeout=10)
        server.shutdown()


def test_cli_live_writes_jsonl(tmp_path: Path) -> None:
    from watchtower_simulator.cli import main

    out = tmp_path / "live.jsonl"
    main(["live", "defrost_cycle", "--speed", "100000", "--sink", str(out), "--duration", "10m"])
    assert len(out.read_text(encoding="utf-8").splitlines()) == 20
