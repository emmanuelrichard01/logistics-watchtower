"""wt-sim: run a scenario faster than real time.

    wt-sim run compressor_gradual_degradation --out readings.jsonl \\
        --recording fleet.jsonl --truth truth.jsonl [--duration 2h]
"""

import argparse
import gzip
import io
import json
import sys
from pathlib import Path
from typing import Any

import yaml

from watchtower_simulator import fleetday
from watchtower_simulator import live as live_mode
from watchtower_simulator import scenario as scenarios
from watchtower_simulator.engine import Simulation, iter_jsonl_ready
from watchtower_simulator.scenario import duration_ms


def write_jsonl(path: Path, rows: Any) -> int:
    """JSON lines; gzip-compressed when the path ends in .gz (with a zero timestamp, so the
    same run still produces byte-identical files)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with path.open("wb") as raw:
        gz = path.suffix == ".gz"
        sink: Any = gzip.GzipFile(filename="", mode="wb", fileobj=raw, mtime=0) if gz else raw
        with io.TextIOWrapper(sink, encoding="utf-8", newline="\n") as f:
            for row in rows:
                f.write(json.dumps(row, separators=(",", ":"), ensure_ascii=False) + "\n")
                count += 1
    return count


def run(args: argparse.Namespace) -> None:
    sc = scenarios.load(args.scenario)
    if args.duration:
        sc = sc.with_duration(duration_ms(args.duration))
    result = Simulation(sc).run()
    n = write_jsonl(Path(args.out), iter_jsonl_ready(result.readings))
    print(f"{sc.name}: {n} readings -> {args.out}", file=sys.stderr)
    if args.recording:
        n = write_jsonl(Path(args.recording), result.recording)
        print(f"{sc.name}: {n} fleet-state rows -> {args.recording}", file=sys.stderr)
    if args.truth:
        n = write_jsonl(Path(args.truth), result.truth)
        print(f"{sc.name}: {n} truth intervals -> {args.truth}", file=sys.stderr)
    if args.stops:
        n = write_jsonl(Path(args.stops), result.stops)
        print(f"{sc.name}: {n} stop visits -> {args.stops}", file=sys.stderr)


def fleet_day(args: argparse.Namespace) -> None:
    day = fleetday.generate(
        args.seed, args.trucks, args.vans, args.hours, args.start, args.handovers
    )
    out = Path(args.scenario_out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(yaml.safe_dump(day.doc, sort_keys=False), encoding="utf-8", newline="\n")
    labels = out.with_name(out.stem + ".labels.yaml")
    labels.write_text(yaml.safe_dump(day.labels, sort_keys=False), encoding="utf-8", newline="\n")
    print(
        f"{day.doc['name']}: {len(day.doc['fleet'])} vehicles, {len(day.doc['events'])} events "
        f"-> {out} (+ {labels.name})",
        file=sys.stderr,
    )


def live(args: argparse.Namespace) -> None:
    sc = scenarios.load(args.scenario)
    if args.duration:
        sc = sc.with_duration(duration_ms(args.duration))
    sink: live_mode.Sink
    if args.sink.startswith(("http://", "https://")):
        keys = live_mode.load_keys(Path(args.keys))
        sc = live_mode.assign_devices(sc, keys)
        sink = live_mode.HttpSink(args.sink, keys, batch=args.batch)
    elif args.sink == "stdout":
        sink = live_mode.StreamSink(sys.stdout)
    else:
        stream = Path(args.sink).open("w", encoding="utf-8", newline="\n")  # noqa: SIM115
        sink = live_mode.StreamSink(stream)  # the sink owns the file and closes it
    anchor = args.anchor or ("now" if isinstance(sink, live_mode.HttpSink) else "scenario")
    if anchor == "now":
        sc = live_mode.anchored(sc, args.speed, live_mode.WallClock().now_ms())
    runner = live_mode.LiveRunner(sc, sink, speed=args.speed)
    server = None
    if args.control:
        host, _, port = args.control.rpartition(":")
        origins = frozenset(o for o in args.cors_origin if o)
        server = live_mode.control_server(runner, host or "127.0.0.1", int(port), origins)
        print(f"control API on http://{host or '127.0.0.1'}:{port}", file=sys.stderr)
    print(f"{sc.name}: live at {args.speed}x -> {args.sink}", file=sys.stderr)
    try:
        runner.run()
    except KeyboardInterrupt:
        runner.stop_event.set()
        sink.close()
    if server is not None:
        server.shutdown()
    if isinstance(sink, live_mode.HttpSink):
        print(f"{sc.name}: {sink.stats}", file=sys.stderr)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        prog="wt-sim", description="Run a scenario faster than real time."
    )
    sub = parser.add_subparsers(dest="command", required=True)
    p_run = sub.add_parser("run", help="run a scenario to JSONL")
    p_run.add_argument("scenario", help="scenario name under data/scenarios, or a YAML path")
    p_run.add_argument("--out", required=True, help="telemetry readings, in gateway arrival order")
    p_run.add_argument("--recording", help="fleet-state rows every 15 simulated seconds")
    p_run.add_argument("--truth", help="ground-truth intervals")
    p_run.add_argument(
        "--stops", help="urban stop visits: planned versus actual arrival, delivered temps"
    )
    p_run.add_argument("--duration", help="override the scenario duration, e.g. 2h")
    p_run.set_defaults(func=run)
    p_live = sub.add_parser("live", help="run a scenario paced in real time, streaming readings")
    p_live.add_argument("scenario", help="scenario name under data/scenarios, or a YAML path")
    p_live.add_argument("--speed", type=float, default=1.0, help="time scale: 1, 10, 60...")
    p_live.add_argument(
        "--sink",
        default="stdout",
        help="stdout, a .jsonl path, or the gateway URL (e.g. http://127.0.0.1:18090)",
    )
    p_live.add_argument(
        "--keys",
        default="infra/compose/gateway/device-keys.json",
        help="device keys for signing (HTTP sink)",
    )
    p_live.add_argument("--batch", type=int, default=200, help="readings per gateway request")
    p_live.add_argument(
        "--anchor",
        choices=["now", "scenario"],
        help="now: shift the run to end at wall-clock now (default for the gateway)",
    )
    p_live.add_argument("--control", help="host:port for the control API, e.g. 127.0.0.1:18091")
    p_live.add_argument(
        "--cors-origin",
        action="append",
        default=["http://localhost:5173", "http://127.0.0.1:5173"],
        help="browser origin allowed to call the control API (repeatable)",
    )
    p_live.add_argument("--duration", help="override the scenario duration, e.g. 2h")
    p_live.set_defaults(func=live)
    p_day = sub.add_parser("fleet-day", help="generate a seeded fleet-day scenario and labels")
    p_day.add_argument("--seed", type=int, required=True)
    p_day.add_argument("--trucks", type=int, default=20)
    p_day.add_argument("--vans", type=int, default=6)
    p_day.add_argument("--hours", type=float, default=24.0)
    p_day.add_argument("--handovers", type=int, default=2, help="trucks that cross-dock to vans")
    p_day.add_argument("--start", default="2026-10-06T04:00:00Z")
    p_day.add_argument("--scenario-out", required=True, help="scenario YAML; labels go beside it")
    p_day.set_defaults(func=fleet_day)
    args = parser.parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main()
