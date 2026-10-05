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
    args = parser.parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main()
