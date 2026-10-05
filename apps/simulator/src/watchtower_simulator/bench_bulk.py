"""Bulk-mode throughput benchmark: writes a CSV of raw runs and prints a summary.

    uv run python -m watchtower_simulator.bench_bulk docs/benchmarks/simulator-bulk.csv \\
        --trucks 1000 10000 25000 --steps 20 --repeats 3
"""

import argparse
import csv
import platform
import statistics
import subprocess
from pathlib import Path

import numpy as np

from watchtower_simulator.bulk import bench


def commit() -> str:
    try:
        return subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"], capture_output=True, text=True, check=True
        ).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        return "unknown"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("out")
    parser.add_argument("--trucks", type=int, nargs="+", default=[1000, 10000, 25000])
    parser.add_argument("--steps", type=int, default=20)
    parser.add_argument("--repeats", type=int, default=3)
    args = parser.parse_args()

    bench(1000, 3)  # warm up imports, caches and the allocator
    rows = []
    for n in args.trucks:
        for rep in range(args.repeats):
            r = bench(n, args.steps, seed=rep + 1)
            rows.append(
                {
                    "trucks": n,
                    "repeat": rep + 1,
                    "steps": r.steps,
                    "readings": r.readings,
                    "physics_s": round(r.physics_s, 4),
                    "records_s": round(r.records_s, 4),
                    "json_s": round(r.json_s, 4),
                    "physics_eps": round(r.physics_eps),
                    "end_to_end_eps": round(r.end_to_end_eps),
                    "python": platform.python_version(),
                    "numpy": np.__version__,
                    "commit": commit(),
                }
            )
            print(
                f"{n:>6} trucks  repeat {rep + 1}: "
                f"physics {r.physics_eps:>10,.0f} ev/s   end-to-end {r.end_to_end_eps:>8,.0f} ev/s"
            )
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0]), lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
    for n in args.trucks:
        mine = [r for r in rows if r["trucks"] == n]
        physics = statistics.median(r["physics_eps"] for r in mine)
        e2e = statistics.median(r["end_to_end_eps"] for r in mine)
        print(f"median {n:>6}: physics {physics:,.0f} ev/s, end-to-end {e2e:,.0f} ev/s")


if __name__ == "__main__":
    main()
