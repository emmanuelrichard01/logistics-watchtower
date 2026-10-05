"""Generate docs/simulator/validation.md and its SVG charts from fresh simulator runs.

    uv run python -m watchtower_simulator.validation_report docs/simulator

Charts are plain SVG written here (no plotting dependency), so the report regenerates in CI.
"""

import statistics
import sys
from collections import Counter
from collections.abc import Sequence
from datetime import datetime
from pathlib import Path
from typing import Any

from watchtower_simulator import scenario as scenarios
from watchtower_simulator.clock import to_ms
from watchtower_simulator.engine import Simulation
from watchtower_simulator.fleetday import INCIDENTS, generate
from watchtower_simulator.testing import label_failures, run

PALETTE = ["#2563eb", "#dc2626", "#059669", "#d97706", "#7c3aed"]
W, H, PAD_L, PAD_R, PAD_T, PAD_B = 720, 300, 56, 16, 36, 40

Series = tuple[str, list[float], list[float]]


def hours_since(stamps: list[str], start: str) -> list[float]:
    t0 = to_ms(datetime.fromisoformat(start.replace("Z", "+00:00")))
    return [
        (to_ms(datetime.fromisoformat(s.replace("Z", "+00:00"))) - t0) / 3_600_000 for s in stamps
    ]


def line_chart(
    title: str,
    y_label: str,
    series: list[Series],
    bands: Sequence[tuple[float, float]] = (),
    x_label: str = "hours from start",
    points: bool = False,
) -> str:
    xs = [x for _, sx, _ in series for x in sx]
    ys = [y for _, _, sy in series for y in sy] + [v for b in bands for v in b]
    x0, x1 = min(xs), max(xs) or 1.0
    y0, y1 = min(ys), max(ys)
    pad = (y1 - y0) * 0.08 or 1.0
    y0, y1 = y0 - pad, y1 + pad

    def sx(x: float) -> float:
        return PAD_L + (x - x0) / ((x1 - x0) or 1) * (W - PAD_L - PAD_R)

    def sy(y: float) -> float:
        return H - PAD_B - (y - y0) / (y1 - y0) * (H - PAD_T - PAD_B)

    out = [
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {W} {H}" font-family="system-ui, sans-serif" font-size="12">',
        f'<rect width="{W}" height="{H}" fill="#ffffff"/>',
        f'<text x="{PAD_L}" y="20" font-size="14" font-weight="600" fill="#111827">{title}</text>',
    ]
    for lo, hi in bands:
        out.append(
            f'<rect x="{PAD_L}" y="{sy(hi):.1f}" width="{W - PAD_L - PAD_R}" height="{sy(lo) - sy(hi):.1f}" fill="#16a34a" opacity="0.08"/>'
        )
    for k in range(5):
        y = y0 + (y1 - y0) * k / 4
        out.append(
            f'<line x1="{PAD_L}" x2="{W - PAD_R}" y1="{sy(y):.1f}" y2="{sy(y):.1f}" stroke="#e5e7eb"/>'
        )
        out.append(
            f'<text x="{PAD_L - 6}" y="{sy(y) + 4:.1f}" text-anchor="end" fill="#6b7280">{y:.1f}</text>'
        )
        x = x0 + (x1 - x0) * k / 4
        out.append(
            f'<text x="{sx(x):.1f}" y="{H - PAD_B + 16}" text-anchor="middle" fill="#6b7280">{x:.1f}</text>'
        )
    out.append(
        f'<text x="{W / 2}" y="{H - 6}" text-anchor="middle" fill="#374151">{x_label}</text>'
    )
    out.append(
        f'<text x="14" y="{H / 2}" transform="rotate(-90 14 {H / 2})" text-anchor="middle" fill="#374151">{y_label}</text>'
    )
    for n, (label, xs_, ys_) in enumerate(series):
        colour = PALETTE[n % len(PALETTE)]
        if points:
            out += [
                f'<circle cx="{sx(x):.1f}" cy="{sy(y):.1f}" r="1.6" fill="{colour}"/>'
                for x, y in zip(xs_, ys_, strict=True)
            ]
        else:
            path = " ".join(f"{sx(x):.1f},{sy(y):.1f}" for x, y in zip(xs_, ys_, strict=True))
            out.append(
                f'<polyline points="{path}" fill="none" stroke="{colour}" stroke-width="1.6"/>'
            )
        out.append(
            f'<rect x="{W - PAD_R - 200}" y="{PAD_T + n * 16 - 9}" width="10" height="10" fill="{colour}"/>'
        )
        out.append(
            f'<text x="{W - PAD_R - 185}" y="{PAD_T + n * 16}" fill="#111827">{label}</text>'
        )
    out.append("</svg>")
    return "\n".join(out) + "\n"


def col(rows: list[dict[str, Any]], key: str) -> list[float]:
    return [float(r[key]) for r in rows]


def section_pulldown(img: Path) -> str:
    warm = [r for r in run("warm_loading").recording if r["vehicle_id"] == "TRK-801"]
    ok = [r for r in run("warm_loading").recording if r["vehicle_id"] == "TRK-802"]
    start = warm[0]["t"]
    t = hours_since([r["t"] for r in warm], start)
    (img / "pulldown.svg").write_text(
        line_chart(
            "Warm-loaded produce: cargo pulls down for hours while box air looks normal",
            "°C",
            [
                ("TRK-801 cargo (loaded at 14 °C)", t, col(warm, "cargo_c")),
                ("TRK-801 box air", t, col(warm, "air_c")),
                ("TRK-802 cargo (loaded correctly)", t, col(ok, "cargo_c")),
            ],
            bands=[(1.0, 8.0)],
        ),
        encoding="utf-8",
        newline="\n",
    )
    hours_out = sum(1 for r in warm if r["cargo_c"] > 8.0) * 15 / 3600
    loaded = next(i for i, r in enumerate(warm) if r["stop_reason"] is None)  # door shut, driving
    air_ok = next((t[i] for i in range(loaded, len(warm)) if warm[i]["air_c"] <= 8.0), float("nan"))
    # Defrost heats the box air above the cargo; for a few minutes after it ends the air is
    # still warmer, which is physics, not a fault. Those rows are excluded and counted.
    recovering = {
        j for i, r in enumerate(warm) if r["defrost"] for j in range(i, min(i + 21, len(warm)))
    }
    running = [
        r
        for i, r in enumerate(warm[loaded:], start=loaded)
        if r["compressor"] == "RUNNING" and i not in recovering
    ]
    cargo_above_air = all(r["cargo_c"] >= r["air_c"] - 0.05 for r in running)
    ok_in_range = all(1.0 <= r["cargo_c"] <= 8.0 for r in ok)
    return (
        "## Pull-down of a warm load\n\n![Pull-down curves](img/pulldown.svg)\n\n"
        "TRK-801 was loaded with produce at 14 °C against a 4 °C setpoint (1-8 °C allowed). "
        f"Box air is back under 8 °C at **{air_ok:.1f} h**, but the cargo stays above 8 °C for "
        f"**{hours_out:.1f} h** of the 10 h run: a reefer is built to hold temperature, not to pull "
        "down a warm load.\n\nChecked by this script:\n\n"
        "- while the compressor runs, the cargo never reads below box air, except in the "
        "5 minutes after a defrost, when defrost heat still sits in the air: "
        f"**{'holds' if cargo_above_air else 'FAILS'}**;\n"
        "- the correctly loaded TRK-802 stays within 1-8 °C throughout: "
        f"**{'holds' if ok_in_range else 'FAILS'}**.\n"
    )


def section_degradation(img: Path) -> str:
    rows = [
        r for r in run("compressor_gradual_degradation").recording if r["vehicle_id"] == "TRK-101"
    ]
    t = hours_since([r["t"] for r in rows], rows[0]["t"])
    (img / "degradation.svg").write_text(
        line_chart(
            "Compressor degradation: air rises first, cargo lags by hours",
            "°C",
            [
                ("cargo", t, col(rows, "cargo_c")),
                ("box air", t, col(rows, "air_c")),
                ("compressor health x 10", t, [10 * h for h in col(rows, "compressor_health")]),
            ],
            bands=[(2.0, 8.0)],
        ),
        encoding="utf-8",
        newline="\n",
    )
    breach = next((t[i] for i, r in enumerate(rows) if r["cargo_c"] > 8.0), None)
    bottomed = next(t[i] for i, r in enumerate(rows) if r["compressor_health"] <= 0.2 + 1e-9)
    lead = f"{breach - bottomed:.1f} h" if breach else "beyond the run"
    return (
        "## Compressor degradation and lead time\n\n![Degradation](img/degradation.svg)\n\n"
        f"Health falls from 1.0 to 0.2 over 90 minutes (bottoming at {bottomed:.1f} h). The cargo crosses 8 °C "
        f"**{lead}** after that, because thermal mass makes the breach late. This is the lead time a "
        "time-to-breach forecast should earn.\n"
    )


def section_duty() -> str:
    def mean_duty(name: str, vehicle: str, after_h: float = 1.0) -> float:
        rows = [r for r in run(name).recording if r["vehicle_id"] == vehicle]
        t = hours_since([r["t"] for r in rows], rows[0]["t"])
        return statistics.mean(
            r["duty_cycle_pct"] for r, h in zip(rows, t, strict=True) if h >= after_h
        )

    cases = [
        ("Healthy frozen trailer, hot day", "compressor_gradual_degradation", "TRK-102"),
        ("Healthy pharma trailer", "compressor_gradual_degradation", "TRK-104"),
        ("Degrading compressor (health 0.2)", "compressor_gradual_degradation", "TRK-101"),
        ("Warm-loaded produce", "warm_loading", "TRK-801"),
        ("Correctly loaded produce", "warm_loading", "TRK-802"),
    ]
    lines = ["| Case | Mean compressor duty after the first hour |", "| --- | ---: |"]
    lines += [f"| {label} | {mean_duty(name, v):.0f}% |" for label, name, v in cases]
    return (
        "## Duty cycle\n\n" + "\n".join(lines) + "\n\n"
        "Rising duty at a steady temperature is the early sign of a struggling unit, and the "
        "`COMPRESSOR_DEGRADING` rule keys on it. Healthy units cycle; degraded and overloaded units run "
        "near flat out.\n"
    )


def section_replay(img: Path) -> str:
    result = run("dead_zone_with_excursion")
    readings = [r for r in result.readings if r["vehicle_id"] == "TRK-501"]
    t0 = readings[0]["event_time"]
    ev = [(r["event_time"] - t0).total_seconds() / 3600 for r in readings]
    lag = [(r["ingest_time"] - r["event_time"]).total_seconds() / 60 for r in readings]
    live = [
        (e, lag_min)
        for e, lag_min, r in zip(ev, lag, readings, strict=True)
        if not r["link"]["buffered"]
    ]
    buffered = [
        (e, lag_min)
        for e, lag_min, r in zip(ev, lag, readings, strict=True)
        if r["link"]["buffered"]
    ]
    (img / "replay.svg").write_text(
        line_chart(
            "Dead zone: readings buffered on the device arrive late, in order",
            "minutes from measurement to gateway",
            [
                ("live", [e for e, _ in live], [lag_min for _, lag_min in live]),
                ("buffered replay", [e for e, _ in buffered], [lag_min for _, lag_min in buffered]),
            ],
            x_label="event time, hours from start",
            points=True,
        ),
        encoding="utf-8",
        newline="\n",
    )
    return (
        "## Dead-zone replay\n\n![Replay timeline](img/replay.svg)\n\n"
        "During the 35-minute outage TRK-501 keeps sampling, at the 10 s alarm rate because its "
        f"compressor has faulted. {len(buffered)} readings are buffered, then replayed in "
        f"order once the link returns, alongside live data. The oldest arrives {max(lag_min for _, lag_min in buffered):.0f} minutes "
        "after it was measured. The cargo excursion starts inside the outage, so an alert must carry its true event-time "
        "start, not the time the data arrived.\n"
    )


def section_fleet_days(days: int = 20) -> str:
    counts: Counter[str] = Counter()
    for seed in range(days):
        day = generate(seed, trucks=20, vans=6, hours=24.0)
        counts.update(i["incident"] for i in day.labels["injected"])
    # Incidents are drawn for the 20 trucks and the 4 vans on their own rounds; the 2 cross-dock
    # truck-van pairs carry the handover story and draw none.
    exposed = {"truck": 20, "any": 24}
    expected = {k: rate * exposed[applies] * days for k, (rate, _, applies) in INCIDENTS.items()}
    lines = [
        "| Incident | Applies to | Expected (Poisson mean) | Generated |",
        "| --- | --- | ---: | ---: |",
    ]
    lines += [
        f"| {k} | {'trucks' if INCIDENTS[k][2] == 'truck' else 'trucks and vans'} "
        f"| {expected[k]:.1f} | {counts[k]} |"
        for k in INCIDENTS
    ]
    # Check the truth guarantee on a shorter stressed day: every injected incident must show.
    stressed = generate(99, trucks=6, vans=3, hours=8.0, handovers=1, rate_scale=20.0)
    result = Simulation(scenarios.parse(stressed.doc)).run()
    failures = len(label_failures(result, stressed.labels, scenarios.parse(stressed.doc).start_ms))
    return (
        f"## Fleet-day incident mix\n\n{days} seeded 24-hour days, each with 20 trucks, 4 vans on "
        "their own rounds and 2 cross-dock truck-van pairs. Incidents are drawn per vehicle-day "
        "from illustrative Poisson rates; generated counts should scatter around the mean "
        "(roughly ± 2√mean).\n\n"
        + "\n".join(lines)
        + f"\n\nTruth guarantee: on a stressed day ({len(stressed.labels['injected'])} injected incidents), "
        f"**{failures}** label failures: every injected incident appears in ground truth.\n"
    )


LIMITS = """## What this model does not capture

Honest limits, so nobody mistakes the simulator for a calibrated digital twin:

- **No calibration to real units.** Thermal, reefer and environment parameters are plausible engineering values, not fitted to any manufacturer's data or field logs. Lead times and duty cycles show the right shape, not the right number for a specific trailer.
- **Air is one well-mixed node.** Real boxes stratify (warm air at the door and ceiling, cold near the evaporator), so probe placement matters in reality and doesn't here.
- **Door openings are binary.** No air-curtain or strip-curtain effectiveness, and no partial opening.
- **Weather is per truck,** not a spatial field: two trucks side by side can see different storms. There's no Harmattan dust effect on the unit, only on sunlight.
- **Roads are approximate.** Corridor geometry is real; road class is derived from distance to towns, not OSM tags. Checkpoint, toll and congestion rates are illustrative, not surveyed.
- **Cellular coverage is synthetic.** Dead zones are named placements and the link is a two-state Markov chain, not measured network data.
- **Customers and depots are invented.** Urban stops are synthetic; delivery windows and dwell times are plausible guesses.
- **Products are simple.** Respiration uses a Q10 rule, with no ripening, ethylene, chilling-injury dynamics or product quality model. Mean kinetic temperature and shelf life are left to the risk engine.
- **Bulk mode simplifies further** (see `bulk.py`): one cargo node, no weather or operations, and outages drop readings instead of buffering them.
- **People are absent.** Driver behaviour is a profile, not a decision-maker: no reactions to alerts, no theft resistance, no route changes.
"""


def main() -> None:
    out = Path(sys.argv[1])
    img = out / "img"
    img.mkdir(parents=True, exist_ok=True)
    parts = [
        "# Simulator validation\n\n"
        "Generated by `uv run python -m watchtower_simulator.validation_report docs/simulator` from fresh, seeded "
        "runs: rerunning it reproduces this page and its charts exactly. It shows the simulator's sanity properties, "
        "not a calibration.\n",
        section_pulldown(img),
        section_degradation(img),
        section_duty(),
        section_replay(img),
        section_fleet_days(),
        LIMITS,
    ]
    (out / "validation.md").write_text("\n".join(parts), encoding="utf-8", newline="\n")
    print(f"wrote {out / 'validation.md'}")


if __name__ == "__main__":
    main()
