# Simulator bulk mode: throughput

Measured 5 Oct 2026 with `apps/simulator/src/watchtower_simulator/bench_bulk.py`. Raw runs: `docs/benchmarks/simulator-bulk.csv`.

**Hardware.** Intel Core i7-10510U (4 cores / 8 threads), 7.8 GB RAM, Windows 11 Pro. Python 3.12.15 and NumPy 2.5.3, on a single process and a single core, with no container. Other agents were working on the same laptop during the run, so treat the spread as noise and read medians.

**Method.** Each run simulates 20 reporting intervals (30 s each) for N trucks, after one warm-up run, three repeats per size. Three stages are timed separately with `perf_counter`:

1. vectorised physics (position, link, thermal, motion);
2. building contract-shaped readings, including a uuid5 `event_id` each;
3. JSON encoding.

"End-to-end" is readings divided by the sum of all three. Readings taken during link outages are dropped, so the counts are slightly under N × 20.

| Trucks | Physics, events/s (median, range) | End-to-end, events/s (median, range) |
| ---: | --- | --- |
| 1,000 | 278,000 (158,000-303,000) | 14,500 (10,400-15,200) |
| 10,000 | 718,000 (683,000-816,000) | 17,900 (15,800-21,300) |
| 25,000 | 485,000 (429,000-615,000) | 11,900 (11,700-13,500) |

## What it means

- **Physics is not the bottleneck.** At 25,000 trucks one reporting interval simulates in about 50 ms.
- **Python-level record building is the bottleneck** (about 60% of end-to-end time): one dict and one SHA-1 uuid5 per reading. JSON encoding takes most of the rest.
- **Rate needed versus available.** At the default 30 s interval, 25,000 trucks need about 830 readings/s, roughly 14 times below what one process delivers. At 1 Hz (load tests), 25,000 trucks need 25,000/s, about 2 times what one process delivers, so split the fleet across 2-3 processes. Each process uses its own seed and device-ID range.
- **Next to try if more is needed:** columnar Avro encoding straight from the arrays, and computing uuid5s in a worker pool.

## Not shown

These numbers say nothing about the gateway or the broker. The end-to-end pipeline benchmark (plan section 16, experiments 1 and 2) uses an open-loop generator built on this mode. Physics fidelity is simplified in bulk mode; see the module docstring in `bulk.py`.
