# Handoff: where the v2 rebuild stands

**Last updated:** Mon 5 Oct 2026, end of the first build session (week 1, day 1 of 14).
**Branch:** `v2`, pushed to `origin/v2`. `main` is still v1 (`abc2a08`).
**Read this first** when you come back. It tells you what exists, what is half-done, what to do next, and which traps cost time. Detail lives in the linked docs; this page is the map.

---

## 1. The 60-second picture

Watchtower is a cold-chain risk platform: it tells an operator which refrigerated shipment will breach soonest, how sure the system is, why, and what to do. v2 is a 14-week solo rebuild (5 Oct 2026 to 10 Jan 2027), driven by [`plan/Logistics Watchtower 2.0 Rebuild Plan.md`](../plan/Logistics%20Watchtower%202.0%20Rebuild%20Plan.md). The plan is the source of truth, amended by the ADRs.

The work ran far ahead of the calendar: on day 1, parts of phases 0, 1, 2, 4 and 5 already exist. **The stream is still not wired end to end.** Each piece works and is tested on its own. Nothing yet consumes the input log, persists alerts or streams to the console.

```text
simulator ──signed HTTP──▶ gateway ──▶ wt.input.v1 ──▶ [processor: core done, shell in flight] ──▶ (alerts, risk) ──▶ [projector, API: not built] ──▶ console (replays recordings today)
```

| Piece | State | Where |
| --- | --- | --- |
| v1 audit and runtime baseline | Done | `docs/audit/v1-baseline.md`, `legacy/v1/` (frozen, tag `v1-final`) |
| Contracts and event identity | Done | `packages/contracts` (Avro `telemetry_event` v1, `input_record` v1, uuid5 IDs) |
| Core infra (Compose) | Done | `infra/compose`: Redpanda, Schema Registry, PostGIS, SeaweedFS, topics-init, gateway |
| DB schema | Done | `migrations/` (Alembic): alerts, interventions, outbox, hash-chained audit |
| Ingest gateway | Done | `apps/gateway` (ADR-0021): validation order, quarantine reasons, HMAC, 202 only after broker ack |
| Domain core (pure) | Done | `packages/domain`: buckets, dedup, trust, forecast, risk, alert state machine, vehicle evaluator |
| Simulator v2 | Done | `apps/simulator`: thermal model, real routes, dead zones, faults, city rounds, live mode, bulk mode, validation report |
| Processor shell + engine spike | **In flight** (background branch, see section 4) | `worktree-agent-a3c5a4745031cbd2a` |
| Projector, outbox relay, API, push protocol | Not started | Plan sections 9 and 12; ADR-0006, 0008, 0018 |
| Archiver, Bronze/Silver/Gold, replay CLI | Not started | Plan section 11 |
| Operator console | Built on recordings | `apps/dashboard` (see section 3) |
| Evaluation harness, benchmarks, docs site, case study | Not started | Plan sections 15, 16, 18 (weeks 12-14) |

---

## 2. Resume in five minutes

```bash
git checkout v2 && git pull
make install          # uv workspace + pre-commit hooks
make check            # ruff, pyright, pytest: what CI runs (328 passed on 5 Oct)
make console          # production console at http://localhost:4173 (use this for demos)
make up && make migrate   # the Compose stack, when you need the backbone
```

Then:

1. **Check CI** on GitHub (Actions, branch `v2`). It went green on its first real run on 5 Oct (section 6); confirm it still is.
2. **Check the processor branch** (section 4) before starting new backbone work, so you don't build the same thing twice.
3. Skim the newest entries of [`docs/devlog.md`](devlog.md). It's newest first and records every surprise.

On Windows, read [section 7](#7-traps-and-gotchas) before anything else: half of day 1 went into toolchain problems.

---

## 3. What exists, area by area

### Domain (`packages/domain`): the processor's brain, as pure functions

- Stdlib only. No I/O, clock or randomness: `tests/unit/test_domain_purity.py` enforces an import allowlist. Pyright strict.
- `buckets.py`: minute buckets keyed by integer epoch-minute; `evaluate(view, reading) -> Delta`. Values are quantised with `Decimal(str(v))`, half away from zero (ADR-0015, amended after `floor(x*100+0.5)` turned 0.285 into 28).
- `seq_ranges.py`: `(device_id, boot_id)` sequence-range dedup (ADR-0016).
- `trust.py`, `forecast.py` (time-to-breach, an exponential approach with a p10-p90 range; MKT; exposure), `risk.py` (aspect, P(breach before arrival), expected loss).
- `alerts.py`: the alert state machine. It has debounce, hysteresis, escalation from `raised_at`, latched resolve, and deterministic IDs (ADR-0006).
- `vehicle.py`: `handle(head, buckets, record)` composes all of the above over one ordered input log. First measured lead time: **21 min** on a synthetic unit scenario (`tests/unit/test_vehicle.py`). That's not the evaluation harness, which doesn't exist yet.

### Gateway (`apps/gateway`)

- FastAPI. It returns 202 only after Redpanda acknowledges every record in the batch; otherwise 503 with `Retry-After`.
- Quarantine reason codes, per-device HMAC, golden payloads in Avro JSON encoding. Rules in ADR-0021.

### Simulator (`apps/simulator`, CLI `wt-sim`; run it as `uv run python -m watchtower_simulator.cli`)

- A two-node reefer thermal model on real OSRM routes (`data/routes`), with:
  - dead zones and edge buffering;
  - sensor and device faults;
  - Lagos and Abuja last-mile rounds;
  - seeded fleet days with ground-truth labels (`data/scenarios`).
- **Live mode:** signed delivery to the gateway, plus a control API (`POST /inject`, `GET /fleet`, `GET /health`).
- **Bulk mode** (NumPy): about 0.5M physics events/s at 25k trucks, but only about 12k/s end to end on one core ([benchmark](simulator/bulk-benchmark.md)).
- **Generated [validation report](simulator/validation.md):** every number in it is computed by a committed script.

### Console (`apps/dashboard`)

- React 19, TypeScript, Vite, TanStack Router, MapLibre GL, deck.gl. Design direction "Signal Box": brief in [`docs/design/console.md`](design/console.md), product context in `PRODUCT.md`, direction contract in `.impeccable/`. Full docs: [`docs/console/README.md`](console/README.md).
- **Views:**
  - Lanes (track diagrams with signal aspects);
  - an evidence layer (verdict, minute-mean chart, "why this score");
  - Map (2D/3D, trip card, follow, city rounds, 3D signal masts, off-route);
  - Incidents (keyboard lifecycle board);
  - Health (simulated values, labelled as such);
  - phone layouts with bottom sheets.

  One time handle drives live and replay for every view.
- **Data:**
  - It replays the simulator's `console_showcase` recording (`src/data/recording.ts`), using the device-reported probe values. `?data=synthetic` switches to a seeded synthetic fleet (`src/data/synthetic.ts`).
  - Both go through `src/data/frames.ts`. The time-to-breach there is a **provisional client-side estimator**, to be replaced by streamed `risk.assessments.v1`.
- **Tests:** `src/data/recording.test.ts` pins each showcase story beat; `src/data/synthetic.test.ts` pins the synthetic scenarios.
- **Performance:** a cold first visit settles in 5.8-6.7 s, then holds about 59 fps (production build, reference laptop). The way there: deck.gl draws only the moving vehicles with one IconLayer program, while lines, stops and labels are MapLibre layers and HTML. Before that, a first visit took 23-27 s. Probes: `scripts/perf-map.mjs`, `scripts/perf-shaders.mjs`. The story is in [console docs, Performance](console/README.md#performance).
- **Media:** `scripts/media.mjs` renders the screenshots and three captioned clips frame by frame on a fake clock. Output goes to `docs/media`; needs ffmpeg and a production preview on :4173.

### Docs

- `README.md`: portfolio front page. Every number in it links to a measurement (honest-claims rule).
- `docs/architecture/overview.md` (component status, sequences, topics, data model) and `review-2026-10-05.md` (the adversarial review that produced ADRs 0005-0018).
- `docs/adr/`: 13 ADRs plus an index. Numbers not yet written: 0003 (stream engine, in flight) and 0004, 0007, 0009, 0011, 0012, 0019, 0020, which the plan reserves for later decisions.
- `docs/guides/local-development.md` and `testing.md`, `docs/devlog.md`.

---

## 4. Work in flight

### Processor shell and engine gate (Track F)

- **Branch:** `worktree-agent-a3c5a4745031cbd2a`, worktree under `.claude/worktrees/` (locked while its agent runs). It is 2 commits ahead of `v2`, the latest being `1fafb00 feat(processor): runtime, output contracts and two engine shells`.
- **Expected deliverables:**
  - the processor shell around `vehicle.handle`;
  - a Quix Streams versus plain-consumer + SQLite engine spike (spikes S1, S2, S3 and S5);
  - **ADR-0003** (stream engine decision);
  - a processor container;
  - an end-to-end determinism test.
- **On return:**
  1. Check its state with `git log v2..worktree-agent-a3c5a4745031cbd2a`.
  2. If the agent didn't finish, read the branch's devlog entry and its ADR draft, then finish or redo the spike in a fresh session.
  3. Merge into `v2` with `git merge --no-ff`. Expect conflicts only in `docs/devlog.md` (resolve as a union, both entries kept) and possibly `README.md`.
  4. Run `make check` and `make test-integration` after merging.
- **Clean-up:** the worktrees for `a2157bfa…`, `a2831cd5…`, `a6ffd010…` and `ac95d629…` are fully merged. Remove them with `git worktree remove <path>` and `git branch -d <branch>`.

---

## 5. What to do next, in order

The plan's gates, mapped to reality:

**Gate 1 (week 1): CI green, v1 baseline written, one event from simulator to gateway to raw topic.**
- [x] v1 baseline measured and written.
- [x] CI green on GitHub (section 6).
- [ ] A recorded end-to-end smoke. The pieces are tested separately (gateway against Compose; simulator live mode against the gateway app). What's missing is a single run of `make up`, `wt-sim live … --sink http://127.0.0.1:18090`, then `rpk topic consume wt.input.v1` showing the record, written up in the devlog or as an integration test.

**Gate 2 (weeks 2-3): invariants hold under duplicate storms and crashes; a fixed seed replays deterministically; engine decision recorded.**
- [ ] Merge Track F; ADR-0003.
- [ ] Late-event lane and staleness ticker.
- [ ] Duplicate-storm and crash tests against the running processor.

**After that, roughly in plan order:**
1. Projector with version-guarded upserts; outbox relay (Gate 3: Postgres kill test, double-click acknowledge gives one intervention).
2. API v1 from OpenAPI first; session auth (ADR-0008); push protocol (ADR-0018). **Then wire the console to the stream** and retire the provisional estimator.
3. Archiver to SeaweedFS (Parquet), replay CLI, dbt Silver and Gold (Gate 4).
4. Evaluation harness: precision, recall and lead time against `data/scenarios/*.labels.yaml`, with v1 rules against v2 (Gate 5). The showcase labels are ready for it.
5. Console Gate 6: Playwright incident workflow at 1440 px and 390 px; 1,000 live vehicles at frame budget (**not measured yet**; the current fleet is 10-14 vehicles); axe and keyboard-only runs.
6. Weeks 12-14: benchmarks, game days, docs site (Astro Starlight, ADR-0014), case-study page, tag `v2.0` (Gate 7).

---

## 6. CI status

- The first-ever GitHub run (5 Oct, run 37323950989) failed to start four Python jobs. `astral-sh/setup-uv@v10` doesn't exist, because setup-uv stopped publishing floating major tags after v7. Fixed in `42171ce` by pinning `@v10.2.0`.
- The `secrets` and `console` jobs passed on that first run.
- The re-run (37324177350) is **green**: secrets, lint, typecheck, test (1m02s), integration with Testcontainers (57s) and console all passed.
- The "Failed to save: Unable to reserve cache" warnings are harmless: parallel jobs race to create the same uv cache.
- If CI turns red later, look first at Linux-versus-Windows differences that never show locally: path case, line endings.
- **Annotation to act on before 19 Oct 2026:** `ubuntu-latest` moves to Ubuntu 26. Pin `ubuntu-24.04` if anything breaks then.

---

## 7. Traps and gotchas

### Windows and OneDrive

- **The repo lives under OneDrive, which corrupts `.venv` launcher shims** ("null bytes" errors from `pytest`, `pre-commit`, `wt-sim`). The Makefile now runs tools as `python -m …`. If a shim breaks, run `uv sync --all-packages --reinstall`. The real fix is to move the repo out of OneDrive.
- Windows Defender once quarantined `uv.exe`. The system Python 3.11 and 3.14 installs are broken; use uv's managed 3.12 (`.python-version`).
- Python `write_text` on Windows writes CRLF. Pass `newline="\n"` in scripts that edit files; the pre-commit hook normalises otherwise.
- `pre-commit run --all-files` skips untracked files. The commit hook is the backstop.

### Repository

- The `.gitignore` rule must stay `/state/` (anchored). A bare `state/` once hid `apps/dashboard/src/state` from git.
- Prettier is installed in the console but **not enforced**. HEAD isn't Prettier-clean, so don't treat its warnings as regressions.

### Console and map

- **Use the production build for any performance judgement.** The dev server measured 0.2-47.5 fps on the map.
- **Each new deck.gl layer type costs seconds of shader compilation** on ANGLE/D3D11 on a first visit: PathLayer took 3+ s per variant. Run `node scripts/perf-shaders.mjs` before adding one, and prefer MapLibre layers for anything that doesn't move every frame.
- `map.isStyleLoaded()` also waits for tiles. Use the `styleReady()` helper in `MapView.tsx`, or the `style.load` and `load` events.
- MapLibre's worker must be imported explicitly (`?worker&url` plus `setWorkerUrl`), or production builds ship without it.
- The map canvas has `isolation: isolate`. Without it, deck.gl and the label layer draw over the floating panels.
- Measure performance with a **cold** profile (`perf-map.mjs` does). A warm-up period hides the first-visit cost.

### Data

- The simulator's fleet rows carry both truth (`cargo_c`) and device values (`cargo_probe_c`). The console must use the **probe** values, and must not treat an empty box (no shipments, or `probe_t` before loading) as cargo.
- **Known simulator label gap:** VAN-ABJ1 drives with the door open at 09:49 UTC in `console_showcase`, but `console_showcase.truth.jsonl` has no `door_open_moving` interval for it. Fix it in the simulator before the evaluation harness scores against these labels.
- The console's provisional estimator raises a few short-lived false forecasts (VAN-LAG1, VAN-ABJ1 at 5 °C). Expected; they go away when real risk assessments stream in.

### Media and CI

- `scripts/media.mjs` can crash the GPU process after many browser contexts. Run shots and clips separately (`ONLY=shots`, then `ONLY=clips CLIPS=lanes`, and so on).
- `tour-map.gif` is 6.1 MB: a moving basemap compresses badly. It's fine for GitHub; the MP4s are the high-quality versions.
- GitHub Actions: pin exact action versions and check that each tag exists (`gh api repos/<owner>/<action>/git/ref/tags/<tag>`).

---

## 8. Decisions still owed

These need an owner's call. Record each answer as an ADR or a plan amendment.

1. **Device reporting interval.** The simulator uses 15 s steps and the gateway accepts any interval. The real device spec decides buffer sizes and the 25k-truck load.
2. **What the 25,000-truck target is for:** a load test of the backbone only, or an end-to-end claim? Bulk mode can't drive a reconnect storm yet: it drops readings during outages instead of buffering them.
3. **ADR-0001 open question:** may a gate depend on a Tier 1 item, or only on Tier 0?
4. **Merge `v2` into `main`, and when?** No pull request is open yet (`https://github.com/emmanuelrichard01/logistics-watchtower/pull/new/v2`). `main` still shows v1 to visitors.
5. **Move the repo out of OneDrive** (section 7)?

---

## 9. Rules that keep the project honest

From `CLAUDE.md` and ADR-0001. Re-read them before adding anything.

- `packages/domain` stays pure: stdlib only, with time coming from event data.
- **No number in the README without a committed measurement.** Unmeasured figures are called targets.
- Tier 2 items are never built. A new component must pass ADR-0001's admission test and arrive with a test in the same commit.
- Decisions become MADR files in `docs/adr/` when they're made. Surprises and measurements go in `docs/devlog.md`.
- Conventional Commits. Seed every random element and use virtual clocks in tests; no sleeps.
- `legacy/v1/` is frozen.

---

## 10. Where to look

| Need | File |
| --- | --- |
| Scope, phases, gates | `plan/Logistics Watchtower 2.0 Rebuild Plan.md` (section 18 for the roadmap) |
| Why something is the way it is | `docs/adr/README.md`, then the ADR |
| What happened and what surprised us | `docs/devlog.md` |
| System shape, topics, data model | `docs/architecture/overview.md` |
| Running and testing locally | `docs/guides/local-development.md`, `docs/guides/testing.md` |
| Console design and behaviour | `docs/design/console.md`, `docs/console/README.md`, `PRODUCT.md` |
| Simulator scenarios and fixtures | `data/scenarios/`, `apps/dashboard-fixtures/README.md`, `docs/simulator/` |
| Processor logic | `packages/domain/src/watchtower_domain/vehicle.py`, then its imports |
| Console data pipeline | `apps/dashboard/src/data/frames.ts`, `recording.ts` |
| Map rendering | `apps/dashboard/src/views/MapView.tsx` (the comments explain each performance choice) |
