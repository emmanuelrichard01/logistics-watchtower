# Logistics Watchtower 2.0

Solo 12-week rebuild (5 Oct to 27 Dec 2026). `plan/Logistics Watchtower 2.0 Rebuild Plan.md` is the source of truth for scope, phases and gates. Work happens on the `v2` branch.

## Commands

- `make install`: sync the uv workspace and install pre-commit hooks
- `make check`: lint + pyright + pytest, the same as CI. Run it before every commit.
- Add a workspace package under `packages/` or `apps/`; `uv sync --all-packages` picks it up.

## Rules

- `packages/domain` is pure: no Kafka, Postgres, HTTP or other I/O imports, and pyright strict. It's guarded by `tests/unit/test_domain_purity.py` until the import-linter contract lands.
- Honest claims: no number goes in the README unless a committed benchmark produced it. Unmeasured figures are labelled targets.
- Tier 2 items (plan section 2) are never built. Any new component must pass the admission test in ADR-0001.
- Every new component arrives with a test (and a metric, once metrics exist) in the same commit.
- Record decisions as MADR files in `docs/adr/NNNN-title.md` at the time they're made. Log surprises and measurements in `docs/devlog.md`.
- Use Conventional Commits.
- `legacy/v1/` is a frozen snapshot. Don't edit it except to keep it runnable for the baseline and the v1-versus-v2 comparison.
- Seed every random element and use virtual clocks in tests. No sleeps.

## graphify

This project has a knowledge graph at graphify-out/ with god nodes, community structure, and cross-file relationships.

Rules:
- For codebase questions, first run `graphify query "<question>"` when graphify-out/graph.json exists. Use `graphify path "<A>" "<B>"` for relationships and `graphify explain "<concept>"` for focused concepts. These return a scoped subgraph, usually much smaller than GRAPH_REPORT.md or raw grep output.
- If graphify-out/wiki/index.md exists, use it for broad navigation instead of raw source browsing.
- Read graphify-out/GRAPH_REPORT.md only for broad architecture review or when query/path/explain do not surface enough context.
- After modifying code, run `graphify update .` to keep the graph current (AST-only, no API cost).
