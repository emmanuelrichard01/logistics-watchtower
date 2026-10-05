# Logistics Watchtower 2.0

A cold-chain risk decision platform for refrigerated fleets: which shipment is at risk, how long until the cargo is compromised, how confident the system is, and what to do next.

> **Status: under construction.** Week 1 of a 14-week rebuild. Nothing here is a finished feature, and this README contains no performance numbers. Under the [honest-claims rule](docs/adr/0001-mission-scope-and-non-goals.md), every number that appears later will link to a benchmark in this repo.

The previous version is preserved at tag [`v1-final`](../../tree/v1-final) and in [`legacy/v1/`](legacy/v1/). The [v1 audit](docs/audit/v1-baseline.md) explains why it is being rebuilt.

## Quick start

Requires [uv](https://docs.astral.sh/uv/) and GNU Make.

```bash
make install   # sync the workspace, install git hooks
make check     # lint, type check and tests: exactly what CI runs
make up        # start the local stack (Docker), then: make migrate
```

`make help` lists all targets.

## Layout

| Path | Contents |
| --- | --- |
| `packages/domain/` | Pure domain logic. No I/O, strict type checking |
| `packages/contracts/` | Avro schemas and event identity |
| `apps/gateway/` | Ingest gateway: validates and signature-checks device batches, produces to `wt.input.v1`, quarantines the rest (ADR-0021) |
| `packages/platform/` | I/O adapters: Kafka producer factory with pinned settings, Schema Registry client |
| `infra/compose/` | Local stack: Redpanda, Schema Registry, Console, PostGIS, SeaweedFS, gateway on 127.0.0.1:18090 (`make up`) |
| `migrations/` | Alembic migrations for the Postgres schema (`make migrate`) |
| `tests/integration/` | Container-backed tests (`make test-integration`, needs Docker) |
| `tests/` | Test suite |
| `docs/adr/` | Architecture decision records |
| `docs/audit/` | v1 audit and baseline |
| `docs/devlog.md` | Running log of surprises and measurements |
| `plan/` | The rebuild plan |
| `legacy/v1/` | Frozen v1, kept for the v1-versus-v2 comparison |

The target layout is in section 17 of the [rebuild plan](plan/Logistics%20Watchtower%202.0%20Rebuild%20Plan.md). Directories appear when their first code does.

## Author

**Emmanuel Richard**, Data Engineer · [GitHub](https://github.com/emmanuelrichard01)
