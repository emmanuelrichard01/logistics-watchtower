# Local Development

## Prerequisites

| Tool | Version used | Notes |
| --- | --- | --- |
| [uv](https://docs.astral.sh/uv/) | 0.12 | Manages Python 3.12 (`.python-version`) and the workspace lockfile |
| Docker Desktop | Engine 29.8 | Runs the core stack and the integration tests (Testcontainers) |
| Node.js | 24 or later | The console (`apps/dashboard`) |
| GNU Make | 4.4 | Every workflow has a target; `make help` lists them |
| ffmpeg | any recent | Only for regenerating documentation media |

## First run

```bash
make install         # uv sync of the whole workspace, plus pre-commit hooks
make check           # lint, pyright, fast tests: exactly what CI runs
make up              # Redpanda, Schema Registry, Console, Postgres/PostGIS, SeaweedFS, gateway; waits until healthy
make migrate         # apply the Alembic schema to the stack's Postgres
make console         # build the console and serve it at http://localhost:4173
```

`make down` stops the stack and keeps its volumes. `make reset` also deletes the data.

## Ports

All host ports bind to `127.0.0.1` and use a `1xxxx` prefix, so they don't collide with other local stacks (`infra/compose/compose.yaml`).

| Service | Host port | Notes |
| --- | --- | --- |
| Kafka API (Redpanda) | 19092 | Inside the Compose network: `redpanda:9092` |
| Schema Registry | 18081 | |
| Redpanda HTTP proxy | 18082 | |
| Redpanda admin | 19644 | |
| Redpanda Console | 18080 | http://localhost:18080 |
| Postgres / PostGIS | 15432 | User and database `watchtower`; dev password `watchtower`, overridable with `POSTGRES_PASSWORD` |
| SeaweedFS S3 | 18333 | Dev-only credentials, never used outside the local stack |
| Ingest gateway | 18090 | `POST` batches of signed readings (ADR-0021) |
| Console, production preview | 4173 | `make console` |
| Console, dev server | 5173 | `make console-dev` |

Topics are created by a one-shot `topics-init` container **before** anything consumes them, and topic auto-creation is switched off. v1 lost about 10,000 alerts to a start-order race ([audit](../audit/v1-baseline.md), defect 17).

## Console: dev server or production build

Use `make console` (the production build) for demos and any performance judgement. On the reference laptop, the map holds 59.8-59.9 fps in the production build, against 0.2-47.5 fps on the dev server ([console docs](../console/README.md#performance)). Use `make console-dev` while editing, for hot reload.

## Windows notes

The project is developed on Windows 11. A few things bit during week 1 (details in the [devlog](../devlog.md)):

- **Antivirus and `uv.exe`.** Windows Defender briefly quarantined `uv.exe` as "potentially unwanted software", and later released it. If it happens, resolve it in Defender rather than working around it.
- **Stray Python installs.** Two system Python installs had lost their `python.exe`. uv's managed CPython 3.12 is the one to rely on, and `uv sync` creates `.venv` from it.
- **OneDrive.** The repository lives in a OneDrive folder, and mid-session the `.venv` console-script launchers broke (`SyntaxError: source code cannot contain null bytes`). `uv sync --all-packages --reinstall` repairs them; `uv run python -m <tool>` works in the meantime. Keeping the repository outside OneDrive avoids the problem.
- **Line endings.** `.gitattributes` forces LF, and a pre-commit hook normalises mixed endings. Editors that write CRLF will see that hook "fail" once, after it has fixed the file: stage again and commit.
- **Docker Desktop** must be running before `make up` or `make test-integration`.

## Project conventions

See [`CLAUDE.md`](../../CLAUDE.md) for the working rules (domain purity, honest claims, ADRs, devlog, Conventional Commits) and the [ADR index](../adr/README.md) for decisions.
