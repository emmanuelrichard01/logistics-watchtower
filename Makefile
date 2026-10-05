.DEFAULT_GOAL := help
# Tools run as `python -m`: the .venv console-script shims get corrupted when the repo
# lives in a synced folder (OneDrive), and `python -m` doesn't depend on them.
.PHONY: help install lint format typecheck test test-integration check up down reset logs migrate console console-dev console-check

COMPOSE := docker compose -f infra/compose/compose.yaml --profile core

help: ## List targets
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*## "}; {printf "  %-17s %s\n", $$1, $$2}'

install: ## Sync the workspace and install git hooks
	uv sync --all-packages
	uv run python -m pre_commit install

lint: ## Run every pre-commit hook on all files
	uv run python -m pre_commit run --all-files --show-diff-on-failure

format: ## Apply ruff fixes and formatting
	uv run ruff check --fix .
	uv run ruff format .

typecheck: ## Run pyright (strict for the domain and contracts packages)
	uv run python -m pyright

test: ## Run the fast test suite (no containers)
	uv run python -m pytest

test-integration: ## Run integration tests against throwaway containers (needs Docker)
	uv run python -m pytest -m integration

check: lint typecheck test ## Everything CI runs on a pull request

up: ## Build changed images, start the core stack and wait until every service is healthy
	$(COMPOSE) up -d --wait --build

down: ## Stop the core stack, keeping its data volumes
	$(COMPOSE) down --remove-orphans

reset: ## Stop the core stack and delete its data volumes
	$(COMPOSE) down --remove-orphans --volumes

logs: ## Follow the core stack's logs
	$(COMPOSE) logs -f --tail=100

migrate: ## Apply database migrations to the core stack's Postgres
	uv run python -m alembic -c migrations/alembic.ini upgrade head

console-check: ## Console: lint, type check, tests and production build
	npm --prefix apps/dashboard ci
	npm --prefix apps/dashboard run lint
	npm --prefix apps/dashboard run typecheck
	npm --prefix apps/dashboard test
	npm --prefix apps/dashboard run build

console: ## Console: production build served at http://localhost:4173 (smooth; use this for demos)
	npm --prefix apps/dashboard run build
	npm --prefix apps/dashboard run preview -- --port 4173

console-dev: ## Console: dev server with hot reload at http://localhost:5173 (slower; for editing)
	npm --prefix apps/dashboard run dev
