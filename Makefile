.DEFAULT_GOAL := help
.PHONY: help install lint format typecheck test test-integration check up down reset logs migrate

COMPOSE := docker compose -f infra/compose/compose.yaml --profile core

help: ## List targets
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*## "}; {printf "  %-17s %s\n", $$1, $$2}'

install: ## Sync the workspace and install git hooks
	uv sync --all-packages
	uv run pre-commit install

lint: ## Run every pre-commit hook on all files
	uv run pre-commit run --all-files --show-diff-on-failure

format: ## Apply ruff fixes and formatting
	uv run ruff check --fix .
	uv run ruff format .

typecheck: ## Run pyright (strict for the domain and contracts packages)
	uv run pyright

test: ## Run the fast test suite (no containers)
	uv run pytest

test-integration: ## Run integration tests against throwaway containers (needs Docker)
	uv run pytest -m integration

check: lint typecheck test ## Everything CI runs on a pull request

up: ## Start the core stack and wait until every service is healthy
	$(COMPOSE) up -d --wait

down: ## Stop the core stack, keeping its data volumes
	$(COMPOSE) down --remove-orphans

reset: ## Stop the core stack and delete its data volumes
	$(COMPOSE) down --remove-orphans --volumes

logs: ## Follow the core stack's logs
	$(COMPOSE) logs -f --tail=100

migrate: ## Apply database migrations to the core stack's Postgres
	uv run alembic -c migrations/alembic.ini upgrade head
