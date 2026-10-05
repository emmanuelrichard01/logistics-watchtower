.DEFAULT_GOAL := help
.PHONY: help install lint format typecheck test check

help: ## List targets
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*## "}; {printf "  %-10s %s\n", $$1, $$2}'

install: ## Sync the workspace and install git hooks
	uv sync --all-packages
	uv run pre-commit install

lint: ## Run every pre-commit hook on all files
	uv run pre-commit run --all-files --show-diff-on-failure

format: ## Apply ruff fixes and formatting
	uv run ruff check --fix .
	uv run ruff format .

typecheck: ## Run pyright (strict for the domain package)
	uv run pyright

test: ## Run the test suite
	uv run pytest

check: lint typecheck test ## Everything CI runs on a pull request
