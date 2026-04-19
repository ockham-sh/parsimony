.PHONY: test lint typecheck format check sync clean

# Workspace-aware Makefile. Operates on every package under packages/* via uv workspace.
# `test` and `typecheck` walk packages/* so adding a new plugin needs no Makefile edit.
# Per-package pytest invocation is required: pytest binds one configfile per rootdir,
# and running across multiple pyproject configs cross-contaminates coverage targets.

sync:  ## Resolve and install workspace dependencies
	uv sync --all-extras --all-packages

test:  ## Run tests for every package that has a tests/ directory
	@for pkg in packages/*/; do \
	  if [ -d "$$pkg/tests" ]; then \
	    name=$$(awk -F'"' '/^name = /{print $$2; exit}' $$pkg/pyproject.toml); \
	    echo "=== $$name ==="; \
	    (cd $$pkg && uv run --package $$name pytest --tb=short -q) || exit $$?; \
	  fi; \
	done

lint:  ## Lint workspace
	uv run ruff check packages/

format:  ## Auto-format and auto-fix workspace
	uv run ruff format packages/
	uv run ruff check --fix packages/

typecheck:  ## Type-check every package source tree
	uv run mypy packages/*/parsimony*/

check: lint typecheck test  ## Lint + typecheck + test

clean:
	rm -rf build/ dist/ *.egg-info .mypy_cache .ruff_cache .pytest_cache htmlcov/
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
