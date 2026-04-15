.PHONY: test lint typecheck format docs clean install

# Default Python — override with PYTHON=python3.13 make test
PYTHON ?= .venv/bin/python

install:  ## Install dev dependencies
	uv pip install -e ".[all,dev,docs]"

test:  ## Run tests with coverage
	$(PYTHON) -m pytest tests/ -x --tb=short -q

test-cov:  ## Run tests with coverage report
	$(PYTHON) -m pytest tests/ --cov=parsimony --cov-report=term-missing --cov-fail-under=80

lint:  ## Run ruff linter
	$(PYTHON) -m ruff check parsimony/ tests/

format:  ## Auto-format code
	$(PYTHON) -m ruff format parsimony/ tests/
	$(PYTHON) -m ruff check --fix parsimony/ tests/

typecheck:  ## Run mypy type checker
	$(PYTHON) -m mypy parsimony/

docs:  ## Serve docs locally
	$(PYTHON) -m mkdocs serve

docs-build:  ## Build docs
	$(PYTHON) -m mkdocs build --strict

check: lint typecheck test  ## Run all checks (lint + typecheck + test)

clean:  ## Remove build artifacts
	rm -rf build/ dist/ *.egg-info .mypy_cache .ruff_cache .pytest_cache htmlcov/
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true

help:  ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-15s\033[0m %s\n", $$1, $$2}'
