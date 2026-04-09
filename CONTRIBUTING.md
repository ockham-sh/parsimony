# Contributing to ockham

Thank you for your interest in contributing! This guide will help you get started.

## Development Setup

We use [uv](https://docs.astral.sh/uv/) for dependency management:

```bash
git clone https://github.com/<your-username>/ockham.git
cd ockham
uv venv && source .venv/bin/activate
uv pip install -e ".[sdmx,embeddings]"
uv pip install pytest pytest-asyncio ruff mypy
```

## Running Checks

```bash
# Tests
pytest tests/ -v

# Linting
ruff check .

# Formatting
ruff format --check .

# Type checking
mypy ockham/
```

## Making Changes

1. **Fork** this repository
2. **Create a feature branch** from `main`
3. **Write tests** for new functionality
4. **Run checks** (tests, linting, type checking)
5. **Submit a pull request** with a clear description

### Code Style

- We use [ruff](https://docs.astral.sh/ruff/) for linting and formatting (line length: 120)
- Type hints on all public function signatures
- Docstrings on public classes and functions
- Pydantic models for external contracts

### Pull Request Guidelines

- Keep PRs focused on a single change
- Include tests for new connectors or features
- Update CHANGELOG.md under the `[Unreleased]` section
- Reference any related issues

### Adding a New Connector

See the FRED connector (`ockham/connectors/fred.py`) as a reference implementation. A new connector needs:

1. A Pydantic params model
2. An async function decorated with `@connector()`
3. A `CONNECTORS` export bundling all connector functions
4. Tests in `tests/`

## Repository Structure

This repository is a read-only mirror of `packages/ockham/` in our development monorepo. Your PR will be reviewed here and synced upstream.

## Code of Conduct

Please read our [Code of Conduct](../../CODE_OF_CONDUCT.md). We are committed to providing a welcoming and inclusive experience for everyone.
