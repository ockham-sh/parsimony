# Development

This page is for contributors to `parsimony-core` itself: how to set up a development
environment, run the quality gates locally, and what continuous integration enforces on every
push. Everything here is derived from the repository's `Makefile`, `pyproject.toml`, and the
GitHub Actions workflows — run the same commands locally before opening a pull request and CI
will have nothing left to complain about.

Parsimony is **`parsimony-core` 0.7.0**, Apache-2.0 licensed, and supports **Python >=3.11**
(tested on 3.11, 3.12, and 3.13). The build backend is `hatchling`; the package builds from the
`parsimony` directory.

## Setting up

The project uses [`uv`](https://docs.astral.sh/uv/) for dependency management. Install the
editable package with the `dev` extra, which pulls the test, lint, and type-check toolchain plus
the heavy catalog dependencies needed to exercise the full suite:

```bash
uv pip install -e ".[dev]"
```

This is exactly what `make install` runs. The `dev` extra is defined as:

| Tool | Pin | Purpose |
| --- | --- | --- |
| `pytest` | `>=9.0.3` | test runner |
| `pytest-asyncio` | `>=1.3.0` | async test support (`asyncio_mode = auto`) |
| `pytest-cov` | `>=5.0` | coverage measurement and the 80% gate |
| `ruff` | `>=0.15.10` | linter and formatter |
| `mypy` | `>=1.10` | static type checker |
| `types-requests` | `>=2.31` | stubs |
| `pip-audit` | `>=2.7` | dependency vulnerability audit |
| `parsimony-core[standard,litellm]` | — | the FAISS + BM25 + sentence-transformers + litellm surface |

!!! warning "The full test suite needs the `standard` extra"
    A bare `pip install -e .` installs only the mandatory kernel (`pydantic`, `pandas`,
    `pyarrow`, `httpx`, `platformdirs`) — it does **not** pull `faiss-cpu`. The catalog index
    tests `import faiss` at collection time, so without the `standard` extra (which `dev`
    includes) `pytest` aborts collection with a `ModuleNotFoundError: No module named 'faiss'`.
    Always install `.[dev]` to run the whole suite. See
    [Installation](getting-started/installation.md) for the full optional-extras matrix.

## Make targets

The `Makefile` is the single entry point for every routine task. `PYTHON` defaults to
`.venv/bin/python` and is overridable — for example `PYTHON=python3.13 make test`. All tools run
through `$(PYTHON) -m <tool>`.

| Target | Runs | Notes |
| --- | --- | --- |
| `make install` | `uv pip install -e ".[dev]"` | editable install with the dev toolchain |
| `make test` | `pytest tests/ -x --tb=short -q` | fast feedback; stops at first failure |
| `make test-cov` | `pytest tests/ --cov=parsimony --cov-report=term-missing --cov-fail-under=80` | full run with the coverage gate |
| `make lint` | `ruff check …` then `ruff format --check …` over `parsimony/ tests/ examples/` | matches CI scope |
| `make format` | `ruff format …` then `ruff check --fix …` | auto-fixes in place |
| `make typecheck` | `mypy parsimony/` | only the package is type-checked |
| `make docs` | `mkdocs serve` | live-reload docs locally |
| `make docs-build` | `mkdocs build --strict` | strict build, fails on warnings |
| `make check` | `lint typecheck test` | the pre-PR aggregate |
| `make clean` | removes `build/ dist/ *.egg-info`, the tool caches, and `__pycache__` | — |
| `make help` | self-documenting target list | — |

Run `make help` to print the target list from the machine:

```text
check          Run all checks (lint + typecheck + test)
clean          Remove build artifacts
docs-build     Build docs
docs           Serve docs locally
format         Auto-format code
help           Show this help
install        Install dev dependencies
lint           Run ruff lint + format check (matches CI: parsimony/ tests/ examples/)
test-cov       Run tests with coverage report
test           Run tests with coverage
typecheck      Run mypy type checker
```

!!! tip "Reproduce CI in one command"
    `make check` runs `lint`, `typecheck`, and `test` in sequence — the same three gates CI
    enforces (CI adds a fourth, `pip-audit`). Running it before you push means the CI cycle has
    nothing new to find.

## Testing

Tests live under `tests/` and run with `pytest`. The configuration in
`[tool.pytest.ini_options]` applies to **every** invocation, including a bare `pytest`:

```toml
[tool.pytest.ini_options]
addopts = "--import-mode=importlib --cov=parsimony --cov-report=term-missing --cov-fail-under=80"
asyncio_mode = "auto"
pythonpath = ["."]
markers = [
    "integration: hits live APIs (may be slow, requires env vars)",
    "slow: heavy local test (e.g. large fixture); opt-in",
]
```

What this means in practice:

- **The 80% coverage gate is always on.** Because `--cov-fail-under=80` is baked into the
  default `addopts`, any plain `pytest` run fails if line coverage over the `parsimony` package
  drops below 80% — there is no way to run the suite without the gate.
- **`asyncio_mode = "auto"`** lets you write `async def test_*` functions directly; no
  `@pytest.mark.asyncio` decorator is needed. The public surface — connector calls,
  `Catalog.build`/`search`/`save`/`load`, embedder methods, `DataStore` methods, and
  `HttpClient.request` — is synchronous, so you rarely need this.
- **`pythonpath = ["."]`** and `--import-mode=importlib` let tests import `parsimony` from the
  working tree without an installed package on the path.

### Test markers

Two markers select subsets of the suite:

| Marker | Meaning | How to run / skip |
| --- | --- | --- |
| `integration` | hits live APIs; may be slow and needs env vars / credentials | CI runs `-m "not integration"` to skip these |
| `slow` | heavy local test, e.g. a large fixture; opt-in | select with `-m slow` or skip with `-m "not slow"` |

The CI test job deliberately excludes integration tests, so the gate stays hermetic:

```bash
pytest tests/ -v -m "not integration" --cov=parsimony --cov-report=term-missing --cov-fail-under=80
```

### Coverage omissions

Two modules are excluded from coverage measurement in `[tool.coverage.run]`:

```toml
[tool.coverage.run]
omit = [
    "parsimony/embedder.py",
    "parsimony/indexes.py",
]
```

`parsimony/embedder.py` requires sentence-transformers / litellm network I/O (unit tests use
fake embedders), and `parsimony/indexes.py` is a thin wrapper around the native FAISS library —
both are exercised by the catalog save/load integration tests rather than by unit tests, so
they would otherwise drag the unit-test coverage number down artificially. See
[Embedders](catalog/embedders.md) and [Indexes](catalog/indexes.md) for what those modules do.

!!! note "faiss is required for a clean collection"
    The adaptive-index tests `import faiss` at module top level. If you installed without the
    `standard` extra, `pytest` will report `1 error during collection` and interrupt the run
    before any test executes. This is an environment gap, not a repository defect — install
    `.[dev]` (which transitively pulls `[standard,litellm]`) to get a green collection.

## Linting and formatting

Linting and formatting are handled entirely by [`ruff`](https://docs.astral.sh/ruff/). The
configuration in `pyproject.toml`:

```toml
[tool.ruff]
target-version = "py311"
line-length = 120

[tool.ruff.lint]
select = ["E", "F", "I", "UP", "B", "SIM"]
```

The selected rule families are:

| Code | Family |
| --- | --- |
| `E` | pycodestyle errors |
| `F` | pyflakes |
| `I` | isort (import ordering) |
| `UP` | pyupgrade |
| `B` | flake8-bugbear |
| `SIM` | flake8-simplify |

The lint scope is `parsimony/ tests/ examples/` in both the `Makefile` and CI. `make lint` runs
`ruff check` followed by `ruff format --check` (the latter fails if any file is not formatted);
`make format` rewrites files in place with `ruff format` and then applies `ruff check --fix`.

## Type checking

Type checking uses [`mypy`](https://mypy-lang.org/), configured as:

```toml
[tool.mypy]
python_version = "3.11"
warn_return_any = true
warn_unused_ignores = true
ignore_missing_imports = true
```

`make typecheck` runs `mypy parsimony/` — only the `parsimony` package is type-checked, not
`tests/` or `examples/`. `ignore_missing_imports = true` keeps the optional, lazily-imported
heavy dependencies (faiss, sentence-transformers, litellm, huggingface_hub) from producing
stub-not-found errors when they are not installed.

## Building the docs

The documentation site is built with [mkdocs-material](https://squidfunk.github.io/mkdocs-material/).
Install the `docs` extra and use the Make targets:

```bash
uv pip install -e ".[docs]"
make docs        # mkdocs serve — live-reload at http://127.0.0.1:8000
make docs-build  # mkdocs build --strict — the build CI runs
```

The `docs` extra installs `mkdocs-material` and `mkdocs-multirepo-plugin`; the latter pulls the
`parsimony-connectors` repository's `docs/` subtree into the unified site at build time.
`mkdocs build --strict` fails on any warning (broken links, missing nav entries), so building
locally with the `--strict` flag catches the same problems CI would.

## Continuous integration

CI runs on GitHub Actions. Every push and pull request to `main` triggers the test and CodeQL
workflows; releases trigger publishing; doc-touching pushes redeploy the site. All jobs run on
`ubuntu-latest` with `astral-sh/setup-uv`.

### `CI` (test.yml) — push / PR to `main`

Four jobs, each a hard gate:

| Job | Python | What it runs |
| --- | --- | --- |
| `lint` | 3.12 | `ruff check parsimony/ tests/ examples/` then `ruff format --check …` |
| `typecheck` | 3.12 | installs `.[dev]`, then `mypy parsimony/` |
| `test` | matrix **3.11, 3.12, 3.13** | installs `.[dev]`, then `pytest tests/ -v -m "not integration" --cov=parsimony --cov-report=term-missing --cov-fail-under=80` |
| `audit` | 3.12 | installs `.[dev]`, then `pip-audit --strict` |

The `audit` job is a hard supply-chain gate, not a dev-only convenience: it audits the resolved
environment including the `[standard,litellm]` surface, on the reasoning that any CVE in
`torch` / `faiss` / `huggingface_hub` / `litellm` becomes a Parsimony CVE. You can reproduce it
locally with `uv tool run pip-audit --strict` (or `pip-audit --strict`) after installing
`.[dev]`.

### `CodeQL` (codeql.yml) — push / PR to `main` + weekly

CodeQL runs the `security-and-quality` query suite over the Python source on every push and
pull request to `main`, plus a weekly Monday cron. The repository's CodeQL config prunes two
queries — `py/call/wrong-named-argument` and `py/call/wrong-named-class-argument` — because they
are redundant with the mypy gate and false-positive on intentional bad-input tests (for example
`pytest.raises` blocks that pass deliberately wrong arguments). The config also ignores the
`.venv`, `dist`, and `build` paths.

### `Publish to PyPI` (publish.yml) — on release

Publishing uses [PyPI trusted publishing via OIDC](https://docs.pypi.org/trusted-publishers/) —
there is no long-lived API token. The workflow runs on a published GitHub release, or via
manual `workflow_dispatch` with a `target` input (`testpypi` by default, or `pypi`):

- The `build` job runs `uv build` to produce the sdist and wheel and uploads them as an
  artifact.
- `publish-pypi` runs on a release (or a `pypi` dispatch) and publishes to
  `https://pypi.org/p/parsimony-core` with `pypa/gh-action-pypi-publish`.
- `publish-testpypi` runs on a `testpypi` dispatch and publishes to TestPyPI.

### `Deploy Docs` (deploy-docs.yml) — on docs changes

Pushes to `main` that touch `docs/**`, `mkdocs.yml`, `pyproject.toml`, or the workflow file
itself (plus manual `workflow_dispatch`) rebuild the site with `pip install '.[docs]'` and
`mkdocs build --strict`, then deploy it to GitHub Pages with `actions/deploy-pages`.

!!! tip "Run the gates locally first"
    The whole local checklist before a PR is short: `make check` covers lint + typecheck +
    test, then add `pip-audit --strict` and `mkdocs build --strict`. Each is a few seconds; the
    CI round-trip is several minutes.

## See also

- [Installation](getting-started/installation.md) — the optional-extras matrix and why the base install stays lean.
- [Command-line interface](cli.md) — the `parsimony` console script the package ships.
- [Conformance testing](plugins/conformance.md) — the `parsimony.testing` toolkit for validating provider plugins.
- [Public API & import map](reference/api.md) — what to import from where.
