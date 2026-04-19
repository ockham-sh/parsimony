# Contributing to parsimony

Thank you for your interest in contributing! This guide covers everything from setting up your development environment to submitting a pull request.

## Development Setup

### Option 1: uv (recommended)

[uv](https://docs.astral.sh/uv/) is the fastest way to get a working environment:

```bash
git clone https://github.com/<your-username>/parsimony.git
cd parsimony
uv venv && source .venv/bin/activate
uv pip install -e ".[dev]"
```

### Option 2: pip

Standard pip works fine if you prefer it:

```bash
git clone https://github.com/<your-username>/parsimony.git
cd parsimony
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
```

### Verify your setup

```bash
pytest tests/ -v --tb=short
ruff check .
```

If the tests pass and ruff reports no errors, you are ready to contribute.

### Quick commands

A `Makefile` is provided for common tasks:

```bash
make check      # lint + typecheck + test (all three)
make test       # pytest
make test-cov   # pytest with 80% coverage threshold
make lint       # ruff check
make typecheck  # mypy
make format     # ruff format + auto-fix
make docs       # mkdocs serve (localhost:8000)
```

Or run them directly:

```bash
pytest tests/ -v
ruff check .
ruff format --check .
mypy parsimony/
```

Run all checks before submitting a PR. CI will run the same commands.

## Making Changes

1. **Fork** this repository
2. **Create a feature branch** from `main` (`git checkout -b feat/my-feature`)
3. **Write tests** for new functionality (we follow TDD -- write tests first)
4. **Run checks** (tests, linting, type checking)
5. **Submit a pull request** with a clear description

### Code Style

- We use [ruff](https://docs.astral.sh/ruff/) for linting and formatting (line length: 120)
- Type hints on all public function signatures
- Docstrings on public classes and functions
- Pydantic models for all external contracts (parameter boundaries)
- Immutable data structures preferred (`frozen=True` dataclasses, `Connectors` is immutable)

## Adding a New Connector

New connectors are the most common contribution. Here is the step-by-step process.

### 1. Create the connector module

Add a new file in `parsimony/connectors/`. Use `parsimony/connectors/fred.py` as the reference implementation. Your module needs:

- **Pydantic params model(s)** -- one model per connector function, defining the user-facing parameters with types, defaults, and descriptions.
- **OutputConfig** (optional but recommended) -- declares column roles (KEY, TITLE, DATA, METADATA) for typed results. Required for connectors that will feed the catalog.
- **Async connector function(s)** decorated with `@connector()`, `@enumerator()`, or `@loader()`.
- **A `CONNECTORS` export** -- a `Connectors([...])` bundle containing all connector instances from the module.

For detailed patterns, see [docs/connector-implementation-guide.md](docs/connector-implementation-guide.md).

### 2. Wire up the factory (if applicable)

If your connector requires an API key, add it to `build_connectors_from_env()` in `parsimony/connectors/__init__.py`. Add a `ProviderSpec` entry to the `PROVIDERS` registry and export an `ENV_VARS` dict from your module. Follow the existing pattern:

```python
my_key = _env.get("MY_API_KEY")
if my_key:
    from parsimony.connectors.my_source import CONNECTORS as MY_SOURCE
    result = result + MY_SOURCE.bind_deps(api_key=my_key)
```

### 3. Write tests

Add tests in `tests/`. At minimum:

- **Unit tests** for param validation (required fields, defaults, validators)
- **Integration tests** for the connector function (mock HTTP responses with `pytest` fixtures)
- Verify the result type (`Result` vs `SemanticTableResult`)
- Verify provenance is populated correctly
- Verify column roles match the `OutputConfig`

### 4. Update documentation

- Add your connector to the env var table in `docs/user-guide.md`
- Add a usage example showing common call patterns

### Connector PR checklist

Before submitting your PR, verify:

- [ ] Pydantic params model with `Field(description=...)` on every field
- [ ] Async function with a descriptive docstring (used as the connector description)
- [ ] `OutputConfig` with correct column roles (if the connector returns tabular data)
- [ ] Dependencies (API keys, HTTP clients) declared as keyword-only args after `*`
- [ ] `CONNECTORS` bundle exported from the module
- [ ] Tests pass: `pytest tests/ -v`
- [ ] Linting passes: `ruff check .`
- [ ] Type checking passes: `mypy parsimony/`
- [ ] No hardcoded API keys or secrets in the source
- [ ] User guide and env var table updated

## Good First Issues

If you are looking for a way to get started, here are some approachable tasks:

- **Add tests for an existing connector** -- several connectors have minimal test coverage. Pick one and add param validation + mocked response tests.
- **Improve error messages** -- find a connector that raises a generic `ValueError` and make the message more helpful (include the parameter that failed, suggest a fix).
- **Add a new OutputConfig** -- some connectors return raw `Result` without column roles. Adding an `OutputConfig` makes them catalog-indexable.
- **Documentation examples** -- add a new example script in `examples/` showing a workflow not yet covered.

Look for issues labeled `good first issue` in the issue tracker.

## Pull Request Guidelines

- Keep PRs focused on a single change
- Include tests for new connectors or features
- Update CHANGELOG.md under the `[Unreleased]` section
- Reference any related issues
- Use conventional commit messages: `feat:`, `fix:`, `refactor:`, `docs:`, `test:`, `chore:`

## Project Structure

```
parsimony/
├── connector.py          # @connector, @enumerator, @loader decorators + Connectors collection
├── result.py             # Result, SemanticTableResult, OutputConfig, Provenance, Column (+ Arrow/Parquet)
├── errors.py             # ConnectorError hierarchy + BundleNotFoundError
├── catalog/              # BaseCatalog ABC, SeriesEntry/SeriesMatch models, EmbedderInfo
├── _standard/            # Canonical Catalog (Parquet + FAISS + BM25 + RRF), embedders, URL sources
├── bundles/              # CatalogSpec/CatalogPlan declarative layer + LazyNamespaceCatalog wrapper
├── discovery/            # Plugin entry-point discovery (parsimony.providers) + build_connectors_from_env
├── stores/               # DataStore + InMemoryDataStore (observation persistence for @loader)
├── transport/            # HttpClient with credential redaction, JSON helpers
├── cli/                  # `parsimony list-plugins`, `parsimony conformance verify`, `parsimony bundles {list,build}`
└── testing.py            # assert_plugin_valid (procedural) + ProviderTestSuite (pytest class)
```
(MCP server lives in the separate `parsimony-mcp` distribution.)

### Key architectural decisions

- **Three decorator primitives**: `@connector` (fetch/search), `@enumerator` (catalog population), `@loader` (data persistence). All produce the same `Connector` type.
- **Frozen dataclasses + immutable collections** -- `Connector` is `@dataclass(frozen=True)`, `Connectors` is immutable.
- **Pydantic models at boundaries** -- every connector validates params via a Pydantic `BaseModel`.
- **Lazy loading** in `__init__.py` via `__getattr__` -- keeps `import parsimony` fast.
- **Provider registry** -- `PROVIDERS` tuple in `connectors/__init__.py` drives `build_connectors_from_env`.
- **Dependency injection** -- keyword-only args after `*` in connector functions, bound via `bind_deps()`.

## Code of Conduct

Please read our [Code of Conduct](CODE_OF_CONDUCT.md). We are committed to providing a welcoming and inclusive experience for everyone.
