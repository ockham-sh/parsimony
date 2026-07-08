# Contributing to parsimony

Thank you for your interest in contributing! This guide covers **kernel
development**.

**Want to write a connector instead?** The kernel accepts no
provider-specific code — that's structurally enforced by
[`tests/test_kernel_purity.py`](tests/test_kernel_purity.py). Connectors are
separate distributions. Start with
[**Author your first connector**](docs/authoring-connectors.md), the end-to-end
guide covering scaffolding, a worked example, conformance, and publishing. To
contribute a public source to the official monorepo, see
[ockham-sh/parsimony-connectors](https://github.com/ockham-sh/parsimony-connectors).

## Development setup

### Option 1: uv (recommended)

[uv](https://docs.astral.sh/uv/) is the fastest way to get a working
environment:

```bash
git clone https://github.com/<your-username>/parsimony.git
cd parsimony
uv venv && source .venv/bin/activate
uv pip install -e ".[dev]"
```

### Option 2: pip

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

## Making changes

1. **Fork** this repository.
2. **Create a feature branch** from `main` (`git checkout -b feat/my-feature`).
3. **Write tests** for new functionality (we follow TDD — write tests first).
4. **Run checks** (tests, linting, type checking).
5. **Submit a pull request** with a clear description.

### Code style

- We use [ruff](https://docs.astral.sh/ruff/) for linting and formatting
  (line length: 120).
- Type hints on all public function signatures.
- Docstrings on public classes and functions.
- Pydantic models for all external contracts (parameter boundaries).
- Immutable data structures preferred (`frozen=True` dataclasses,
  `Connectors` is immutable).

### What belongs in the kernel

The kernel ships connector primitives, the `Catalog` reference implementation,
plugin discovery, conformance, the CLI, and shared HTTP utilities. That's it.

Provider-specific code — API wrappers, endpoint knowledge, response-shape
transformations — lives in individual `parsimony-<name>` plugin
distributions, not here.

If you're unsure whether a change belongs in the kernel or a plugin,
open an issue before you start coding.

## Pull request guidelines

- Keep PRs focused on a single change.
- Include tests for new features.
- Update `CHANGELOG.md` under the `[Unreleased]` section.
- Reference any related issues.
- Use conventional commit messages: `feat:`, `fix:`, `refactor:`,
  `docs:`, `test:`, `chore:`.

## Project structure

```
parsimony/
├── __init__.py         Public surface (lazy-loaded via PEP 562).
├── connector.py        Connector + Connectors + @connector / @enumerator / @loader.
├── result.py           Result + Provenance + OutputConfig + Column + ColumnRole.
├── catalog/            Catalog runtime, indexes, query parsing, search helpers, snapshot I/O.
├── catalog/search.py   make_local_search_connector and related helpers.
├── embedder.py         EmbeddingProvider Protocol + SentenceTransformerEmbedder + OnnxEmbedder + LiteLLMEmbeddingProvider.
├── indexes.py          FAISS + BM25 pure functions (private).
├── catalog/indexes.py  BM25Index, VectorIndex, HybridIndex, CatalogIndex.
├── discover.py         Provider + iter_providers + load + load_all (~70 LOC).
├── stores.py           InMemoryDataStore + LoadResult.
├── errors.py           ConnectorError hierarchy.
├── transport/          HttpClient, pooled_client, check_status, redact_url + helpers.py.
├── testing.py          assert_plugin_valid + ProviderTestSuite.
└── cli.py              Two verbs: `parsimony list`, `parsimony cache`.
```

Individual connectors live in the `parsimony-connectors` monorepo.

### Key architectural decisions

- **Three decorator primitives**: `@connector` (fetch/search),
  `@enumerator` (catalog population), `@loader` (data persistence). All
  produce the same `Connector` type.
- **Structural typing over ABCs**: `EmbeddingProvider` is a
  `typing.Protocol` class. Custom embedders match the shape; no
  subclassing required.
- **Module layout**: connector/result/discover/errors at the top level;
  `catalog/` is the catalog subpackage; heavy ranking/index helpers live
  in sibling modules.
- **Frozen dataclasses + immutable collections**: `Connector` is
  `@dataclass(frozen=True)`, `Connectors` is immutable.
- **Typed boundaries**: connector parameters use normal Python signatures
  (`inspect.Signature.bind`); Pydantic models are used for `Result`,
  `Provenance`, and catalog row types.
- **Lazy loading** in `__init__.py` via `__getattr__` (PEP 562) — keeps
  `import parsimony` fast.
- **Dependency injection** — bind operator-supplied values via `Connector.bind()` or `Connectors.bind()`.

## Reporting bugs

For bugs in the kernel itself, open a GitHub issue with:

- The `parsimony-core` version (`pip show parsimony-core`).
- A minimal reproduction (ideally a failing test).
- The full traceback.

For security issues: see [`SECURITY.md`](SECURITY.md) — do **not** open a
public issue.

## Code of conduct

Please read our [Code of Conduct](CODE_OF_CONDUCT.md). We are committed to
providing a welcoming and inclusive experience for everyone.
