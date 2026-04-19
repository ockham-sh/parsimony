# Building a new parsimony plugin

A parsimony plugin is a standalone Python package that exposes connectors to the `parsimony` kernel via an entry point. This guide walks through building one from scratch. For the authoritative contract, see [`contract.md`](contract.md).

## Canonical template

The reference implementation is **[`ockham-sh/parsimony-fred`](https://github.com/ockham-sh/parsimony-fred)** — copy its structure and adjust.

```
parsimony-fred/
├── parsimony_fred/
│   ├── __init__.py          # connectors, CONNECTORS, ENV_VARS, PROVIDER_METADATA
│   └── py.typed
├── tests/
│   ├── test_conformance.py  # assert_plugin_valid — release-blocking
│   └── test_<...>.py        # unit tests with respx-mocked HTTP
├── .github/workflows/
│   ├── test.yml             # lint + type + test + conformance
│   └── publish.yml          # PyPI trusted publishing on release
├── pyproject.toml           # entry-point registration, deps, metadata
├── README.md
├── CHANGELOG.md
├── LICENSE                  # Apache-2.0 for official plugins
└── .gitignore
```

## Minimum `pyproject.toml`

```toml
[project]
name = "parsimony-<your-name>"
version = "0.1.0"
license = "Apache-2.0"
requires-python = ">=3.11"
dependencies = [
    "parsimony-core>=0.1.0a1,<0.2",
    "pydantic>=2.11.1,<3",
    "pandas>=2.3.0,<3",
]

[project.optional-dependencies]
dev = [
    "pytest>=9.0.3",
    "pytest-asyncio>=1.3.0",
    "respx>=0.22.0",
    "ruff>=0.15.10",
    "mypy>=1.10",
]

[project.entry-points."parsimony.providers"]
<your-name> = "parsimony_<your_name>"

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[tool.hatch.build.targets.wheel]
packages = ["parsimony_<your_name>"]
```

## Minimum module

```python
# parsimony_<your_name>/__init__.py
from parsimony import Connectors, connector
from parsimony.result import Result, Provenance

ENV_VARS: dict[str, str] = {"api_key": "<YOUR>_API_KEY"}

PROVIDER_METADATA: dict = {
    "homepage": "https://example.com",
    "pricing": "freemium",
}


@connector(tags=["tool"])
async def <your_name>_search(params: SearchParams, *, api_key: str) -> Result:
    """At least 40 chars — MCP tool descriptions need enough context for LLMs."""
    ...


CONNECTORS = Connectors([<your_name>_search])
```

## Conformance gate

Every release must pass the conformance test:

```python
# tests/test_conformance.py
import parsimony_<your_name>
from parsimony.testing import assert_plugin_valid

def test_plugin_conforms() -> None:
    assert_plugin_valid(parsimony_<your_name>)
```

Run locally:

```bash
pip install -e .[dev]
pytest tests/test_conformance.py
```

In CI, make the conformance job release-blocking.

## Verify end-to-end discovery

After installing alongside `parsimony-core`:

```bash
parsimony list-plugins
```

Your plugin should appear with `CONFORMANCE: pass`.

## Publishing

1. Configure [PyPI trusted publishing](https://docs.pypi.org/trusted-publishers/) for your GitHub repo.
2. Copy the workflow files from `parsimony-fred/.github/workflows/` into your repo.
3. Tag a release (`git tag v0.1.0 && git push --tags`); GitHub Actions publishes to PyPI.

## Checklist before cutting v0.1.0

- [ ] `parsimony_<your_name>` module exports `CONNECTORS`, `ENV_VARS`, `PROVIDER_METADATA`.
- [ ] Entry point registered in `pyproject.toml` under `parsimony.providers`.
- [ ] `parsimony.testing.assert_plugin_valid(module)` passes.
- [ ] Tool-tagged connectors have ≥40-character descriptions.
- [ ] Unit tests cover happy path + at least one error path (401, 429, empty).
- [ ] README documents install, setup, example usage.
- [ ] Apache-2.0 `LICENSE` file present.
- [ ] CI workflows green on main.

## When to create a per-provider vs protocol-grouped plugin

See the full decision rule in `PLAN-plugin-migration.md` § 4. TL;DR:

- Per-provider (`parsimony-<provider>`) when the API is bespoke. **Default.**
- Protocol-grouped (`parsimony-<protocol>`) only when multiple providers share a wire protocol, >60% of implementation, dependency tree, and maintenance cadence. Examples: `parsimony-sdmx`, `parsimony-pxweb`.
