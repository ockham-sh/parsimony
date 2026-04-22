# Building a new parsimony plugin

A parsimony plugin is a standalone Python package that exposes connectors to
the `parsimony` kernel via an entry point. This guide walks through building
one from scratch.

For the authoritative contract: [`contract.md`](contract.md).
For internal / private plugins: [`building-a-private-connector.md`](building-a-private-connector.md).
For the full implementation walkthrough (provider research, schema design,
error mapping, testing): [`connector-implementation-guide.md`](connector-implementation-guide.md).

## Canonical template

The reference implementation is
[`ockham-sh/parsimony-connectors/packages/fred`](https://github.com/ockham-sh/parsimony-connectors/tree/main/packages/fred).
Copy its structure and adjust:

```
parsimony-<yourname>/
├── parsimony_<yourname>/
│   ├── __init__.py         CONNECTORS (+ optional CATALOGS / RESOLVE_CATALOG)
│   ├── connectors.py       @connector / @enumerator / @loader functions
│   └── py.typed
├── tests/
│   ├── test_conformance.py          assert_plugin_valid — release-blocking
│   └── test_<yourname>_connectors.py happy path + error mapping (respx mocks)
├── .github/workflows/
│   ├── ci.yml              lint + type + test + conformance
│   └── release.yml         OIDC PyPI publish on tag
├── pyproject.toml          entry-point registration, kernel pin, metadata, [project.urls] homepage
├── README.md
├── CHANGELOG.md
├── LICENSE                 Apache-2.0 for official plugins
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
    "parsimony-core>=0.4,<0.5",
    "pydantic>=2.11,<3",
    "pandas>=2.3,<3",
    "httpx>=0.27,<1",
]

[project.urls]
Homepage = "https://your-provider.example"

[project.optional-dependencies]
dev = [
    "pytest>=9.0",
    "pytest-asyncio>=1.3",
    "respx>=0.22",
    "ruff>=0.15",
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

The kernel pin (`parsimony-core>=0.4,<0.5`) is the stability boundary.
There is no separate contract-version classifier — plugins depend on
`parsimony-core` via a standard range pin and rely on the [stability
markings](contract.md#2-versioning) in the API reference.

`[project.urls] Homepage` is what the kernel surfaces via
`Provider.homepage` (e.g. in `parsimony list` output and in the
`parsimony-mcp` `init` env-template generator). Provider version is read
from the distribution metadata; do not export a module-level `__version__`.

## Minimum plugin module

```python
# parsimony_<your_name>/__init__.py
from parsimony import Connectors, connector, Result


@connector(env={"api_key": "<YOUR>_API_KEY"}, tags=["<your_name>", "tool"])
async def <your_name>_search(params: SearchParams, *, api_key: str) -> Result:
    """At least 40 chars — MCP tool descriptions need enough context for LLMs."""
    ...


CONNECTORS = Connectors([<your_name>_search])
```

Per-connector env vars live on the `@connector(env={...})` decorator —
the consumer resolves them via `Connectors.bind_env()`. There is no
module-level `ENV_VARS`, `PROVIDER_METADATA`, or `__version__`.

If your plugin publishes catalog bundles, add a `CATALOGS` export:

```python
# Static (namespaces known at import time):
CATALOGS = [("<your_name>", <your_name>_enumerate)]

# Or dynamic (async generator):
async def CATALOGS():
    async for region in _fetch_regions():
        yield f"<your_name>_{region.code.lower()}", partial(_enumerate, region=region)
```

See [`contract.md`](contract.md) §6 for the full `CATALOGS` /
`RESOLVE_CATALOG` spec.

## Conformance gate

Every release must pass the conformance suite:

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
parsimony list
```

Your plugin should appear. Add `--strict` to run the conformance suite
and exit non-zero on any failure:

```bash
parsimony list --strict
```

## Publishing

1. Configure [PyPI trusted publishing](https://docs.pypi.org/trusted-publishers/)
   for your GitHub repo.
2. Copy the workflow files from
   `parsimony-connectors/packages/fred/.github/workflows/` into your repo.
3. Tag a release (`git tag v0.1.0 && git push --tags`); GitHub Actions
   publishes to PyPI via OIDC (no tokens in secrets).

## Checklist before cutting `v0.1.0`

- [ ] `parsimony_<your_name>` module exports `CONNECTORS`.
- [ ] Optional: `CATALOGS`, `RESOLVE_CATALOG`.
- [ ] Per-connector `@connector(env={...})` declarations cover every required keyword-only dep.
- [ ] `[project.urls] Homepage` set in `pyproject.toml`.
- [ ] Entry point registered in `pyproject.toml` under `parsimony.providers`.
- [ ] `parsimony.testing.assert_plugin_valid(module)` passes.
- [ ] Tool-tagged connectors have ≥40-character descriptions.
- [ ] Unit tests cover happy path + at least one error path (401, 429, empty).
- [ ] `README.md` documents install, setup, example usage.
- [ ] Apache-2.0 `LICENSE` file present.
- [ ] CI workflows green on main.

## When to create a per-provider vs protocol-grouped plugin

- **Per-provider (`parsimony-<provider>`)** when the API is bespoke.
  **Default.**
- **Protocol-grouped (`parsimony-<protocol>`)** only when multiple
  providers share a wire protocol, >60% of implementation, dependency
  tree, and maintenance cadence. Examples: `parsimony-sdmx`,
  `parsimony-pxweb`.
