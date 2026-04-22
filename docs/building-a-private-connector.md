# Building a Private Parsimony Connector

This guide walks through building a parsimony connector for an internal or
proprietary data source — one that cannot ship to public PyPI because the
underlying data, API credentials, or provider terms of service forbid it.
The final package installs from your private Python index (Artifactory,
internal Nexus, GitHub Packages, wheel file) and plugs into the kernel
exactly like an officially-maintained connector.

**Authoritative contract:** [`contract.md`](contract.md). If anything below
contradicts the contract spec, the spec wins.

---

## 1. When this guide applies

Use this path when any of the following is true:

- The data source is behind your company's firewall (trading system,
  internal pricing feed, proprietary research database).
- The upstream provider's Terms of Service forbid redistributing their
  client code, endpoint documentation, or example responses under
  Apache 2.0.
- You want your own release cadence separate from the
  [`parsimony-connectors`](https://github.com/ockham-sh/parsimony-connectors)
  monorepo.

If none apply and your connector can ship under Apache 2.0, contribute it
to `parsimony-connectors` instead — you get free matrix CI, shared trust-
root identity, and the monorepo's maintainer rotation.

---

## 2. Scaffold the package

Use the [plugin template](https://github.com/ockham-sh/parsimony-plugin-template):

```bash
uvx cookiecutter gh:ockham-sh/parsimony-plugin-template
```

Answer the prompts (`provider_name`, `description`, author info). The
scaffold writes a minimal working connector with:

- `pyproject.toml` — entry-point registration, kernel version pin, Python
  classifier range
- `parsimony_<name>/__init__.py` — a placeholder `CONNECTORS` export
- `tests/test_conformance.py` — release-blocking conformance test
- `.github/workflows/ci.yml` — test + lint + conformance on every PR

Commit and push to your internal repository (or GitHub, if public).

---

## 3. Write the connector

Replace the placeholder in `parsimony_<name>/__init__.py`. The minimum
shape:

```python
from pydantic import BaseModel, Field
from parsimony import (
    Column, ColumnRole, Connectors, OutputConfig, Provenance, Result,
    connector,
)


class YourFetchParams(BaseModel):
    entity_id: str = Field(..., description="The thing to fetch.")
    start: str | None = None


_OUTPUT = OutputConfig(
    columns=[
        Column(name="entity_id", role=ColumnRole.KEY, namespace="your_name"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="date", dtype="datetime", role=ColumnRole.DATA),
        Column(name="value", dtype="numeric", role=ColumnRole.DATA),
    ]
)


@connector(output=_OUTPUT, env={"api_key": "YOUR_API_KEY"}, tags=["your_name", "tool"])
async def your_fetch(params: YourFetchParams, *, api_key: str) -> Result:
    """One-line description. First sentence becomes the MCP tool description."""
    # ... call your internal API, return a Result ...


CONNECTORS = Connectors([your_fetch])
```

Add a `[project.urls] Homepage = "https://your-provider.example"` entry
to `pyproject.toml` so the kernel can surface it via `Provider.homepage`.
There is no module-level `ENV_VARS`, `PROVIDER_METADATA`, or
`__version__` — env-var backings live on the decorator and provider
metadata lives in the package's PEP 621 metadata.

Run the conformance suite locally:

```bash
pip install -e .[dev]
pytest tests/test_conformance.py
parsimony list --strict                        # fails if any plugin flunks conformance
```

`parsimony list --strict` exits non-zero on any conformance failure; the
report (in `--json` mode) is the machine-readable artefact your security
team can consume.

### Publishing catalogs (optional)

If your internal connector should produce searchable catalog bundles for
your agents to load via `Catalog.from_url(...)`, export `CATALOGS` on
the module:

```python
# Static — namespaces known at import time
CATALOGS = [("your_name", your_enumerate)]

# Or async generator — namespaces discovered at build time
async def CATALOGS():
    async for division in _fetch_divisions():
        yield f"your_name_{division.code.lower()}", partial(your_enumerate, division=division)
```

Run `parsimony publish --provider your_name --target 'file:///shared/catalogs/{namespace}'`
(or against your internal Hugging Face mirror / S3 bucket) to build and
push catalogs. See [`contract.md`](contract.md) §6 for the full spec and
optional `RESOLVE_CATALOG` reverse lookup.

---

## 4. Publish to your private index

### Artifactory / internal Nexus

Configure a per-package trusted-publisher or API-token credential on your
private index. Your CI workflow builds the wheel and uploads:

```yaml
# .github/workflows/release.yml (or your CI system equivalent)
- name: Build
  run: uv build

- name: Publish
  env:
    TWINE_USERNAME: ${{ secrets.ARTIFACTORY_USERNAME }}
    TWINE_PASSWORD: ${{ secrets.ARTIFACTORY_PASSWORD }}
  run: |
    uvx twine upload --repository-url https://your-artifactory.example/api/pypi/internal \
                     dist/*
```

### Wheel file distribution

No index, just a wheel:

```bash
uv build
# produces dist/parsimony_yourname-0.1.0-py3-none-any.whl
# ship the wheel file via your normal internal distribution channel
```

### GitHub Package Registry (private repo)

Same shape as public PyPI but against the GitHub-hosted index; see
[GitHub's Python package docs](https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-pypi-registry).

---

## 5. Install and compose against the kernel

Your users install from the private index:

```bash
pip install --index-url https://your-artifactory.example/api/pypi/internal/simple/ \
            parsimony-core parsimony-<yourname>
```

Or, in a `uv sync` workflow with a pinned private index in the
consuming project's `pyproject.toml`:

```toml
[[tool.uv.index]]
name = "internal"
url = "https://your-artifactory.example/api/pypi/internal/simple/"
explicit = true

[tool.uv.sources]
"parsimony-yourname" = { index = "internal" }
```

### Discovery

The kernel walks the `parsimony.providers` entry-point group on
`discover.load_all()` and loads every installed plugin. Your internal
plugin is treated identically to officially-maintained ones — the kernel
does not differentiate.

```python
from parsimony import discover
connectors = discover.load_all().bind_env()
# "your_fetch" is now available alongside any other installed plugin
result = await connectors["your_fetch"](entity_id="E123")
```

If `YOUR_API_KEY` is not set in the environment, `your_fetch` stays in
the collection but is marked `bound=False`; calling it raises
`UnauthorizedError("YOUR_API_KEY is not set")`. Inspect via
`connectors.unbound`.

---

## 6. Regulated-finance security review checklist

If your security team needs to approve `parsimony-yourname` before
production rollout, the deliverable for them is:

```bash
parsimony list --strict --json > verify-report.json
```

Exit code `0` and no `"conformance": {"passed": false}` entries in the
JSON is the machine-readable pass. The report schema is **stable** across
kernel MINOR releases — see [`contract.md`](contract.md) §7.

Pair it with:

- `pip-audit` on the built wheel
- `bandit` on the source
- Your internal static-analysis pipeline
- A human review against the contract spec

### What the conformance suite verifies

Three checks run against every plugin module:

1. `check_connectors_exported` — module exports `CONNECTORS`, a non-empty
   `parsimony.Connectors`.
2. `check_descriptions_non_empty` — every connector carries a non-empty
   description (no silently empty LLM tool schemas).
3. `check_env_map_matches_deps` — for every connector, each key in its
   decorator-declared `env_map` names a real keyword-only dependency on
   that connector (catches typos and renames).

These are integrity checks, not behavioural tests. Your
`tests/test_<name>_connectors.py` file is where behavioural coverage
lives (happy path, 401 → `UnauthorizedError`, 429 → `RateLimitError`).

---

## 7. Upgrading across kernel releases

Plugins pin a range on the kernel distribution, not a single version:

```toml
dependencies = ["parsimony-core>=0.4,<0.5"]
```

When a kernel MAJOR release lands (e.g. `0.5.0`), your plugin keeps
working until you update the pin. The kernel publishes a changelog
naming any **stable** symbols that were removed — see
[`contract.md`](contract.md) §8 for the deprecation window guarantees.

The upgrade recipe:

1. Bump the pin: `"parsimony-core>=0.5,<0.7"`.
2. Re-run `parsimony list --strict` against the new kernel to catch any
   provisional-surface breakage flagged by the suite.
3. Run your unit tests to catch behavioural drift.
4. Release a new patch version of your plugin.

There is no separate contract-version classifier or keyword to bump.

---

## 8. Getting help

- **Contract questions:** [`contract.md`](contract.md) is the
  authoritative spec; file an issue at
  [ockham-sh/parsimony](https://github.com/ockham-sh/parsimony/issues) if
  it's ambiguous.
- **Scaffolding issues:** [ockham-sh/parsimony-plugin-template](https://github.com/ockham-sh/parsimony-plugin-template)
- **Security disclosures:** see `SECURITY.md` at the kernel repo root on
  [GitHub](https://github.com/ockham-sh/parsimony/blob/main/SECURITY.md)
  — do **not** open a public issue.

---

*This guide covers the private-connector path. For contributions to the
officially-maintained set, see
[parsimony-connectors/CONTRIBUTING.md](https://github.com/ockham-sh/parsimony-connectors/blob/main/CONTRIBUTING.md).*
