# Building a Private Parsimony Connector

This guide walks through building a parsimony connector for an internal or
proprietary data source — one that cannot ship to public PyPI because the
underlying data, API credentials, or provider terms of service forbid it.
The final package installs from your private Python index (Artifactory,
internal Nexus, GitHub Packages, wheel file) and plugs into the kernel
exactly like an officially-maintained connector.

**Binding spec:** [`contract.md`](contract.md). If anything below
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
  `parsimony-connectors` monorepo.

If none apply and your connector can ship under Apache 2.0, contribute it
to [ockham-sh/parsimony-connectors](https://github.com/ockham-sh/parsimony-connectors)
instead — you get free matrix CI, shared trust-root identity, and the
monorepo's maintainer rotation.

---

## 2. Scaffold the package

Use the [plugin template](https://github.com/ockham-sh/parsimony-plugin-template):

```bash
uvx cookiecutter gh:ockham-sh/parsimony-plugin-template
```

Answer the prompts (`provider_name`, `description`, author info). The
scaffold writes a minimal working connector with:

- `pyproject.toml` — entry-point registration, contract classifier, mandatory
  kernel pin, Python classifier range
- `src/parsimony_<name>/__init__.py` — a placeholder `CONNECTORS` export
- `tests/test_conformance.py` — release-blocking conformance test
- `.github/workflows/ci.yml` — test + lint + conformance on every PR

Commit and push to your internal repository (or GitHub, if public).

---

## 3. Write the connector

Replace the placeholder in `src/parsimony_<name>/__init__.py`. The minimum
shape:

```python
from pydantic import BaseModel, Field
from parsimony import Connectors, connector
from parsimony.result import Column, ColumnRole, OutputConfig, Provenance, Result


class YourFetchParams(BaseModel):
    entity_id: str = Field(..., description="The thing to fetch.")
    start: str | None = None


ENV_VARS = {"api_key": "YOUR_API_KEY"}
PROVIDER_METADATA = {
    "homepage": "https://your-provider.example",
    "pricing": "internal",
}


_OUTPUT = OutputConfig(
    columns=[
        Column(name="entity_id", role=ColumnRole.KEY, namespace="your_name"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="date", dtype="datetime", role=ColumnRole.DATA),
        Column(name="value", dtype="numeric", role=ColumnRole.DATA),
    ]
)


@connector(output=_OUTPUT, tags=["your_name", "tool"])
async def your_fetch(params: YourFetchParams, *, api_key: str) -> Result:
    """One-line description. First sentence becomes the MCP tool description."""
    # ... call your internal API, return a Result ...


CONNECTORS = Connectors([your_fetch])
```

Run the conformance suite locally:

```bash
uv sync --all-extras
uv run pytest
uv run parsimony conformance verify parsimony-<yourname>
```

If `conformance verify` exits `0`, the package meets the contract.

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
            parsimony parsimony-<yourname>
```

Or, if your users run in a `uv sync` workflow with a pinned private index
in their own project's `pyproject.toml`:

```toml
[[tool.uv.index]]
name = "internal"
url = "https://your-artifactory.example/api/pypi/internal/simple/"
explicit = true

[tool.uv.sources]
"parsimony-yourname" = { index = "internal" }
```

### The trust gate

The kernel's default allow-list does **not** include your private
package — it only auto-trusts officially-maintained
`parsimony-<name>` distributions from
[ockham-sh/parsimony-connectors](https://github.com/ockham-sh/parsimony-connectors).
Users of your package must explicitly opt in:

```bash
export PARSIMONY_TRUST_PLUGINS="parsimony-yourname,parsimony-other-internal"
```

Or in a Python application bootstrap:

```python
import os
os.environ.setdefault(
    "PARSIMONY_TRUST_PLUGINS",
    "parsimony-yourname,parsimony-other-internal",
)

from parsimony import build_connectors_from_env
connectors = build_connectors_from_env()
```

The structured log line emitted when your plugin loads says exactly why:

```
INFO parsimony.discovery: parsimony: loading non-official plugin 'parsimony-yourname' (opted in via PARSIMONY_TRUST_PLUGINS)
```

---

## 6. Regulated-finance security review checklist

If your security team needs to approve `parsimony-yourname` before
production rollout, the deliverable for them is:

```bash
parsimony conformance verify parsimony-yourname --json > verify-report.json
```

Exit code `0` + `"passed": true` in the JSON is the machine-readable
pass/fail they're looking for. The report schema is stable across kernel
MINOR releases (see [`contract.md`](contract.md) §5).

Pair it with:

- `pip-audit` on the built wheel
- `bandit` on the source
- Your internal static-analysis pipeline
- A human review against the contract spec (`docs/contract.md`)

---

## 7. Upgrading across kernel releases

Plugins declare a contract-version classifier, not a specific kernel
version. When the kernel bumps `CONTRACT_VERSION` (rare; see
`contract.md` §7), your plugin keeps working as long as the kernel still
supports its declared version. The deprecation window is at least one
year for **stable** surface.

When the kernel drops support for your declared contract version:

1. Update the classifier in `pyproject.toml`:
   `Framework :: Parsimony :: Contract 2`
2. Address any use of **provisional** or **private** symbols flagged in
   the kernel's CHANGELOG.
3. Re-run `parsimony conformance verify`.
4. Release a new version of your plugin.

---

## 8. Getting help

- **Contract questions:** [`contract.md`](contract.md) is the
  authoritative spec; file an issue at
  [ockham-sh/parsimony](https://github.com/ockham-sh/parsimony/issues) if
  it's ambiguous.
- **Scaffolding issues:** [ockham-sh/parsimony-plugin-template](https://github.com/ockham-sh/parsimony-plugin-template)
- **Security disclosures:** see [`SECURITY.md`](../SECURITY.md) at the
  kernel repo root — do **not** open a public issue.

---

*This guide covers the private-connector path. For contributions to the
officially-maintained set, see
[parsimony-connectors/CONTRIBUTING.md](https://github.com/ockham-sh/parsimony-connectors/blob/main/CONTRIBUTING.md).*
