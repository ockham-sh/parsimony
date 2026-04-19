# Extracting a bundled connector into its own plugin package

The provider registry in `packages/parsimony/parsimony/connectors/__init__.py`
still lists ~20 connectors as *bundled* providers (loaded from
`parsimony.connectors.<name>`). The plugin contract is identical to the
extracted ones (`parsimony-sdmx`, `parsimony-edgar`,
`parsimony-financial-reports`); the remaining HTTP-only connectors stay
bundled until external pressure justifies a coordinated migration.

This document is the migration recipe for any individual connector. Each
extraction is mechanical and contained; the only cross-cutting work is
updating downstream callers (parsimony-agents, terminal/server, docs).

## Steps

For a bundled connector at `parsimony.connectors.<name>`:

### 1. Scaffold the package

```
packages/parsimony-<name>/
├── pyproject.toml
├── README.md
├── parsimony_<name>/
│   ├── __init__.py
│   ├── _connectors.py
│   └── py.typed
└── tests/
    └── test_conformance.py
```

### 2. `pyproject.toml`

Use `packages/parsimony-edgar/pyproject.toml` as a template. Two things matter:

* `dependencies` must include `parsimony>=...` and any third-party libraries
  the connector needs (HTTP-only connectors only need core).
* The provider registration:

  ```toml
  [project.entry-points."parsimony.providers"]
  <name> = "parsimony_<name>:PROVIDER"
  ```

### 3. Move the connector

```bash
git mv packages/parsimony/parsimony/connectors/<name>.py \
       packages/parsimony-<name>/parsimony_<name>/_connectors.py
```

No code changes are needed inside the file: it imports from `parsimony.*`
which is still public.

### 4. Define `__init__.py`

Re-export everything the old module exposed, plus a `PROVIDER`:

```python
from parsimony.connectors import ProviderSpec
from parsimony_<name>._connectors import CONNECTORS, ENV_VARS  # ENV_VARS optional

PROVIDER = ProviderSpec(
    name="<name>",
    connectors=CONNECTORS,
    env_vars=ENV_VARS,  # omit if no auth
)
```

### 5. Conformance test

```python
# tests/test_conformance.py
from parsimony_plugin_tests import ProviderTestSuite
from parsimony_<name> import PROVIDER

class Test<Name>ProviderConformance(ProviderTestSuite):
    provider = PROVIDER
    entry_point_name = "<name>"
```

### 6. Drop the bundled entry from core

In `packages/parsimony/parsimony/connectors/__init__.py`, replace the
`ProviderSpec(name="<name>", module=...)` line with a comment pointing to the
new package. Remove the connector's optional extra (if any) from
`packages/parsimony/pyproject.toml` and remove its entry from the coverage
omit list.

### 7. Update downstream callers

Search every workspace for `parsimony.connectors.<name>` and replace with
`parsimony_<name>`. Common targets:

* `packages/parsimony/README.md`, `packages/parsimony/examples/*.py`
* `parsimony-agents/` — `__init__.py`, examples, docs
* `terminal/server/` — any catalog or job file that imports the connector
* `docs/`

### 8. Add to release-please manifest

* Add an entry in `release-please-config.json#packages`.
* Add a starting version in `.release-please-manifest.json`.

### 9. Add to `parsimony-starter`

If the connector belongs in the curated batteries-included install, add it
to `packages/parsimony-starter/pyproject.toml`'s `dependencies` and
`[tool.uv.sources]`.

## Backlog

Bundled connectors that have not been extracted yet. None block adoption —
they all work today through the bundled `ProviderSpec(..., module=...)`
mechanism — but each extraction removes one more module from the core
distribution and lets that connector ship and version independently.

* `fred` (canonical, ~20 cross-repo callers — extract first when a
  coordinated rollout is scheduled)
* `fmp`, `fmp_screener`
* `eodhd`, `coingecko`, `finnhub`, `tiingo`
* `eia`, `bdf`, `alpha_vantage`
* `riksbank`, `destatis`, `bls`
* `polymarket`, `treasury`
* `snb`, `rba`, `bde`, `boc`, `boj`, `bdp`
