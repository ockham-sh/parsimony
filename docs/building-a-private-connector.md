# Building a private connector

For data sources you can't ship to public PyPI: vendor SaaS under restrictive
ToS, internal databases (Postgres, Snowflake, S3), or anything firewalled.
Same plugin shape as a public connector — different distribution and
credential injection.

**Authoritative contract:** [`contract.md`](contract.md). When this guide and
the contract disagree, the contract wins.

---

## Two paths, one shape

| Path | Distribution | Credential injection |
|---|---|---|
| **Private package** — vendor SaaS, proprietary feed | Private PyPI / Artifactory / wheel | `@connector(env={...})` + `bind_env()` |
| **Internal data source** — Postgres, Snowflake, S3 | Same, but typically not redistributed | Plain `bind(pool=...)` at startup |

The kernel doesn't differentiate. `discover.load_all()` picks up both.

---

## Path 1 — private package

Use this when the connector wraps a credential-bearing API (string keys or
tokens) and ships as an installable distribution.

### Scaffold

```bash
uvx cookiecutter gh:ockham-sh/parsimony-plugin-template
```

You get a working connector skeleton: `pyproject.toml` with the entry-point
already wired, a placeholder `CONNECTORS` export, a release-blocking
conformance test, and CI.

### Connector shape

```python
from pydantic import BaseModel, Field
from parsimony import (
    Column, ColumnRole, Connectors, OutputConfig, connector,
)


class YourFetchParams(BaseModel):
    entity_id: str = Field(..., description="The thing to fetch.")
    start: str | None = None


_OUTPUT = OutputConfig(columns=[
    Column(name="entity_id", role=ColumnRole.KEY, namespace="your_name"),
    Column(name="title", role=ColumnRole.TITLE),
    Column(name="date", dtype="datetime", role=ColumnRole.DATA),
    Column(name="value", dtype="numeric", role=ColumnRole.DATA),
])


@connector(output=_OUTPUT, env={"api_key": "YOUR_API_KEY"}, tags=["your_name", "tool"])
async def your_fetch(params: YourFetchParams, *, api_key: str):
    """One-line description. First sentence becomes the MCP tool description."""
    # ... call your API, return a Result or DataFrame ...


CONNECTORS = Connectors([your_fetch])
```

The decorator's `env={"api_key": "YOUR_API_KEY"}` is the binding for
`Connectors.bind_env()`; it reads `os.environ["YOUR_API_KEY"]` at bind time
and never serializes the value into `Provenance.params`.

Add a `[project.urls] Homepage = ...` to `pyproject.toml` so the kernel can
surface it via `Provider.homepage`. There is no module-level `ENV_VARS`,
`PROVIDER_METADATA`, or `__version__` — those live on the decorator and in
PEP 621 metadata.

### Publish to your private index

The simplest path is wheel + private index:

```bash
uv build
uvx twine upload --repository-url https://artifactory.example/api/pypi/internal dist/*
```

Pinned consumer install:

```toml
# consumer's pyproject.toml
[[tool.uv.index]]
name = "internal"
url = "https://artifactory.example/api/pypi/internal/simple/"
explicit = true

[tool.uv.sources]
"parsimony-yourname" = { index = "internal" }
```

For GitHub Package Registry or wheel-file distribution, the build step is
identical — only the upload target changes.

---

## Path 2 — internal data source

Parsimony ships no Postgres, Snowflake, or S3 connector on purpose. Every
organization's schema is different; a generic `run_sql` blob would be
opaque, and a hard-coded schema would be too rigid. The kernel gives you
the layer above the raw client:

- `@connector` / `@enumerator` for typed params, provenance, agent-discoverable schemas
- `bind` for credential injection (pools, connection parameter dicts)
- `OutputConfig` for KEY / TITLE / DATA / METADATA roles

You bring the client (`asyncpg`, `snowflake-connector-python`, `boto3`).

### Postgres example

```python
"""Internal Postgres connectors for the analytics warehouse."""
from __future__ import annotations

import asyncpg
import pandas as pd
from pydantic import BaseModel, Field

from parsimony import (
    Column, ColumnRole, Connectors, OutputConfig, connector, enumerator,
)


class PgQueryParams(BaseModel):
    metric_id: str = Field(..., description="Metric identifier (e.g. revenue_monthly)")
    start_date: str | None = None
    end_date: str | None = None


METRIC_OUTPUT = OutputConfig(columns=[
    Column(name="metric_id", role=ColumnRole.KEY, namespace="pg_metrics"),
    Column(name="metric_name", role=ColumnRole.TITLE),
    Column(name="date", dtype="datetime", role=ColumnRole.DATA),
    Column(name="value", dtype="numeric", role=ColumnRole.DATA),
])


@connector(output=METRIC_OUTPUT, tags=["internal", "postgres"])
async def pg_fetch_metric(
    params: PgQueryParams,
    *,
    pool: asyncpg.Pool,        # bound at startup; never in provenance
) -> pd.DataFrame:
    """Fetch time series observations for an internal metric."""
    rows = await pool.fetch(
        """
        SELECT metric_id, metric_name, date, value
        FROM analytics.metrics_timeseries
        WHERE metric_id = $1
          AND ($2::date IS NULL OR date >= $2::date)
          AND ($3::date IS NULL OR date <= $3::date)
        ORDER BY date
        """,
        params.metric_id, params.start_date, params.end_date,
    )
    if not rows:
        raise ValueError(f"No data for metric_id={params.metric_id!r}")
    return pd.DataFrame([dict(r) for r in rows])


CONNECTORS = Connectors([pg_fetch_metric])
```

Wire it at startup:

```python
import asyncpg
from my_connectors.postgres import CONNECTORS as PG

pool = await asyncpg.create_pool(dsn="postgresql://user:pass@host/db")
bound = PG.bind(pool=pool)
```

### Patterns for sync SDKs and cloud clients

Snowflake's connector and `boto3` are sync. Wrap them in
`asyncio.to_thread()` so the connector stays async:

```python
def _sf_query(conn_params, sql, binds):
    with snowflake.connector.connect(**conn_params) as conn, conn.cursor() as cur:
        cur.execute(sql, binds)
        cols = [d[0].lower() for d in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=cols)


@connector(output=KPI_OUTPUT, tags=["internal", "snowflake"])
async def sf_fetch_kpi(params, *, sf_conn_params: dict):
    return await asyncio.to_thread(_sf_query, sf_conn_params, SQL, (params.kpi_code,))
```

Same pattern for `boto3` (S3 list/read), Snowflake, BigQuery, and any other
sync SDK. The connector signature stays async; the thread offload is
internal.

---

## Composing internal + standard connectors

```python
from parsimony import Connectors
from parsimony_fred import CONNECTORS as fred
from my_connectors.postgres import CONNECTORS as pg

all_connectors = Connectors.merge(
    fred.bind_env(),                # reads FRED_API_KEY
    pg.bind(pool=pg_pool),          # internal pool
)
```

---

## Conformance and security review

Run the conformance suite locally — it's the same gate the public monorepo
uses:

```bash
pip install -e .[dev]
pytest tests/test_conformance.py
parsimony list --strict             # exits non-zero on any conformance failure
```

For a regulated-finance security review, `parsimony list --strict --json`
produces a machine-readable artefact. The schema is **stable** across kernel
MINOR releases (see [`contract.md`](contract.md) §7). Pair it with
`pip-audit` on the wheel, `bandit` on the source, and a human read of the
contract spec.

### Security checklist

1. **Environment variables only.** No hardcoded credentials. String creds
   on the decorator (`env={"api_key": "..."}`); rich deps (DB pools,
   connection dicts) read `os.environ` once at startup and inject via
   `bind`.
2. **`bind` keeps secrets out of provenance.** Keyword-only deps are never
   serialized into `Provenance.params`.
3. **Parameterized queries always.** `$1` (asyncpg) or `%s` (Snowflake) —
   never f-strings or concatenation.
4. **Least-privilege roles.** Read-only DB roles. The connector never needs
   `INSERT`, `UPDATE`, or DDL.
5. **Fail fast at startup.** Validate required env vars before building
   pools or clients.

---

## Upgrading across kernel releases

Plugins pin a range, not a single version:

```toml
dependencies = ["parsimony-core>=0.4,<0.5"]
```

When a kernel MAJOR release lands, your plugin keeps working until you
update the pin. The recipe: bump the pin → re-run `parsimony list --strict`
→ run unit tests → release a patch. See [`contract.md`](contract.md) §8 for
deprecation guarantees.

---

## Getting help

- **Contract questions:** [`contract.md`](contract.md) — file an issue at
  [ockham-sh/parsimony](https://github.com/ockham-sh/parsimony/issues) if
  ambiguous.
- **Scaffolding:** [ockham-sh/parsimony-plugin-template](https://github.com/ockham-sh/parsimony-plugin-template).
- **Security disclosures:** see
  [`SECURITY.md`](https://github.com/ockham-sh/parsimony/blob/main/SECURITY.md)
  — do not open a public issue.
