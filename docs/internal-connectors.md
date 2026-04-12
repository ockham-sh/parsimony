# Internal Connectors: Wrapping Your Own Data Sources

## Philosophy

Ockham does not ship database or cloud-storage connectors. That is intentional.
Every organization has its own Postgres schemas, Snowflake warehouses, and S3 bucket
layouts. Writing generic connectors for these would be either too opaque (a single
`run_sql` blob) or too rigid (one schema per connector).

Instead, parsimony gives you the **layer above** the raw client:

- **`@connector` / `@enumerator`** -- typed parameter models, provenance tracking,
  and agent-discoverable schemas via `to_llm()`.
- **`Catalog`** -- searchable index of what your internal data contains,
  so an agent can find series by keyword instead of memorizing table names.
- **`bind_deps`** -- inject credentials at startup; they never appear in provenance
  or agent-visible parameter schemas.
- **`OutputConfig`** -- declare KEY / TITLE / DATA / METADATA roles so downstream
  tools (charts, exports, catalog indexing) work automatically.

You bring the client library (`asyncpg`, `snowflake-connector-python`, `boto3`).
Ockham wraps it in a connector that an agent or CLI can discover and call.

---

## Postgres Template

Uses `asyncpg` for native async access. The pool is injected via `bind_deps`
so credentials stay out of the connector signature.

```python
"""Internal Postgres connectors for the analytics warehouse."""

from __future__ import annotations

from typing import Any

import asyncpg
import pandas as pd
from pydantic import BaseModel, Field

from parsimony.connector import Connectors, connector, enumerator
from parsimony.result import Column, ColumnRole, OutputConfig, Provenance, Result


# ---------------------------------------------------------------------------
# Parameter models
# ---------------------------------------------------------------------------

class PgQueryParams(BaseModel):
    """Fetch a named metric series from the warehouse."""
    metric_id: str = Field(..., description="Metric identifier (e.g. revenue_monthly)")
    start_date: str | None = Field(default=None, description="Start date YYYY-MM-DD")
    end_date: str | None = Field(default=None, description="End date YYYY-MM-DD")


class PgEnumerateParams(BaseModel):
    """List available metrics in a schema."""
    schema_name: str = Field(default="analytics", description="Database schema to enumerate")


# ---------------------------------------------------------------------------
# Output configs
# ---------------------------------------------------------------------------

# Connector output: KEY identifies the metric, DATA holds observations.
METRIC_OUTPUT = OutputConfig(columns=[
    Column(name="metric_id", role=ColumnRole.KEY, namespace="pg_metrics"),
    Column(name="metric_name", role=ColumnRole.TITLE),
    Column(name="date", dtype="datetime", role=ColumnRole.DATA),
    Column(name="value", dtype="numeric", role=ColumnRole.DATA),
])

# Enumerator output: KEY + TITLE + METADATA, no DATA columns.
ENUMERATE_OUTPUT = OutputConfig(columns=[
    Column(name="metric_id", role=ColumnRole.KEY, namespace="pg_metrics"),
    Column(name="metric_name", role=ColumnRole.TITLE),
    Column(name="description", role=ColumnRole.METADATA),
    Column(name="frequency", role=ColumnRole.METADATA),
])


# ---------------------------------------------------------------------------
# Connectors
# ---------------------------------------------------------------------------

@connector(output=METRIC_OUTPUT, tags=["internal", "postgres"])
async def pg_fetch_metric(
    params: PgQueryParams,
    *,
    pool: asyncpg.Pool,        # injected via bind_deps -- never in provenance
) -> pd.DataFrame:
    """Fetch time series observations for an internal metric.

    Returns date + value with metric identity. Use pg_list_metrics to discover
    valid metric_id values.
    """
    # Always use parameterized queries -- never f-string SQL.
    query = """
        SELECT metric_id, metric_name, date, value
        FROM analytics.metrics_timeseries
        WHERE metric_id = $1
          AND ($2::date IS NULL OR date >= $2::date)
          AND ($3::date IS NULL OR date <= $3::date)
        ORDER BY date
    """
    rows = await pool.fetch(query, params.metric_id, params.start_date, params.end_date)
    if not rows:
        raise ValueError(f"No data for metric_id={params.metric_id!r}")
    # Return a DataFrame; the @connector(output=...) decorator applies the
    # OutputConfig automatically, producing a SemanticTableResult.
    return pd.DataFrame([dict(r) for r in rows])


@enumerator(output=ENUMERATE_OUTPUT, tags=["internal", "postgres"])
async def pg_list_metrics(
    params: PgEnumerateParams,
    *,
    pool: asyncpg.Pool,
) -> pd.DataFrame:
    """List available metrics in the analytics warehouse.

    Index the result into a Catalog for keyword search.
    """
    query = """
        SELECT metric_id, metric_name, description, frequency
        FROM analytics.metric_registry
        WHERE schema_name = $1
        ORDER BY metric_id
    """
    rows = await pool.fetch(query, params.schema_name)
    if not rows:
        raise ValueError(f"No metrics in schema {params.schema_name!r}")
    return pd.DataFrame([dict(r) for r in rows])


# Export: bind at startup with bind_deps(pool=pool)
CONNECTORS = Connectors([pg_fetch_metric, pg_list_metrics])
```

### Startup wiring

```python
import asyncpg
from my_connectors.postgres import CONNECTORS as PG_CONNECTORS

pool = await asyncpg.create_pool(dsn="postgresql://user:pass@host/db")
bound = PG_CONNECTORS.bind_deps(pool=pool)
# Combine with standard bundle:
# all_connectors = fred_connectors + sdmx_connectors + bound
```

---

## Snowflake Template

Uses the synchronous `snowflake-connector-python` SDK wrapped in
`asyncio.to_thread()` so the connector stays async.

```python
"""Internal Snowflake connectors for the enterprise data warehouse."""

from __future__ import annotations

import asyncio
from typing import Any

import pandas as pd
import snowflake.connector
from pydantic import BaseModel, Field

from parsimony.connector import Connectors, connector, enumerator
from parsimony.result import Column, ColumnRole, OutputConfig, Provenance, Result


# ---------------------------------------------------------------------------
# Parameter models
# ---------------------------------------------------------------------------

class SfQueryParams(BaseModel):
    """Fetch a KPI series from Snowflake."""
    kpi_code: str = Field(..., description="KPI code (e.g. arr_monthly)")
    start_date: str | None = Field(default=None, description="Start date YYYY-MM-DD")


class SfEnumerateParams(BaseModel):
    """List available KPIs in a Snowflake schema."""
    database: str = Field(default="ANALYTICS", description="Snowflake database")
    schema_name: str = Field(default="KPIS", description="Snowflake schema")


# ---------------------------------------------------------------------------
# Output configs
# ---------------------------------------------------------------------------

KPI_OUTPUT = OutputConfig(columns=[
    Column(name="kpi_code", role=ColumnRole.KEY, namespace="sf_kpis"),
    Column(name="kpi_name", role=ColumnRole.TITLE),
    Column(name="date", dtype="datetime", role=ColumnRole.DATA),
    Column(name="value", dtype="numeric", role=ColumnRole.DATA),
])

KPI_ENUMERATE_OUTPUT = OutputConfig(columns=[
    Column(name="kpi_code", role=ColumnRole.KEY, namespace="sf_kpis"),
    Column(name="kpi_name", role=ColumnRole.TITLE),
    Column(name="unit", role=ColumnRole.METADATA),
    Column(name="frequency", role=ColumnRole.METADATA),
])


# ---------------------------------------------------------------------------
# Sync helper -- runs in a thread via asyncio.to_thread()
# ---------------------------------------------------------------------------

def _sf_query(conn_params: dict[str, Any], sql: str, binds: tuple[Any, ...]) -> pd.DataFrame:
    """Execute a parameterized query and return a DataFrame. Runs in a thread."""
    with snowflake.connector.connect(**conn_params) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, binds)
            cols = [desc[0].lower() for desc in cur.description]
            rows = cur.fetchall()
    return pd.DataFrame(rows, columns=cols)


# ---------------------------------------------------------------------------
# Connectors
# ---------------------------------------------------------------------------

@connector(output=KPI_OUTPUT, tags=["internal", "snowflake"])
async def sf_fetch_kpi(
    params: SfQueryParams,
    *,
    sf_conn_params: dict[str, Any],  # injected via bind_deps
) -> pd.DataFrame:
    """Fetch time series for an internal KPI from Snowflake.

    Use sf_list_kpis to discover valid kpi_code values.
    """
    sql = """
        SELECT kpi_code, kpi_name, date, value
        FROM ANALYTICS.KPIS.KPI_TIMESERIES
        WHERE kpi_code = %s
          AND (%s IS NULL OR date >= %s)
        ORDER BY date
    """
    df = await asyncio.to_thread(
        _sf_query, sf_conn_params, sql,
        (params.kpi_code, params.start_date, params.start_date),
    )
    if df.empty:
        raise ValueError(f"No data for kpi_code={params.kpi_code!r}")
    return df


@enumerator(output=KPI_ENUMERATE_OUTPUT, tags=["internal", "snowflake"])
async def sf_list_kpis(
    params: SfEnumerateParams,
    *,
    sf_conn_params: dict[str, Any],
) -> pd.DataFrame:
    """List available KPIs in a Snowflake schema.

    Index the result into a Catalog for keyword search.
    """
    sql = """
        SELECT kpi_code, kpi_name, unit, frequency
        FROM {db}.{schema}.KPI_REGISTRY
        ORDER BY kpi_code
    """.format(db=params.database, schema=params.schema_name)
    df = await asyncio.to_thread(_sf_query, sf_conn_params, sql, ())
    if df.empty:
        raise ValueError(f"No KPIs in {params.database}.{params.schema_name}")
    return df


CONNECTORS = Connectors([sf_fetch_kpi, sf_list_kpis])
```

### Startup wiring

```python
import os
from my_connectors.snowflake import CONNECTORS as SF_CONNECTORS

sf_params = {
    "account": os.environ["SNOWFLAKE_ACCOUNT"],
    "user": os.environ["SNOWFLAKE_USER"],
    "password": os.environ["SNOWFLAKE_PASSWORD"],
    "warehouse": "COMPUTE_WH",
}
bound = SF_CONNECTORS.bind_deps(sf_conn_params=sf_params)
```

---

## S3/Parquet Template

Uses `boto3` for bucket listing and `pyarrow` for Parquet reads. Enumerator
lists available datasets; connector reads a specific one.

```python
"""Internal S3/Parquet connectors for the data lake."""

from __future__ import annotations

import asyncio
from typing import Any

import boto3
import pandas as pd
import pyarrow.parquet as pq
from pydantic import BaseModel, Field

from parsimony.connector import Connectors, connector, enumerator
from parsimony.result import Column, ColumnRole, OutputConfig


# ---------------------------------------------------------------------------
# Parameter models
# ---------------------------------------------------------------------------

class S3ReadParams(BaseModel):
    """Read a Parquet dataset from S3."""
    key: str = Field(..., description="S3 object key (e.g. datasets/revenue/2024.parquet)")


class S3ListParams(BaseModel):
    """List available Parquet datasets under a prefix."""
    prefix: str = Field(default="datasets/", description="S3 key prefix")


# ---------------------------------------------------------------------------
# Output configs
# ---------------------------------------------------------------------------

# Generic: KEY identifies the dataset, DATA columns come from the Parquet file.
# Adjust per your actual schema; this is a minimal example.
DATASET_OUTPUT = OutputConfig(columns=[
    Column(name="dataset_key", role=ColumnRole.KEY, namespace="s3_datasets"),
    Column(name="dataset_name", role=ColumnRole.TITLE),
    Column(name="date", dtype="datetime", role=ColumnRole.DATA),
    Column(name="value", dtype="numeric", role=ColumnRole.DATA),
])

ENUMERATE_OUTPUT = OutputConfig(columns=[
    Column(name="dataset_key", role=ColumnRole.KEY, namespace="s3_datasets"),
    Column(name="dataset_name", role=ColumnRole.TITLE),
    Column(name="size_mb", role=ColumnRole.METADATA),
])


# ---------------------------------------------------------------------------
# Sync helpers
# ---------------------------------------------------------------------------

def _s3_list(bucket: str, prefix: str) -> pd.DataFrame:
    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")
    rows: list[dict[str, Any]] = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".parquet"):
                rows.append({
                    "dataset_key": key,
                    "dataset_name": key.rsplit("/", 1)[-1].replace(".parquet", ""),
                    "size_mb": round(obj["Size"] / 1_048_576, 2),
                })
    return pd.DataFrame(rows)


def _s3_read(bucket: str, key: str) -> pd.DataFrame:
    s3 = boto3.client("s3")
    obj = s3.get_object(Bucket=bucket, Key=key)
    table = pq.read_table(obj["Body"])
    return table.to_pandas()


# ---------------------------------------------------------------------------
# Connectors
# ---------------------------------------------------------------------------

@connector(tags=["internal", "s3"])
async def s3_read_dataset(
    params: S3ReadParams,
    *,
    bucket: str,  # injected via bind_deps
) -> pd.DataFrame:
    """Read a Parquet dataset from S3.

    Returns the full DataFrame. Use s3_list_datasets to discover available keys.
    """
    df = await asyncio.to_thread(_s3_read, bucket, params.key)
    if df.empty:
        raise ValueError(f"Empty dataset at s3://{bucket}/{params.key}")
    return df


@enumerator(output=ENUMERATE_OUTPUT, tags=["internal", "s3"])
async def s3_list_datasets(
    params: S3ListParams,
    *,
    bucket: str,
) -> pd.DataFrame:
    """List Parquet datasets in an S3 prefix.

    Index the result into a Catalog for keyword search.
    """
    df = await asyncio.to_thread(_s3_list, bucket, params.prefix)
    if df.empty:
        raise ValueError(f"No .parquet files under s3://{bucket}/{params.prefix}")
    return df


CONNECTORS = Connectors([s3_read_dataset, s3_list_datasets])
```

### Startup wiring

```python
import os
from my_connectors.s3 import CONNECTORS as S3_CONNECTORS

bound = S3_CONNECTORS.bind_deps(bucket=os.environ["DATA_LAKE_BUCKET"])
```

---

## Integration: Combining with Standard Connectors

All connector collections compose via `+`. Build one `Connectors` instance that
an agent or CLI can query:

```python
import os
from parsimony.connectors.fred import CONNECTORS as FRED
from parsimony.connectors.sdmx import SDMX_FETCH_CONNECTORS as SDMX
from my_connectors.postgres import CONNECTORS as PG
from my_connectors.snowflake import CONNECTORS as SF
from my_connectors.s3 import CONNECTORS as S3

fred_key = os.environ["FRED_API_KEY"]
all_connectors = (
    FRED.bind_deps(api_key=fred_key)
    + SDMX                             # SDMX connectors have no deps
    + PG.bind_deps(pool=pg_pool)
    + SF.bind_deps(sf_conn_params=sf_params)
    + S3.bind_deps(bucket="my-data-lake")
)

# Auto-index into catalog on every fetch:
from parsimony import Catalog, SQLiteCatalogStore
catalog = Catalog(SQLiteCatalogStore(":memory:"))
all_connectors = all_connectors.with_callback(catalog.index_result)
```

---

## Security Guidance

1. **Environment variables only.** Never hardcode credentials in connector modules.
   Use `os.environ["KEY"]` at startup and pass via `bind_deps`.

2. **`bind_deps` keeps secrets out of provenance.** Keyword-only dependencies
   (`*, api_key: str, pool: asyncpg.Pool`) are bound at startup and never
   serialized into `Provenance.params`. The agent sees the Pydantic param model
   (e.g. `metric_id`, `start_date`), not the pool or API key.

3. **Parameterized queries always.** Never use f-strings or string concatenation
   for SQL. Use `$1` placeholders (asyncpg) or `%s` (Snowflake connector).

4. **Least-privilege database roles.** Create a read-only role for the connector
   pool. The connector should never need `INSERT`, `UPDATE`, or `DDL`.

5. **Validate at startup.** Check that required environment variables are set
   before constructing pools or connection parameters. Fail fast with a clear
   error rather than a cryptic connection timeout.

```python
import os

def require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value

pg_dsn = require_env("POSTGRES_DSN")
sf_account = require_env("SNOWFLAKE_ACCOUNT")
```
