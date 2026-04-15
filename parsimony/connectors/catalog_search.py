"""Catalog search and namespace discovery connectors.

Provides ``catalog_search`` (tagged ``"tool"``) for keyword/semantic search
across all indexed providers, and ``catalog_list_namespaces`` for discovering
which namespaces are available in the store.

Both connectors accept a ``catalog`` dependency (bound via
``bind_deps(catalog=...)``).  ``catalog_search`` also accepts an optional
``default_namespaces`` list that applies when the caller omits namespaces.
"""

from __future__ import annotations

from typing import Any, List, Optional

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field, field_validator

from parsimony.connector import Connectors, connector
from parsimony.result import Column, ColumnRole, OutputConfig

# ---------------------------------------------------------------------------
# Parameter models
# ---------------------------------------------------------------------------

_MAX_LIMIT = 50
"""Hard cap on result count regardless of caller-supplied limit."""


class CatalogSearchParams(BaseModel):
    """Search the catalog by keyword."""

    model_config = ConfigDict(extra="forbid")

    query: str = Field(
        ...,
        min_length=1,
        description="Natural-language search query.",
    )

    @field_validator("query")
    @classmethod
    def _query_not_empty(cls, value: str) -> str:
        stripped = value.strip()
        if not stripped:
            raise ValueError("query must be non-empty")
        return stripped
    namespaces: Optional[List[str]] = Field(
        default=None,
        description="Restrict to these namespaces; omit to use app default or search all.",
    )
    limit: int = Field(
        default=10,
        ge=1,
        le=_MAX_LIMIT,
        description=f"Maximum results to return (1\u2013{_MAX_LIMIT}).",
    )


class CatalogListNamespacesParams(BaseModel):
    """List distinct catalog namespaces (for discovery and targeting)."""

    model_config = ConfigDict(extra="forbid")


# ---------------------------------------------------------------------------
# OutputConfig constants
# ---------------------------------------------------------------------------

CATALOG_SEARCH_OUTPUT = OutputConfig(
    columns=[
        Column(name="namespace", role=ColumnRole.METADATA),
        Column(name="code", role=ColumnRole.KEY),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="similarity", role=ColumnRole.DATA),
    ]
)

CATALOG_LIST_NAMESPACES_OUTPUT = OutputConfig(
    columns=[
        Column(name="namespace", role=ColumnRole.DATA),
    ]
)

# ---------------------------------------------------------------------------
# Namespace resolution helper
# ---------------------------------------------------------------------------


def resolve_namespaces(
    explicit: list[str] | None,
    default: list[str] | None,
) -> list[str] | None:
    """Three-tier namespace resolution: explicit > default > all (None).

    Returns the namespace list to pass to ``Catalog.search()``.
    """
    if explicit is not None:
        return explicit
    return default


# ---------------------------------------------------------------------------
# Connectors
# ---------------------------------------------------------------------------


@connector(output=CATALOG_SEARCH_OUTPUT, tags=["catalog", "tool"])
async def catalog_search(
    params: CatalogSearchParams,
    *,
    catalog: Any,
    default_namespaces: Optional[List[str]] = None,
) -> pd.DataFrame:
    """Search the catalog by keyword (semantic / keyword, per store configuration).

    If the agent passes ``namespaces``, those are used.  Otherwise
    ``default_namespaces`` from app wiring applies (``None`` searches all).
    Returns an empty table (same columns) when there are no matches.
    """
    effective = resolve_namespaces(params.namespaces, default_namespaces)
    matches = await catalog.search(
        params.query,
        limit=params.limit,
        namespaces=effective,
    )
    rows = [
        {
            "namespace": m.namespace,
            "code": m.code,
            "title": m.title,
            "similarity": round(m.similarity, 4) if m.similarity is not None else None,
        }
        for m in matches
    ]
    if not rows:
        return pd.DataFrame(
            columns=["namespace", "code", "title", "similarity"],
        )
    return pd.DataFrame(rows)


@connector(output=CATALOG_LIST_NAMESPACES_OUTPUT)
async def catalog_list_namespaces(
    params: CatalogListNamespacesParams,
    *,
    catalog: Any,
) -> pd.DataFrame:
    """Return all distinct namespaces present in the catalog."""
    namespaces = await catalog.list_namespaces()
    return pd.DataFrame({"namespace": namespaces})


# Convenience collection — MCP and terminal wire via bind_deps().
CONNECTORS = Connectors([catalog_search, catalog_list_namespaces])
