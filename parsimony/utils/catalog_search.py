"""Catalog search helpers for provider packages (not kernel primitives)."""

from __future__ import annotations

import asyncio
import os
from collections import OrderedDict
from collections.abc import Awaitable, Callable, Sequence
from typing import Annotated

import pandas as pd
from huggingface_hub.errors import RepositoryNotFoundError
from pydantic import BaseModel, Field

from parsimony.catalog import Catalog, CatalogEntry, CatalogMatch
from parsimony.connector import Connector, connector
from parsimony.errors import ConnectorError, EmptyDataError
from parsimony.result import Column, ColumnRole, OutputConfig

DEFAULT_SEARCH_COLUMNS: tuple[Column, ...] = (
    Column(name="code", role=ColumnRole.KEY),
    Column(name="title", role=ColumnRole.TITLE),
    Column(name="score", role=ColumnRole.METADATA),
)


def resolved_catalog_url(
    env_var: str,
    default: str,
    *,
    override: str | None = None,
) -> str:
    """Resolve catalog root URL: explicit override, then env, then default."""
    return (override or os.environ.get(env_var, default)).rstrip("/")


class CatalogLRU:
    """In-process LRU of hydrated :class:`Catalog` instances keyed by URL."""

    def __init__(self, size: int = 4) -> None:
        if size < 1:
            raise ValueError("catalog_lru_size must be >= 1")
        self._size = size
        self._cache: OrderedDict[str, Catalog] = OrderedDict()
        self._lock = asyncio.Lock()

    async def get_or_load(self, url: str) -> Catalog:
        async with self._lock:
            if url in self._cache:
                self._cache.move_to_end(url)
                return self._cache[url]
            try:
                catalog = await Catalog.load(url)
            except RepositoryNotFoundError as exc:
                raise ConnectorError(
                    f"Catalog repo not found at {url}. DO NOT retry.",
                    provider="catalog",
                ) from exc
            except FileNotFoundError as exc:
                raise ConnectorError(
                    f"Catalog bundle not present at {url}. DO NOT retry.",
                    provider="catalog",
                ) from exc
            self._cache[url] = catalog
            while len(self._cache) > self._size:
                self._cache.popitem(last=False)
            return catalog

    def clear(self) -> None:
        self._cache.clear()


class CatalogSearchParams(BaseModel):
    """Common parameters for provider catalog search connectors."""

    query: Annotated[
        str,
        Field(
            min_length=1,
            max_length=512,
            description="Structured field query (preferred) or plain text for broad search.",
        ),
    ]
    limit: int = Field(default=10, ge=1, le=50, description="Top-N results.")
    catalog_url: str | None = Field(default=None, description="Override catalog URL.")
    fallback_bm25: bool = Field(
        default=False,
        description="Build a local BM25 catalog from live enumeration when the snapshot is missing.",
    )


def _matches_to_dataframe(
    matches: list[CatalogMatch],
    *,
    code_column: str = "code",
) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                code_column: m.code,
                "title": m.title,
                "score": round(m.score, 6),
            }
            for m in matches
        ]
    )


def make_catalog_search_connector(
    *,
    provider: str,
    default_url: str,
    env_var: str,
    tags: Sequence[str],
    description: str,
    fallback_enumerator: Callable[[], Awaitable[list[CatalogEntry]]] | None = None,
    fallback_env_var: str | None = None,
    output_columns: Sequence[Column] = DEFAULT_SEARCH_COLUMNS,
    code_column: str = "code",
    empty_message: str | None = None,
) -> Connector:
    """Factory for standard single-catalog search connectors."""

    output = OutputConfig(columns=list(output_columns))
    _lru = CatalogLRU()
    _fallback_catalog: Catalog | None = None
    _fallback_lock = asyncio.Lock()

    async def _load_catalog(params: CatalogSearchParams) -> Catalog:
        nonlocal _fallback_catalog
        url = resolved_catalog_url(env_var, default_url, override=params.catalog_url)
        use_fallback = params.fallback_bm25
        if not use_fallback and fallback_env_var:
            raw = os.environ.get(fallback_env_var, "").strip().lower()
            use_fallback = raw in {"1", "true", "yes", "on"}
        if not use_fallback or fallback_enumerator is None:
            return await _lru.get_or_load(url)
        async with _fallback_lock:
            if _fallback_catalog is not None:
                return _fallback_catalog
            try:
                return await _lru.get_or_load(url)
            except (ConnectorError, FileNotFoundError, RepositoryNotFoundError):
                entries = await fallback_enumerator()
                from parsimony.catalog import BM25Index, Catalog

                catalog = Catalog(
                    provider,
                    indexes=[
                        BM25Index("code_bm25", field="code"),
                        BM25Index("title_bm25", field="title"),
                    ],
                )
                catalog.set_entries(entries)
                await catalog.build()
                _fallback_catalog = catalog
                return catalog

    async def _search(
        query: str,
        limit: int = 10,
        catalog_url: str | None = None,
        fallback_bm25: bool = False,
    ) -> pd.DataFrame:
        params = CatalogSearchParams(
            query=query,
            limit=limit,
            catalog_url=catalog_url,
            fallback_bm25=fallback_bm25,
        )
        catalog = await _load_catalog(params)
        matches, _ = await catalog.search(params.query, limit=params.limit)
        if not matches:
            msg = empty_message or f"No catalog matches for query={params.query!r}."
            raise EmptyDataError(provider=provider, message=msg)
        return _matches_to_dataframe(matches, code_column=code_column)

    _search.__doc__ = description
    _search.__name__ = f"{provider}_search"
    return connector(output=output, tags=list(tags), description=description)(_search)
