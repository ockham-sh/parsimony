"""Local catalog search helpers for connector packages."""

from __future__ import annotations

import asyncio
import logging
from collections import OrderedDict
from collections.abc import Awaitable, Callable, Sequence
from pathlib import Path
from typing import Annotated

import pandas as pd
from pydantic import BaseModel, Field

from parsimony.catalog import Catalog, CatalogMatch
from parsimony.catalog.source import lazy_catalog_dir
from parsimony.connector import Connector, connector
from parsimony.errors import CatalogNotFoundError, ConnectorError, EmptyDataError
from parsimony.result import Column, ColumnRole, OutputConfig

logger = logging.getLogger(__name__)

DEFAULT_SEARCH_COLUMNS: tuple[Column, ...] = (
    Column(name="code", role=ColumnRole.KEY),
    Column(name="title", role=ColumnRole.TITLE),
    Column(name="score", role=ColumnRole.DATA),
)

CatalogBuilder = Callable[[], Awaitable[Catalog]]


def resolved_catalog_url(
    catalog_url_env_var: str,
    default: str,
    *,
    override: str | None = None,
) -> str:
    """Resolve catalog root URL: explicit override, then env, then default."""
    import os

    return (override or os.environ.get(catalog_url_env_var, default)).rstrip("/")


def _to_catalog_not_found(exc: Exception, *, url: str) -> CatalogNotFoundError:
    if isinstance(exc, CatalogNotFoundError):
        return exc
    if type(exc).__name__ == "RepositoryNotFoundError":
        return CatalogNotFoundError(f"Catalog repo not found at {url}")
    if isinstance(exc, FileNotFoundError):
        return CatalogNotFoundError(f"Catalog bundle not present at {url}")
    if isinstance(exc, ConnectorError):
        message = str(exc).lower()
        if "not found" in message or "not present" in message or "does not exist" in message:
            return CatalogNotFoundError(f"Catalog bundle not present at {url}")
    return CatalogNotFoundError(f"Catalog bundle not present at {url}")


def _catalog_missing(exc: Exception) -> bool:
    if isinstance(exc, CatalogNotFoundError):
        return True
    if isinstance(exc, FileNotFoundError):
        return True
    if type(exc).__name__ == "RepositoryNotFoundError":
        return True
    if isinstance(exc, ConnectorError):
        message = str(exc).lower()
        return "not found" in message or "not present" in message or "does not exist" in message
    return False


async def load_or_build_catalog(
    url: str,
    *,
    cache_path: Path | str,
    build: CatalogBuilder | None = None,
) -> Catalog:
    """Load a catalog from *url*, lazy cache, or *build* when missing."""
    cache = Path(cache_path)
    load_exc: Exception | None = None
    try:
        return await Catalog.load(url)
    except Exception as exc:
        if not _catalog_missing(exc):
            raise
        load_exc = exc

    lazy_meta = cache / "meta.json"
    if lazy_meta.is_file():
        try:
            return await Catalog.load(f"file://{cache}")
        except Exception as lazy_exc:
            logger.warning("Lazy catalog at %s unreadable (%s); rebuilding", cache, lazy_exc)

    if build is None:
        raise _to_catalog_not_found(load_exc, url=url) from load_exc

    logger.info("Building catalog for %s into %s", url, cache)
    catalog = await build()
    await catalog.save(f"file://{cache}", builder="lazy")
    return catalog


class CatalogLRU:
    """Bounded in-process cache of hydrated :class:`Catalog` instances keyed by URL."""

    def __init__(self, size: int = 4) -> None:
        if size < 1:
            raise ValueError("catalog_lru_size must be >= 1")
        self._size = size
        self._cache: OrderedDict[str, Catalog] = OrderedDict()
        self._lock = asyncio.Lock()

    async def get_or_load(
        self,
        url: str,
        *,
        cache_path: Path | str | None = None,
        build: CatalogBuilder | None = None,
    ) -> Catalog:
        async with self._lock:
            if url in self._cache:
                self._cache.move_to_end(url)
                return self._cache[url]

            if build is not None and cache_path is not None:
                catalog = await load_or_build_catalog(url, cache_path=cache_path, build=build)
            else:
                try:
                    catalog = await Catalog.load(url)
                except Exception as exc:
                    if _catalog_missing(exc):
                        raise _to_catalog_not_found(exc, url=url) from exc
                    raise

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


def make_local_search_connector(
    *,
    provider: str,
    default_url: str,
    catalog_url_env_var: str,
    env_var: str | None = None,
    tags: Sequence[str],
    description: str,
    build_catalog: CatalogBuilder | None = None,
    catalog_subdirectory: str | None = None,
    output_columns: Sequence[Column] = DEFAULT_SEARCH_COLUMNS,
    code_column: str = "code",
    empty_message: str | None = None,
) -> Connector:
    """Factory for standard single-catalog search connectors."""
    resolved_env = catalog_url_env_var if env_var is None else env_var
    lazy_namespace = catalog_subdirectory or provider
    output = OutputConfig(columns=list(output_columns))
    _lru = CatalogLRU()

    async def _load_catalog(params: CatalogSearchParams) -> Catalog:
        override = params.catalog_url
        root = resolved_catalog_url(resolved_env, default_url, override=override)
        url = root if catalog_subdirectory is None else f"{root.rstrip('/')}/{catalog_subdirectory}"
        cache_path = lazy_catalog_dir(provider, lazy_namespace)
        return await _lru.get_or_load(
            url,
            cache_path=cache_path,
            build=build_catalog,
        )

    async def _search(
        query: str,
        limit: int = 10,
        catalog_url: str | None = None,
    ) -> pd.DataFrame:
        params = CatalogSearchParams(query=query, limit=limit, catalog_url=catalog_url)
        catalog = await _load_catalog(params)
        matches, _ = await catalog.search(params.query, limit=params.limit)
        if not matches:
            msg = empty_message or f"No catalog matches for query={params.query!r}."
            raise EmptyDataError(provider=provider, message=msg)
        return _matches_to_dataframe(matches, code_column=code_column)

    _search.__doc__ = description
    _search.__name__ = f"{provider}_search"
    return connector(output=output, tags=list(tags), description=description)(_search)
