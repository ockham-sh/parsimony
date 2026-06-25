"""Local catalog search helpers for connector packages."""

from __future__ import annotations

import logging
import threading
from collections import OrderedDict
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Annotated

import pandas as pd
from pydantic import BaseModel, Field, ValidationError

from parsimony.catalog import Catalog, CatalogMatch
from parsimony.catalog.source import lazy_catalog_dir
from parsimony.connector import Connector, connector
from parsimony.embedder import PARSIMONY_CATALOG_PACKAGE
from parsimony.errors import CatalogNotFoundError, ConnectorError, EmptyDataError, InvalidParameterError, ProviderError
from parsimony.result import Column, ColumnRole, OutputConfig

logger = logging.getLogger(__name__)

DEFAULT_SEARCH_COLUMNS: tuple[Column, ...] = (
    Column(name="code", role=ColumnRole.KEY),
    Column(name="title", role=ColumnRole.TITLE),
    Column(name="score", role=ColumnRole.DATA),
)

CatalogBuilder = Callable[[], Catalog]


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


def _raise_catalog_dependency_error(exc: ImportError, *, provider: str) -> None:
    raise ProviderError(
        provider,
        status_code=503,
        message=(
            f"{provider}: catalog runtime is not installed — "
            f"install with: pip install '{PARSIMONY_CATALOG_PACKAGE}'. "
            f"DO NOT retry until the catalog extra is installed."
        ),
    ) from exc


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


def load_or_build_catalog(
    url: str,
    *,
    cache_path: Path | str,
    build: CatalogBuilder | None = None,
) -> Catalog:
    """Load a catalog from *url*, lazy cache, or *build* when missing."""
    cache = Path(cache_path)
    load_exc: Exception | None = None
    try:
        return Catalog.load(url)
    except ImportError as exc:
        _raise_catalog_dependency_error(exc, provider="catalog")
    except Exception as exc:
        if not _catalog_missing(exc):
            raise
        load_exc = exc

    lazy_meta = cache / "meta.json"
    if lazy_meta.is_file():
        try:
            return Catalog.load(f"file://{cache}")
        except Exception as lazy_exc:
            logger.warning("Lazy catalog at %s unreadable (%s); rebuilding", cache, lazy_exc)

    if build is None:
        if load_exc is None:
            raise CatalogNotFoundError(f"Catalog bundle not present at {url}")
        raise _to_catalog_not_found(load_exc, url=url) from load_exc

    logger.info("Building catalog for %s into %s", url, cache)
    catalog = build()
    catalog.save(f"file://{cache}", builder="lazy")
    return catalog


class CatalogLRU:
    """Bounded in-process cache of hydrated :class:`Catalog` instances keyed by URL."""

    def __init__(self, size: int = 4) -> None:
        if size < 1:
            raise ValueError("catalog_lru_size must be >= 1")
        self._size = size
        self._cache: OrderedDict[str, Catalog] = OrderedDict()
        self._lock = threading.Lock()

    def get_or_load(
        self,
        url: str,
        *,
        cache_path: Path | str | None = None,
        build: CatalogBuilder | None = None,
        refresh: bool = False,
    ) -> Catalog:
        key = url
        with self._lock:
            if refresh:
                # Drop the in-memory copy so the load below re-runs. The HF
                # downloader revalidates the remote etag, so a republished
                # catalog is picked up without restarting the process.
                self._cache.pop(key, None)
            if key in self._cache:
                self._cache.move_to_end(key)
                return self._cache[key]

            if build is not None and cache_path is not None:
                catalog = load_or_build_catalog(url, cache_path=cache_path, build=build)
            else:
                try:
                    catalog = Catalog.load(url)
                except ImportError as exc:
                    _raise_catalog_dependency_error(exc, provider="catalog")
                except Exception as exc:
                    if _catalog_missing(exc):
                        raise _to_catalog_not_found(exc, url=url) from exc
                    raise

            self._cache[key] = catalog
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
    refresh: bool = Field(
        default=False,
        description=(
            "Re-fetch the catalog from its source instead of the in-process cached copy. "
            "Set true once after the catalog has been republished to pick up the new "
            "version in a running session; leave false otherwise."
        ),
    )


def _matches_to_dataframe(
    matches: list[CatalogMatch],
    *,
    code_column: str = "code",
    metadata_columns: Sequence[str] = (),
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for m in matches:
        row: dict[str, object] = {
            code_column: m.code,
            "title": m.title,
            "score": round(m.score, 6),
        }
        for col in metadata_columns:
            row[col] = m.metadata.get(col)
        rows.append(row)
    return pd.DataFrame(rows)


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
    metadata_columns: Sequence[str] = (),
    empty_message: str | None = None,
) -> Connector:
    """Factory for standard single-catalog search connectors."""
    resolved_env = catalog_url_env_var if env_var is None else env_var
    lazy_namespace = catalog_subdirectory or provider
    output = OutputConfig(columns=list(output_columns))
    _lru = CatalogLRU()

    def _load_catalog(params: CatalogSearchParams) -> Catalog:
        override = params.catalog_url
        root = resolved_catalog_url(resolved_env, default_url, override=override)
        url = root if catalog_subdirectory is None else f"{root.rstrip('/')}/{catalog_subdirectory}"
        cache_path = lazy_catalog_dir(provider, lazy_namespace)
        return _lru.get_or_load(
            url,
            cache_path=cache_path,
            build=build_catalog,
            refresh=params.refresh,
        )

    def _search(
        query: str,
        limit: int = 10,
        catalog_url: str | None = None,
        refresh: bool = False,
    ) -> pd.DataFrame:
        try:
            params = CatalogSearchParams(query=query, limit=limit, catalog_url=catalog_url, refresh=refresh)
        except ValidationError as exc:
            raise InvalidParameterError(provider=provider, message=str(exc)) from exc
        catalog = _load_catalog(params)
        matches = catalog.search(params.query, limit=params.limit)
        if not matches:
            msg = empty_message or f"No catalog matches for query={params.query!r}."
            raise EmptyDataError(provider=provider, message=msg)
        return _matches_to_dataframe(matches, code_column=code_column, metadata_columns=metadata_columns)

    _search.__doc__ = description
    _search.__name__ = f"{provider}_search"
    return connector(output=output, tags=list(tags), description=description)(_search)
