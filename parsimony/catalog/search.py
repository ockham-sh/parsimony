"""Local catalog search helpers for connector packages."""

from __future__ import annotations

import logging
import threading
import time
from collections import OrderedDict
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import Annotated, Any

import pandas as pd
from pydantic import BaseModel, Field, ValidationError

from parsimony.catalog import Catalog, CatalogMatch, SearchDetail
from parsimony.catalog.filters import FilterLike, as_filter
from parsimony.catalog.source import lazy_catalog_dir
from parsimony.connector import Connector, connector
from parsimony.embedder import PARSIMONY_CATALOG_PACKAGE
from parsimony.errors import CatalogNotFoundError, ConnectorError, EmptyDataError, InvalidParameterError, ProviderError
from parsimony.result import Column, ColumnRole, OutputSpec

logger = logging.getLogger(__name__)


def _default_search_columns(provider: str) -> tuple[Column, ...]:
    """Search-result row schema for *provider*'s own catalog namespace."""
    return (
        Column(name="code", role=ColumnRole.KEY, namespace=provider),
        Column(name="title", role=ColumnRole.TITLE),
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
    started = time.monotonic()
    catalog = build()
    catalog.save(f"file://{cache}", builder="lazy")
    logger.info("Built catalog for %s in %.1fs", url, time.monotonic() - started)
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


#: A free-text ``query`` is a ranked shortlist read into context; keep it small. Omitting
#: ``query`` and passing only ``filter`` is an exact enumeration read into a kernel variable,
#: so it may run up to ``ENUMERATION_LIMIT`` (the cap protects context, not the variable).
RANKED_LIMIT = 50
ENUMERATION_LIMIT = 10_000


class CatalogSearchParams(BaseModel):
    """Common parameters for provider catalog search connectors."""

    query: Annotated[
        str | None,
        Field(
            default=None,
            max_length=512,
            description="Plain-text query matched against catalog titles and descriptions. "
            "Omit for a filter-only enumeration read.",
        ),
    ] = None
    limit: int = Field(
        default=10,
        ge=1,
        le=ENUMERATION_LIMIT,
        description=(
            f"Max rows returned. With query= this is a ranked shortlist (limit <= {RANKED_LIMIT}); "
            f"a filter-only enumeration may go up to {ENUMERATION_LIMIT}."
        ),
    )
    filter: Any = Field(
        default=None,
        description="Row constraint: equality mapping shorthand ({'code': 'CP0000'}, "
        "{'freq': ['M', 'Q']}), typed Filter (F('code').eq(...)/.prefix(...)/.matches(...)), "
        "or expression form ({'field': 'code', 'prefix': 'D.'}). Combine with query, or use "
        "alone (omit query) to enumerate the matching slice from the cached catalog.",
    )
    catalog_url: str | None = Field(default=None, description="Override catalog URL.")
    refresh: bool = Field(
        default=False,
        description=(
            "Drop the in-process cached copy and reload the catalog. Set true once after "
            "the catalog has been republished; leave false otherwise. Note: the Hub "
            "snapshot itself is revalidated at most every ~5 minutes, so a republish can "
            "take up to that long to appear even with refresh=true."
        ),
    )


def wire_score(score: float | None) -> float | None:
    """Round a ranked score for connector output; leave unranked as ``None``."""
    return None if score is None else round(score, 6)


def wire_search_detail(detail: SearchDetail | None) -> str | None:
    """Canonical JSON for a hit's :class:`SearchDetail`, or ``None`` when unranked."""
    return None if detail is None else detail.model_dump_json()


def _metadata_names(columns: Sequence[Column]) -> tuple[str, ...]:
    """``ColumnRole.METADATA`` names — the bag keys projected onto each search hit."""
    return tuple(c.name for c in columns if c.role == ColumnRole.METADATA)


def search_hits_dataframe(
    matches: list[CatalogMatch],
    *,
    code_column: str = "code",
    columns: Sequence[Column],
    extras: Mapping[str, object] | None = None,
    extra_for: Callable[[CatalogMatch], Mapping[str, object]] | None = None,
) -> pd.DataFrame:
    """Shape catalog matches into a search hit table.

    Copies ``ColumnRole.METADATA`` fields from each match's metadata bag (same
    names ``describe()`` / cards show). *extras* apply to every row; *extra_for*
    adds per-match overrides (e.g. computed dispatch hints). Trailing
    ``score`` / ``search_detail`` always come last — same pair :data:`RANKING_COLUMNS`
    appends to factory output specs.
    """
    meta_names = _metadata_names(columns)
    rows: list[dict[str, object]] = []
    for m in matches:
        row: dict[str, object] = {code_column: m.code, "title": m.title}
        bag = m.metadata or {}
        for col in meta_names:
            row[col] = bag.get(col)
        if extras:
            row.update(extras)
        if extra_for is not None:
            row.update(extra_for(m))
        row["score"] = wire_score(m.score)
        row["search_detail"] = wire_search_detail(m.search_detail)
        rows.append(row)
    return pd.DataFrame(rows)


SCORE_COLUMN = Column(
    name="score",
    role=None,
    description=(
        "Query-relative relevance in (0, 1] with this page's best hit at 1.0; "
        "null when the read was filter-only (nothing ranked). Not comparable "
        "across queries or catalogs. Ranked rows are a shortlist — commit from "
        "provider metadata, not from score alone."
    ),
    render_description_in_card=False,
)
SEARCH_DETAIL_COLUMN = Column(
    name="search_detail",
    role=None,
    description=(
        "Canonical JSON of SearchDetail (per-field values, weights, component "
        "raw scores/ranks, candidate_limit). Null on filter-only reads. Optional "
        "debugging evidence for why a row ranked where it did — not a correctness "
        "signal. Rehydrate with SearchDetail.model_validate_json(...)."
    ),
    exclude_from_llm_view=True,
    render_description_in_card=False,
)

#: The ranking pair every catalog-backed search emits — same two trailing columns,
#: same order, same meanings, on every provider. Like :data:`RANKED_SEARCH_CAVEAT`
#: below, this is a property of catalog ranking, not of any one provider: defined
#: once here, appended to every factory-built output spec, and reused by the
#: hand-rolled catalog search connectors.
#:
#: Both columns use ``role=None`` (uncategorized framework output): present in the
#: frame and OutputSpec, excluded from entity/data projection. ``score`` stays
#: LLM-visible; ``search_detail`` is hidden from ``to_llm()``.
#:
#: The descriptions here are the single source of truth for what the pair means
#: (quoted into the ``parsimony`` skill, not duplicated there) — but
#: ``render_description_in_card`` is False so a connector's compact ``to_llm()``
#: card doesn't repeat the same sentences on every ranked connector in a bound
#: catalog. ``describe()`` still shows them.
RANKING_COLUMNS = (SCORE_COLUMN, SEARCH_DETAIL_COLUMN)


#: The entity recipe: the surface a standard discovery catalog declares — curated
#: title and description text, weighted uniformly. Both are curated prose about the
#: same row and no measurement so far justifies preferring one; a connector with
#: evidence for a different balance declares its own weights. Ontology-shaped
#: catalogs (rows composed of codelist members, no curated text) declare their own
#: label fields instead.
ENTITY_RANKING_FIELDS: Mapping[str, float] = {"title": 1.0, "description": 1.0}


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
    output: OutputSpec | None = None,
    code_column: str = "code",
    ranking_fields: Mapping[str, float] | None = None,
    empty_message: str | None = None,
) -> Connector:
    """Factory for standard single-catalog search connectors.

    A ranked query searches the declared surface as literal text — there is no
    ``FIELD: value`` grammar; anything to be enforced exactly goes in ``filter=``.

    *ranking_fields* is ``{field: positive weight}``, ranked by
    :meth:`Catalog.multi_field_search` with Level-2 Reciprocal Rank Fusion over
    per-field row rankings (weights scale rank contribution, not raw magnitudes).
    The connector owns the weights, so ranking policy is declared
    rather than inferred from how many fields happened to be passed. Default is
    the entity recipe, :data:`ENTITY_RANKING_FIELDS`. The declaration is
    intersected with the loaded catalog's indexes at query time, so a published
    catalog missing one index still searches.

    *output* is the provider column declaration (KEY / TITLE / METADATA) — same
    ``OutputSpec`` shape as ``@connector(output=...)``. The factory appends
    :data:`RANKING_COLUMNS` and projects ``ColumnRole.METADATA`` names from each
    match's metadata bag onto the hit table (one list drives ``describe()`` and
    the frame).
    """
    resolved_env = catalog_url_env_var if env_var is None else env_var
    lazy_namespace = catalog_subdirectory or provider
    weights = dict(ranking_fields) if ranking_fields is not None else dict(ENTITY_RANKING_FIELDS)
    declared = tuple(weights)
    columns = tuple(output.columns) if output is not None else _default_search_columns(provider)
    connector_output = OutputSpec(columns=[*columns, *RANKING_COLUMNS])
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
        query: str | None = None,
        limit: int = 10,
        filter: FilterLike | None = None,
        catalog_url: str | None = None,
        refresh: bool = False,
    ) -> pd.DataFrame:
        try:
            params = CatalogSearchParams(
                query=query, limit=limit, filter=filter, catalog_url=catalog_url, refresh=refresh
            )
        except ValidationError as exc:
            raise InvalidParameterError(provider=provider, message=str(exc)) from exc
        q = (params.query or "").strip() or None
        predicate = as_filter(params.filter)
        if q is None and predicate is None:
            raise InvalidParameterError(provider=provider, message=f"{provider}_search requires query= and/or filter=.")
        if q is not None and params.limit > RANKED_LIMIT:
            raise InvalidParameterError(
                provider=provider,
                message=(
                    f"query= is a ranked shortlist (limit <= {RANKED_LIMIT}). To read a whole "
                    f"slice, drop query= and pass filter= (limit up to {ENUMERATION_LIMIT})."
                ),
            )
        catalog = _load_catalog(params)
        # The declared surface, kept to fields this catalog actually indexes.
        # The query is literal text either way — the factory exposes no
        # `FIELD: value` DSL; exact reads go through filter=.
        surface = [f for f in declared if f in catalog.indexes]
        if q is None:
            matches = catalog.search(None, limit=params.limit, filter=predicate)
        else:
            if not surface:
                raise ProviderError(
                    provider,
                    status_code=503,
                    message=(
                        f"{provider}: the published catalog indexes none of the declared ranking "
                        f"fields {sorted(weights)}. Catalog indexes: {sorted(catalog.indexes)}."
                    ),
                )
            matches = catalog.multi_field_search(
                q,
                fields={name: weights[name] for name in surface},
                filter=predicate,
                limit=params.limit,
            )
        if not matches:
            msg = empty_message or f"No catalog matches for query={q!r} filter={params.filter!r}."
            raise EmptyDataError(provider=provider, message=msg)
        return search_hits_dataframe(matches, code_column=code_column, columns=columns)

    plural = "fields" if len(declared) > 1 else "field"
    surface_note = f" Matches query words against the catalog's {', '.join(declared)} {plural}."
    full_description = description.rstrip() + surface_note
    _search.__doc__ = full_description
    _search.__name__ = f"{provider}_search"
    return connector(output=connector_output, tags=list(tags), description=full_description)(_search)
