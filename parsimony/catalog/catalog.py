"""Catalog runtime: entries, search, and snapshot persistence.

Ranking model, in one paragraph: a match carries a *score* and optional
*search_detail*. Independently produced rankings (lexical vs semantic within a
field; one field vs another across a row) are fused by weighted Reciprocal Rank
Fusion — ranks, never raw magnitudes — and :func:`parsimony.indexes.rrf` always
top-normalizes so every fused score sits in ``(0, 1]`` with the best item at
``1.0``. Rows order by ``(score desc, namespace, code)`` — fully deterministic,
with no tier the caller did not ask for. *search_detail* preserves component and
field evidence for inspection; it is not a correctness signal. Anything a caller
wants enforced rather than estimated is a ``filter``, not a query; anything it
wants ranked above relevance is its own tier, because only the caller knows what
its fields mean. The full story with worked examples lives in
``docs/catalog/ranking-and-fusion.md``.
"""

from __future__ import annotations

import json
import shutil
import tempfile
import threading
from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from parsimony.catalog.backends import (
    ParquetRowBackend,
    entity_field_names,
    entity_from_row,
    entity_row,
    row_identity,
)
from parsimony.catalog.contracts import CatalogBackendConfig
from parsimony.catalog.filters import FieldIn, Filter, FilterLike, all_of, any_of, as_filter
from parsimony.catalog.indexes import (
    BM25Index,
    CatalogIndex,
    ComponentEvidence,
    IndexBuildContext,
    QueryContext,
    ScoredValue,
    _load_index,
    _save_index,
    search_index_values,
)
from parsimony.catalog.models import (
    BroadSearchUnavailableError,
    CatalogMatch,
    CatalogValueMatch,
    ComponentSearchDetail,
    FieldSearchDetail,
    SearchDetail,
    UnknownIndexedFieldError,
    catalog_match_from_entity,
)
from parsimony.catalog.remote import _upload_hf, resolve_catalog_dir
from parsimony.catalog.storage import (
    ENTRIES_FILENAME,
    INDEXES_DIRNAME,
    META_FILENAME,
    BackendMeta,
    BuildInfo,
    CatalogMeta,
    _compute_content_sha256,
    _read_parquet,
    read_meta,
)
from parsimony.catalog.urls import parse_catalog_url
from parsimony.catalog.validation import compute_manifest_contract_sha256, validate_catalog_snapshot
from parsimony.entity import Entity, entity_key, field_value, normalize_namespace
from parsimony.errors import InvalidParameterError
from parsimony.indexes import competition_ranks, rrf_traced


@dataclass(frozen=True)
class _FieldValueHit:
    """One field's scored value, retained through the row scan for SearchDetail."""

    logical_field: str
    weight: float
    scored: ScoredValue


def _components_to_models(components: tuple[ComponentEvidence, ...]) -> list[ComponentSearchDetail]:
    return [ComponentSearchDetail(kind=c.kind, raw_score=c.raw_score, rank=c.rank) for c in components]


def _value_search_detail(field: str, scored: Sequence[ScoredValue], *, candidate_limit: int) -> dict[str, SearchDetail]:
    """Build per-value SearchDetail maps for :meth:`Catalog.search_values`."""
    relevances = {sv.text: sv.relevance for sv in scored}
    ranks = competition_ranks(relevances) if relevances else {}
    out: dict[str, SearchDetail] = {}
    for sv in scored:
        out[sv.text] = SearchDetail(
            candidate_limit=candidate_limit,
            fields=[
                FieldSearchDetail(
                    field=field,
                    value=sv.text,
                    weight=1.0,
                    relevance=sv.relevance,
                    exact=sv.exact,
                    fused_rank=ranks[sv.text],
                    components=_components_to_models(sv.components),
                )
            ],
        )
    return out


def _is_boolish(value: Any) -> bool:
    """True for Python / NumPy bools — facets for ``filter=``, not BM25 surfaces."""
    if isinstance(value, (bool, np.bool_)):
        return True
    # Arrow / numpy scalars often expose ``.item()``; unwrap once.
    if hasattr(value, "item") and not isinstance(value, (bytes, bytearray, str)):
        try:
            return isinstance(value.item(), (bool, np.bool_))
        except (ValueError, TypeError):
            return False
    return False


def _default_indexes_from_entities(entries: list[Entity]) -> dict[str, CatalogIndex]:
    """BM25 indexes for code, title, and every *text/number* metadata key in *entries*.

    Nested metadata (list/dict/set) is display-only and is not indexed by the
    default policy — operators that need it searchable must declare an explicit
    derived scalar field. Bool metadata is also skipped: a flag is a facet for
    ``filter=``, not a ranking surface (BM25 over ``"true"``/``"false"`` is noise).
    An operator that truly wants a bool ranked can pass an explicit ``indexes``
    map naming that field.
    """
    from parsimony.entity import require_scalar_text

    meta_keys: set[str] = set()
    skipped_keys: set[str] = set()
    for entry in entries:
        for key, value in entry.metadata.items():
            if key in skipped_keys:
                continue
            if _is_boolish(value):
                skipped_keys.add(key)
                meta_keys.discard(key)
                continue
            try:
                require_scalar_text(value, field=key, identity=f"{entry.namespace}/{entry.code}")
            except ValueError:
                skipped_keys.add(key)
                meta_keys.discard(key)
                continue
            meta_keys.add(key)
    fields = ["code", "title", *sorted(meta_keys - skipped_keys)]
    return {field: BM25Index() for field in fields}


def _assert_scalar_search_columns(
    backend: ParquetRowBackend,
    *,
    indexes: Mapping[str, CatalogIndex],
    config: CatalogBackendConfig,
) -> None:
    """Reject list/struct parquet columns that an index would score or filter on."""
    import pyarrow as pa

    schema = backend._dataset.schema
    aliases = {"code": config.code_column, "title": config.title_column}
    for logical in indexes:
        physical = aliases.get(logical, logical)
        if physical not in schema.names:
            continue
        field_type = schema.field(physical).type
        if pa.types.is_list(field_type) or pa.types.is_large_list(field_type) or pa.types.is_struct(field_type):
            raise ValueError(
                f"Indexed column {physical!r} (field {logical!r}) must be scalar; "
                f"parquet type is {field_type}. Nested columns are display-only — "
                "expose a derived scalar field for search."
            )


class Catalog:
    """Portable catalog over entries and configured indexes.

    Pass ``indexes=None`` to use the framework default index policy: at
    :meth:`build`, BM25 indexes are created for ``code``, ``title``, and each
    text/number metadata key on the catalog entries (nested values and bool
    flags are skipped — bools remain filterable facets only). Pass an explicit
    ``indexes`` dict to take full control — no extra indexes are added silently.

    Keys in ``indexes`` are *logical search-surface names*: they are what a
    caller names in ``field=``, and what ``UnknownIndexedFieldError`` reports.
    They match the Entity field name each index reads. Multi-field scoring is a
    query-time concern — pass ``fields={name: weight}`` to
    :meth:`multi_field_search`.

    A catalog searches in one of two layouts. In the default *row-indexed*
    layout the entities are the rows: :meth:`build` indexes them and
    :meth:`search` returns them directly. In the *value-indexed* layout the
    entities are codelist members (distinct dimension values such as
    ``geo:DE``) and each row of a parquet file attached via
    :meth:`attach_parquet_rows` is a composition of members; search matches
    members in the indexes, then streams the rows composed with them.
    """

    def __init__(
        self,
        name: str,
        *,
        indexes: dict[str, CatalogIndex] | None = None,
        field_links: dict[str, str] | None = None,
    ) -> None:
        self.name = normalize_namespace(name)
        self._field_links = dict(field_links or {})
        self._backend_config = CatalogBackendConfig()
        self._default_index_policy = indexes is None
        self._indexes: dict[str, CatalogIndex] = dict(indexes) if indexes is not None else {}
        self._entities: list[Entity] = []
        self._key_to_idx: dict[tuple[str, str], int] = {}
        self._backend: ParquetRowBackend | None = None
        self._dirty = True
        self._lock = threading.Lock()

    # ------------------------------------------------------------------
    # Public properties
    # ------------------------------------------------------------------

    @property
    def entities(self) -> list[Entity]:
        return list(self._entities)

    @property
    def indexes(self) -> dict[str, CatalogIndex]:
        return dict(self._indexes)

    def __len__(self) -> int:
        return self._backend.count() if self._backend is not None else len(self._entities)

    # ------------------------------------------------------------------
    # Mutation
    # ------------------------------------------------------------------

    def set_entities(self, entries: Iterable[Entity]) -> None:
        """Replace entries without rebuilding indexes.

        Accepts any iterable of :class:`Entity` — a plain ``list``, or the
        ``dict_values`` view returned by ``result.entities.values()``.
        """
        if self._backend is not None:
            raise ValueError(
                "set_entities() is not supported after attach_parquet_rows() — the parquet file is the row population"
            )
        self._entities = []
        self._key_to_idx = {}
        self._upsert_entities(entries)
        self._invalidate()

    def set_indexes(self, indexes: dict[str, CatalogIndex]) -> None:
        """Replace all field indexes without rebuilding."""
        self._indexes = dict(indexes)
        self._default_index_policy = False
        self._invalidate()

    def attach_parquet_rows(self, parquet_path: Path, *, config: CatalogBackendConfig) -> None:
        """Switch the catalog to the value-indexed layout: rows come from *parquet_path*.

        Call exactly once, after :meth:`build`. The entities indexed by
        :meth:`build` are discarded into the indexes as the scoring surface —
        they are members (distinct dimension values), while the parquet rows
        are the compositions searched over: a different, usually far larger
        population. After attaching, :attr:`entities` is empty and :meth:`get`
        returns ``None`` (honest emptiness, not an error), and calling
        :meth:`build` or :meth:`set_entities` raises.
        """
        if self._dirty:
            raise ValueError("attach_parquet_rows() requires built indexes — call build() first")
        if self._backend is not None:
            raise ValueError("attach_parquet_rows() was already called — a catalog takes its rows exactly once")
        self._backend_config = config
        backend = ParquetRowBackend(Path(parquet_path))
        _assert_scalar_search_columns(backend, indexes=self._indexes, config=config)
        self._backend = backend
        self._entities = []
        self._key_to_idx = {}

    def build(self) -> None:
        """Build configured indexes over the catalog's current entries."""
        if self._backend is not None:
            raise ValueError(
                "build() is not supported after attach_parquet_rows() — "
                "it would rebuild the indexes over the emptied entity list"
            )
        if self._default_index_policy:
            self._indexes = _default_indexes_from_entities(self._entities)

        with self._lock:
            self._rebuild_indices()
            self._dirty = False

    def get(self, namespace: str, code: str) -> Entity | None:
        idx = self._key_to_idx.get(entity_key(namespace, code))
        return self._entities[idx] if idx is not None else None

    # ------------------------------------------------------------------
    # Search
    # ------------------------------------------------------------------

    def search(
        self,
        query: str | None = None,
        limit: int = 50,
        *,
        field: str | None = None,
        filter: FilterLike | None = None,
        top_k_values: int = 50,
    ) -> list[CatalogMatch]:
        """Rank rows by one field's relevance to *query*, or enumerate a filter.

        *query* is literal text — always. There is no query grammar: a colon or an
        ``&&`` in the text is punctuation to be matched, not a field scope or a
        boolean operator. Constraints you want *enforced* go in *filter*, which
        excludes non-matching rows outright; *query* only orders what survives.

        Scoring reads one index, named by *field* (default: the catalog's broad
        search field). Each match carries ``score`` and optional ``search_detail``
        (component/field evidence for inspection). Ranked rows are a shortlist —
        commit from provider metadata, not from ranking evidence alone.

        Parameters
        ----------
        query:
            Literal text to rank by. Omit for a filter-only enumeration.
        limit:
            Maximum results returned.
        field:
            The indexed field to score. Omit to use the catalog's default broad
            field. To combine several fields, use :meth:`multi_field_search`,
            which takes an explicit weight per field.
        filter:
            Exact AND constraint — the mapping shorthand ``{column: value}`` /
            ``{column: [values, …]}`` or a composable expression. Combines with
            *query*.
        top_k_values:
            Per-field cap on the scored-value table (the fuzzy-evidence pool). A
            deliberate noise floor: values beyond the top ``top_k_values``
            contribute no score.
        """
        self._ensure_built("searched")
        if query is not None and not query.strip():
            query = None
        if query is None and filter is None:
            raise InvalidParameterError("catalog", "search requires query= and/or filter=")

        if query is None:
            if field is not None:
                raise InvalidParameterError("catalog", "field= requires a non-empty query=")
            return self._filter_only(filter, limit=limit)

        scored = field if field is not None else self._resolve_default_field()
        if scored is None:
            raise BroadSearchUnavailableError(
                f"This catalog has no default search field, so field= is required. "
                f"Indexed fields: [{', '.join(repr(f) for f in sorted(self._indexes))}]"
            )
        return self.multi_field_search(
            query,
            fields={scored: 1.0},
            filter=filter,
            limit=limit,
            candidate_values=top_k_values,
        )

    def search_values(
        self,
        query: str,
        field: str,
        *,
        limit: int = 20,
    ) -> list[CatalogValueMatch]:
        """Return distinct indexed values for *field*, ranked by (exact, score).

        The value the query literally names ranks above every fuzzy candidate. If
        *field* is in ``field_links``, each result also carries the linked value
        (e.g. the canonical code for a human-readable label).

        This is the resolution primitive: when a caller knows what a value *means*
        but not how it is spelled in the data, it resolves the value here, reads
        the code off the result, and then filters exactly on it.
        """
        self._ensure_built("searched")
        try:
            index = self.index_for(field)
        except KeyError as err:
            raise UnknownIndexedFieldError(f"No index configured for field {field!r}") from err
        ctx = QueryContext(query=query)
        scored = search_index_values(index, ctx, limit=limit)
        details = _value_search_detail(field, scored, candidate_limit=limit)
        link_field = self._field_links.get(field)
        if link_field is None:
            return [
                CatalogValueMatch(
                    value=sv.text,
                    score=sv.relevance,
                    exact=sv.exact,
                    search_detail=details[sv.text],
                )
                for sv in scored
            ]
        return [
            CatalogValueMatch(
                value=sv.text,
                score=sv.relevance,
                exact=sv.exact,
                search_detail=details[sv.text],
                linked_value=self._linked_value(field, sv.text, link_field),
            )
            for sv in scored
        ]

    def iter_rows(
        self,
        *,
        filter: FilterLike | None = None,
        columns: Sequence[str] | None = None,
    ) -> Iterator[dict[str, Any]]:
        """Stream the catalog's selected rows as lightweight mappings.

        Works over both layouts: a parquet-backed catalog compiles *filter* to
        an Arrow predicate and projects *columns* in the scan, while a
        row-indexed catalog evaluates the same filter tree over its entities.
        Rows are plain mappings, not validated result models — this is the
        efficient read path for enumerating a slice.

        *filter* accepts the mapping shorthand or a composable expression, and
        addresses fields by their logical names (``code`` and ``title`` map to
        whatever physical columns the catalog carries them in).

        *columns* projects the output and is likewise logical: each requested
        name is the key you get back. Omit it to receive the native row —
        parquet column names, or ``namespace``/``code``/``title`` plus metadata
        keys for a row-indexed catalog. Ordering is unspecified.
        """
        # Validate before returning the generator so a bad filter raises at the
        # call, not on first iteration.
        self._ensure_built("searched")
        predicate = self._resolve_filter(filter)
        requested = list(columns) if columns is not None else None
        if requested is not None and not requested:
            raise InvalidParameterError("catalog", "columns= must name at least one column, or be omitted")
        return self._iter_rows(predicate, requested)

    def _iter_rows(self, predicate: Filter | None, requested: list[str] | None) -> Iterator[dict[str, Any]]:
        if self._backend is not None:
            projection = [self._physical_column(name) for name in requested] if requested is not None else None
            for row in self._backend.iter_rows(expression=predicate, columns=projection):
                yield self._project(row, requested)
            return

        for entity in self._entities:
            row = entity_row(entity)
            if predicate is not None and not predicate.matches(row):
                continue
            yield self._project(row, requested)

    def multi_field_search(
        self,
        query: str,
        *,
        fields: Mapping[str, float],
        filter: FilterLike | None = None,
        limit: int = 20,
        candidate_values: int = 50,
    ) -> list[CatalogMatch]:
        """Rank rows by weighted RRF over per-field value rankings.

        Per field, distinct values are scored and standardized by
        :func:`~parsimony.indexes.rrf` (best ``1.0``). Candidate rows are those
        carrying any scored value, ANDed with the caller's *filter*. Each field
        then becomes a ranking of those rows (by the relevance of the value they
        carry); :func:`~parsimony.indexes.rrf` fuses those rankings with the
        field weights and returns row scores already in ``(0, 1]`` with the best
        hit at ``1.0``.

        Candidates are pooled at the *distinct value* level, never by joining
        per-field row pages: thousands of rows can share one scored value, so a
        truncated row page is not a truncated value table. *candidate_values*
        caps each field's value table and is a deliberate noise floor as well as
        a cost cap.

        *query* is literal text. Rows order by ``(score desc, namespace, code)``
        and only the winners are materialized into :class:`CatalogMatch`.
        """
        self._ensure_built("searched")
        if not query or not query.strip():
            raise InvalidParameterError("catalog", "multi_field_search requires a non-empty query=")
        if not fields:
            raise InvalidParameterError("catalog", "multi_field_search requires at least one field weight")
        if limit < 1:
            raise InvalidParameterError("catalog", "limit must be at least 1")
        if candidate_values < 1:
            raise InvalidParameterError("catalog", "candidate_values must be at least 1")

        weights: dict[str, float] = {}
        for name, weight in fields.items():
            value = float(weight)
            if not np.isfinite(value) or value <= 0.0:
                raise InvalidParameterError(
                    "catalog", f"Field weight for {name!r} must be a positive finite number, got {weight!r}"
                )
            try:
                self.index_for(name)
            except KeyError as err:
                raise UnknownIndexedFieldError(f"No index configured for field {name!r}") from err
            weights[name] = value

        # Per field: {value text: FieldValueHit}, keyed by the physical column so
        # row lookup needs no alias pass. Uses the index directly rather than
        # search_values() because that resolves linked values, which would cost a
        # parquet scan per value.
        # One QueryContext across fields: shared embedders embed the query once.
        ctx = QueryContext(query=query)
        value_tables: dict[str, dict[str, _FieldValueHit]] = {}
        weight_by_column: dict[str, float] = {}
        for name, weight in weights.items():
            scored = search_index_values(self.index_for(name), ctx, limit=candidate_values)
            if not scored:
                continue
            table = {
                sv.text: _FieldValueHit(logical_field=name, weight=weight, scored=sv)
                for sv in scored
                if sv.exact or sv.relevance > 0.0
            }
            if table:
                column = self._physical_column(name)
                value_tables[column] = table
                weight_by_column[column] = weight
        if not value_tables:
            return []

        candidates = any_of(*(FieldIn(column, tuple(table)) for column, table in value_tables.items()))
        caller = self._resolve_filter(filter)
        predicate = candidates if caller is None else all_of(caller, candidates)

        # Level-2: each field ranks the candidate *rows* by the relevance of the
        # value they carry; rrf fuses those rankings (and top-normalizes).
        field_row_rankings: dict[str, dict[tuple[str, str], float]] = {column: {} for column in value_tables}
        row_meta: dict[tuple[str, str], tuple[Entity | None, dict[str, Any], dict[str, _FieldValueHit]]] = {}
        for row, entity in self._candidate_scan(predicate):
            namespace, code = (
                (entity.namespace, entity.code)
                if entity is not None
                else row_identity(row, config=self._backend_config)
            )
            key = (namespace, code)
            hits: dict[str, _FieldValueHit] = {}
            for column, table in value_tables.items():
                hit = table.get(_cell_text(row.get(column)))
                if hit is not None:
                    field_row_rankings[column][key] = hit.scored.relevance
                    hits[column] = hit
            if hits:
                row_meta[key] = (entity, row, hits)

        field_row_rankings = {column: ranking for column, ranking in field_row_rankings.items() if ranking}
        if not field_row_rankings:
            return []

        fused, source_ranks = rrf_traced(
            field_row_rankings,
            weights={column: weight_by_column[column] for column in field_row_rankings},
        )
        ordered_keys = sorted(fused, key=lambda item: (-fused[item], item[0], item[1]))[:limit]
        matches: list[CatalogMatch] = []
        for key in ordered_keys:
            entity, row, hits = row_meta[key]
            if entity is None:
                entity = entity_from_row(row, config=self._backend_config)
            field_details = [
                FieldSearchDetail(
                    field=hit.logical_field,
                    value=hit.scored.text,
                    weight=hit.weight,
                    relevance=hit.scored.relevance,
                    exact=hit.scored.exact,
                    fused_rank=source_ranks[column][key],
                    components=_components_to_models(hit.scored.components),
                )
                for column, hit in hits.items()
            ]
            matches.append(
                catalog_match_from_entity(
                    entity,
                    score=fused[key],
                    search_detail=SearchDetail(candidate_limit=candidate_values, fields=field_details),
                )
            )
        return matches

    def _candidate_scan(self, predicate: Filter | None) -> Iterator[tuple[dict[str, Any], Entity | None]]:
        """Yield ``(row, entity)`` for every row matching *predicate*, in one pass.

        The parquet layout defers entity materialization (``None``) so only
        ranked winners pay for it; the row-indexed layout already holds them. A
        ``None`` predicate admits every row.
        """
        if self._backend is not None:
            for row in self._backend.iter_rows(expression=predicate):
                yield row, None
            return
        for entity in self._entities:
            row = entity_row(entity)
            if predicate is None or predicate.matches(row):
                yield row, entity

    # ------------------------------------------------------------------
    # Internal search helpers
    # ------------------------------------------------------------------

    def _physical_column(self, field: str) -> str:
        """Map a logical search-surface name onto the column that carries it."""
        aliases = {"code": self._backend_config.code_column, "title": self._backend_config.title_column}
        return aliases.get(field, field)

    def _resolve_filter(self, spec: FilterLike | None) -> Filter | None:
        """Normalize a filter and map its logical field names to physical columns."""
        predicate = as_filter(spec)
        if predicate is None:
            return None
        if self._backend is not None:
            return predicate.rename(self._physical_column)
        # Row-indexed rows are addressed by entity field name; only reject
        # names no entity carries, so a typo cannot silently match nothing.
        known = entity_field_names(self._entities)
        unknown = sorted(predicate.fields() - known)
        if unknown:
            raise InvalidParameterError(
                "catalog",
                f"Unknown filter field(s) {unknown}. Available fields: {sorted(known)}",
            )
        return predicate

    def _project(self, row: dict[str, Any], columns: list[str] | None) -> dict[str, Any]:
        """Return the row keyed by the caller's requested logical names."""
        if columns is None:
            return row
        return {name: row.get(self._physical_column(name)) for name in columns}

    def _filter_only(self, filter: FilterLike | None, *, limit: int) -> list[CatalogMatch]:
        """Enumerate a filtered slice as matches, in backend order.

        There is nothing to rank — every row satisfies the filter equally — so
        each match reports ``score=None`` and ``search_detail=None``. Prefer
        :meth:`iter_rows` when you want the rows rather than result models.
        """
        predicate = self._resolve_filter(filter)
        matches: list[CatalogMatch] = []
        for row, entity in self._candidate_scan(predicate):
            resolved = entity if entity is not None else entity_from_row(row, config=self._backend_config)
            matches.append(catalog_match_from_entity(resolved, score=None))
            if len(matches) >= limit:
                break
        return matches

    def _linked_value(self, field: str, value: str, link_field: str) -> str | None:
        """Resolve the linked field value for a given indexed value."""
        if self._backend is not None:
            physical = self._physical_column(field)
            physical_link = self._physical_column(link_field)
            for row in self._backend.iter_rows(
                expression=FieldIn(physical, (value,)),
                columns=[physical_link],
            ):
                lv = row.get(physical_link)
                return str(lv) if lv is not None else None
            return None
        for entity in self._entities:
            fv = field_value(entity, field)
            if fv is not None and str(fv) == value:
                lv = field_value(entity, link_field)
                return str(lv) if lv is not None else None
        return None

    # ------------------------------------------------------------------
    # Index/entity accessors
    # ------------------------------------------------------------------

    def index_for(self, field: str) -> CatalogIndex:
        """Return the index configured for the given field, or raise KeyError."""
        try:
            return self._indexes[field]
        except KeyError as err:
            raise KeyError(f"No index found for field {field!r}") from err

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def save(self, url: str | Path, *, builder: str | None = None) -> None:
        """Save a catalog snapshot to a URL or bare path."""
        self._ensure_built("saved")
        parsed = parse_catalog_url(str(url))
        if parsed.scheme == "file":
            _save_file(self, parsed.root, parsed.sub, builder=builder)
        elif parsed.scheme == "hf":
            # Serialize here (the Catalog owns its own format), then hand the
            # staged directory to the transport — remote.py works in paths, not
            # Catalogs, so it carries no dependency back on this module.
            with tempfile.TemporaryDirectory() as tmpdir:
                staging = Path(tmpdir) / "snapshot"
                self._save_to_path(staging, builder=builder)
                _upload_hf(staging, parsed.root, parsed.sub)
        else:
            raise ValueError(f"Unsupported catalog URL scheme {parsed.scheme!r}. Supported: ['file', 'hf']")

    def _save_to_path(self, path: Path, *, builder: str | None = None) -> None:
        """Atomically write a portable catalog snapshot to a local directory."""
        import os
        import uuid

        target = Path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        tmp = target.with_name(f"{target.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp")
        if tmp.exists():
            shutil.rmtree(tmp)
        try:
            tmp.mkdir(parents=True)
            if self._backend is not None:
                shutil.copy2(self._backend.path, tmp / self._rows_filename())
            else:
                self._write_parquet(tmp / ENTRIES_FILENAME)
            self._write_indexes(tmp / INDEXES_DIRNAME)
            self._write_meta(tmp, builder)
            if target.exists():
                shutil.rmtree(target)
            tmp.rename(target)
        except Exception:
            if tmp.exists():
                shutil.rmtree(tmp, ignore_errors=True)
            raise

    @classmethod
    def load(cls, url: str | Path) -> Catalog:
        """Load a built, searchable catalog snapshot from a URL or bare path.

        Supports local file paths (bare or ``file://``) and remote Hugging Face
        dataset URLs (``hf://``). Caching loaded catalogs is the caller's
        responsibility.
        """
        return cls._load_from_path(resolve_catalog_dir(str(url)))

    @classmethod
    def _load_from_path(cls, path: Path) -> Catalog:
        src = Path(path)
        meta = read_meta(src)
        if meta.schema_version != 1:
            raise ValueError(f"Unsupported catalog schema_version {meta.schema_version}; expected 1")

        if meta.build.content_sha256:
            actual_sha = _compute_content_sha256(src)
            if meta.build.content_sha256 != actual_sha:
                raise ValueError(
                    f"Catalog snapshot integrity check failed for {src}:\n"
                    f"  expected sha256: {meta.build.content_sha256}\n"
                    f"  actual sha256:   {actual_sha}\n"
                    f"  A stale local copy? Clear it with "
                    f"`parsimony cache clear --subdir catalogs --yes` and retry.\n"
                    f"  If the mismatch persists on a fresh download, the published snapshot "
                    f"itself is corrupt — report it to the catalog's maintainers."
                )

        meta = validate_catalog_snapshot(src, meta=meta)

        backend_config = CatalogBackendConfig(
            kind=meta.backend.kind,
            rows_path=meta.backend.rows_filename,
            namespace=meta.backend.namespace,
            code_column=meta.backend.code_column,
            title_column=meta.backend.title_column,
        )
        indexes = _load_indexes(src / INDEXES_DIRNAME, index_fields=meta.index_fields)

        catalog = cls(
            meta.name,
            indexes=indexes,
            field_links=dict(meta.backend.field_links),
        )
        catalog._default_index_policy = False
        catalog._backend_config = backend_config

        if backend_config.kind == "parquet":
            rows_name = backend_config.rows_path or ENTRIES_FILENAME
            catalog._backend = ParquetRowBackend(src / rows_name)
        else:
            entries = _read_parquet(src / ENTRIES_FILENAME)
            catalog._entities = entries
            catalog._key_to_idx = {(entry.namespace, entry.code): i for i, entry in enumerate(entries)}

        catalog._dirty = False
        return catalog

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _resolve_default_field(self) -> str | None:
        """Broad (no-``field=``) search targets the ``title`` index, by convention."""
        return "title" if "title" in self._indexes else None

    def _invalidate(self) -> None:
        self._dirty = True

    def _ensure_built(self, action: str) -> None:
        if self._dirty:
            raise ValueError(f"Catalog entries or indexes changed — call catalog.build() before it can be {action}")

    def _upsert_entities(self, entries: Iterable[Entity]) -> None:
        for entry in entries:
            key = (entry.namespace, entry.code)
            if key in self._key_to_idx:
                self._entities[self._key_to_idx[key]] = entry
            else:
                self._key_to_idx[key] = len(self._entities)
                self._entities.append(entry)

    def _rebuild_indices(self) -> None:
        vector_cache: dict[tuple[str, int, bool], dict[str, np.ndarray]] = {}
        for field, index in self._indexes.items():
            ctx = IndexBuildContext(field=field, vector_cache=vector_cache)
            index.build(self._entities, ctx=ctx)

    def _write_parquet(self, target: Path) -> None:
        if not self._entities:
            schema = pa.schema(
                [
                    ("namespace", pa.string()),
                    ("code", pa.string()),
                    ("title", pa.string()),
                    ("metadata_json", pa.string()),
                ]
            )
            pq.write_table(pa.Table.from_pylist([], schema=schema), target)
            return
        rows = [
            # default=str so a non-JSON-native metadata value (datetime, Decimal,
            # …) coerces to its string form instead of crashing the whole snapshot
            # save (matches Provenance.safe_dump).
            {
                "namespace": e.namespace,
                "code": e.code,
                "title": e.title,
                "metadata_json": json.dumps(e.metadata, default=str),
            }
            for e in self._entities
        ]
        pq.write_table(pa.Table.from_pylist(rows), target, compression="zstd")

    def _write_indexes(self, target: Path) -> None:
        target.mkdir(parents=True, exist_ok=True)
        for field, index in self._indexes.items():
            try:
                _save_index(index, target / field)
            except TypeError as err:
                raise TypeError(f"Catalog index for field {field!r} is runtime-only and cannot be serialized") from err

    def _rows_filename(self) -> str:
        """Snapshot filename for the row data; memory snapshots always use ``entries.parquet``."""
        if self._backend is None:
            return ENTRIES_FILENAME
        return self._backend_config.rows_path or ENTRIES_FILENAME

    def _write_meta(self, target: Path, builder: str | None) -> None:
        content_sha = _compute_content_sha256(target)
        if self._backend is not None:
            row_count = self._backend.count()
            namespaces = [self._backend_config.namespace or self.name]
        else:
            row_count = len(self._entities)
            namespaces = sorted({entry.namespace for entry in self._entities})
        meta = CatalogMeta(
            name=self.name,
            namespaces=namespaces,
            entry_count=row_count,
            index_fields={field: index.kind for field, index in self._indexes.items()},
            # Frozen schema-v1 key: parsed and carried through the manifest digest,
            # no longer consulted at runtime (broad search targets title by convention).
            default_field=None,
            backend=BackendMeta(
                # Derived from backend presence, not from the config, so the
                # persisted kind can never disagree with the actual payload.
                kind="parquet" if self._backend is not None else "memory",
                rows_filename=self._rows_filename(),
                namespace=self._backend_config.namespace,
                code_column=self._backend_config.code_column,
                title_column=self._backend_config.title_column,
                field_links=dict(self._field_links),
            ),
            build=BuildInfo(builder=builder, content_sha256=content_sha),
        )
        meta.build.manifest_contract_sha256 = compute_manifest_contract_sha256(meta)
        (target / META_FILENAME).write_text(meta.model_dump_json(indent=2))


def _cell_text(cell: Any) -> str:
    """The scored text of one row cell (``None`` never matches an indexed value)."""
    return "" if cell is None else str(cell)


# ---------------------------------------------------------------------------
# Index loading
# ---------------------------------------------------------------------------


def _load_indexes(path: Path, *, index_fields: dict[str, str]) -> dict[str, CatalogIndex]:
    if not path.exists():
        raise FileNotFoundError(f"Catalog snapshot missing indexes directory: {path}")
    loaded: dict[str, CatalogIndex] = {}
    for field, kind in index_fields.items():
        index = _load_index(path / field)
        if index.kind != kind:
            raise ValueError(f"Catalog index for field {field!r} has kind {index.kind!r}, expected {kind!r}")
        loaded[field] = index
    return loaded


# ---------------------------------------------------------------------------
# Local snapshot save (hf:// transport lives in catalog.remote)
# ---------------------------------------------------------------------------


def _save_file(catalog: Catalog, root: str, sub: str, *, builder: str | None = None) -> None:
    target = Path(root) / sub if sub else Path(root)
    catalog._save_to_path(target, builder=builder)
