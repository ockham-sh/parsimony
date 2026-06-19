"""Catalog runtime: entries, search, and snapshot persistence."""

from __future__ import annotations

import json
import logging
import shutil
import tempfile
import threading
from collections.abc import Iterable, Sequence
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from parsimony.catalog.backends import (
    InMemoryRowBackend,
    ParquetRowBackend,
    entity_from_row,
    entity_matches_filter,
)
from parsimony.catalog.contracts import CatalogBackendConfig, FilterSpec
from parsimony.catalog.indexes import (
    BM25Index,
    CatalogIndex,
    DisMaxIndex,
    HybridIndex,
    IndexBuildContext,
    VectorIndex,
    embed_query_vectors,
    search_index_values,
)
from parsimony.catalog.models import (
    BroadSearchConfigError,
    BroadSearchUnavailableError,
    CatalogMatch,
    CatalogValueMatch,
    UnknownIndexedFieldError,
    catalog_match_from_entity,
)
from parsimony.catalog.query import parse_query
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
from parsimony.catalog.urls import REPO_TYPE, parse_catalog_url
from parsimony.catalog.validation import compute_manifest_contract_sha256, validate_catalog_snapshot
from parsimony.entity import Entity, entity_key, field_value, normalize_namespace
from parsimony.errors import InvalidParameterError
from parsimony.ranking import Ranking, ranking_from_scores

logger = logging.getLogger(__name__)


def _filter_namespaces(ranking: Ranking, namespaces: list[str]) -> Ranking:
    allowed = {normalize_namespace(ns) for ns in namespaces}
    rows = [(item.namespace, item.code, item.score) for item in ranking.items if item.namespace in allowed]
    return ranking_from_scores(rows, limit=len(rows))


def _default_indexes_from_entities(entries: list[Entity]) -> dict[str, CatalogIndex]:
    """BM25 indexes for code, title, and every metadata key observed in *entries*."""
    meta_keys: set[str] = set()
    for entry in entries:
        meta_keys.update(entry.metadata.keys())
    fields = ["code", "title", *sorted(meta_keys)]
    return {field: BM25Index() for field in fields}


def _starter_default_indexes() -> dict[str, CatalogIndex]:
    """Placeholder until :meth:`Catalog.build` materializes defaults from entries."""
    return {"title": BM25Index()}


class Catalog:
    """Portable catalog over entries and configured indexes.

    Pass ``indexes=None`` to use the framework default index policy: at
    :meth:`build`, BM25 indexes are created for ``code``, ``title``, and each
    metadata key present on the catalog entries. Pass an explicit ``indexes``
    dict to take full control — no extra indexes are added silently.

    Keys in ``indexes`` are *logical search-surface names*: they appear in the
    DSL (``FIELD: value``) and in ``UnknownIndexedFieldError``. By convention
    they match an Entity field name when an index reads exactly one Entity
    field. Composite indexes such as :class:`~parsimony.catalog.indexes.DisMaxIndex`
    may expose one surface name while reading multiple Entity fields internally.

    For large datasets, call :meth:`attach_parquet_rows` after :meth:`build`
    to switch the row store to a lazy parquet backend; the index entities act
    purely as the scoring surface, and actual rows are streamed on demand.
    """

    def __init__(
        self,
        name: str,
        *,
        indexes: dict[str, CatalogIndex] | None = None,
        default_field: str | None = None,
        field_links: dict[str, str] | None = None,
        backend: CatalogBackendConfig | None = None,
    ) -> None:
        self.name = normalize_namespace(name)
        self.default_field = default_field
        self._field_links = dict(field_links or {})
        self._backend_config = backend or CatalogBackendConfig()
        self._default_index_policy = indexes is None
        self._indexes: dict[str, CatalogIndex] = dict(indexes) if indexes is not None else _starter_default_indexes()
        self._entities: list[Entity] = []
        self._key_to_idx: dict[tuple[str, str], int] = {}
        self._backend: InMemoryRowBackend | ParquetRowBackend | None = None
        self._parquet_source: Path | None = None
        self._dirty = True
        # Set by ``load_entities_only``: indexes were never loaded, so ``search``
        # is unavailable and the catalog supports listing ``entities`` only.
        self._entities_only = False
        self._lock = threading.Lock()
        self._validate_indexes()

    # ------------------------------------------------------------------
    # Public properties
    # ------------------------------------------------------------------

    @property
    def entities(self) -> list[Entity]:
        return list(self._entities)

    @property
    def indexes(self) -> dict[str, CatalogIndex]:
        return dict(self._indexes)

    @property
    def field_links(self) -> dict[str, str]:
        return dict(self._field_links)

    @property
    def is_parquet_backend(self) -> bool:
        return self._backend_config.kind == "parquet"

    def __len__(self) -> int:
        return len(self._entities) if not self.is_parquet_backend else (self._backend.count() if self._backend else 0)

    # ------------------------------------------------------------------
    # Mutation
    # ------------------------------------------------------------------

    def set_entities(self, entries: list[Entity]) -> None:
        """Replace entries without rebuilding indexes."""
        self._entities = []
        self._key_to_idx = {}
        self._upsert_entities(entries)
        self._invalidate()

    def set_index(self, field: str, index: CatalogIndex) -> None:
        """Replace one field index without rebuilding."""
        self._indexes[field] = index
        self._default_index_policy = False
        self._validate_indexes()
        self._invalidate()

    def update_indexes(self, indexes: dict[str, CatalogIndex]) -> None:
        """Merge or replace field indexes without rebuilding."""
        self._indexes.update(indexes)
        self._default_index_policy = False
        self._validate_indexes()
        self._invalidate()

    def set_indexes(self, indexes: dict[str, CatalogIndex]) -> None:
        """Replace all field indexes without rebuilding."""
        self._indexes = dict(indexes)
        self._default_index_policy = False
        self._validate_indexes()
        self._invalidate()

    def set_field_links(self, field_links: dict[str, str]) -> None:
        self._field_links = dict(field_links)

    def attach_parquet_rows(self, parquet_path: Path, *, config: CatalogBackendConfig) -> None:
        """Bind a flat parquet file as the row backend.

        Indexes must be built first (call :meth:`build`). After attaching, all
        searches stream rows from *parquet_path* instead of from in-memory entities.
        """
        self._backend_config = config
        self._parquet_source = Path(parquet_path)
        self._backend = ParquetRowBackend(self._parquet_source, config=config)
        self._entities = []
        self._key_to_idx = {}
        self._dirty = False

    def build(self) -> None:
        """Build configured indexes over the catalog's current entries."""
        if self._default_index_policy:
            self._indexes = _default_indexes_from_entities(self._entities)

        if self.default_field is not None:
            try:
                self.index_for(self.default_field)
            except KeyError as err:
                indexed = ", ".join(f"'{f}'" for f in self._indexed_fields())
                raise BroadSearchConfigError(
                    f"No index configured for default_field {self.default_field!r}. Indexed fields: [{indexed}]"
                ) from err

        with self._lock:
            self._rebuild_indices()
            if self._backend_config.kind == "memory":
                self._backend = InMemoryRowBackend(self._entities, config=self._backend_config)
            self._dirty = False

    def get(self, namespace: str, code: str) -> Entity | None:
        idx = self._key_to_idx.get(entity_key(namespace, code))
        return self._entities[idx] if idx is not None else None

    def delete_many(self, keys: Iterable[tuple[str, str]]) -> int:
        with self._lock:
            targets: set[int] = set()
            for ns, code in keys:
                idx = self._key_to_idx.get(entity_key(ns, code))
                if idx is not None:
                    targets.add(idx)
            if not targets:
                return 0
            self._entities = [entry for i, entry in enumerate(self._entities) if i not in targets]
            self._key_to_idx = {(entry.namespace, entry.code): i for i, entry in enumerate(self._entities)}
            self._invalidate()
            return len(targets)

    # ------------------------------------------------------------------
    # Search
    # ------------------------------------------------------------------

    def search(
        self,
        query: str | None = None,
        limit: int = 50,
        *,
        field: str | None = None,
        filter: FilterSpec | None = None,
        top_k_values: int = 50,
        namespaces: list[str] | None = None,
    ) -> list[CatalogMatch]:
        """Search catalog rows.

        Parameters
        ----------
        query:
            Free-text or structured ``FIELD: value`` query.  Omit for filter-only search.
        limit:
            Maximum results returned.
        field:
            Restrict soft scoring to this indexed field.  Requires *query*.
        filter:
            Exact AND filter: ``{column: [allowed_values, …]}``.  Can combine with query.
        top_k_values:
            How many distinct indexed values to score for parquet backends.
        namespaces:
            Post-filter to these entity namespaces.
        """
        if self._entities_only:
            raise BroadSearchUnavailableError(
                "Catalog was loaded entities-only (no search indexes) and cannot be searched. "
                "Reload it with Catalog.load() for search; entities-only supports listing entities."
            )
        self._ensure_built("searched")
        filter_spec = dict(filter or {})

        if query is None and not filter_spec:
            raise InvalidParameterError("catalog", "search requires query= and/or filter=")
        if field is not None and query is None:
            raise InvalidParameterError("catalog", "field= requires a non-empty query=")
        if field is not None:
            try:
                self.index_for(field)
            except KeyError as err:
                raise UnknownIndexedFieldError(f"No index configured for field {field!r}") from err

        # Resolve structured DSL queries ("FIELD: v1, v2") when no explicit field
        multi_values: Sequence[str] | None = None
        if query is not None and field is None:
            parsed = parse_query(query, known_fields=set(self._indexes))
            if parsed is not None:
                if len(parsed.clauses) > 1:
                    raise InvalidParameterError(
                        "catalog",
                        "Multi-clause structured queries are not supported. "
                        "Use filter= for exact constraints and field= for single-field soft search.",
                    )
                field, multi_values = parsed.clauses[0]
                query = None  # consumed by DSL

        filter_spec = self._normalize_filter_spec(filter_spec)

        # Dispatch
        if field is not None:
            values: Sequence[str] = multi_values if multi_values is not None else (query,)  # type: ignore[assignment]
            matches = self._search_field(
                field, values, filter_spec=filter_spec or None, limit=limit, top_k_values=top_k_values
            )
        elif query is not None:
            broad_field = self._resolve_default_field()
            if broad_field is None:
                raise BroadSearchUnavailableError(
                    f"This catalog requires field= or filter=. "
                    f"Indexed fields: [{', '.join(repr(f) for f in sorted(self._indexes))}]"
                )
            matches = self._search_field(
                broad_field, (query,), filter_spec=filter_spec or None, limit=limit, top_k_values=top_k_values
            )
        else:
            matches = self._search_filter_only(filter_spec, limit=limit)

        if namespaces is not None:
            allowed = {normalize_namespace(ns) for ns in namespaces}
            matches = [m for m in matches if m.namespace in allowed]

        return matches

    def search_values(
        self,
        query: str,
        field: str,
        *,
        limit: int = 20,
    ) -> list[CatalogValueMatch]:
        """Return distinct indexed values for *field* ranked by *query*.

        If *field* is in ``field_links``, each result also carries the linked value
        (e.g. the canonical code for a human-readable label).
        """
        self._ensure_built("searched")
        try:
            index = self.index_for(field)
        except KeyError as err:
            raise UnknownIndexedFieldError(f"No index configured for field {field!r}") from err
        qvecs = embed_query_vectors(query, [index])
        scored = search_index_values(index, query, limit=limit, query_vectors=qvecs)
        link_field = self._field_links.get(field)
        if link_field is None:
            return [CatalogValueMatch(value=text, score=score) for text, score in scored]
        return [
            CatalogValueMatch(value=text, score=score, linked_value=self._linked_value(field, text, link_field))
            for text, score in scored
        ]

    # ------------------------------------------------------------------
    # Internal search helpers
    # ------------------------------------------------------------------

    def _normalize_filter_spec(self, filter_spec: dict[str, Sequence[str]]) -> dict[str, Sequence[str]]:
        if not filter_spec or not isinstance(self._backend, ParquetRowBackend):
            return filter_spec
        columns = set(self._backend.column_names())
        aliases = {
            "code": self._backend_config.code_column,
            "title": self._backend_config.title_column,
        }
        normalized: dict[str, Sequence[str]] = {}
        for field, values in filter_spec.items():
            column = aliases.get(field, field)
            if column not in columns:
                raise InvalidParameterError(
                    "catalog",
                    f"Unknown parquet filter column {field!r}. Available columns: {sorted(columns)}",
                )
            normalized[column] = values
        return normalized

    def _search_field(
        self,
        field: str,
        values: Sequence[str],
        *,
        filter_spec: FilterSpec | None,
        limit: int,
        top_k_values: int,
    ) -> list[CatalogMatch]:
        """Score rows by field index over *values* (multi-value → take max per row)."""
        index = self.index_for(field)
        if isinstance(self._backend, ParquetRowBackend):
            return self._parquet_field_search(
                index, field, values, filter_spec=filter_spec, limit=limit, top_k_values=top_k_values
            )
        return self._memory_field_search(index, values, filter_spec=filter_spec, limit=limit)

    def _memory_field_search(
        self,
        index: CatalogIndex,
        values: Sequence[str],
        *,
        filter_spec: FilterSpec | None,
        limit: int,
    ) -> list[CatalogMatch]:
        merged: dict[int, float] = {}
        for value in values:
            qvecs = embed_query_vectors(value, [index])
            for row_id, score in index.score_candidates(value, query_vectors=qvecs).items():
                if score <= 0.0:
                    continue
                if filter_spec is not None and not entity_matches_filter(self._entities[row_id], filter_spec):
                    continue
                if score > merged.get(row_id, 0.0):
                    merged[row_id] = score
        ranking = ranking_from_scores(
            [(self._entities[i].namespace, self._entities[i].code, s) for i, s in merged.items()],
            limit=limit,
        )
        return self._matches_from_ranking(ranking)

    def _parquet_field_search(
        self,
        index: CatalogIndex,
        field: str,
        values: Sequence[str],
        *,
        filter_spec: FilterSpec | None,
        limit: int,
        top_k_values: int,
    ) -> list[CatalogMatch]:
        backend = self._require_parquet_backend()
        value_scores: dict[str, float] = {}
        for value in values:
            qvecs = embed_query_vectors(value, [index])
            for text, score in search_index_values(index, value, limit=top_k_values, query_vectors=qvecs):
                if score > value_scores.get(text, 0.0):
                    value_scores[text] = score
        ranked = backend.top_scored_rows(
            filter_spec=filter_spec,
            score_column=field,
            value_scores=value_scores,
            limit=limit,
        )
        return [
            catalog_match_from_entity(entity_from_row(row, config=self._backend_config), score=score)
            for row, score in ranked
        ]

    def _search_filter_only(self, filter_spec: FilterSpec, *, limit: int) -> list[CatalogMatch]:
        if isinstance(self._backend, ParquetRowBackend):
            backend = self._require_parquet_backend()
            matches: list[CatalogMatch] = []
            for row in backend.iter_rows(filter_spec=filter_spec):
                matches.append(catalog_match_from_entity(entity_from_row(row, config=self._backend_config), score=1.0))
                if len(matches) >= limit:
                    break
            return matches
        return [
            catalog_match_from_entity(e, score=1.0) for e in self._entities if entity_matches_filter(e, filter_spec)
        ][:limit]

    def _linked_value(self, field: str, value: str, link_field: str) -> str | None:
        """Resolve the linked field value for a given indexed value."""
        if isinstance(self._backend, ParquetRowBackend):
            # Scan parquet: find first row where field==value and return link_field
            for row in self._backend.iter_rows(filter_spec={field: [value]}, columns=[link_field]):
                lv = row.get(link_field)
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
            _save_hf(self, parsed.root, parsed.sub, builder=builder)
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
            rows_name = self._backend_config.rows_path or ENTRIES_FILENAME
            if self.is_parquet_backend:
                source = self._parquet_source
                if source is None or not source.is_file():
                    raise ValueError("Parquet catalog save requires attach_parquet_rows() with a readable file")
                shutil.copy2(source, tmp / rows_name)
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
        return _dispatch_load(str(url))

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
                    f"  If this is a cached copy, clear it with "
                    f"`parsimony cache clear --subdir catalogs --yes` and retry."
                )

        meta = validate_catalog_snapshot(src, meta=meta)

        backend_config = CatalogBackendConfig(
            kind=meta.backend.kind,
            rows_path=meta.backend.rows_filename,
            namespace=meta.backend.namespace,
            code_column=meta.backend.code_column,
            title_column=meta.backend.title_column,
            field_links=dict(meta.backend.field_links),
        )
        indexes = _load_indexes(src / INDEXES_DIRNAME, index_fields=meta.index_fields)

        catalog = cls(
            meta.name,
            indexes=indexes,
            default_field=meta.default_field,
            field_links=backend_config.field_links,
            backend=backend_config,
        )
        catalog._default_index_policy = False

        if backend_config.kind == "parquet":
            rows_name = backend_config.rows_path or ENTRIES_FILENAME
            catalog._backend = ParquetRowBackend(src / rows_name, config=backend_config)
            catalog._entities = []
            catalog._key_to_idx = {}
        else:
            entries = _read_parquet(src / ENTRIES_FILENAME)
            catalog._entities = entries
            catalog._key_to_idx = {(entry.namespace, entry.code): i for i, entry in enumerate(entries)}
            catalog._backend = InMemoryRowBackend(entries, config=backend_config)

        catalog._dirty = False
        return catalog

    @classmethod
    def load_entities_only(cls, url: str | Path) -> Catalog:
        """Load only a catalog's entities — no search indexes, no integrity hash.

        Reads just the entries parquet (plus ``meta.json`` for the name), skipping
        ``_load_indexes`` (the FAISS vector read and embedder hydration) and the
        whole-dir content-SHA verification. The result can iterate and page
        :attr:`entities` but :meth:`search` raises. Use for browse/list flows where
        the vector index is dead weight — e.g. listing a small codelist to pick a
        code by hand. For ``hf://`` sources only the entries + meta files are
        fetched, not the (often large) ``vectors.faiss``.

        The integrity digest is computed over the whole snapshot dir including the
        index files this path deliberately does not read, so it cannot be verified
        here — hence it is skipped (logged at debug). Only reach for this when the
        caller is going to list rather than search.
        """
        return _dispatch_load(str(url), entities_only=True)

    @classmethod
    def _load_entities_only_from_path(cls, path: Path) -> Catalog:
        """Build an entities-only catalog from a local snapshot dir (no indexes, no SHA)."""
        src = Path(path)
        meta = read_meta(src)
        if meta.schema_version != 1:
            raise ValueError(f"Unsupported catalog schema_version {meta.schema_version}; expected 1")
        logger.debug("Loading entities-only catalog from %s (indexes + integrity check skipped)", src)
        rows_name = meta.backend.rows_filename or ENTRIES_FILENAME
        entries = _read_parquet(src / rows_name)
        catalog = cls(meta.name, indexes={}, default_field=None)
        catalog._default_index_policy = False
        catalog._entities = entries
        catalog._key_to_idx = {(entry.namespace, entry.code): i for i, entry in enumerate(entries)}
        catalog._dirty = False
        catalog._entities_only = True
        return catalog

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _resolve_default_field(self) -> str | None:
        if self.default_field is not None:
            return self.default_field
        if "title" in self._indexes:
            return "title"
        return None

    def _indexed_fields(self) -> list[str]:
        return sorted(self._indexes)

    def _validate_indexes(self) -> None:
        missing_default = (
            self.default_field is not None
            and self.default_field not in self._indexes
            and not self._default_index_policy
        )
        if missing_default:
            indexed = ", ".join(f"'{f}'" for f in sorted(self._indexes))
            raise BroadSearchConfigError(
                f"No index configured for default_field {self.default_field!r}. Indexed fields: [{indexed}]"
            )

    def _invalidate(self) -> None:
        self._dirty = True

    def _ensure_built(self, action: str) -> None:
        if self._dirty:
            raise ValueError(f"Catalog entries or indexes changed — call catalog.build() before it can be {action}")

    def _upsert_entities(self, entries: list[Entity]) -> None:
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

    def _matches_from_ranking(self, ranking: Ranking) -> list[CatalogMatch]:
        return [
            catalog_match_from_entity(self._entities[idx], score=float(row.score))
            for row in ranking.items
            if (idx := self._key_to_idx.get((row.namespace, row.code))) is not None
        ]

    def _require_parquet_backend(self) -> ParquetRowBackend:
        if not isinstance(self._backend, ParquetRowBackend):
            raise ValueError("Catalog parquet backend is not configured")
        return self._backend

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
            if isinstance(index, (VectorIndex, BM25Index, HybridIndex, DisMaxIndex)):
                index.save(target / field)
            else:
                raise TypeError(f"Catalog index for field {field!r} is runtime-only and cannot be serialized")

    def _write_meta(self, target: Path, builder: str | None) -> None:
        content_sha = _compute_content_sha256(target)
        row_count = self._backend.count() if self._backend is not None else len(self._entities)
        if self.is_parquet_backend:
            namespaces = [self._backend_config.namespace or self.name]
        else:
            namespaces = sorted({entry.namespace for entry in self._entities})
        meta = CatalogMeta(
            name=self.name,
            namespaces=namespaces,
            entry_count=row_count,
            index_fields={field: index.kind for field, index in self._indexes.items()},
            default_field=self.default_field,
            backend=BackendMeta(
                kind=self._backend_config.kind,
                rows_filename=self._backend_config.rows_path or ENTRIES_FILENAME,
                namespace=self._backend_config.namespace,
                code_column=self._backend_config.code_column,
                title_column=self._backend_config.title_column,
                field_links=dict(self._field_links),
            ),
            build=BuildInfo(builder=builder, content_sha256=content_sha),
        )
        meta.build.manifest_contract_sha256 = compute_manifest_contract_sha256(meta)
        (target / META_FILENAME).write_text(meta.model_dump_json(indent=2))


# ---------------------------------------------------------------------------
# Index loading
# ---------------------------------------------------------------------------


def _load_indexes(path: Path, *, index_fields: dict[str, str]) -> dict[str, CatalogIndex]:
    if not path.exists():
        raise FileNotFoundError(f"Catalog snapshot missing indexes directory: {path}")
    indexes: dict[str, CatalogIndex] = {}
    for field, kind in index_fields.items():
        field_path = path / field
        if kind == "vector":
            indexes[field] = VectorIndex.load(field_path)
        elif kind == "bm25":
            indexes[field] = BM25Index.load(field_path)
        elif kind == "hybrid":
            indexes[field] = HybridIndex.load(field_path)
        elif kind == "dis_max":
            indexes[field] = DisMaxIndex.load(field_path)
        else:
            raise ValueError(f"Unsupported catalog index kind {kind!r} for field {field!r}")
    return indexes


# ---------------------------------------------------------------------------
# Snapshot load/save dispatch
# ---------------------------------------------------------------------------


def _load_file(root: str, sub: str, *, entities_only: bool = False) -> Catalog:
    path = Path(root) / sub if sub else Path(root)
    if not path.exists():
        raise FileNotFoundError(f"Catalog directory does not exist: {path}")
    if entities_only:
        return Catalog._load_entities_only_from_path(path)
    return Catalog._load_from_path(path)


def _save_file(catalog: Catalog, root: str, sub: str, *, builder: str | None = None) -> None:
    target = Path(root) / sub if sub else Path(root)
    catalog._save_to_path(target, builder=builder)


def _load_hf(root: str, sub: str, *, revision: str | None = None, entities_only: bool = False) -> Catalog:
    from huggingface_hub import snapshot_download

    from parsimony import cache

    cache_dir = cache.catalogs_dir()
    # Entities-only fetches just the entries + manifest, skipping the (often large)
    # vector index, so browse/list loads neither download nor read it.
    patterns: list[str] | None
    if entities_only:
        only = [META_FILENAME, ENTRIES_FILENAME]
        patterns = [f"{sub}/{name}" for name in only] if sub else only
    else:
        patterns = [f"{sub}/*"] if sub else None
    if sub:
        local = Path(
            snapshot_download(
                repo_id=root,
                repo_type=REPO_TYPE,
                revision=revision,
                cache_dir=cache_dir,
                allow_patterns=patterns,
            )
        )
        target = local / sub
    else:
        local = Path(
            snapshot_download(
                repo_id=root,
                repo_type=REPO_TYPE,
                revision=revision,
                cache_dir=cache_dir,
                allow_patterns=patterns,
            )
        )
        target = local
    if entities_only:
        return Catalog._load_entities_only_from_path(target)
    return Catalog._load_from_path(target)


def _save_hf(catalog: Catalog, root: str, sub: str, *, builder: str | None = None) -> None:
    from huggingface_hub import HfApi

    with tempfile.TemporaryDirectory() as tmpdir:
        staging = Path(tmpdir) / "snapshot"
        catalog._save_to_path(staging, builder=builder)
        api = HfApi()
        api.create_repo(repo_id=root, repo_type=REPO_TYPE, exist_ok=True)
        api.upload_folder(
            folder_path=str(staging),
            repo_id=root,
            repo_type=REPO_TYPE,
            path_in_repo=sub or None,
        )


def _dispatch_load(url: str, *, entities_only: bool = False) -> Catalog:
    parsed = parse_catalog_url(url)
    if parsed.scheme == "file":
        return _load_file(parsed.root, parsed.sub, entities_only=entities_only)
    if parsed.scheme == "hf":
        return _load_hf(parsed.root, parsed.sub, revision=parsed.revision, entities_only=entities_only)
    raise ValueError(f"Unsupported catalog URL scheme {parsed.scheme!r}. Supported: ['file', 'hf']")
