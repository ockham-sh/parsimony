"""Catalog runtime: entries, search, and snapshot persistence."""

from __future__ import annotations

import json
import shutil
import tempfile
import threading
from collections.abc import Callable, Iterable, Sequence
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from parsimony.catalog.backends import (
    ParquetRowBackend,
    entity_from_row,
    entity_matches_filter,
)
from parsimony.catalog.contracts import CatalogBackendConfig, FilterSpec
from parsimony.catalog.indexes import (
    BM25Index,
    CatalogIndex,
    HybridIndex,
    IndexBuildContext,
    VectorIndex,
    consumed_value_tokens,
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
from parsimony.entity import Entity, entity_key, field_value, field_values, normalize_namespace
from parsimony.errors import InvalidParameterError
from parsimony.indexes import tokenize


def _default_indexes_from_entities(entries: list[Entity]) -> dict[str, CatalogIndex]:
    """BM25 indexes for code, title, and every metadata key observed in *entries*."""
    meta_keys: set[str] = set()
    for entry in entries:
        meta_keys.update(entry.metadata.keys())
    fields = ["code", "title", *sorted(meta_keys)]
    return {field: BM25Index() for field in fields}


class Catalog:
    """Portable catalog over entries and configured indexes.

    Pass ``indexes=None`` to use the framework default index policy: at
    :meth:`build`, BM25 indexes are created for ``code``, ``title``, and each
    metadata key present on the catalog entries. Pass an explicit ``indexes``
    dict to take full control — no extra indexes are added silently.

    Keys in ``indexes`` are *logical search-surface names*: they appear in the
    DSL (``FIELD: value``) and in ``UnknownIndexedFieldError``. They match the
    Entity field name each index reads. Multi-field scoring is a query-time
    concern — pass ``fields=[...]`` to :meth:`search`.

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
        default_field: str | None = None,
        field_links: dict[str, str] | None = None,
        backend: CatalogBackendConfig | None = None,
    ) -> None:
        self.name = normalize_namespace(name)
        self.default_field = default_field
        self._field_links = dict(field_links or {})
        self._backend_config = backend or CatalogBackendConfig()
        self._default_index_policy = indexes is None
        self._indexes: dict[str, CatalogIndex] = dict(indexes) if indexes is not None else {}
        self._entities: list[Entity] = []
        self._key_to_idx: dict[tuple[str, str], int] = {}
        self._backend: ParquetRowBackend | None = None
        self._dirty = True
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
    def is_parquet_backend(self) -> bool:
        return self._backend is not None

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
        self._validate_indexes()
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
        self._backend = ParquetRowBackend(Path(parquet_path))
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
        fields: str | Sequence[str] | None = None,
        filter: FilterSpec | None = None,
        top_k_values: int = 50,
        namespaces: list[str] | None = None,
    ) -> list[CatalogMatch]:
        """Search catalog rows, ranked by (coverage desc, fuzzy score desc).

        *Coverage* is the fraction of the query's tokens consumed by the union
        of the row's fully-consumed field values (a field value counts only when
        all its tokens appear in the query). An exact value hit is coverage 1.0
        and ranks first; near-misses stay visible below in the fuzzy band. When
        no field value is contained in the query, the ranking is pure fuzzy.

        Parameters
        ----------
        query:
            Free-text or structured ``FIELD: value`` query.  Omit for filter-only search.
        limit:
            Maximum results returned.
        fields:
            Declare the scoring surface: one indexed field name for single-field
            scoring, or several to fuse (fuzzy score is the row's best per-field
            score; coverage unions consumed tokens across the named fields).
            Requires *query*, which is then literal text — never parsed as a
            structured ``FIELD: value`` DSL.  Omit to search the catalog's
            default field with DSL resolution.
        filter:
            Exact AND filter: ``{column: [allowed_values, …]}``.  Can combine with query.
        top_k_values:
            How many distinct indexed values to score for parquet backends.
        namespaces:
            Post-filter to these entity namespaces.
        """
        self._ensure_built("searched")
        filter_spec = dict(filter or {})
        surface: list[str] | None
        if fields is None:
            surface = None
        elif isinstance(fields, str):
            surface = [fields]
        else:
            surface = list(fields)

        if query is None and not filter_spec:
            raise InvalidParameterError("catalog", "search requires query= and/or filter=")
        if surface is not None:
            if query is None:
                raise InvalidParameterError("catalog", "fields= requires a non-empty query=")
            if not surface:
                raise InvalidParameterError("catalog", "fields= must name at least one indexed field")
            for name in surface:
                try:
                    self.index_for(name)
                except KeyError as err:
                    raise UnknownIndexedFieldError(f"No index configured for field {name!r}") from err

        # Resolve structured DSL queries ("FIELD: v1, v2") when no scope is declared.
        # A single-name surface takes the plain single-field path below.
        field: str | None = surface[0] if surface is not None and len(surface) == 1 else None
        multi_values: Sequence[str] | None = None
        if query is not None and surface is None:
            parsed = parse_query(query, known_fields=set(self._indexes))
            if parsed is not None:
                if len(parsed.clauses) > 1:
                    raise InvalidParameterError(
                        "catalog",
                        "Multi-clause structured queries are not supported. "
                        "Use filter= for exact constraints and fields= for soft search.",
                    )
                field, multi_values = parsed.clauses[0]
                query = None  # consumed by DSL

        filter_spec = self._normalize_filter_spec(filter_spec)

        # Dispatch
        if surface is not None and len(surface) > 1:
            assert query is not None  # guaranteed by the fields= validation above
            matches = self._search_fields(
                surface, query, filter_spec=filter_spec or None, limit=limit, top_k_values=top_k_values
            )
        elif field is not None:
            values: Sequence[str] = multi_values if multi_values is not None else (query,)  # type: ignore[assignment]
            matches = self._search_field(
                field, values, filter_spec=filter_spec or None, limit=limit, top_k_values=top_k_values
            )
        elif query is not None:
            broad_field = self._resolve_default_field()
            if broad_field is None:
                raise BroadSearchUnavailableError(
                    f"This catalog requires fields= or filter=. "
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
        """Return distinct indexed values for *field*, ranked by (coverage, score).

        A value fully consumed by *query* (an exact hit is coverage 1.0) ranks
        above every fuzzy-only candidate. If *field* is in ``field_links``, each
        result also carries the linked value (e.g. the canonical code for a
        human-readable label).
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
            return [CatalogValueMatch(value=text, score=score, coverage=cov) for text, score, cov in scored]
        return [
            CatalogValueMatch(
                value=text,
                score=score,
                coverage=cov,
                linked_value=self._linked_value(field, text, link_field),
            )
            for text, score, cov in scored
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

    def _search_fields(
        self,
        fields: list[str],
        query: str,
        *,
        filter_spec: FilterSpec | None,
        limit: int,
        top_k_values: int,
    ) -> list[CatalogMatch]:
        """Multi-field surface search — one pass over candidate rows."""
        return self._scored_search(fields, (query,), filter_spec=filter_spec, limit=limit, top_k_values=top_k_values)

    def _search_field(
        self,
        field: str,
        values: Sequence[str],
        *,
        filter_spec: FilterSpec | None,
        limit: int,
        top_k_values: int,
    ) -> list[CatalogMatch]:
        """Single-surface search (a DSL clause arrives as multiple *values*)."""
        return self._scored_search([field], values, filter_spec=filter_spec, limit=limit, top_k_values=top_k_values)

    def _scored_search(
        self,
        fields: list[str],
        values: Sequence[str],
        *,
        filter_spec: FilterSpec | None,
        limit: int,
        top_k_values: int,
    ) -> list[CatalogMatch]:
        """Rank rows by (coverage desc, score desc) in one backend pass.

        Per query value and field, the index's distinct values are scored once
        (top *top_k_values*, normalized by the field's best score) and the
        fully-consumed values are gated (:func:`consumed_value_tokens`). Every
        row matching any scored or consumed value is then graded in a single
        scan: *coverage* is the fraction of the query's tokens consumed by the
        union of the row's consumed field values; *score* sums the row's
        normalized per-field relevance, so weak agreeing evidence across fields
        accumulates while no single field's raw magnitude can dominate the
        ranking. Multi-value queries (DSL clauses) keep each row's best pair.
        Because candidates come straight from the backend, a deserving row can
        never be lost to a per-field pool truncation.
        """
        plans = self._value_plans(fields, values, top_k_values=top_k_values)
        if not plans:
            return []
        any_of: dict[str, set[str]] = {}
        for _, tables, consumed in plans:
            for column, scored in tables.items():
                any_of.setdefault(column, set()).update(scored)
            for column, gated in consumed.items():
                any_of.setdefault(column, set()).update(gated)
        candidates = {column: sorted(texts) for column, texts in any_of.items() if texts}
        if not candidates:
            return []

        scored_rows: list[tuple[float, float, Entity]] = []
        if isinstance(self._backend, ParquetRowBackend):
            for row in self._backend.iter_rows(filter_spec=filter_spec or None, any_of=candidates):
                row_values = {
                    column: [str(row[column])] if row.get(column) is not None else [] for column in candidates
                }
                coverage, score = _grade_row(row_values, plans)
                if coverage <= 0.0 and score <= 0.0:
                    continue
                scored_rows.append((coverage, score, entity_from_row(row, config=self._backend_config)))
        else:
            columns_by_field = {self._coverage_column(name): name for name in fields}
            for entity in self._entities:
                if filter_spec and not entity_matches_filter(entity, filter_spec):
                    continue
                row_values = {
                    column: [str(v) for v in field_values(entity, name)] for column, name in columns_by_field.items()
                }
                coverage, score = _grade_row(row_values, plans)
                if coverage <= 0.0 and score <= 0.0:
                    continue
                scored_rows.append((coverage, score, entity))

        scored_rows.sort(key=lambda item: (-item[0], -item[1], item[2].namespace, item[2].code))
        return [
            catalog_match_from_entity(entity, score=score, coverage=coverage)
            for coverage, score, entity in scored_rows[:limit]
        ]

    def _value_plans(self, fields: list[str], values: Sequence[str], *, top_k_values: int) -> list[_ValuePlan]:
        """Per query value: (query tokens, normalized score tables, consumed values) by column."""
        plans: list[_ValuePlan] = []
        for value in values:
            if not value or not value.strip():
                continue
            query_vectors = embed_query_vectors(value, [self.index_for(name) for name in fields])
            tables: dict[str, dict[str, float]] = {}
            consumed: dict[str, dict[str, frozenset[str]]] = {}
            for name in fields:
                column = self._coverage_column(name)
                index = self.index_for(name)
                scored = search_index_values(index, value, limit=top_k_values, query_vectors=query_vectors)
                top = max((score for _, score, _ in scored), default=0.0)
                tables[column] = {text: score / top for text, score, _ in scored if top > 0.0 and score > 0.0}
                consumed[column] = consumed_value_tokens(index, value)
            plans.append((frozenset(tokenize(value)), tables, consumed))
        return plans

    def _coverage_column(self, field: str) -> str:
        aliases = {"code": self._backend_config.code_column, "title": self._backend_config.title_column}
        return aliases.get(field, field)

    def _search_filter_only(self, filter_spec: FilterSpec, *, limit: int) -> list[CatalogMatch]:
        if isinstance(self._backend, ParquetRowBackend):
            backend = self._backend
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
            default_field=meta.default_field,
            field_links=dict(meta.backend.field_links),
            backend=backend_config,
        )
        catalog._default_index_policy = False

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
            if isinstance(index, (VectorIndex, BM25Index, HybridIndex)):
                index.save(target / field)
            else:
                raise TypeError(f"Catalog index for field {field!r} is runtime-only and cannot be serialized")

    def _rows_filename(self) -> str:
        """Snapshot filename for the row data; memory snapshots always use ``entries.parquet``."""
        if self._backend is None:
            return ENTRIES_FILENAME
        return self._backend_config.rows_path or ENTRIES_FILENAME

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


#: One query value's scoring plan: (query tokens, per-column normalized score
#: tables, per-column consumed values with their token sets).
_ValuePlan = tuple[frozenset[str], dict[str, dict[str, float]], dict[str, dict[str, frozenset[str]]]]


def _grade_row(row_values: dict[str, list[str]], plans: list[_ValuePlan]) -> tuple[float, float]:
    """Best (coverage, score) of one row across the query's values."""
    best = (0.0, 0.0)
    for qtokens, tables, consumed in plans:
        union: set[str] = set()
        matched = False
        score = 0.0
        for column, texts in row_values.items():
            table = tables.get(column)
            if table and texts:
                score += max((table.get(text, 0.0) for text in texts), default=0.0)
            gated = consumed.get(column)
            if gated:
                for text in texts:
                    tokens = gated.get(text)
                    if tokens is not None:
                        matched = True
                        union.update(tokens)
        # A tokenless query only gathers string-equal values: full coverage.
        coverage = len(union & qtokens) / len(qtokens) if qtokens else (1.0 if matched else 0.0)
        pair = (coverage, score)
        if pair > best:
            best = pair
    return best


# ---------------------------------------------------------------------------
# Index loading
# ---------------------------------------------------------------------------


_INDEX_LOADERS: dict[str, Callable[[Path], CatalogIndex]] = {
    "vector": VectorIndex.load,
    "bm25": BM25Index.load,
    "hybrid": HybridIndex.load,
}


def _load_indexes(path: Path, *, index_fields: dict[str, str]) -> dict[str, CatalogIndex]:
    if not path.exists():
        raise FileNotFoundError(f"Catalog snapshot missing indexes directory: {path}")
    return {field: _INDEX_LOADERS[kind](path / field) for field, kind in index_fields.items()}


# ---------------------------------------------------------------------------
# Local snapshot save (hf:// transport lives in catalog.remote)
# ---------------------------------------------------------------------------


def _save_file(catalog: Catalog, root: str, sub: str, *, builder: str | None = None) -> None:
    target = Path(root) / sub if sub else Path(root)
    catalog._save_to_path(target, builder=builder)
