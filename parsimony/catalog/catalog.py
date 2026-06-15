"""Catalog runtime: entries, search, and snapshot persistence."""

from __future__ import annotations

import json
import shutil
import tempfile
import threading
from collections.abc import Iterable
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from parsimony.catalog.indexes import (
    BM25Index,
    CatalogIndex,
    DisMaxIndex,
    HybridIndex,
    IndexBuildContext,
    VectorIndex,
    _ranking_from_row_scores,
    embed_query_vectors,
)
from parsimony.catalog.models import (
    BroadSearchConfigError,
    BroadSearchUnavailableError,
    CatalogMatch,
    SearchDiagnostic,
    catalog_match_from_entity,
)
from parsimony.catalog.query import StructuredQuery, parse_query
from parsimony.catalog.storage import (
    ENTRIES_FILENAME,
    INDEXES_DIRNAME,
    META_FILENAME,
    BuildInfo,
    CatalogMeta,
    _compute_content_sha256,
    _read_parquet,
    read_meta,
)
from parsimony.catalog.urls import REPO_TYPE, parse_catalog_url
from parsimony.entity import Entity, entity_key, normalize_namespace
from parsimony.ranking import Ranking, ranking_from_scores


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
    """Portable in-memory catalog over entries and configured indexes.

    Pass ``indexes=None`` to use the framework default index policy: at
    :meth:`build`, BM25 indexes are created for ``code``, ``title``, and each
    metadata key present on the catalog entries. Pass an explicit ``indexes``
    dict to take full control — no extra indexes are added silently.

    Keys in ``indexes`` are *logical search-surface names*: they appear in the
    DSL (``FIELD: value``) and in ``UnknownIndexedFieldError``. By convention
    they match an Entity field name when an index reads exactly one Entity
    field. Composite indexes such as :class:`~parsimony.catalog.indexes.DisMaxIndex`
    may expose one surface name while reading multiple Entity fields internally.
    """

    def __init__(
        self,
        name: str,
        *,
        indexes: dict[str, CatalogIndex] | None = None,
        default_field: str | None = None,
    ) -> None:
        self.name = normalize_namespace(name)
        self.default_field = default_field
        self._default_index_policy = indexes is None
        if indexes is not None:
            self._indexes = dict(indexes)
        else:
            self._indexes = _starter_default_indexes()
        self._entities: list[Entity] = []
        self._key_to_idx: dict[tuple[str, str], int] = {}
        self._dirty = True
        self._lock = threading.Lock()
        self._validate_indexes()

    @property
    def entities(self) -> list[Entity]:
        return list(self._entities)

    @property
    def indexes(self) -> dict[str, CatalogIndex]:
        return dict(self._indexes)

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

    def index_for(self, field: str) -> CatalogIndex:
        """Return the index configured for the given field, or raise KeyError."""
        try:
            return self._indexes[field]
        except KeyError as err:
            raise KeyError(f"No index found for field {field!r}") from err

    def __len__(self) -> int:
        return len(self._entities)

    def _resolve_default_field(self) -> str | None:
        """Field name used for broad (unstructured) search, or None if disabled."""
        if self.default_field is not None:
            return self.default_field
        if "title" in self._indexes:
            return "title"
        return None

    def _indexed_fields(self) -> list[str]:
        return sorted(self._indexes)

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
            self._dirty = False

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

    def _execute_structured(
        self,
        query: StructuredQuery,
        *,
        limit: int,
        query_vectors: dict[tuple[str, int, bool], list[float]] | None,
    ) -> Ranking:
        for field, _ in query.clauses:
            try:
                self.index_for(field)
            except KeyError as err:
                raise ValueError(f"No index configured for field {field!r}") from err

        clause_results: list[dict[int, float]] = []
        for field, values in query.clauses:
            idx = self.index_for(field)
            merged_clause_scores: dict[int, float] = {}
            for val in values:
                scores = idx.score_candidates(val, query_vectors=query_vectors)
                for row_id, score in scores.items():
                    if score <= 0.0:
                        continue
                    prev = merged_clause_scores.get(row_id)
                    if prev is None or score > prev:
                        merged_clause_scores[row_id] = score
            clause_results.append(merged_clause_scores)

        if not clause_results:
            return Ranking.empty()

        intersection = set(clause_results[0])
        for clause in clause_results[1:]:
            intersection &= set(clause)

        if not intersection:
            return Ranking.empty()

        final_scores: dict[int, float] = {
            row_id: sum(clause[row_id] for clause in clause_results) for row_id in intersection
        }
        rows = [
            (self._entities[row_id].namespace, self._entities[row_id].code, score)
            for row_id, score in final_scores.items()
        ]
        return ranking_from_scores(rows, limit=limit)

    def search(
        self,
        query: str,
        limit: int,
        *,
        namespaces: list[str] | None = None,
    ) -> tuple[list[CatalogMatch], SearchDiagnostic]:
        """Search entries."""

        self._ensure_built("searched")
        known_fields = set(self._indexes)
        parsed = parse_query(query, known_fields=known_fields)
        if parsed is None:
            broad_field = self._resolve_default_field()
            if broad_field is None:
                indexed = ", ".join(f"'{f}'" for f in sorted(known_fields))
                raise BroadSearchUnavailableError(
                    f"This catalog only supports structured queries. "
                    f"Use 'field: value' syntax. Indexed fields: [{indexed}]"
                )
            broad_index = self.index_for(broad_field)
            query_vectors = embed_query_vectors(query, [broad_index])
            row_scores = broad_index.score_candidates(query, query_vectors=query_vectors)
            ranking = _ranking_from_row_scores(self._entities, row_scores, limit=limit)
            diagnostic = SearchDiagnostic(mode="broad")
        else:
            indexes = [self.index_for(field) for field, _ in parsed.clauses]
            query_vectors = embed_query_vectors(query, indexes)
            ranking = self._execute_structured(parsed, limit=limit, query_vectors=query_vectors)
            diagnostic = SearchDiagnostic(mode="structured")

        if namespaces is not None:
            ranking = _filter_namespaces(ranking, namespaces)

        return self._matches_from_ranking(ranking), diagnostic

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

        Supports local file paths (bare or file://) and remote Hugging Face
        dataset URLs (hf://). Caching loaded catalogs is the caller's
        responsibility.
        """
        return _dispatch_load(str(url))

    @classmethod
    def _load_from_path(cls, path: Path) -> Catalog:
        """Load a clean-slate catalog snapshot from a local directory."""
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

        entries = _read_parquet(src / ENTRIES_FILENAME)
        indexes = _load_indexes(src / INDEXES_DIRNAME, index_fields=meta.index_fields)

        catalog = cls(meta.name, indexes=indexes, default_field=meta.default_field)
        catalog._default_index_policy = False
        catalog._entities = entries
        catalog._key_to_idx = {(entry.namespace, entry.code): i for i, entry in enumerate(entries)}
        catalog._dirty = False
        return catalog

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
        matches: list[CatalogMatch] = []
        for row in ranking.items:
            idx = self._key_to_idx.get((row.namespace, row.code))
            if idx is not None:
                matches.append(catalog_match_from_entity(self._entities[idx], score=float(row.score)))
        return matches

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
            {
                "namespace": entry.namespace,
                "code": entry.code,
                "title": entry.title,
                # default=str so a non-JSON-native metadata value (datetime,
                # Decimal, …) coerces to its string form instead of crashing the
                # whole snapshot save (matches Provenance.safe_dump).
                "metadata_json": json.dumps(entry.metadata, default=str),
            }
            for entry in self._entities
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
        meta = CatalogMeta(
            name=self.name,
            namespaces=sorted({entry.namespace for entry in self._entities}),
            entry_count=len(self._entities),
            index_fields={field: index.kind for field, index in self._indexes.items()},
            default_field=self.default_field,
            build=BuildInfo(builder=builder, content_sha256=content_sha),
        )
        (target / META_FILENAME).write_text(meta.model_dump_json(indent=2))


def _load_indexes(
    path: Path,
    *,
    index_fields: dict[str, str],
) -> dict[str, CatalogIndex]:
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
# Snapshot load/save dispatch.
#
# These helpers live here (rather than in :mod:`parsimony.catalog.urls`) so the
# module that knows :class:`Catalog` is the one that depends on the URL parser,
# not the other way around — keeping the import graph acyclic.
# ---------------------------------------------------------------------------


def _load_file(root: str, sub: str) -> Catalog:
    path = Path(root) / sub if sub else Path(root)
    if not path.exists():
        raise FileNotFoundError(f"Catalog directory does not exist: {path}")
    return Catalog._load_from_path(path)


def _save_file(catalog: Catalog, root: str, sub: str, *, builder: str | None = None) -> None:
    target = Path(root) / sub if sub else Path(root)
    catalog._save_to_path(target, builder=builder)


def _load_hf(root: str, sub: str, *, revision: str | None = None) -> Catalog:
    from huggingface_hub import snapshot_download

    from parsimony import cache

    cache_dir = cache.catalogs_dir()
    if sub:
        local = Path(
            snapshot_download(
                repo_id=root,
                repo_type=REPO_TYPE,
                revision=revision,
                cache_dir=cache_dir,
                allow_patterns=[f"{sub}/*"],
            )
        )
        return Catalog._load_from_path(local / sub)
    local = Path(snapshot_download(repo_id=root, repo_type=REPO_TYPE, revision=revision, cache_dir=cache_dir))
    return Catalog._load_from_path(local)


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


def _dispatch_load(url: str) -> Catalog:
    parsed = parse_catalog_url(url)
    if parsed.scheme == "file":
        return _load_file(parsed.root, parsed.sub)
    if parsed.scheme == "hf":
        return _load_hf(parsed.root, parsed.sub, revision=parsed.revision)
    raise ValueError(f"Unsupported catalog URL scheme {parsed.scheme!r}. Supported: ['file', 'hf']")
