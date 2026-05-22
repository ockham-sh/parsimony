"""Catalog runtime: entries, search, and snapshot persistence."""

from __future__ import annotations

import asyncio
import json
import shutil
from collections.abc import Iterable, Sequence
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from parsimony.catalog import urls
from parsimony.catalog.indexes import BM25Index, CatalogIndex, HybridIndex, VectorIndex
from parsimony.catalog.models import (
    BroadSearchConfigError,
    BroadSearchUnavailableError,
    CatalogEntry,
    CatalogMatch,
    SearchDiagnostic,
    catalog_key,
    catalog_match_from_entry,
    normalize_code,
    normalize_entity_code,
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
from parsimony.catalog.urls import parse_catalog_url
from parsimony.ranking import Ranking, ranking_from_scores


def _entries_from_dataframe(
    df: pd.DataFrame,
    *,
    namespace: str,
    key_column: str,
    title_column: str | None,
    metadata_columns: Sequence[str],
) -> list[CatalogEntry]:
    """Build :class:`CatalogEntry` rows from a DataFrame with explicit column roles.

    Internal helper. The public entry point is :meth:`OutputConfig.build_entries`.
    """

    if df.empty:
        return []
    static_ns = normalize_code(namespace)
    if key_column not in df.columns:
        raise ValueError(f"DataFrame missing key column {key_column!r}. Available: {list(df.columns)}")
    title_name = title_column
    if title_name is not None and title_name not in df.columns:
        raise ValueError(f"DataFrame missing title column {title_name!r}. Available: {list(df.columns)}")
    meta_names = list(metadata_columns)
    for meta_name in meta_names:
        if meta_name not in df.columns:
            raise ValueError(f"DataFrame missing metadata column {meta_name!r}. Available: {list(df.columns)}")

    key_name = key_column
    needed_cols = {key_name, *meta_names}
    if title_name:
        needed_cols.add(title_name)
    sub_df = df[list(needed_cols)]
    grouped = sub_df.groupby(key_name, sort=False, dropna=True)

    entries: list[CatalogEntry] = []
    for raw_code, sub in grouped:
        code = normalize_entity_code(str(raw_code))
        if title_name and title_name in sub.columns:
            titles = sub[title_name].dropna()
            title = str(titles.iloc[0]) if len(titles) > 0 else code
        else:
            title = code
        metadata: dict[str, Any] = {}
        for meta_name in meta_names:
            vals = sub[meta_name].dropna()
            if len(vals) > 0:
                value = vals.iloc[0]
                metadata[meta_name] = _metadata_value(value)
        entries.append(CatalogEntry(namespace=static_ns, code=code, title=title, metadata=metadata))
    return entries


def _metadata_value(value: Any) -> Any:
    if hasattr(value, "tolist"):
        value = value.tolist()
    elif hasattr(value, "item"):
        value = value.item()
    if isinstance(value, list):
        return [item.item() if hasattr(item, "item") else item for item in value]
    return value


def _filter_namespaces(ranking: Ranking, namespaces: list[str]) -> Ranking:
    allowed = {normalize_code(ns) for ns in namespaces}
    rows = [
        (item.namespace, item.code, item.score)
        for item in ranking.items
        if item.namespace in allowed
    ]
    return ranking_from_scores(rows, limit=len(rows))


def _indexes_to_field_dict(indexes: Iterable[CatalogIndex]) -> dict[str, CatalogIndex]:
    out: dict[str, CatalogIndex] = {}
    for idx in indexes:
        if idx.field in out:
            raise ValueError(f"Exactly one index is allowed per field; duplicate field {idx.field!r}")
        out[idx.field] = idx
    return out


def _bm25_index_name(field: str) -> str:
    if field == "title":
        return "title_bm25"
    if field == "code":
        return "code_bm25"
    return f"{field}_bm25"


def _default_indexes_from_entries(entries: list[CatalogEntry]) -> dict[str, CatalogIndex]:
    """BM25 indexes for code, title, and every metadata key observed in *entries*."""
    meta_keys: set[str] = set()
    for entry in entries:
        meta_keys.update(entry.metadata.keys())
    fields = ["code", "title", *sorted(meta_keys)]
    return {field: BM25Index(_bm25_index_name(field), field=field) for field in fields}


def _starter_default_indexes() -> dict[str, CatalogIndex]:
    """Placeholder until :meth:`Catalog.build` materializes defaults from entries."""
    return {"title": BM25Index("title_bm25", field="title")}


class Catalog:
    """Portable in-memory catalog over entries and configured indexes.

    Pass ``indexes=None`` to use the framework default index policy: at
    :meth:`build`, BM25 indexes are created for ``code``, ``title``, and each
    metadata key present on the catalog entries. Pass an explicit ``indexes``
    list to take full control — no extra indexes are added silently.
    """

    def __init__(
        self,
        name: str,
        *,
        indexes: list[CatalogIndex] | None = None,
        default_field: str | None = None,
    ) -> None:
        self.name = normalize_code(name)
        self.default_field = default_field
        self._default_index_policy = indexes is None
        if indexes is not None:
            self._indexes_by_field = _indexes_to_field_dict(indexes)
        else:
            self._indexes_by_field = _starter_default_indexes()
        self._entries: list[CatalogEntry] = []
        self._key_to_idx: dict[tuple[str, str], int] = {}
        self._dirty = True
        self._lock = asyncio.Lock()
        self._validate_indexes()

    @property
    def entries(self) -> list[CatalogEntry]:
        return list(self._entries)

    @property
    def _indexes(self) -> list[CatalogIndex]:
        """Index list view (stable iteration order by field name)."""
        return [self._indexes_by_field[f] for f in sorted(self._indexes_by_field)]

    def _validate_indexes(self) -> None:
        names = [idx.name for idx in self._indexes_by_field.values()]
        if len(names) != len(set(names)):
            raise ValueError("Catalog index names must be unique")

    def index_for(self, field: str) -> CatalogIndex:
        """Return the index configured for the given field, or raise KeyError."""
        try:
            return self._indexes_by_field[field]
        except KeyError as err:
            raise KeyError(f"No index found for field {field!r}") from err

    def __len__(self) -> int:
        return len(self._entries)

    def _resolve_default_field(self) -> str | None:
        """Field name used for broad (unstructured) search, or None if disabled."""
        if self.default_field is not None:
            return self.default_field
        try:
            self.index_for("title")
        except KeyError:
            return None
        return "title"

    def _indexed_fields(self) -> list[str]:
        return sorted(self._indexes_by_field)

    async def build(self) -> None:
        """Build configured indexes over the catalog's current entries."""

        if self._default_index_policy:
            self._indexes_by_field = _default_indexes_from_entries(self._entries)
            self._validate_indexes()

        if self.default_field is not None:
            try:
                self.index_for(self.default_field)
            except KeyError as err:
                indexed = ", ".join(f"'{f}'" for f in self._indexed_fields())
                raise BroadSearchConfigError(
                    f"No index configured for default_field {self.default_field!r}. "
                    f"Indexed fields: [{indexed}]"
                ) from err

        async with self._lock:
            await self._rebuild_indices()
            self._dirty = False

    def set_entries(self, entries: list[CatalogEntry]) -> None:
        """Replace entries without rebuilding indexes."""

        self._entries = []
        self._key_to_idx = {}
        self._upsert_entries(entries)
        self._invalidate()

    def set_indexes(self, indexes: list[CatalogIndex]) -> None:
        """Replace index channels without rebuilding indexes."""

        self._indexes_by_field = _indexes_to_field_dict(indexes)
        self._default_index_policy = False
        self._validate_indexes()
        self._invalidate()

    async def _execute_structured(self, query: StructuredQuery, *, limit: int) -> Ranking:
        for field, _ in query.clauses:
            try:
                self.index_for(field)
            except KeyError as err:
                raise ValueError(f"No index configured for field {field!r}") from err

        clause_results: list[dict[tuple[str, str], float]] = []
        for field, values in query.clauses:
            idx = self.index_for(field)
            merged_clause_scores: dict[tuple[str, str], float] = {}
            for val in values:
                ranking = await idx.ranking(val, limit=max(limit * 5, 50))
                for row in ranking.items:
                    score = float(row.score)
                    if score <= 0.0:
                        continue
                    key = (row.namespace, row.code)
                    if key not in merged_clause_scores:
                        merged_clause_scores[key] = score
                    else:
                        merged_clause_scores[key] = max(merged_clause_scores[key], score)
            clause_results.append(merged_clause_scores)

        if not clause_results:
            return Ranking.empty()

        final_scores: dict[tuple[str, str], float] = {}
        first_clause = clause_results[0]
        for key, score in first_clause.items():
            final_scores[key] = score

        for clause in clause_results[1:]:
            intersection_keys = set(final_scores.keys()).intersection(clause.keys())
            new_scores: dict[tuple[str, str], float] = {}
            for key in intersection_keys:
                new_scores[key] = final_scores[key] + clause[key]
            final_scores = new_scores

        if not final_scores:
            return Ranking.empty()

        rows = [(ns, code, score) for (ns, code), score in final_scores.items()]
        return ranking_from_scores(rows, limit=limit)

    async def search(
        self,
        query: str,
        limit: int,
        *,
        namespaces: list[str] | None = None,
    ) -> tuple[list[CatalogMatch], SearchDiagnostic]:
        """Search entries."""

        self._ensure_built("searched")
        known_fields = set(self._indexes_by_field)
        parsed = parse_query(query, known_fields=known_fields)
        if parsed is None:
            broad_field = self._resolve_default_field()
            if broad_field is None:
                indexed = ", ".join(f"'{f}'" for f in sorted(known_fields))
                raise BroadSearchUnavailableError(
                    f"This catalog only supports structured queries. "
                    f"Use 'field: value' syntax. Indexed fields: [{indexed}]"
                )
            ranking = await self.index_for(broad_field).ranking(query, limit=limit)
            diagnostic = SearchDiagnostic(mode="broad")
        else:
            ranking = await self._execute_structured(parsed, limit=limit)
            diagnostic = SearchDiagnostic(mode="structured")

        if namespaces is not None:
            ranking = _filter_namespaces(ranking, namespaces)

        return self._matches_from_ranking(ranking), diagnostic

    async def get(self, namespace: str, code: str) -> CatalogEntry | None:
        idx = self._key_to_idx.get(catalog_key(namespace, code))
        return self._entries[idx] if idx is not None else None

    async def delete_many(self, keys: Iterable[tuple[str, str]]) -> int:
        async with self._lock:
            targets: set[int] = set()
            for ns, code in keys:
                idx = self._key_to_idx.get(catalog_key(ns, code))
                if idx is not None:
                    targets.add(idx)
            if not targets:
                return 0
            self._entries = [entry for i, entry in enumerate(self._entries) if i not in targets]
            self._key_to_idx = {(entry.namespace, entry.code): i for i, entry in enumerate(self._entries)}
            self._invalidate()
            return len(targets)

    async def save(self, url: str | Path, *, builder: str | None = None) -> None:
        """Save a catalog snapshot to a URL or bare path."""
        self._ensure_built("saved")
        parsed = parse_catalog_url(str(url))
        if parsed.scheme == "file":
            await urls._save_file(self, parsed.root, parsed.sub, builder=builder)
        elif parsed.scheme == "hf":
            await urls._save_hf(self, parsed.root, parsed.sub, builder=builder)
        else:
            raise ValueError(f"Unsupported catalog URL scheme {parsed.scheme!r}. Supported: ['file', 'hf']")

    async def _save_to_path(self, path: Path, *, builder: str | None = None) -> None:
        """Atomically write a portable catalog snapshot to a local directory."""
        target = Path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        tmp = target.with_name(target.name + ".tmp")
        if tmp.exists():
            shutil.rmtree(tmp)
        tmp.mkdir(parents=True)
        await asyncio.to_thread(self._write_parquet, tmp / ENTRIES_FILENAME)
        await asyncio.to_thread(self._write_indexes, tmp / INDEXES_DIRNAME)
        await asyncio.to_thread(self._write_meta, tmp, builder)
        if target.exists():
            shutil.rmtree(target)
        tmp.rename(target)

    @classmethod
    async def load(cls, url: str | Path) -> Catalog:
        """Load a built, searchable catalog snapshot from a URL or bare path.

        Supports local file paths (bare or file://) and remote Hugging Face
        dataset URLs (hf://). Caching loaded catalogs is the caller's
        responsibility.
        """
        return await urls._dispatch_load(str(url))

    @classmethod
    async def _load_from_path(cls, path: Path) -> Catalog:
        """Load a clean-slate catalog snapshot from a local directory."""
        src = Path(path)
        meta = read_meta(src)
        if meta.schema_version != 4:
            raise ValueError(f"Unsupported catalog schema_version {meta.schema_version}; expected 4")

        if meta.build.content_sha256:
            actual_sha = _compute_content_sha256(src)
            if meta.build.content_sha256 != actual_sha:
                raise ValueError(
                    f"Catalog snapshot integrity check failed for {src}:\n"
                    f"  expected sha256: {meta.build.content_sha256}\n"
                    f"  actual sha256:   {actual_sha}"
                )

        entries = await asyncio.to_thread(_read_parquet, src / ENTRIES_FILENAME)
        indexes = _load_indexes(src / INDEXES_DIRNAME, manifests=meta.indexes)

        catalog = cls(meta.name, indexes=list(indexes), default_field=meta.default_field)
        catalog._default_index_policy = False
        catalog._entries = entries
        catalog._key_to_idx = {(entry.namespace, entry.code): i for i, entry in enumerate(entries)}
        catalog._dirty = False
        return catalog

    def _invalidate(self) -> None:
        self._dirty = True

    def _ensure_built(self, action: str) -> None:
        if self._dirty:
            raise ValueError(f"Catalog must be built before it can be {action}")

    def _upsert_entries(self, entries: list[CatalogEntry]) -> None:
        for entry in entries:
            key = (entry.namespace, entry.code)
            if key in self._key_to_idx:
                self._entries[self._key_to_idx[key]] = entry
            else:
                self._key_to_idx[key] = len(self._entries)
                self._entries.append(entry)

    async def _rebuild_indices(self) -> None:
        for index in self._indexes_by_field.values():
            await index.build(self._entries)

    def _matches_from_ranking(self, ranking: Ranking) -> list[CatalogMatch]:
        matches: list[CatalogMatch] = []
        for row in ranking.items:
            idx = self._key_to_idx.get((row.namespace, row.code))
            if idx is not None:
                matches.append(catalog_match_from_entry(self._entries[idx], score=float(row.score)))
        return matches

    def _write_parquet(self, target: Path) -> None:
        if not self._entries:
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
                "metadata_json": json.dumps(entry.metadata),
            }
            for entry in self._entries
        ]
        pq.write_table(pa.Table.from_pylist(rows), target, compression="zstd")

    def _write_indexes(self, target: Path) -> None:
        target.mkdir(parents=True, exist_ok=True)
        for index in self._indexes_by_field.values():
            if isinstance(index, (VectorIndex, BM25Index, HybridIndex)):
                index.save(target / index.name)
            else:
                raise TypeError(f"Catalog index {index.name!r} is runtime-only and cannot be serialized")

    def _write_meta(self, target: Path, builder: str | None) -> None:
        content_sha = _compute_content_sha256(target)
        meta = CatalogMeta(
            name=self.name,
            namespaces=sorted({entry.namespace for entry in self._entries}),
            entry_count=len(self._entries),
            indexes=[_index_manifest(index) for index in self._indexes_by_field.values()],
            default_field=self.default_field,
            build=BuildInfo(builder=builder, content_sha256=content_sha),
        )
        (target / META_FILENAME).write_text(meta.model_dump_json(indent=2))


def _index_manifest(index: CatalogIndex) -> dict[str, Any]:
    if isinstance(index, VectorIndex):
        return {"name": index.name, "kind": index.kind, "field": index.field}
    if isinstance(index, BM25Index):
        return {"name": index.name, "kind": index.kind, "field": index.field}
    if isinstance(index, HybridIndex):
        return {"name": index.name, "kind": index.kind, "field": index.field}
    return {"name": index.name, "kind": "runtime"}


def _load_indexes(
    path: Path,
    *,
    manifests: list[dict[str, Any]],
) -> list[CatalogIndex]:
    indexes: list[CatalogIndex] = []
    if not path.exists():
        raise FileNotFoundError(f"Catalog snapshot missing indexes directory: {path}")
    children = [path / manifest["name"] for manifest in manifests]
    for child in children:
        raw = json.loads((child / META_FILENAME).read_text())
        kind = raw.get("kind")
        if kind == "vector":
            indexes.append(VectorIndex.load(child))
        elif kind == "bm25":
            indexes.append(BM25Index.load(child))
        elif kind == "hybrid":
            indexes.append(HybridIndex.load(child))
        else:
            raise ValueError(f"Unsupported catalog index kind {kind!r} in {child}")
    return indexes
