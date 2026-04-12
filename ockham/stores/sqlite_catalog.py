"""SQLite-backed catalog store with FTS5 keyword search and sqlite-vec vector search.

Uses stdlib ``sqlite3`` wrapped in ``asyncio.to_thread``.  The database file
is created (with tables) on first access.

When ``sqlite-vec`` is installed (``pip install sqlite-vec``), vector search
and hybrid (BM25 + cosine RRF) are available.  Without it, search falls back
to FTS5 keyword-only — graceful degradation, zero hard dependencies.
"""

from __future__ import annotations

import asyncio
import builtins
import json
import sqlite3
import struct
from pathlib import Path

from ockham.catalog.models import (
    SeriesEntry,
    SeriesMatch,
    catalog_key,
    normalize_code,
    series_match_from_entry,
)
from ockham.stores.catalog_store import CatalogStore

_SCHEMA = """\
CREATE TABLE IF NOT EXISTS series_catalog (
    namespace TEXT NOT NULL,
    code      TEXT NOT NULL,
    title     TEXT NOT NULL,
    tags      TEXT NOT NULL DEFAULT '[]',
    description TEXT,
    metadata  TEXT NOT NULL DEFAULT '{}',
    properties TEXT NOT NULL DEFAULT '{}',
    embedding BLOB,
    observable_id TEXT,
    PRIMARY KEY (namespace, code)
);

CREATE VIRTUAL TABLE IF NOT EXISTS series_catalog_fts USING fts5(
    title, code, tags, description, metadata,
    content='series_catalog',
    content_rowid='rowid'
);

-- Triggers to keep FTS in sync with content table.
CREATE TRIGGER IF NOT EXISTS series_catalog_ai AFTER INSERT ON series_catalog BEGIN
    INSERT INTO series_catalog_fts(rowid, title, code, tags, description, metadata)
    VALUES (new.rowid, new.title, new.code, new.tags, new.description, new.metadata);
END;

CREATE TRIGGER IF NOT EXISTS series_catalog_ad AFTER DELETE ON series_catalog BEGIN
    INSERT INTO series_catalog_fts(series_catalog_fts, rowid, title, code, tags, description, metadata)
    VALUES ('delete', old.rowid, old.title, old.code, old.tags, old.description, old.metadata);
END;

CREATE TRIGGER IF NOT EXISTS series_catalog_au AFTER UPDATE ON series_catalog BEGIN
    INSERT INTO series_catalog_fts(series_catalog_fts, rowid, title, code, tags, description, metadata)
    VALUES ('delete', old.rowid, old.title, old.code, old.tags, old.description, old.metadata);
    INSERT INTO series_catalog_fts(rowid, title, code, tags, description, metadata)
    VALUES (new.rowid, new.title, new.code, new.tags, new.description, new.metadata);
END;
"""

# vec0 virtual table — created only when sqlite-vec is loaded.
_VEC_SCHEMA = """\
CREATE VIRTUAL TABLE IF NOT EXISTS series_catalog_vec USING vec0(
    embedding float[{dim}],
    namespace text,
    +code text
);
"""

# RRF constant (standard value from "Reciprocal Rank Fusion" paper).
_RRF_K = 60


def _encode_embedding(emb: builtins.list[float]) -> bytes:
    return struct.pack(f"<{len(emb)}f", *emb)


def _decode_embedding(blob: bytes) -> builtins.list[float]:
    count = len(blob) // 4
    return list(struct.unpack(f"<{count}f", blob))


def _row_to_entry(row: sqlite3.Row) -> SeriesEntry:
    return SeriesEntry(
        namespace=row["namespace"],
        code=row["code"],
        title=row["title"],
        tags=json.loads(row["tags"]),
        description=row["description"],
        metadata=json.loads(row["metadata"]),
        properties=json.loads(row["properties"]),
        embedding=_decode_embedding(row["embedding"]) if row["embedding"] else None,
        observable_id=row["observable_id"],
    )


def _try_load_sqlite_vec(conn: sqlite3.Connection) -> bool:
    """Load the sqlite-vec extension if available. Returns True on success."""
    try:
        import sqlite_vec  # type: ignore[import-untyped]

        conn.enable_load_extension(True)
        sqlite_vec.load(conn)
        conn.enable_load_extension(False)
        return True
    except (ImportError, Exception):
        return False


class SQLiteCatalogStore(CatalogStore):
    """File-backed catalog store using SQLite + FTS5 + optional sqlite-vec.

    Parameters
    ----------
    db_path:
        Path to the SQLite database file. Created (with parent dirs) on first
        access. Use ``":memory:"`` for an ephemeral in-process store.
    embedding_dim:
        Dimension of embedding vectors for the vec0 table. Only used when
        sqlite-vec is available. Defaults to 768.
    """

    def __init__(
        self,
        db_path: str | Path = ":memory:",
        *,
        embedding_dim: int = 768,
    ) -> None:
        self._db_path = str(db_path)
        self._embedding_dim = embedding_dim
        self._conn: sqlite3.Connection | None = None
        self._has_vec: bool = False

    @property
    def has_vec(self) -> bool:
        """Whether sqlite-vec is loaded and the vec0 table is available."""
        self._get_conn()  # ensure initialized
        return self._has_vec

    def _get_conn(self) -> sqlite3.Connection:
        if self._conn is None:
            if self._db_path != ":memory:":
                Path(self._db_path).parent.mkdir(parents=True, exist_ok=True)
            conn = sqlite3.connect(self._db_path, check_same_thread=False)
            conn.row_factory = sqlite3.Row
            conn.executescript(_SCHEMA)

            # Try loading sqlite-vec for vector search
            self._has_vec = _try_load_sqlite_vec(conn)
            if self._has_vec:
                conn.executescript(
                    _VEC_SCHEMA.format(dim=self._embedding_dim)
                )

            self._conn = conn
        return self._conn

    def _run_sync(self, fn, *args):  # noqa: ANN001, ANN002, ANN202
        return fn(self._get_conn(), *args)

    async def _run(self, fn, *args):  # noqa: ANN001, ANN002, ANN202
        return await asyncio.to_thread(self._run_sync, fn, *args)

    # ------------------------------------------------------------------
    # CatalogStore implementation
    # ------------------------------------------------------------------

    async def upsert(self, entries: builtins.list[SeriesEntry]) -> None:
        has_vec = self._has_vec
        dim = self._embedding_dim

        def _upsert(conn: sqlite3.Connection, entries: builtins.list[SeriesEntry]) -> None:
            with conn:
                conn.executemany(
                    """
                    INSERT INTO series_catalog
                        (namespace, code, title, tags, description, metadata, properties, embedding, observable_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(namespace, code) DO UPDATE SET
                        title = excluded.title,
                        tags = excluded.tags,
                        description = excluded.description,
                        metadata = excluded.metadata,
                        properties = excluded.properties,
                        embedding = COALESCE(excluded.embedding, series_catalog.embedding),
                        observable_id = COALESCE(excluded.observable_id, series_catalog.observable_id)
                    """,
                    [
                        (
                            e.namespace,
                            e.code,
                            e.title,
                            json.dumps(e.tags),
                            e.description,
                            json.dumps(e.metadata),
                            json.dumps(e.properties),
                            _encode_embedding(e.embedding) if e.embedding else None,
                            e.observable_id,
                        )
                        for e in entries
                    ],
                )

                # Sync vec0 table for entries with correctly-dimensioned embeddings
                if has_vec:
                    vec_rows = [
                        (
                            _encode_embedding(e.embedding),
                            e.namespace,
                            e.code,
                        )
                        for e in entries
                        if e.embedding is not None and len(e.embedding) == dim
                    ]
                    if vec_rows:
                        conn.executemany(
                            """
                            INSERT OR REPLACE INTO series_catalog_vec(embedding, namespace, code)
                            VALUES (?, ?, ?)
                            """,
                            vec_rows,
                        )

        await self._run(_upsert, entries)

    async def get(self, namespace: str, code: str) -> SeriesEntry | None:
        ns, c = catalog_key(namespace, code)

        def _get(conn: sqlite3.Connection) -> SeriesEntry | None:
            row = conn.execute(
                "SELECT * FROM series_catalog WHERE namespace = ? AND code = ?",
                (ns, c),
            ).fetchone()
            return _row_to_entry(row) if row else None

        return await self._run(_get)

    async def exists(self, keys: builtins.list[tuple[str, str]]) -> set[tuple[str, str]]:
        if not keys:
            return set()
        normalized = [catalog_key(ns, c) for ns, c in keys]

        def _exists(conn: sqlite3.Connection) -> set[tuple[str, str]]:
            placeholders = ",".join(["(?, ?)"] * len(normalized))
            flat = [v for pair in normalized for v in pair]
            rows = conn.execute(
                f"SELECT namespace, code FROM series_catalog WHERE (namespace, code) IN (VALUES {placeholders})",  # noqa: S608
                flat,
            ).fetchall()
            return {(r["namespace"], r["code"]) for r in rows}

        return await self._run(_exists)

    async def delete(self, namespace: str, code: str) -> None:
        ns, c = catalog_key(namespace, code)
        has_vec = self._has_vec

        def _delete(conn: sqlite3.Connection) -> None:
            with conn:
                conn.execute(
                    "DELETE FROM series_catalog WHERE namespace = ? AND code = ?",
                    (ns, c),
                )
                if has_vec:
                    conn.execute(
                        "DELETE FROM series_catalog_vec WHERE namespace = ? AND code = ?",
                        (ns, c),
                    )

        await self._run(_delete)

    async def search(
        self,
        query: str,
        limit: int,
        *,
        namespaces: builtins.list[str] | None = None,
        query_embedding: builtins.list[float] | None = None,
    ) -> builtins.list[SeriesMatch]:
        """Search the catalog.

        When *query_embedding* is provided and sqlite-vec is loaded, uses
        hybrid RRF (Reciprocal Rank Fusion) combining FTS5 BM25 keyword
        scores with vec0 cosine distance.  Otherwise falls back to FTS5-only.
        """
        if not query or not query.strip():
            return []
        if namespaces is not None and len(namespaces) == 0:
            return []

        tokens = query.strip().split()
        fts_query = " AND ".join(f'"{tok}"*' for tok in tokens if tok)
        if not fts_query:
            return []

        ns_filter: set[str] | None = None
        if namespaces is not None:
            ns_filter = {normalize_code(n) for n in namespaces}

        use_hybrid = (
            query_embedding is not None
            and self._has_vec
        )

        if use_hybrid:
            return await self._search_hybrid(
                fts_query, query_embedding, limit, ns_filter
            )
        return await self._search_fts(fts_query, limit, ns_filter)

    async def _search_fts(
        self,
        fts_query: str,
        limit: int,
        ns_filter: set[str] | None,
    ) -> builtins.list[SeriesMatch]:
        """FTS5-only keyword search with BM25 ranking."""

        def _search(conn: sqlite3.Connection) -> builtins.list[SeriesMatch]:
            if ns_filter:
                ns_placeholders = ",".join(["?"] * len(ns_filter))
                sql = f"""
                    SELECT sc.*, bm25(series_catalog_fts) AS rank
                    FROM series_catalog_fts
                    JOIN series_catalog sc ON sc.rowid = series_catalog_fts.rowid
                    WHERE series_catalog_fts MATCH ?
                      AND sc.namespace IN ({ns_placeholders})
                    ORDER BY rank
                    LIMIT ?
                """  # noqa: S608
                params: builtins.list = [fts_query, *ns_filter, limit]
            else:
                sql = """
                    SELECT sc.*, bm25(series_catalog_fts) AS rank
                    FROM series_catalog_fts
                    JOIN series_catalog sc ON sc.rowid = series_catalog_fts.rowid
                    WHERE series_catalog_fts MATCH ?
                    ORDER BY rank
                    LIMIT ?
                """
                params = [fts_query, limit]

            rows = conn.execute(sql, params).fetchall()
            results: builtins.list[SeriesMatch] = []
            for row in rows:
                entry = _row_to_entry(row)
                raw_rank = row["rank"]
                similarity = max(0.0, min(1.0, 1.0 / (1.0 - raw_rank)))
                results.append(series_match_from_entry(entry, similarity=similarity))
            return results

        return await self._run(_search)

    async def _search_hybrid(
        self,
        fts_query: str,
        query_embedding: builtins.list[float],
        limit: int,
        ns_filter: set[str] | None,
    ) -> builtins.list[SeriesMatch]:
        """Hybrid search: FTS5 BM25 + vec0 cosine via Reciprocal Rank Fusion."""
        emb_bytes = _encode_embedding(query_embedding)
        # Fetch more candidates from each source for better RRF merging
        candidate_limit = limit * 5

        def _search(conn: sqlite3.Connection) -> builtins.list[SeriesMatch]:
            # --- FTS5 candidates ---
            if ns_filter:
                ns_placeholders = ",".join(["?"] * len(ns_filter))
                fts_sql = f"""
                    SELECT sc.namespace, sc.code, bm25(series_catalog_fts) AS fts_rank
                    FROM series_catalog_fts
                    JOIN series_catalog sc ON sc.rowid = series_catalog_fts.rowid
                    WHERE series_catalog_fts MATCH ?
                      AND sc.namespace IN ({ns_placeholders})
                    ORDER BY fts_rank
                    LIMIT ?
                """  # noqa: S608
                fts_params: builtins.list = [fts_query, *ns_filter, candidate_limit]
            else:
                fts_sql = """
                    SELECT sc.namespace, sc.code, bm25(series_catalog_fts) AS fts_rank
                    FROM series_catalog_fts
                    JOIN series_catalog sc ON sc.rowid = series_catalog_fts.rowid
                    WHERE series_catalog_fts MATCH ?
                    ORDER BY fts_rank
                    LIMIT ?
                """
                fts_params = [fts_query, candidate_limit]

            fts_rows = conn.execute(fts_sql, fts_params).fetchall()

            # --- Vec0 candidates ---
            if ns_filter:
                ns_placeholders = ",".join(["?"] * len(ns_filter))
                vec_sql = f"""
                    SELECT namespace, code, distance
                    FROM series_catalog_vec
                    WHERE embedding MATCH ? AND k = ?
                      AND namespace IN ({ns_placeholders})
                """  # noqa: S608
                vec_params: builtins.list = [emb_bytes, candidate_limit, *ns_filter]
            else:
                vec_sql = """
                    SELECT namespace, code, distance
                    FROM series_catalog_vec
                    WHERE embedding MATCH ? AND k = ?
                """
                vec_params = [emb_bytes, candidate_limit]

            vec_rows = conn.execute(vec_sql, vec_params).fetchall()

            # --- RRF merge ---
            # Build rank maps (1-indexed position)
            fts_ranks: dict[tuple[str, str], int] = {}
            for i, row in enumerate(fts_rows):
                key = (row["namespace"], row["code"])
                fts_ranks[key] = i + 1

            vec_ranks: dict[tuple[str, str], int] = {}
            for i, row in enumerate(vec_rows):
                key = (row["namespace"], row["code"])
                vec_ranks[key] = i + 1

            all_keys = set(fts_ranks.keys()) | set(vec_ranks.keys())
            scored: builtins.list[tuple[float, tuple[str, str]]] = []
            for key in all_keys:
                fts_r = fts_ranks.get(key)
                vec_r = vec_ranks.get(key)
                rrf = 0.0
                if fts_r is not None:
                    rrf += 1.0 / (_RRF_K + fts_r)
                if vec_r is not None:
                    rrf += 1.0 / (_RRF_K + vec_r)
                scored.append((rrf, key))

            scored.sort(key=lambda t: -t[0])
            top_keys = scored[:limit]

            # Fetch full entries for top results
            results: builtins.list[SeriesMatch] = []
            for rrf_score, (ns, code) in top_keys:
                row = conn.execute(
                    "SELECT * FROM series_catalog WHERE namespace = ? AND code = ?",
                    (ns, code),
                ).fetchone()
                if row:
                    entry = _row_to_entry(row)
                    # Normalize RRF score to 0..1 range
                    max_rrf = 2.0 / (_RRF_K + 1)  # perfect score in both
                    similarity = min(1.0, rrf_score / max_rrf)
                    results.append(series_match_from_entry(entry, similarity=similarity))

            return results

        return await self._run(_search)

    async def list_namespaces(self) -> builtins.list[str]:
        def _list_ns(conn: sqlite3.Connection) -> builtins.list[str]:
            rows = conn.execute(
                "SELECT DISTINCT namespace FROM series_catalog ORDER BY namespace"
            ).fetchall()
            return [r["namespace"] for r in rows]

        return await self._run(_list_ns)

    async def list(
        self,
        *,
        namespace: str | None = None,
        q: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> tuple[builtins.list[SeriesEntry], int]:
        ns = normalize_code(namespace) if namespace is not None else None
        needle = q.strip().lower() if q and q.strip() else None

        def _list(conn: sqlite3.Connection) -> tuple[builtins.list[SeriesEntry], int]:
            conditions: builtins.list[str] = []
            params: builtins.list = []

            if ns is not None:
                conditions.append("namespace = ?")
                params.append(ns)
            if needle is not None:
                conditions.append("(LOWER(title) LIKE ? OR LOWER(code) LIKE ?)")
                like = f"%{needle}%"
                params.extend([like, like])

            where = f"WHERE {' AND '.join(conditions)}" if conditions else ""

            count_row = conn.execute(
                f"SELECT COUNT(*) AS cnt FROM series_catalog {where}",  # noqa: S608
                params,
            ).fetchone()
            total = count_row["cnt"]

            rows = conn.execute(
                f"SELECT * FROM series_catalog {where} ORDER BY namespace, code LIMIT ? OFFSET ?",  # noqa: S608
                [*params, limit, offset],
            ).fetchall()
            return ([_row_to_entry(r) for r in rows], total)

        return await self._run(_list)

    async def close(self) -> None:
        """Close the underlying SQLite connection."""
        if self._conn is not None:
            self._conn.close()
            self._conn = None
