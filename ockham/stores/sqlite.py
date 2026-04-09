"""SQLite-backed catalog store with FTS5 keyword search.

Uses stdlib ``sqlite3`` wrapped in ``asyncio.to_thread`` — zero extra
dependencies.  The database file is created (with tables) on first access.
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
from ockham.catalog.store import CatalogStore

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


class SQLiteCatalogStore(CatalogStore):
    """File-backed catalog store using SQLite + FTS5 for keyword search.

    Parameters
    ----------
    db_path:
        Path to the SQLite database file. Created (with parent dirs) on first
        access. Use ``":memory:"`` for an ephemeral in-process store.
    """

    def __init__(self, db_path: str | Path = ":memory:") -> None:
        self._db_path = str(db_path)
        self._conn: sqlite3.Connection | None = None

    def _get_conn(self) -> sqlite3.Connection:
        if self._conn is None:
            if self._db_path != ":memory:":
                Path(self._db_path).parent.mkdir(parents=True, exist_ok=True)
            conn = sqlite3.connect(self._db_path, check_same_thread=False)
            conn.row_factory = sqlite3.Row
            conn.executescript(_SCHEMA)
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

        def _delete(conn: sqlite3.Connection) -> None:
            with conn:
                conn.execute(
                    "DELETE FROM series_catalog WHERE namespace = ? AND code = ?",
                    (ns, c),
                )

        await self._run(_delete)

    async def search(
        self,
        query: str,
        limit: int,
        *,
        namespaces: builtins.list[str] | None = None,
    ) -> builtins.list[SeriesMatch]:
        if not query or not query.strip():
            return []

        # Build FTS5 query: each token becomes a prefix match joined by AND
        tokens = query.strip().split()
        fts_query = " AND ".join(f'"{tok}"*' for tok in tokens if tok)
        if not fts_query:
            return []

        ns_filter: set[str] | None = None
        if namespaces is not None:
            ns_filter = {normalize_code(n) for n in namespaces}

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
                # BM25 returns negative scores (lower = better); normalize to 0..1
                raw_rank = row["rank"]
                similarity = max(0.0, min(1.0, 1.0 / (1.0 - raw_rank)))
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

    async def list_codes_missing_embedding(
        self,
        limit: int | None,
        *,
        only_keys: builtins.list[tuple[str, str]] | None = None,
        namespace: str | None = None,
    ) -> builtins.list[tuple[str, str]]:
        lim = limit if limit is not None else 10_000
        ns_filter = normalize_code(namespace) if namespace is not None else None

        def _missing(conn: sqlite3.Connection) -> builtins.list[tuple[str, str]]:
            conditions = ["embedding IS NULL"]
            params: builtins.list = []

            if ns_filter is not None:
                conditions.append("namespace = ?")
                params.append(ns_filter)

            if only_keys is not None:
                normalized = [catalog_key(ns, c) for ns, c in only_keys]
                placeholders = ",".join(["(?, ?)"] * len(normalized))
                conditions.append(f"(namespace, code) IN (VALUES {placeholders})")
                params.extend(v for pair in normalized for v in pair)

            where = " AND ".join(conditions)
            params.append(lim)
            rows = conn.execute(
                f"SELECT namespace, code FROM series_catalog WHERE {where} ORDER BY namespace, code LIMIT ?",  # noqa: S608
                params,
            ).fetchall()
            return [(r["namespace"], r["code"]) for r in rows]

        return await self._run(_missing)

    async def update_embeddings(
        self, updates: builtins.list[tuple[tuple[str, str], builtins.list[float]]]
    ) -> None:
        if not updates:
            return

        def _update(conn: sqlite3.Connection) -> None:
            with conn:
                conn.executemany(
                    "UPDATE series_catalog SET embedding = ? WHERE namespace = ? AND code = ?",
                    [
                        (_encode_embedding(emb), catalog_key(ns, c)[0], catalog_key(ns, c)[1])
                        for (ns, c), emb in updates
                    ],
                )

        await self._run(_update)

    async def close(self) -> None:
        """Close the underlying SQLite connection."""
        if self._conn is not None:
            self._conn.close()
            self._conn = None
