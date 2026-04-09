"""In-memory catalog store for development, tests, and local tooling."""

from __future__ import annotations

import builtins
import json

from ockham.catalog.models import (
    SeriesEntry,
    SeriesMatch,
    catalog_key,
    normalize_code,
    series_match_from_entry,
)
from ockham.catalog.store import CatalogStore


def _namespace_filter(
    namespaces: builtins.list[str] | None,
) -> set[str] | None:
    if namespaces is None:
        return None
    return {normalize_code(n) for n in namespaces}


def _entry_text_haystack(entry: SeriesEntry) -> str:
    """Lowercased concatenation of fields indexed for FTS in Postgres (approximate)."""
    meta_s = json.dumps(entry.metadata, sort_keys=True)
    parts = [
        entry.title,
        entry.code,
        " ".join(entry.tags),
        meta_s,
    ]
    return " ".join(parts).lower()


def _search_text_tokens(query: str) -> builtins.list[str]:
    return [t for t in query.strip().lower().split() if t]


class InMemoryCatalogStore(CatalogStore):
    """Process-local series catalog: dict-backed token-based text search (tests / dev)."""

    def __init__(self) -> None:
        self._rows: dict[tuple[str, str], SeriesEntry] = {}

    async def upsert(self, entries: builtins.list[SeriesEntry]) -> None:
        for e in entries:
            k = catalog_key(e.namespace, e.code)
            self._rows[k] = e.model_copy(deep=True)

    async def get(self, namespace: str, code: str) -> SeriesEntry | None:
        k = catalog_key(namespace, code)
        return self._rows.get(k)

    async def exists(self, keys: builtins.list[tuple[str, str]]) -> set[tuple[str, str]]:
        out: set[tuple[str, str]] = set()
        for ns, c in keys:
            k = catalog_key(ns, c)
            if k in self._rows:
                out.add(k)
        return out

    async def delete(self, namespace: str, code: str) -> None:
        k = catalog_key(namespace, code)
        self._rows.pop(k, None)

    async def search(
        self,
        query: str,
        limit: int,
        *,
        namespaces: builtins.list[str] | None = None,
    ) -> builtins.list[SeriesMatch]:
        tokens = _search_text_tokens(query)
        if not tokens:
            return []
        allowed = _namespace_filter(namespaces)
        scored: builtins.list[tuple[float, SeriesEntry]] = []
        for e in self._rows.values():
            if allowed is not None and e.namespace not in allowed:
                continue
            haystack = _entry_text_haystack(e)
            if not all(tok in haystack for tok in tokens):
                continue
            rank = sum(haystack.count(tok) for tok in tokens)
            scored.append((float(rank), e))
        scored.sort(key=lambda t: (-t[0], t[1].namespace, t[1].code))
        return [
            series_match_from_entry(
                e.model_copy(deep=True),
                similarity=min(1.0, s / max(len(tokens), 1)),
            )
            for s, e in scored[:limit]
        ]

    async def list_namespaces(self) -> builtins.list[str]:
        return sorted({e.namespace for e in self._rows.values()})

    async def list(
        self,
        *,
        namespace: str | None = None,
        q: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> tuple[builtins.list[SeriesEntry], int]:
        rows = list(self._rows.values())
        if namespace is not None:
            ns = normalize_code(namespace)
            rows = [e for e in rows if e.namespace == ns]
        if q and q.strip():
            needle = q.strip().lower()
            rows = [
                e
                for e in rows
                if needle in e.title.lower() or needle in e.code.lower()
            ]
        total = len(rows)
        rows = sorted(rows, key=lambda e: (e.namespace, e.code))
        page = rows[offset : offset + limit]
        return ([e.model_copy(deep=True) for e in page], total)

    async def list_codes_missing_embedding(
        self,
        limit: int | None,
        *,
        only_keys: builtins.list[tuple[str, str]] | None = None,
        namespace: str | None = None,
    ) -> builtins.list[tuple[str, str]]:
        lim = limit if limit is not None else 10_000
        ns_filter = normalize_code(namespace) if namespace is not None else None
        out: builtins.list[tuple[str, str]] = []
        only_set: set[tuple[str, str]] | None = None
        if only_keys is not None:
            only_set = {catalog_key(ns, c) for ns, c in only_keys}
        for k in sorted(self._rows.keys()):
            if only_set is not None and k not in only_set:
                continue
            e = self._rows[k]
            if ns_filter is not None and e.namespace != ns_filter:
                continue
            if e.embedding is None:
                out.append(k)
                if len(out) >= lim:
                    break
        return out

    async def update_embeddings(
        self, updates: builtins.list[tuple[tuple[str, str], builtins.list[float]]]
    ) -> None:
        for key, emb in updates:
            k = catalog_key(key[0], key[1])
            e = self._rows.get(k)
            if e is None:
                continue
            self._rows[k] = e.model_copy(update={"embedding": list(emb)})
