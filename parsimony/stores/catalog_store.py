from __future__ import annotations

import builtins
from abc import ABC, abstractmethod

from parsimony.catalog.models import SeriesEntry, SeriesMatch


class CatalogStore(ABC):
    """Persistence and search for the series catalog.

    Implementations choose how :meth:`search` works (keyword, vector, hybrid, etc.).
    """

    @abstractmethod
    async def upsert(self, entries: builtins.list[SeriesEntry]) -> None:
        """Insert or update series rows (embedding optional)."""
        ...

    @abstractmethod
    async def get(self, namespace: str, code: str) -> SeriesEntry | None: ...

    @abstractmethod
    async def exists(self, keys: builtins.list[tuple[str, str]]) -> set[tuple[str, str]]:
        """Return the subset of (namespace, code) pairs that already exist."""
        ...

    @abstractmethod
    async def delete(self, namespace: str, code: str) -> None: ...

    @abstractmethod
    async def search(
        self,
        query: str,
        limit: int,
        *,
        namespaces: builtins.list[str] | None = None,
        query_embedding: builtins.list[float] | None = None,
    ) -> builtins.list[SeriesMatch]:
        """Search the catalog by natural-language query.

        When *namespaces* is set, restrict results to those namespace strings
        (after normalization). When ``None``, search all namespaces.

        When *query_embedding* is provided, implementations may use it for
        vector/hybrid search. Implementations that don't support vector search
        should ignore it gracefully.
        """
        ...

    @abstractmethod
    async def list_namespaces(self) -> builtins.list[str]:
        """Distinct catalog namespaces, sorted lexicographically."""
        ...

    @abstractmethod
    async def list(
        self,
        *,
        namespace: str | None = None,
        q: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> tuple[builtins.list[SeriesEntry], int]:
        """Return (entries, total_count) for pagination. Optional substring q on title/code."""
        ...
