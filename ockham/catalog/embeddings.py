from __future__ import annotations

from abc import ABC, abstractmethod


class EmbeddingProvider(ABC):
    """Text-to-vector embedding for catalog search."""

    @property
    @abstractmethod
    def dimension(self) -> int:
        ...

    @abstractmethod
    async def embed_texts(self, texts: list[str]) -> list[list[float]]:
        """Embeddings for corpus documents (indexing)."""
        ...

    @abstractmethod
    async def embed_query(self, query: str) -> list[float]:
        """Single embedding optimized for retrieval queries."""
        ...
