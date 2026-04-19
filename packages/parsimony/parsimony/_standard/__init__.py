"""Internal canonical catalog implementation.

Public surface is :class:`parsimony.Catalog`. This subpackage groups the
heavy-dependency implementation (FAISS, BM25, sentence-transformers,
huggingface-hub) so that ``import parsimony`` stays cheap; symbols are
loaded only when ``parsimony.Catalog`` is first referenced.

Direct imports from ``parsimony._standard`` are not part of the public API
and may move between minor releases.
"""

from __future__ import annotations

from parsimony._standard.catalog import Catalog
from parsimony._standard.embedder import (
    EmbeddingProvider,
    LiteLLMEmbeddingProvider,
    SentenceTransformerEmbedder,
)
from parsimony._standard.meta import (
    ENTRIES_FILENAME,
    INDEX_FILENAME,
    META_FILENAME,
    SCHEMA_VERSION,
    BuildInfo,
    CatalogMeta,
)

__all__ = [
    "ENTRIES_FILENAME",
    "INDEX_FILENAME",
    "META_FILENAME",
    "SCHEMA_VERSION",
    "BuildInfo",
    "Catalog",
    "CatalogMeta",
    "EmbeddingProvider",
    "LiteLLMEmbeddingProvider",
    "SentenceTransformerEmbedder",
]
