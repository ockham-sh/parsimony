"""Embedding provider implementations."""

from __future__ import annotations

from typing import Any

from parsimony.embeddings.sentence_transformers import (
    SentenceTransformersEmbeddingProvider,
)

__all__ = [
    "LiteLLMEmbeddingProvider",
    "SentenceTransformersEmbeddingProvider",
]


def __getattr__(name: str) -> Any:
    if name == "LiteLLMEmbeddingProvider":
        # Lazy: litellm lives in the [search] extra, not base deps.
        from parsimony.embeddings.litellm import LiteLLMEmbeddingProvider

        return LiteLLMEmbeddingProvider
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
