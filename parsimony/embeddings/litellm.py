"""Hosted-API embedding provider via LiteLLM.

**Compatibility.** This provider works with :class:`~parsimony.catalog.catalog.Catalog`
and :class:`~parsimony.stores.sqlite_catalog.SQLiteCatalogStore`. It does
**NOT** work with :class:`~parsimony.stores.hf_bundle.HFBundleCatalogStore`:
HF bundle manifests pin ``embedding_model`` to a HuggingFace identity
(model id + commit SHA) so any consumer can recompute query vectors with
the same weights. Hosted-API embedding endpoints lack a stable
recomputable identity, so they cannot pass the bundle's integrity check.

Use this when you embed locally into SQLite-backed catalogs and need
hosted models (OpenAI, Cohere, Voyage). Use
:class:`~parsimony.embeddings.sentence_transformers.SentenceTransformersEmbeddingProvider`
when you publish or consume HF bundles.

Optional dependency: install via the ``[search]`` extra
(``pip install parsimony-core[search]``).
"""

from __future__ import annotations

import logging
import math
import time

import litellm

from parsimony.catalog.models import EmbeddingProvider

logger = logging.getLogger(__name__)

_EMBED_BATCH_SIZE = 100


def _normalize_embedding(vec: list[float]) -> list[float]:
    norm = math.sqrt(sum(x * x for x in vec))
    if norm <= 0:
        return vec
    return [x / norm for x in vec]


def _validate_embedding_response(
    response: dict,
    expected_count: int,
    expected_dim: int,
) -> list[list[float]]:
    """Extract and validate embeddings from a litellm response."""
    if "data" not in response:
        raise ValueError(f"Embedding response missing 'data' key. Keys present: {list(response.keys())}")
    items = response["data"]
    if len(items) != expected_count:
        raise ValueError(f"Embedding response returned {len(items)} items, expected {expected_count}")
    embeddings: list[list[float]] = []
    for i, item in enumerate(items):
        # litellm returns Pydantic Embedding objects; support both attribute
        # and dict-style access so we work with dicts and model instances.
        if hasattr(item, "embedding"):
            vec = item.embedding
        elif isinstance(item, dict) and "embedding" in item:
            vec = item["embedding"]
        else:
            raise ValueError(f"Embedding item {i} missing 'embedding' key")
        if len(vec) != expected_dim:
            raise ValueError(f"Embedding item {i} has dimension {len(vec)}, expected {expected_dim}")
        embeddings.append(vec)
    return embeddings


class LiteLLMEmbeddingProvider(EmbeddingProvider):
    """Litellm-backed embeddings (e.g. Gemini) with configurable model and dimension."""

    def __init__(self, *, model: str, dimension: int) -> None:
        self._model = model
        self._dimension = dimension

    @property
    def dimension(self) -> int:
        return self._dimension

    async def _embed(
        self,
        input_texts: list[str],
        *,
        task_type: str,
    ) -> dict:
        kwargs: dict = {
            "model": self._model,
            "input": input_texts,
            "task_type": task_type,
        }
        t0 = time.monotonic()
        try:
            try:
                response = await litellm.aembedding(
                    **kwargs,
                    dimensions=self._dimension,
                )
            except TypeError:
                response = await litellm.aembedding(
                    **kwargs,
                    output_dimensionality=self._dimension,
                )
        except TypeError:
            raise
        except Exception as exc:
            logger.error(
                "Embedding call failed: model=%s, batch_size=%d, error=%s",
                self._model,
                len(input_texts),
                exc,
            )
            raise RuntimeError(f"Embedding call failed for model {self._model!r}: {exc}") from exc
        elapsed = time.monotonic() - t0
        logger.info(
            "Embedding call: model=%s, batch_size=%d, duration=%.2fs",
            self._model,
            len(input_texts),
            elapsed,
        )
        return response  # type: ignore[no-any-return]

    async def embed_texts(self, texts: list[str]) -> list[list[float]]:
        if not texts:
            return []
        out: list[list[float]] = []
        for i in range(0, len(texts), _EMBED_BATCH_SIZE):
            chunk = texts[i : i + _EMBED_BATCH_SIZE]
            response = await self._embed(chunk, task_type="RETRIEVAL_DOCUMENT")
            embeddings = _validate_embedding_response(response, len(chunk), self._dimension)
            out.extend([_normalize_embedding(e) for e in embeddings])
        return out

    async def embed_query(self, query: str) -> list[float]:
        response = await self._embed([query], task_type="RETRIEVAL_QUERY")
        embeddings = _validate_embedding_response(response, 1, self._dimension)
        return _normalize_embedding(embeddings[0])
