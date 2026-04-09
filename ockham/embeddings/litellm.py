from __future__ import annotations

import math

import litellm

from ockham.catalog.models import EmbeddingProvider

_EMBED_BATCH_SIZE = 100


def _normalize_embedding(vec: list[float]) -> list[float]:
    norm = math.sqrt(sum(x * x for x in vec))
    if norm <= 0:
        return vec
    return [x / norm for x in vec]


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
        return response

    async def embed_texts(self, texts: list[str]) -> list[list[float]]:
        if not texts:
            return []
        out: list[list[float]] = []
        for i in range(0, len(texts), _EMBED_BATCH_SIZE):
            chunk = texts[i : i + _EMBED_BATCH_SIZE]
            response = await self._embed(chunk, task_type="RETRIEVAL_DOCUMENT")
            embeddings = [item["embedding"] for item in response["data"]]
            out.extend([_normalize_embedding(e) for e in embeddings])
        return out

    async def embed_query(self, query: str) -> list[float]:
        response = await self._embed([query], task_type="RETRIEVAL_QUERY")
        embedding = response["data"][0]["embedding"]
        return _normalize_embedding(embedding)
