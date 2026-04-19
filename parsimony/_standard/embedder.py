"""Embedding providers used by the standard :class:`parsimony.Catalog`.

:class:`EmbeddingProvider` is the contract every embedder satisfies. It is
*not* a plugin axis — users instantiate one of the bundled implementations
(or write their own) and pass it to ``Catalog("name", embedder=...)``. Two
implementations ship out of the box:

* :class:`SentenceTransformerEmbedder` — local model (``BAAI/bge-small-en-v1.5``
  by default). Requires ``parsimony-core[standard]``.
* :class:`LiteLLMEmbeddingProvider` — hosted embeddings via the
  `litellm <https://github.com/BerriAI/litellm>`_ unified API (OpenAI,
  Gemini, Cohere, Voyage, …). Requires ``parsimony-core[litellm]``.

Both modules import their heavy dependencies lazily so that ``import parsimony``
does not pull torch or litellm into memory.
"""

from __future__ import annotations

import asyncio
import logging
import math
import time
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

from parsimony.catalog.embedder_info import EmbedderInfo

if TYPE_CHECKING:
    from sentence_transformers import SentenceTransformer

logger = logging.getLogger(__name__)

DEFAULT_MODEL = "BAAI/bge-small-en-v1.5"
PARSIMONY_STANDARD_PACKAGE = "parsimony-core[standard]"
PARSIMONY_LITELLM_PACKAGE = "parsimony-core[litellm]"

_LITELLM_BATCH_SIZE = 100


class EmbeddingProvider(ABC):
    """Text-to-vector embedding consumed by the standard catalog."""

    @property
    @abstractmethod
    def dimension(self) -> int: ...

    @abstractmethod
    async def embed_texts(self, texts: list[str]) -> list[list[float]]:
        """Embeddings for corpus documents (indexing)."""
        ...

    @abstractmethod
    async def embed_query(self, query: str) -> list[float]:
        """Single embedding optimized for retrieval queries."""
        ...

    @abstractmethod
    def info(self) -> EmbedderInfo:
        """Persisted identity for this embedder, used in catalog metadata."""
        ...


class SentenceTransformerEmbedder(EmbeddingProvider):
    """Wraps a :class:`sentence_transformers.SentenceTransformer` model.

    Default model is :data:`DEFAULT_MODEL`. The first attribute access that
    needs the model triggers loading; instantiation alone is cheap.

    Parameters
    ----------
    model:
        Hugging Face Hub identifier (``BAAI/bge-small-en-v1.5``,
        ``intfloat/e5-small-v2``, etc.).
    normalize:
        L2-normalize output vectors. Recorded in :class:`EmbedderInfo` so the
        loading catalog applies the same normalization at query time.
    device:
        Optional device override (``"cpu"``, ``"cuda"``, ``"mps"``, ...).
    batch_size:
        Encoding batch size.
    """

    def __init__(
        self,
        *,
        model: str = DEFAULT_MODEL,
        normalize: bool = True,
        device: str | None = None,
        batch_size: int = 64,
    ) -> None:
        self._model_name = model
        self._normalize = normalize
        self._device = device
        self._batch_size = batch_size
        self._model: SentenceTransformer | None = None

    @property
    def model_name(self) -> str:
        return self._model_name

    @property
    def normalize(self) -> bool:
        return self._normalize

    @property
    def dimension(self) -> int:
        dim = self._get_model().get_sentence_embedding_dimension()
        if dim is None:  # pragma: no cover -- guard against models that don't report
            raise RuntimeError(
                f"sentence-transformers model {self._model_name!r} did not report an embedding dimension"
            )
        return int(dim)

    def info(self) -> EmbedderInfo:
        return EmbedderInfo(
            model=self._model_name,
            dim=self.dimension,
            normalize=self._normalize,
            package=PARSIMONY_STANDARD_PACKAGE,
        )

    async def embed_texts(self, texts: list[str]) -> list[list[float]]:
        if not texts:
            return []
        return await asyncio.to_thread(self._encode, texts)

    async def embed_query(self, query: str) -> list[float]:
        result = await asyncio.to_thread(self._encode, [query])
        return result[0]

    def _encode(self, texts: list[str]) -> list[list[float]]:
        model = self._get_model()
        vectors = model.encode(
            texts,
            batch_size=self._batch_size,
            convert_to_numpy=True,
            normalize_embeddings=self._normalize,
            show_progress_bar=False,
        )
        return [vec.tolist() for vec in vectors]

    def _get_model(self) -> SentenceTransformer:
        if self._model is None:
            from sentence_transformers import SentenceTransformer

            self._model = SentenceTransformer(self._model_name, device=self._device)
        return self._model


class LiteLLMEmbeddingProvider(EmbeddingProvider):
    """Hosted embeddings via the `litellm`_ unified API.

    Use this when the catalog should call an embedding API (OpenAI,
    Gemini, Cohere, Voyage, AWS Bedrock, …) instead of running a local
    model. Identity (model + dimension) must be supplied at construction
    time; this class does not introspect the remote endpoint.

    Outputs are L2-normalized so they round-trip cleanly with the
    inner-product FAISS index used by :class:`parsimony.Catalog`.

    Parameters
    ----------
    model:
        litellm model identifier (e.g. ``"openai/text-embedding-3-small"``,
        ``"gemini/text-embedding-004"``, ``"cohere/embed-english-v3.0"``).
    dimension:
        Vector dimension produced by the model. Must match what the API
        returns; mismatches raise :class:`ValueError`.
    batch_size:
        Maximum batch size per API call. Defaults to 100.

    .. _litellm: https://github.com/BerriAI/litellm
    """

    def __init__(self, *, model: str, dimension: int, batch_size: int = _LITELLM_BATCH_SIZE) -> None:
        self._model = model
        self._dimension = dimension
        self._batch_size = batch_size

    @property
    def dimension(self) -> int:
        return self._dimension

    def info(self) -> EmbedderInfo:
        return EmbedderInfo(
            model=self._model,
            dim=self._dimension,
            normalize=True,
            package=PARSIMONY_LITELLM_PACKAGE,
        )

    async def embed_texts(self, texts: list[str]) -> list[list[float]]:
        if not texts:
            return []
        out: list[list[float]] = []
        for start in range(0, len(texts), self._batch_size):
            chunk = texts[start : start + self._batch_size]
            response = await self._embed(chunk, task_type="RETRIEVAL_DOCUMENT")
            vectors = _validate_litellm_response(response, len(chunk), self._dimension)
            out.extend(_l2_normalize(v) for v in vectors)
        return out

    async def embed_query(self, query: str) -> list[float]:
        response = await self._embed([query], task_type="RETRIEVAL_QUERY")
        vectors = _validate_litellm_response(response, 1, self._dimension)
        return _l2_normalize(vectors[0])

    async def _embed(self, input_texts: list[str], *, task_type: str) -> Any:
        try:
            import litellm
        except ImportError as exc:
            raise ImportError(
                f"LiteLLMEmbeddingProvider requires litellm. Install with: pip install '{PARSIMONY_LITELLM_PACKAGE}'"
            ) from exc

        kwargs: dict[str, Any] = {"model": self._model, "input": input_texts, "task_type": task_type}
        t0 = time.monotonic()
        try:
            try:
                response = await litellm.aembedding(**kwargs, dimensions=self._dimension)
            except TypeError:
                # Some providers expose the parameter under a different name
                # (Gemini accepts ``output_dimensionality``).
                response = await litellm.aembedding(**kwargs, output_dimensionality=self._dimension)
        except Exception as exc:
            logger.error(
                "Embedding call failed: model=%s, batch_size=%d, error=%s",
                self._model,
                len(input_texts),
                exc,
            )
            raise RuntimeError(f"Embedding call failed for model {self._model!r}: {exc}") from exc
        logger.info(
            "Embedding call: model=%s, batch_size=%d, duration=%.2fs",
            self._model,
            len(input_texts),
            time.monotonic() - t0,
        )
        return response


# ----------------------------------------------------------------------
# litellm helpers
# ----------------------------------------------------------------------


def _l2_normalize(vec: list[float]) -> list[float]:
    norm = math.sqrt(sum(x * x for x in vec))
    if norm <= 0:
        return vec
    return [x / norm for x in vec]


def _validate_litellm_response(response: Any, expected_count: int, expected_dim: int) -> list[list[float]]:
    """Extract and validate vectors from a litellm embedding response.

    litellm normalizes responses to OpenAI shape: ``{"data": [{"embedding": [...]}]}``.
    Newer versions return Pydantic ``Embedding`` objects; both attribute and
    item access are supported here.
    """
    items = response["data"] if isinstance(response, dict) else getattr(response, "data", None)
    if items is None:
        raise ValueError(f"Embedding response missing 'data'; got {type(response).__name__}")
    if len(items) != expected_count:
        raise ValueError(f"Embedding response returned {len(items)} items, expected {expected_count}")
    out: list[list[float]] = []
    for i, item in enumerate(items):
        if hasattr(item, "embedding"):
            vec = item.embedding
        elif isinstance(item, dict) and "embedding" in item:
            vec = item["embedding"]
        else:
            raise ValueError(f"Embedding item {i} missing 'embedding' field")
        if len(vec) != expected_dim:
            raise ValueError(f"Embedding item {i} has dimension {len(vec)}, expected {expected_dim}")
        out.append(list(vec))
    return out
