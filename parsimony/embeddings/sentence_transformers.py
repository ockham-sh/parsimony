"""Sentence-Transformers embedding provider for HF bundle queries.

The provider is lazy: the model is only loaded on first embed call, and
the loaded model is cached **process-wide** keyed on ``(repo_id, revision)``
so that multiple :class:`~parsimony.stores.hf_bundle.store.HFBundleCatalogStore`
instances share one set of model weights.

Heavy imports (``torch``, ``sentence_transformers``) live inside method
bodies so that importing :mod:`parsimony` without ever touching a bundle
never pays the multi-second / multi-hundred-MB cost.
"""

from __future__ import annotations

import asyncio
import collections
import logging
import os
import threading
from typing import Any

from parsimony.catalog.models import EmbeddingProvider
from parsimony.stores.hf_bundle.errors import BundleIntegrityError
from parsimony.stores.hf_bundle.format import ALLOWED_MODEL_ID_PREFIXES

logger = logging.getLogger(__name__)

# Process-wide model cache (Dodds R6, Collina R6, Performance R2). LRU-bounded
# to 2 so long-running processes that load multiple revisions don't hoard
# torch weights for process lifetime. Key is (repo_id, revision).
_MODEL_CACHE_SIZE = 2
_MODEL_CACHE: collections.OrderedDict[tuple[str, str], Any] = collections.OrderedDict()
_MODEL_CACHE_LOCK = threading.Lock()


def _load_model_sync(repo_id: str, revision: str) -> Any:
    """Synchronously load a sentence-transformers model, memoized (LRU).

    Returns the cached model on subsequent calls with the same key. Holds a
    threading.Lock across the slow first load so concurrent callers don't
    double-download the weights. Cache is LRU-bounded — evicts the
    least-recently-used entry on overflow.
    """
    key = (repo_id, revision)
    cached = _MODEL_CACHE.get(key)
    if cached is not None:
        with _MODEL_CACHE_LOCK:
            if key in _MODEL_CACHE:
                _MODEL_CACHE.move_to_end(key, last=True)
        return cached

    with _MODEL_CACHE_LOCK:
        cached = _MODEL_CACHE.get(key)
        if cached is not None:
            _MODEL_CACHE.move_to_end(key, last=True)
            return cached

        try:
            from sentence_transformers import SentenceTransformer
        except ImportError as exc:
            raise BundleIntegrityError(
                "sentence-transformers is not installed",
                resource=repo_id,
                next_action="install parsimony's base dependencies: pip install 'parsimony'",
            ) from exc

        try:
            model = SentenceTransformer(repo_id, revision=revision)
        except Exception as exc:  # broad catch: sentence-transformers raises various types
            raise BundleIntegrityError(
                f"Failed to load embedding model {repo_id!r} at revision {revision!r}: {exc}",
                resource=f"{repo_id}@{revision}",
                next_action=(
                    "verify the model and revision exist on HuggingFace, "
                    "check network connectivity, and ensure the HF cache is writable"
                ),
            ) from exc

        _MODEL_CACHE[key] = model
        while len(_MODEL_CACHE) > _MODEL_CACHE_SIZE:
            _MODEL_CACHE.popitem(last=False)
        logger.info(
            "sentence_transformers.model_loaded model=%s revision=%s",
            repo_id,
            revision,
        )
        return model


def clear_model_cache() -> None:
    """Release all cached sentence-transformers models.

    Called by tests and long-running processes that want to reclaim memory.
    Does not affect any ``HFBundleCatalogStore`` that may be holding a
    reference to a loaded model — they'll keep working, the next lookup
    just re-loads.
    """
    with _MODEL_CACHE_LOCK:
        _MODEL_CACHE.clear()


class SentenceTransformersEmbeddingProvider(EmbeddingProvider):
    """EmbeddingProvider backed by a sentence-transformers model on HuggingFace.

    Parameters
    ----------
    model_id:
        HuggingFace repo id of the sentence-transformers model. Must start
        with an allowlisted prefix from
        :data:`~parsimony.stores.hf_bundle.format.ALLOWED_MODEL_ID_PREFIXES`.
    revision:
        Immutable 40-char commit SHA pinning the model weights.
    expected_dim:
        The expected output dimension. Validated against the model's actual
        dimension on first embed call.
    """

    def __init__(self, *, model_id: str, revision: str, expected_dim: int) -> None:
        if not any(model_id.startswith(p) for p in ALLOWED_MODEL_ID_PREFIXES):
            raise BundleIntegrityError(
                f"embedding model_id {model_id!r} is not under an allowed prefix",
                resource=model_id,
                next_action=f"use a model under one of {ALLOWED_MODEL_ID_PREFIXES}",
            )
        if expected_dim <= 0:
            raise ValueError("expected_dim must be positive")

        self._model_id = model_id
        self._revision = revision
        self._expected_dim = expected_dim
        self._dim_verified = False
        # Instance-level semaphore (Collina R8 / Performance R5): torch.encode
        # holds the GIL and shares the asyncio default thread pool with FAISS
        # searches and HF downloads. Cap to cpu_count so embeds don't starve
        # the pool. Created lazily on first use inside a running loop.
        raw = os.environ.get("PARSIMONY_EMBED_CONCURRENCY")
        self._embed_concurrency = int(raw) if raw and raw.isdigit() else max(1, os.cpu_count() or 2)
        self._embed_sem: asyncio.Semaphore | None = None

    @property
    def dimension(self) -> int:
        return self._expected_dim

    @property
    def model_id(self) -> str:
        return self._model_id

    @property
    def revision(self) -> str:
        return self._revision

    async def embed_texts(self, texts: list[str]) -> list[list[float]]:
        if not texts:
            return []
        if self._embed_sem is None:
            self._embed_sem = asyncio.Semaphore(self._embed_concurrency)
        async with self._embed_sem:
            return await asyncio.to_thread(self._encode_batch, texts)

    async def embed_query(self, query: str) -> list[float]:
        if self._embed_sem is None:
            self._embed_sem = asyncio.Semaphore(self._embed_concurrency)
        async with self._embed_sem:
            vectors = await asyncio.to_thread(self._encode_batch, [query])
        return vectors[0]

    def _encode_batch(self, texts: list[str]) -> list[list[float]]:
        model = _load_model_sync(self._model_id, self._revision)
        try:
            vectors = model.encode(
                texts,
                normalize_embeddings=True,
                convert_to_numpy=True,
                show_progress_bar=False,
            )
        except Exception as exc:
            raise BundleIntegrityError(
                f"encode() failed on model {self._model_id!r}: {exc}",
                resource=self._model_id,
            ) from exc

        self._verify_dim(vectors)
        return [vec.tolist() for vec in vectors]

    def _verify_dim(self, vectors: Any) -> None:
        if self._dim_verified:
            return
        shape = getattr(vectors, "shape", None)
        if shape is None or len(shape) != 2:
            raise BundleIntegrityError(
                f"encode() returned unexpected output shape {shape!r}",
                resource=self._model_id,
            )
        actual_dim = int(shape[1])
        if actual_dim != self._expected_dim:
            raise BundleIntegrityError(
                f"model {self._model_id!r} produced dim={actual_dim}, "
                f"manifest declares embedding_dim={self._expected_dim}",
                resource=self._model_id,
                next_action="rebuild bundle with the correct model, or update manifest embedding_dim",
            )
        self._dim_verified = True


__all__ = [
    "SentenceTransformersEmbeddingProvider",
    "clear_model_cache",
]
