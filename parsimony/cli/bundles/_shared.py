"""Shared helpers used by every ``bundles`` verb.

Hosts the build-time embedding provider factory and the plan-materialization
timeout used wherever the CLI runs a ``CatalogSpec``'s plan generator.
"""

from __future__ import annotations

import asyncio
import os
from typing import Any

from parsimony.bundles.spec import CatalogPlan, materialize
from parsimony.catalog.models import EmbeddingProvider

# Per-plan generator timeout — cap a hostile or stuck plan callable so it
# can't hang the CLI indefinitely.
_PLAN_GEN_TIMEOUT_S: float = 30.0


def _build_provider_from_env() -> EmbeddingProvider:
    """Construct the build-time embedding provider from env vars.

    Reads ``PARSIMONY_EMBED_MODEL`` (default
    ``sentence-transformers/all-MiniLM-L6-v2``), ``PARSIMONY_EMBED_REVISION``
    (required full 40-char commit SHA) and ``PARSIMONY_EMBED_DIM`` (default
    384). Validates ``PARSIMONY_EMBED_MODEL`` against the manifest's allowed
    prefixes before constructing the provider so a bad value never gets the
    chance to import sentence-transformers.
    """
    from parsimony.bundles.format import ALLOWED_MODEL_ID_PREFIXES
    from parsimony.embeddings.sentence_transformers import (
        SentenceTransformersEmbeddingProvider,
    )

    model_id = os.environ.get(
        "PARSIMONY_EMBED_MODEL", "sentence-transformers/all-MiniLM-L6-v2"
    )
    if not any(model_id.startswith(p) for p in ALLOWED_MODEL_ID_PREFIXES):
        raise RuntimeError(
            f"PARSIMONY_EMBED_MODEL={model_id!r} is not under an allowed prefix; "
            f"allowed prefixes are {ALLOWED_MODEL_ID_PREFIXES}"
        )
    revision = os.environ.get("PARSIMONY_EMBED_REVISION")
    if not revision:
        raise RuntimeError(
            "PARSIMONY_EMBED_REVISION must be set to the full 40-char commit SHA "
            "of the embedding model pinned for this build"
        )
    dim = int(os.environ.get("PARSIMONY_EMBED_DIM", "384"))
    return SentenceTransformersEmbeddingProvider(
        model_id=model_id, revision=revision, expected_dim=dim
    )


async def _materialize_with_timeout(spec: Any) -> list[CatalogPlan]:
    async with asyncio.timeout(_PLAN_GEN_TIMEOUT_S):
        return await materialize(spec)
