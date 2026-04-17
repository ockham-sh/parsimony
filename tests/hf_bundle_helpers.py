"""Shared helpers for HF bundle tests.

Provides a deterministic in-memory :class:`EmbeddingProvider` plus a helper
that builds a tiny fixture bundle on-disk. Both are designed to let the
store be exercised end-to-end without network, without sentence-transformers,
and without real HuggingFace Hub access.

Tests that need ``faiss`` (everything that builds a real bundle) should
skip gracefully when it's not installed via the ``requires_faiss``
pytest marker below.
"""

from __future__ import annotations

import hashlib

import pytest

from parsimony.catalog.models import EmbeddingProvider, SeriesEntry

# Standard pinned revision used by every fixture bundle — full 40-char hex
# even though these tests don't touch the real model. The store validates
# the format.
FIXTURE_MODEL_ID = "sentence-transformers/all-MiniLM-L6-v2"
FIXTURE_MODEL_REVISION = "c9745ed1d9f207416be6d2e6f8de32d1f16199bf"
FIXTURE_DIM = 8


def _faiss_unavailable_reason() -> str | None:
    try:
        import faiss  # type: ignore[import-untyped]  # noqa: F401
    except ImportError as exc:
        return f"faiss unavailable: {exc}"
    return None


_FAISS_REASON = _faiss_unavailable_reason()
requires_faiss = pytest.mark.skipif(
    _FAISS_REASON is not None,
    reason=_FAISS_REASON or "",
)


def _stable_vector(text: str, dim: int) -> list[float]:
    """Hash-seeded deterministic L2-normalized vector.

    Using a hash as the RNG seed keeps vectors stable across runs and
    platforms. Nearby texts (sharing many chars) produce similar — but
    distinct — vectors because the hash diffuses bytewise differences.
    """
    import math

    digest = hashlib.sha256(text.encode("utf-8")).digest()
    # Expand digest into `dim` floats deterministically.
    out: list[float] = []
    i = 0
    while len(out) < dim:
        byte = digest[i % len(digest)]
        # Map byte to [-1, 1).
        out.append((byte - 128) / 128.0)
        i += 1
    # Mix the original text length in so near-duplicate texts differ.
    out[0] += len(text) * 1e-3
    norm = math.sqrt(sum(x * x for x in out)) or 1.0
    return [x / norm for x in out]


class FakeEmbeddingProvider(EmbeddingProvider):
    """Deterministic ``EmbeddingProvider`` for HF bundle tests.

    Exposes ``model_id`` / ``revision`` attributes so that the store's
    provider-compatibility check against manifests works end-to-end.
    """

    def __init__(
        self,
        *,
        model_id: str = FIXTURE_MODEL_ID,
        revision: str = FIXTURE_MODEL_REVISION,
        dim: int = FIXTURE_DIM,
    ) -> None:
        self._model_id = model_id
        self._revision = revision
        self._dim = dim

    @property
    def dimension(self) -> int:
        return self._dim

    @property
    def model_id(self) -> str:
        return self._model_id

    @property
    def revision(self) -> str:
        return self._revision

    async def embed_texts(self, texts: list[str]) -> list[list[float]]:
        return [_stable_vector(t, self._dim) for t in texts]

    async def embed_query(self, query: str) -> list[float]:
        return _stable_vector(query, self._dim)


def make_fixture_entries(namespace: str, n: int = 5) -> list[SeriesEntry]:
    """Return *n* deterministic catalog rows for a fixture bundle."""
    topics = [
        ("unemployment_rate", "Unemployment rate, seasonally adjusted"),
        ("gdp_growth", "Gross domestic product, year-over-year growth"),
        ("inflation_cpi", "Consumer price index, year-over-year change"),
        ("policy_rate", "Central bank policy rate"),
        ("yield_10y", "10-year government bond yield"),
        ("balance_of_payments", "Current account balance, percent of GDP"),
        ("industrial_production", "Industrial production index"),
        ("exchange_rate_usd", "Exchange rate against USD"),
    ]
    out: list[SeriesEntry] = []
    for i in range(n):
        code_tail, title = topics[i % len(topics)]
        out.append(
            SeriesEntry(
                namespace=namespace,
                code=f"{namespace}_{i:04d}_{code_tail}",
                title=f"{title} ({namespace.upper()})",
                description=f"Test series {i} for fixture bundle {namespace!r}.",
                tags=["macro", "fixture"],
                metadata={"test": True, "index": i},
            )
        )
    return out
