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
import subprocess
from datetime import UTC, datetime
from pathlib import Path

import numpy as np
import pyarrow.parquet as pq
import pytest

from parsimony.bundles.build import _build_faiss_index, _write_faiss_index
from parsimony.bundles.format import (
    ENTRIES_FILENAME,
    ENTRIES_PARQUET_SCHEMA,
    FAISS_HNSW_EF_SEARCH_DEFAULT,
    INDEX_FILENAME,
    MANIFEST_FILENAME,
    BundleManifest,
    sha256_file,
)
from parsimony.catalog.arrow_adapters import entries_to_arrow_table
from parsimony.catalog.models import EmbeddingProvider, SeriesEntry

# Standard pinned revision used by every fixture bundle — full 40-char hex
# even though these tests don't touch the real model. The store validates
# the format.
FIXTURE_MODEL_ID = "sentence-transformers/all-MiniLM-L6-v2"
FIXTURE_MODEL_REVISION = "c9745ed1d9f207416be6d2e6f8de32d1f16199bf"
FIXTURE_DIM = 8


def _faiss_unavailable_reason() -> str | None:
    try:
        import faiss  # noqa: F401
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


_TOPIC_TOKENS: tuple[str, ...] = (
    "unemployment",
    "gdp",
    "inflation",
    "policy",
    "yield",
    "balance",
    "industrial",
    "exchange",
)


def _topic_one_hot(text: str, dim: int = len(_TOPIC_TOKENS)) -> list[float]:
    """One-hot vector keyed on which topic token appears in *text*.

    Used by :class:`TopicAwareFakeProvider` so the bundle store ranking
    test has predictable winners: a query for "unemployment rate" embeds
    to the same vector as the title containing "unemployment", and FAISS
    ranks it first by inner product.
    """
    lowered = text.lower()
    out = [0.0] * dim
    for i, token in enumerate(_TOPIC_TOKENS[:dim]):
        if token in lowered:
            out[i] = 1.0
            return out
    # No topic match — return a small uniform vector (still L2-normalizable
    # but never dominant against any one-hot).
    fallback = 1.0 / (dim**0.5)
    return [fallback] * dim


class TopicAwareFakeProvider(EmbeddingProvider):
    """Topic-keyed deterministic provider for ranking tests.

    Each text gets a one-hot vector based on which topic token from
    :data:`_TOPIC_TOKENS` appears in it. Inner-product search therefore
    surfaces the entry whose title shares the query's topic.
    """

    def __init__(
        self,
        *,
        model_id: str = FIXTURE_MODEL_ID,
        revision: str = FIXTURE_MODEL_REVISION,
    ) -> None:
        self._model_id = model_id
        self._revision = revision
        self._dim = len(_TOPIC_TOKENS)

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
        return [_topic_one_hot(t, self._dim) for t in texts]

    async def embed_query(self, query: str) -> list[float]:
        return _topic_one_hot(query, self._dim)


def write_bundle_dir(
    out_dir: Path,
    *,
    namespace: str,
    entries: list[SeriesEntry],
    vectors: np.ndarray,
    provider: EmbeddingProvider,
    git_sha: str | None = None,
    ef_search: int = FAISS_HNSW_EF_SEARCH_DEFAULT,
) -> BundleManifest:
    """Synchronous test helper: write a bundle directly from in-memory entries + vectors.

    Tests use this to seed bundles without running the async build orchestrator
    or its enumerator/embedder phases. Mirrors the on-disk shape of
    :func:`parsimony.bundles.build.build_bundle_dir`.
    """
    if vectors.ndim != 2 or vectors.shape[0] != len(entries):
        raise ValueError(
            f"vectors shape {vectors.shape} must be (entries={len(entries)}, dim)"
        )
    if not entries:
        raise ValueError("Cannot finalize a bundle with zero entries")

    model_id = getattr(provider, "model_id", None)
    revision = getattr(provider, "revision", None)
    if model_id is None or revision is None:
        raise RuntimeError(
            "Provider must expose model_id and revision attributes for the bundle manifest"
        )

    out_dir.mkdir(parents=True, exist_ok=True)
    entries_path = out_dir / ENTRIES_FILENAME
    index_path = out_dir / INDEX_FILENAME
    manifest_path = out_dir / MANIFEST_FILENAME

    table = entries_to_arrow_table(entries)
    if table.schema != ENTRIES_PARQUET_SCHEMA:
        raise RuntimeError("entries_to_arrow_table produced a schema mismatch")
    pq.write_table(table, entries_path)

    dim = provider.dimension
    index = _build_faiss_index(vectors, dim=dim, ef_search=ef_search)
    _write_faiss_index(index, index_path)

    if git_sha is None:
        try:
            r = subprocess.run(
                ["git", "rev-parse", "HEAD"],
                capture_output=True, text=True, check=False, timeout=2.0,
            )
            sha = r.stdout.strip()
            git_sha = sha if len(sha) == 40 else None
        except (OSError, subprocess.SubprocessError):
            git_sha = None

    manifest = BundleManifest(
        namespace=namespace,
        built_at=datetime.now(UTC),
        entry_count=len(entries),
        embedding_model=model_id,
        embedding_model_revision=revision,
        embedding_dim=dim,
        faiss_hnsw_ef_search=ef_search,
        entries_sha256=sha256_file(entries_path),
        index_sha256=sha256_file(index_path),
        builder_git_sha=git_sha,
    )
    manifest_path.write_text(
        manifest.model_dump_json(indent=2, round_trip=True) + "\n",
        encoding="utf-8",
    )
    return manifest


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
