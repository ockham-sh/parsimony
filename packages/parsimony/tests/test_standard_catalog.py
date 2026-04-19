"""Tests for the standard :class:`parsimony.Catalog` implementation.

Skipped when ``parsimony-core[standard]`` is not installed (no FAISS / BM25 /
sentence-transformers in the dev environment).
"""

from __future__ import annotations

import importlib.util

import pytest

from parsimony.catalog.embedder_info import EmbedderInfo
from parsimony.catalog.models import SeriesEntry

_HAS_FAISS = importlib.util.find_spec("faiss") is not None
_HAS_BM25 = importlib.util.find_spec("rank_bm25") is not None

pytestmark = pytest.mark.skipif(
    not (_HAS_FAISS and _HAS_BM25),
    reason="parsimony-core[standard] (faiss-cpu, rank-bm25) not installed",
)


class _StubEmbedder:
    """Deterministic, dependency-free embedder so the standard catalog can be
    exercised without sentence-transformers."""

    def __init__(self, dim: int = 4) -> None:
        self._dim = dim

    @property
    def dimension(self) -> int:
        return self._dim

    async def embed_texts(self, texts: list[str]) -> list[list[float]]:
        return [self._vec_for(t) for t in texts]

    async def embed_query(self, query: str) -> list[float]:
        return self._vec_for(query)

    def info(self) -> EmbedderInfo:
        return EmbedderInfo(model="stub", dim=self._dim, normalize=True, package="parsimony[tests]")

    def _vec_for(self, text: str) -> list[float]:
        # Deterministic length-d vector with a tiny token signal so query/document
        # cosine similarity moves with shared tokens.
        vec = [0.0] * self._dim
        for token in text.lower().split():
            vec[hash(token) % self._dim] += 1.0
        return vec


@pytest.mark.asyncio
async def test_upsert_get_delete_roundtrip() -> None:
    from parsimony import Catalog

    catalog = Catalog("fred", embedder=_StubEmbedder())
    entry = SeriesEntry(namespace="fred", code="GDPC1", title="Real GDP", tags=["macro"])
    await catalog.upsert([entry])

    got = await catalog.get("fred", "GDPC1")
    assert got is not None
    assert got.title == "Real GDP"
    assert got.embedding is not None
    assert len(got.embedding) == 4

    await catalog.delete("fred", "GDPC1")
    assert await catalog.get("fred", "GDPC1") is None


@pytest.mark.asyncio
async def test_search_returns_matches() -> None:
    from parsimony import Catalog

    catalog = Catalog("macro", embedder=_StubEmbedder())
    await catalog.upsert(
        [
            SeriesEntry(namespace="fred", code="UNRATE", title="Unemployment Rate"),
            SeriesEntry(namespace="fred", code="GDPC1", title="Real GDP"),
            SeriesEntry(namespace="fmp", code="AAPL", title="Apple Inc."),
        ]
    )
    matches = await catalog.search("unemployment", limit=5)
    assert any(m.code == "UNRATE" for m in matches)


@pytest.mark.asyncio
async def test_save_and_load_roundtrip(tmp_path) -> None:
    from parsimony import Catalog

    catalog = Catalog("fred", embedder=_StubEmbedder())
    await catalog.upsert([SeriesEntry(namespace="fred", code="UNRATE", title="Unemployment Rate")])
    out = tmp_path / "snap"
    await catalog.save(out, builder="test")

    loaded = await Catalog.load(out, embedder=_StubEmbedder())
    assert loaded.name == "fred"
    assert await loaded.list_namespaces() == ["fred"]
    got = await loaded.get("fred", "UNRATE")
    assert got is not None and got.title == "Unemployment Rate"


@pytest.mark.asyncio
async def test_load_rejects_dimension_mismatch(tmp_path) -> None:
    from parsimony import Catalog

    catalog = Catalog("fred", embedder=_StubEmbedder(dim=4))
    await catalog.upsert([SeriesEntry(namespace="fred", code="X", title="t")])
    out = tmp_path / "snap"
    await catalog.save(out)

    with pytest.raises(ValueError, match="dimension"):
        await Catalog.load(out, embedder=_StubEmbedder(dim=8))


@pytest.mark.asyncio
async def test_file_url_load_and_push_roundtrip(tmp_path) -> None:
    from parsimony import Catalog

    catalog = Catalog("fred", embedder=_StubEmbedder())
    await catalog.upsert([SeriesEntry(namespace="fred", code="UNRATE", title="Unemployment Rate")])
    target = tmp_path / "via_url"
    await catalog.push(f"file://{target}")

    # `from_url` constructs its own default sentence-transformers embedder, which
    # would require torch. Use the underlying load helper instead so we stay in
    # the lightweight test environment.
    loaded = await Catalog.load(target, embedder=_StubEmbedder())
    assert loaded.name == "fred"
    assert (await loaded.get("fred", "UNRATE")) is not None


@pytest.mark.asyncio
async def test_extend_combines_two_catalogs() -> None:
    from parsimony import Catalog

    fred = Catalog("fred", embedder=_StubEmbedder())
    await fred.upsert(
        [
            SeriesEntry(namespace="fred", code="GDPC1", title="Real GDP"),
            SeriesEntry(namespace="fred", code="UNRATE", title="Unemployment Rate"),
        ]
    )
    fmp = Catalog("fmp", embedder=_StubEmbedder())
    await fmp.upsert([SeriesEntry(namespace="fmp", code="AAPL", title="Apple Inc.")])

    union = Catalog("union", embedder=_StubEmbedder())
    await union.extend(fred)
    await union.extend(fmp)

    assert sorted(await union.list_namespaces()) == ["fmp", "fred"]
    assert len(union.entries) == 3
    assert (await union.get("fmp", "AAPL")) is not None
    assert (await union.get("fred", "GDPC1")) is not None


# ----------------------------------------------------------------------
# LiteLLMEmbeddingProvider — exercises validation, batching, normalization
# without requiring litellm itself (we stub the module).
# ----------------------------------------------------------------------


@pytest.mark.asyncio
async def test_litellm_provider_normalizes_and_validates(monkeypatch) -> None:
    import sys
    import types

    from parsimony import LiteLLMEmbeddingProvider

    captured: dict = {"calls": []}

    async def _fake_aembedding(**kwargs):
        captured["calls"].append(kwargs)
        # Return un-normalized vectors of matching dimension.
        return {"data": [{"embedding": [3.0, 4.0]} for _ in kwargs["input"]]}

    fake = types.ModuleType("litellm")
    fake.aembedding = _fake_aembedding  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "litellm", fake)

    provider = LiteLLMEmbeddingProvider(model="openai/text-embedding-3-small", dimension=2, batch_size=2)
    vectors = await provider.embed_texts(["alpha", "beta", "gamma"])
    assert len(vectors) == 3
    # L2-normalized: (3,4) → (0.6, 0.8)
    for vec in vectors:
        assert vec == [0.6, 0.8]

    # batch_size=2 across 3 inputs → 2 API calls
    assert len(captured["calls"]) == 2
    assert captured["calls"][0]["task_type"] == "RETRIEVAL_DOCUMENT"

    info = provider.info()
    assert info.model == "openai/text-embedding-3-small"
    assert info.dim == 2
    assert info.normalize is True


@pytest.mark.asyncio
async def test_litellm_provider_rejects_dimension_mismatch(monkeypatch) -> None:
    import sys
    import types

    from parsimony import LiteLLMEmbeddingProvider

    async def _fake_aembedding(**kwargs):
        return {"data": [{"embedding": [1.0, 2.0, 3.0]}]}

    fake = types.ModuleType("litellm")
    fake.aembedding = _fake_aembedding  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "litellm", fake)

    provider = LiteLLMEmbeddingProvider(model="x/y", dimension=4)
    with pytest.raises(ValueError, match="dimension"):
        await provider.embed_query("hello")
