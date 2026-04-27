"""Tests for :class:`parsimony.embedder.FragmentEmbeddingCache` and the
compositional-embedding path through :class:`~parsimony.Catalog`.

Scale problem this covers: SDMX series titles concatenate 5-10 dim
labels per row, and most labels repeat across hundreds of thousands of
series. Embedding the full title per row wastes 100-1000× the
tokenizer/inference budget. These tests lock in:

* ``FragmentEmbeddingCache.compose_many`` dedupes unique fragments and
  calls the base embedder exactly once per fragment in the batch.
* Composed vectors are the L2-renormalized mean of the component
  fragment vectors.
* Empty fragment lists raise — signalling a pipeline bug rather than
  silently returning a zero vector.
* ``SeriesEntry.fragments`` round-trips through
  ``entries_from_result`` when the FRAGMENTS column is declared; column
  disagreement across rows sharing a KEY raises.
* ``Catalog._embed_missing`` routes per-entry: fragmented entries take
  the compose path when a cache is wired, everything else falls back to
  the direct ``semantic_text`` path. A warning fires at most once when a
  catalog sees fragments but has no cache.
* Persist / load round-trips the cache; an embedder-identity mismatch
  discards the cached vectors rather than silently re-using them.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from parsimony.catalog import Catalog, SeriesEntry, entries_from_result
from parsimony.embedder import EmbedderInfo, FragmentEmbeddingCache
from parsimony.result import Column, ColumnRole, OutputConfig, Provenance, Result


class _CountingStubEmbedder:
    """Deterministic, countable embedder for dedup tests."""

    DIM = 8

    def __init__(self, *, model: str = "stub/hash-sha256") -> None:
        self.calls: list[list[str]] = []
        self._model = model

    @property
    def dimension(self) -> int:
        return self.DIM

    async def embed_texts(self, texts: list[str]) -> list[list[float]]:
        self.calls.append(list(texts))
        vectors: list[list[float]] = []
        for text in texts:
            digest = hashlib.sha256(text.encode("utf-8")).digest()
            raw = [digest[i] / 255.0 for i in range(self.DIM)]
            norm = sum(x * x for x in raw) ** 0.5 or 1.0
            vectors.append([x / norm for x in raw])
        return vectors

    async def embed_query(self, query: str) -> list[float]:
        (vec,) = await self.embed_texts([query])
        return vec

    def info(self) -> EmbedderInfo:
        return EmbedderInfo(
            model=self._model,
            dim=self.DIM,
            normalize=True,
            package="test-stub",
        )


def _mean_l2(vectors: list[list[float]]) -> list[float]:
    m = np.asarray(vectors, dtype=np.float32).mean(axis=0)
    norm = float(np.linalg.norm(m))
    if norm > 1e-12:
        m = m / norm
    return m.astype(np.float32).tolist()


# ---------------------------------------------------------------------------
# FragmentEmbeddingCache unit tests
# ---------------------------------------------------------------------------


def test_compose_many_dedupes_fragments() -> None:
    """1000 items over 5 unique fragments → 5 base embed calls (deduped)."""
    emb = _CountingStubEmbedder()
    cache = FragmentEmbeddingCache(emb)

    fragments_per_item = [
        ["Monthly", "Spain", "HICP", "All-items", "Annual rate"],
    ] * 1000

    result = asyncio.run(cache.compose_many(fragments_per_item))

    assert len(result) == 1000
    assert all(len(vec) == _CountingStubEmbedder.DIM for vec in result)
    # Exactly one flat base call with exactly 5 unique texts.
    assert len(emb.calls) == 1
    assert sorted(emb.calls[0]) == sorted({"Monthly", "Spain", "HICP", "All-items", "Annual rate"})


def test_compose_many_matches_mean_then_renormalize() -> None:
    """Composed vector is byte-equivalent to mean(fragment vectors) + L2 renorm."""
    emb = _CountingStubEmbedder()
    cache = FragmentEmbeddingCache(emb)

    fragments = ["Monthly", "Spain", "HICP"]
    result = asyncio.run(cache.compose_many([fragments]))
    (composed,) = result

    # Embed the same fragments directly for an oracle.
    component_vectors = asyncio.run(emb.embed_texts(fragments))
    expected = _mean_l2(component_vectors)

    assert composed == pytest.approx(expected, abs=1e-6)


def test_compose_many_raises_on_empty_fragments() -> None:
    emb = _CountingStubEmbedder()
    cache = FragmentEmbeddingCache(emb)

    with pytest.raises(ValueError, match="item 1 has no fragments"):
        asyncio.run(cache.compose_many([["x"], []]))


def test_compose_many_returns_empty_for_empty_input() -> None:
    emb = _CountingStubEmbedder()
    cache = FragmentEmbeddingCache(emb)

    assert asyncio.run(cache.compose_many([])) == []
    assert emb.calls == []


def test_stats_reports_hits_misses_and_unique() -> None:
    emb = _CountingStubEmbedder()
    cache = FragmentEmbeddingCache(emb)

    asyncio.run(cache.compose_many([["a", "b"], ["a", "c"]]))
    stats = cache.stats()

    assert stats["unique_fragments"] == 3  # a, b, c
    assert stats["misses"] == 3
    assert stats["hits"] == 1  # second "a" is a cache hit


def test_cache_hits_skip_base_embedder_on_second_call() -> None:
    emb = _CountingStubEmbedder()
    cache = FragmentEmbeddingCache(emb)

    asyncio.run(cache.compose_many([["a", "b"]]))
    asyncio.run(cache.compose_many([["a", "b"]]))

    # Second call hits cache entirely — no extra base embed.
    assert len(emb.calls) == 1


def test_persist_and_reload_round_trip(tmp_path: Path) -> None:
    emb = _CountingStubEmbedder()
    cache_dir = tmp_path / "fragments"
    cache = FragmentEmbeddingCache(emb, cache_dir=cache_dir)

    asyncio.run(cache.compose_many([["alpha", "beta", "gamma"]]))
    cache.persist()

    # New cache instance loads the persisted fragments without re-embedding.
    emb2 = _CountingStubEmbedder()
    cache2 = FragmentEmbeddingCache(emb2, cache_dir=cache_dir)
    asyncio.run(cache2.compose_many([["alpha", "beta", "gamma"]]))

    assert emb2.calls == []  # pure cache hit on reload


def test_default_cache_dir_lands_under_pinned_root(
    _pin_parsimony_cache_dir: Path,
) -> None:
    """Without an explicit ``cache_dir``, persist writes under
    ``parsimony.cache.embeddings_dir(slug)`` — pinned to a per-test tmp by the
    autouse fixture."""
    emb = _CountingStubEmbedder()
    cache = FragmentEmbeddingCache(emb)  # no cache_dir → default

    asyncio.run(cache.compose_many([["alpha"]]))
    cache.persist()

    embeddings_root = _pin_parsimony_cache_dir / "embeddings"
    assert embeddings_root.is_dir()
    # Exactly one slug-named subdirectory, holding both expected files.
    (slug_dir,) = list(embeddings_root.iterdir())
    assert (slug_dir / "fragments.parquet").exists()
    assert (slug_dir / "meta.json").exists()


def test_persist_noop_when_nothing_embedded(tmp_path: Path) -> None:
    """Persist before any compose_many call is a no-op (no parquet written)."""
    emb = _CountingStubEmbedder()
    cache_dir = tmp_path / "fragments"
    cache = FragmentEmbeddingCache(emb, cache_dir=cache_dir)

    cache.persist()  # must not raise; nothing in self._cache

    # Directory may exist (constructor may create it) but no parquet yet.
    if cache_dir.exists():
        assert not (cache_dir / "fragments.parquet").exists()


def test_reload_with_mismatched_embedder_identity_discards_cache(tmp_path: Path) -> None:
    emb1 = _CountingStubEmbedder(model="stub/model-A")
    cache_dir = tmp_path / "fragments"
    cache1 = FragmentEmbeddingCache(emb1, cache_dir=cache_dir)
    asyncio.run(cache1.compose_many([["alpha", "beta"]]))
    cache1.persist()

    # Different model identity — on-disk cache should be ignored.
    emb2 = _CountingStubEmbedder(model="stub/model-B")
    cache2 = FragmentEmbeddingCache(emb2, cache_dir=cache_dir)
    asyncio.run(cache2.compose_many([["alpha", "beta"]]))

    # Full re-embed under the new model identity.
    assert len(emb2.calls) == 1
    assert sorted(emb2.calls[0]) == ["alpha", "beta"]


# ---------------------------------------------------------------------------
# ColumnRole.FRAGMENTS + entries_from_result
# ---------------------------------------------------------------------------


def _result_with_fragments(
    df: pd.DataFrame,
    *,
    namespace: str = "test_ns",
) -> Result:
    config = OutputConfig(
        columns=[
            Column(name="code", role=ColumnRole.KEY, namespace=namespace),
            Column(name="title", role=ColumnRole.TITLE),
            Column(name="fragments", role=ColumnRole.FRAGMENTS),
            Column(name="agency", role=ColumnRole.METADATA),
        ]
    )
    return Result(data=df, provenance=Provenance(source="test"), output_schema=config)


def test_entries_from_result_populates_fragments() -> None:
    df = pd.DataFrame(
        {
            "code": ["A.1", "B.2"],
            "title": ["Series A", "Series B"],
            "fragments": [["Monthly", "Spain"], ["Quarterly", "Germany"]],
            "agency": ["ECB", "ECB"],
        }
    )
    entries = entries_from_result(_result_with_fragments(df))

    assert len(entries) == 2
    by_code = {e.code: e for e in entries}
    assert by_code["A.1"].fragments == ["Monthly", "Spain"]
    assert by_code["B.2"].fragments == ["Quarterly", "Germany"]
    assert all(isinstance(f, str) for f in by_code["A.1"].fragments or [])


def test_entries_from_result_handles_ndarray_fragments() -> None:
    """Pyarrow list columns can surface as np.ndarray after groupby — must cast to list[str]."""
    df = pd.DataFrame(
        {
            "code": ["A.1"],
            "title": ["Series A"],
            "fragments": [np.array(["Monthly", "Spain"])],
            "agency": ["ECB"],
        }
    )
    (entry,) = entries_from_result(_result_with_fragments(df))

    assert entry.fragments == ["Monthly", "Spain"]
    assert all(type(f) is str for f in entry.fragments or [])


def test_entries_from_result_raises_on_fragment_disagreement() -> None:
    df = pd.DataFrame(
        {
            "code": ["A.1", "A.1"],
            "title": ["Series A", "Series A"],
            "fragments": [["x", "y"], ["x", "z"]],
            "agency": ["ECB", "ECB"],
        }
    )
    with pytest.raises(ValueError, match="FRAGMENTS column .* disagreement for key='A.1'"):
        entries_from_result(_result_with_fragments(df))


def test_entries_from_result_without_fragments_column_leaves_field_none() -> None:
    """Legacy schemas (no FRAGMENTS role declared) still produce entries with fragments=None."""
    df = pd.DataFrame(
        {
            "code": ["A.1"],
            "title": ["Series A"],
            "agency": ["ECB"],
        }
    )
    config = OutputConfig(
        columns=[
            Column(name="code", role=ColumnRole.KEY, namespace="legacy"),
            Column(name="title", role=ColumnRole.TITLE),
            Column(name="agency", role=ColumnRole.METADATA),
        ]
    )
    table = Result(data=df, provenance=Provenance(source="legacy"), output_schema=config)
    (entry,) = entries_from_result(table)

    assert entry.fragments is None


# ---------------------------------------------------------------------------
# Catalog routing (compose vs direct)
# ---------------------------------------------------------------------------


def test_catalog_routes_fragmented_entries_through_cache() -> None:
    emb = _CountingStubEmbedder()
    cache = FragmentEmbeddingCache(emb)
    cat = Catalog("routing_mixed", embedder=emb, fragment_cache=cache)

    entries = [
        SeriesEntry(
            namespace="routing_mixed",
            code="A.1",
            title="Monthly | Spain | HICP",
            fragments=["Monthly", "Spain", "HICP"],
        ),
        SeriesEntry(
            namespace="routing_mixed",
            code="B.1",
            title="plain title",
        ),
    ]

    asyncio.run(cat.add_all(entries))

    # One base-embed call per path: one with 3 unique fragments, one with
    # the plain title.
    texts_flat = [t for call in emb.calls for t in call]
    assert "Monthly" in texts_flat
    assert "Spain" in texts_flat
    assert "HICP" in texts_flat
    assert "plain title" in texts_flat
    # No full-title-of-A.1 text was embedded (the compose path bypassed it).
    assert "Monthly | Spain | HICP" not in texts_flat


def test_catalog_without_cache_falls_back_to_direct_embed_with_warning(
    caplog: pytest.LogCaptureFixture,
) -> None:
    emb = _CountingStubEmbedder()
    cat = Catalog("no_cache", embedder=emb)  # fragment_cache omitted

    entries = [
        SeriesEntry(
            namespace="no_cache",
            code="A.1",
            title="Monthly | Spain | HICP",
            fragments=["Monthly", "Spain", "HICP"],
        ),
    ]

    with caplog.at_level(logging.WARNING, logger="parsimony.catalog"):
        asyncio.run(cat.add_all(entries))

    assert any("no fragment_cache is configured" in rec.message for rec in caplog.records)
    # Direct path: the full title was embedded, not the individual fragments.
    texts_flat = [t for call in emb.calls for t in call]
    assert "Monthly | Spain | HICP" in texts_flat
    assert "Monthly" not in texts_flat


def test_catalog_warning_fires_only_once(caplog: pytest.LogCaptureFixture) -> None:
    emb = _CountingStubEmbedder()
    cat = Catalog("no_cache_once", embedder=emb)

    entries_a = [
        SeriesEntry(
            namespace="no_cache_once",
            code="A.1",
            title="t1",
            fragments=["a", "b"],
        ),
    ]
    entries_b = [
        SeriesEntry(
            namespace="no_cache_once",
            code="B.1",
            title="t2",
            fragments=["c", "d"],
        ),
    ]

    with caplog.at_level(logging.WARNING, logger="parsimony.catalog"):
        asyncio.run(cat.add_all(entries_a))
        asyncio.run(cat.add_all(entries_b))

    warnings = [rec for rec in caplog.records if "no fragment_cache" in rec.message]
    assert len(warnings) == 1


def test_catalog_mixed_mode_search_returns_both_paths() -> None:
    emb = _CountingStubEmbedder()
    cache = FragmentEmbeddingCache(emb)
    cat = Catalog("mixed_search", embedder=emb, fragment_cache=cache)

    entries = [
        SeriesEntry(
            namespace="mixed_search",
            code="A.1",
            title="Monthly Spain HICP",
            fragments=["Monthly", "Spain", "HICP"],
        ),
        SeriesEntry(
            namespace="mixed_search",
            code="B.1",
            title="plain title for B",
        ),
    ]
    asyncio.run(cat.add_all(entries))

    matches = asyncio.run(cat.search("title", limit=10))
    codes = {m.code for m in matches}
    assert codes == {"A.1", "B.1"}
