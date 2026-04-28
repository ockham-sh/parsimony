"""Tests for :class:`parsimony.embedder.OnnxEmbedder`.

Exercises the ONNX-backed embedding path end-to-end (export → quantize →
encode → roundtrip through Catalog). Skipped when the optional
``parsimony-core[standard-onnx]`` dependency chain is not installed —
the kernel's default test profile does not pull onnxruntime/optimum.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from parsimony.embedder import (
    DEFAULT_MODEL,
    EmbeddingProvider,
    OnnxEmbedder,
    SentenceTransformerEmbedder,
)

pytest.importorskip("onnxruntime", reason="parsimony-core[standard-onnx] not installed")
pytest.importorskip("optimum", reason="parsimony-core[standard-onnx] not installed")


def test_onnx_embedder_satisfies_protocol() -> None:
    emb = OnnxEmbedder()  # lazy — must not load any heavy deps
    assert isinstance(emb, EmbeddingProvider)


def test_onnx_embedder_info_identifies_package(tmp_path: Path) -> None:
    emb = OnnxEmbedder(cache_dir=tmp_path, quantize=True)
    info = emb.info()
    assert info.model == DEFAULT_MODEL
    assert info.dim == 384  # all-MiniLM-L6-v2 is 384-dim
    assert info.normalize is True
    assert info.package is not None and "standard-onnx" in info.package


async def test_onnx_embed_texts_roundtrip(tmp_path: Path) -> None:
    emb = OnnxEmbedder(cache_dir=tmp_path, quantize=True)
    texts = ["10 year euro area yield curve", "5 year AAA spot rate", "apple stock price"]
    vectors = await emb.embed_texts(texts)
    assert len(vectors) == len(texts)
    assert all(len(v) == 384 for v in vectors)
    # L2-normalized — norm ~ 1
    import math
    for v in vectors:
        assert math.isclose(math.sqrt(sum(x * x for x in v)), 1.0, abs_tol=1e-3)


async def test_onnx_query_matches_related_doc_better(tmp_path: Path) -> None:
    """Semantic sanity: a yield-curve query should score higher against a
    yield-curve document than against an unrelated one."""
    emb = OnnxEmbedder(cache_dir=tmp_path, quantize=True)
    docs = await emb.embed_texts(
        [
            "10-year euro area yield curve spot rate",
            "Apple Inc. common stock close price",
        ]
    )
    query = await emb.embed_query("euro area 10Y bond yield")

    def dot(a: list[float], b: list[float]) -> float:
        return sum(x * y for x, y in zip(a, b, strict=True))

    related = dot(query, docs[0])
    unrelated = dot(query, docs[1])
    assert related > unrelated, (
        f"expected yield-curve doc to outrank stock doc "
        f"(related={related}, unrelated={unrelated})"
    )


async def test_onnx_and_sentence_transformer_agree_on_ordering(tmp_path: Path) -> None:
    """Same model via two backends should rank documents identically."""
    onnx = OnnxEmbedder(cache_dir=tmp_path, quantize=False)  # fp32 onnx vs fp32 st
    st = SentenceTransformerEmbedder()

    docs = ["euro area yield curve", "apple stock price", "german labour productivity"]
    onnx_vecs = await onnx.embed_texts(docs)
    st_vecs = await st.embed_texts(docs)

    def dot(a: list[float], b: list[float]) -> float:
        return sum(x * y for x, y in zip(a, b, strict=True))

    query = "10 year bund yield"
    onnx_q = await onnx.embed_query(query)
    st_q = await st.embed_query(query)

    onnx_rank = sorted(range(len(docs)), key=lambda i: -dot(onnx_q, onnx_vecs[i]))
    st_rank = sorted(range(len(docs)), key=lambda i: -dot(st_q, st_vecs[i]))
    assert onnx_rank == st_rank, f"ONNX and SentenceTransformer rankings diverge: {onnx_rank} vs {st_rank}"


async def test_onnx_catalog_end_to_end(tmp_path: Path) -> None:
    """Build a catalog with OnnxEmbedder, save/load, search — top-1 must be correct."""
    from parsimony.catalog import Catalog, SeriesEntry

    cache_dir = tmp_path / "onnx-cache"
    emb = OnnxEmbedder(cache_dir=cache_dir, quantize=True)
    cat = Catalog("test", embedder=emb)
    await cat.add(
        [
            SeriesEntry(namespace="test", code="YC_10Y", title="10 year euro area yield curve spot rate"),
            SeriesEntry(namespace="test", code="AAPL", title="Apple Inc. common stock close price"),
            SeriesEntry(namespace="test", code="HICP", title="Harmonised index of consumer prices euro area"),
        ]
    )
    bundle_dir = tmp_path / "bundle"
    await cat.save(bundle_dir)

    # Reload with the same OnnxEmbedder (explicitly passed) and search.
    emb2 = OnnxEmbedder(cache_dir=cache_dir, quantize=True)
    loaded = await Catalog.load(bundle_dir, embedder=emb2)
    hits = await loaded.search("euro area 10Y bond yield", 1)
    assert hits
    assert hits[0].code == "YC_10Y"
