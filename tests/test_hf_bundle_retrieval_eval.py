"""Retrieval-quality eval for HF bundles.

Opt-in: only runs when ``PARSIMONY_EVAL_HF=1`` is set, because each case
downloads a live bundle (~MB each) and runs a sentence-transformers
encode, which requires network + faiss + torch. In CI we wire this up
behind a non-blocking check that runs on release tags.

The fixture encodes a small golden set per Tier A namespace:

    (query, expected_code_in_top_k, min_similarity)

A regression here is either a model-revision drift, a text-composition
change in :func:`build_embedding_text`, or a FAISS param bump. Bundle
integrity tests will pass; this is the only gate that notices.
"""

from __future__ import annotations

import os

import pytest

pytestmark = pytest.mark.skipif(
    os.environ.get("PARSIMONY_EVAL_HF") != "1",
    reason="retrieval eval requires PARSIMONY_EVAL_HF=1 (downloads live bundles)",
)


GOLDEN: dict[str, list[tuple[str, str, float]]] = {
    # Each entry: (query, substring that must appear in some top-3 code/title,
    #              min top similarity)
    "snb": [
        ("policy rate", "policy", 0.30),
        ("exchange rate", "exchange", 0.30),
    ],
    "riksbank": [
        ("inflation", "inflation", 0.30),
        ("krona rate", "exchange", 0.25),
    ],
    "boc": [
        ("overnight rate", "overnight", 0.30),
        ("gdp", "gdp", 0.30),
    ],
    "rba": [
        ("cash rate", "cash", 0.30),
        ("unemployment", "unemploy", 0.25),
    ],
    "bde": [
        ("inflation", "inflation", 0.25),
        ("interest rate", "interest", 0.25),
    ],
    "treasury": [
        ("10 year yield", "yield", 0.25),
        ("bill rate", "bill", 0.25),
    ],
}


@pytest.fixture(scope="module")
def live_catalog():
    """Construct a live Catalog wired to HF bundles for all Tier A namespaces."""
    from parsimony.catalog.catalog import Catalog
    from parsimony.embeddings.sentence_transformers import (
        SentenceTransformersEmbeddingProvider,
    )
    from parsimony.stores.hf_bundle.store import HFBundleCatalogStore

    provider = SentenceTransformersEmbeddingProvider(
        model_id="sentence-transformers/all-MiniLM-L6-v2",
        revision="c9745ed1d9f207416be6d2e6f8de32d1f16199bf",
        expected_dim=384,
    )
    store = HFBundleCatalogStore(embeddings=provider)
    return Catalog(store, embeddings=provider)


@pytest.mark.parametrize(
    ("namespace", "query", "needle", "min_sim"),
    [(ns, q, n, s) for ns, rows in GOLDEN.items() for (q, n, s) in rows],
)
@pytest.mark.asyncio
async def test_golden_retrieval(live_catalog, namespace, query, needle, min_sim):
    results = await live_catalog.search(query, limit=3, namespaces=[namespace])
    assert results, f"no results for {namespace!r} / {query!r}"
    # Top similarity floor: weak relevance still produces something, but
    # below this floor the encoder is broken or the text-template drifted.
    assert results[0].similarity >= min_sim, (
        f"top similarity {results[0].similarity:.3f} below floor {min_sim} "
        f"for {namespace!r}/{query!r} — embed pipeline regression suspected"
    )
    # The needle should appear somewhere in top-3 codes or titles.
    hay = " ".join(f"{m.code} {m.title}".lower() for m in results)
    assert needle in hay, (
        f"expected substring {needle!r} in top-3 for {namespace!r}/{query!r}, "
        f"got: {[(m.code, m.title) for m in results]}"
    )
