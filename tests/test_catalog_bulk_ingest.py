"""Tests for :meth:`parsimony.catalog.Catalog.add_all` and the
rebuild-once :meth:`_ingest` path.

The scale problem these cover: ``_ingest`` used to call ``add`` in batches
of 100, triggering ``_rebuild_indices`` (FAISS + BM25) once per batch.
A 500k-series flow rebuilt ~5000 times. These tests lock in:

* ``add_all`` produces observable state identical to the old
  ``add``-in-a-loop pipeline (parity).
* ``_ingest`` triggers exactly one index rebuild regardless of input size.
* ``add`` retains per-call rebuild semantics (interactive callers that
  search between inserts are unchanged).
"""

from __future__ import annotations

import hashlib
from typing import Any

from parsimony.catalog import Catalog, SeriesEntry
from parsimony.embedder import EmbedderInfo


class _StubEmbedder:
    """Deterministic, dependency-free embedder for parity tests.

    Hashes the text to a fixed-dim unit vector. Deterministic so two
    catalogs embedding the same texts produce byte-identical vectors
    (and therefore identical FAISS state).
    """

    DIM = 8

    @property
    def dimension(self) -> int:
        return self.DIM

    async def embed_texts(self, texts: list[str]) -> list[list[float]]:
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
            model="stub/hash-sha256",
            dim=self.DIM,
            normalize=True,
            package="test-stub",
        )


def _make_entries(n: int, *, namespace: str = "t", prefix: str = "C") -> list[SeriesEntry]:
    return [
        SeriesEntry(
            namespace=namespace,
            code=f"{prefix}{i:04d}",
            title=f"series title {i}",
            description=f"descriptor {i} with extra vocabulary",
            tags=[f"tag-{i % 5}"],
        )
        for i in range(n)
    ]


# ---------------------------------------------------------------------------
# Parity: add_all vs. add-in-a-loop
# ---------------------------------------------------------------------------


async def test_add_all_matches_add_in_loop_state() -> None:
    """Two catalogs — one using add() in batches of 100, the other a single
    add_all() — must converge to identical observable state."""

    cat_loop = Catalog("t", embedder=_StubEmbedder())
    cat_bulk = Catalog("t", embedder=_StubEmbedder())

    entries = _make_entries(250)

    for start in range(0, len(entries), 100):
        await cat_loop.add(entries[start : start + 100])
    await cat_bulk.add_all(entries)

    assert len(cat_loop) == len(cat_bulk) == 250
    assert cat_loop.entries == cat_bulk.entries
    # pylint: disable=protected-access
    assert cat_loop._key_to_idx == cat_bulk._key_to_idx
    assert cat_loop._tokens == cat_bulk._tokens
    assert cat_loop._faiss is not None and cat_bulk._faiss is not None
    assert cat_loop._faiss.ntotal == cat_bulk._faiss.ntotal


async def test_add_all_search_parity_with_add_in_loop() -> None:
    """Hybrid-search rankings should be identical regardless of ingest path."""

    cat_loop = Catalog("t", embedder=_StubEmbedder())
    cat_bulk = Catalog("t", embedder=_StubEmbedder())

    entries = _make_entries(180)

    for start in range(0, len(entries), 100):
        await cat_loop.add(entries[start : start + 100])
    await cat_bulk.add_all(entries)

    for query in ("series title 42", "descriptor 7", "tag-3"):
        hits_loop = await cat_loop.search(query, limit=10)
        hits_bulk = await cat_bulk.search(query, limit=10)
        assert [h.code for h in hits_loop] == [h.code for h in hits_bulk]
        assert [round(h.similarity, 8) for h in hits_loop] == [round(h.similarity, 8) for h in hits_bulk]


# ---------------------------------------------------------------------------
# add_all edge cases
# ---------------------------------------------------------------------------


async def test_add_all_empty_list_is_noop() -> None:
    cat = Catalog("t", embedder=_StubEmbedder())
    await cat.add_all([])
    assert len(cat) == 0
    # pylint: disable=protected-access
    assert cat._faiss is None
    assert cat._bm25 is None


async def test_add_all_updates_existing_entries() -> None:
    cat = Catalog("t", embedder=_StubEmbedder())
    original = SeriesEntry(namespace="t", code="X", title="original")
    updated = SeriesEntry(namespace="t", code="X", title="updated")

    await cat.add_all([original])
    await cat.add_all([updated])

    assert len(cat) == 1
    assert cat.entries[0].title == "updated"


async def test_add_all_last_write_wins_within_single_call() -> None:
    """If the same key appears twice in one add_all, the last entry wins —
    same semantic as the pre-existing add()."""
    cat = Catalog("t", embedder=_StubEmbedder())
    first = SeriesEntry(namespace="t", code="X", title="first")
    second = SeriesEntry(namespace="t", code="X", title="second")

    await cat.add_all([first, second])

    assert len(cat) == 1
    assert cat.entries[0].title == "second"


async def test_add_all_mixed_insert_and_update() -> None:
    cat = Catalog("t", embedder=_StubEmbedder())
    await cat.add_all(
        [
            SeriesEntry(namespace="t", code="A", title="alpha"),
            SeriesEntry(namespace="t", code="B", title="beta"),
        ]
    )
    await cat.add_all(
        [
            SeriesEntry(namespace="t", code="A", title="alpha-v2"),  # update
            SeriesEntry(namespace="t", code="C", title="gamma"),  # insert
        ]
    )

    assert len(cat) == 3
    by_code = {e.code: e.title for e in cat.entries}
    assert by_code == {"A": "alpha-v2", "B": "beta", "C": "gamma"}


# ---------------------------------------------------------------------------
# add() back-compat: still rebuilds per call, searches work between adds
# ---------------------------------------------------------------------------


async def test_add_still_rebuilds_per_call_supporting_interleaved_search() -> None:
    cat = Catalog("t", embedder=_StubEmbedder())
    await cat.add([SeriesEntry(namespace="t", code="A", title="alpha document")])
    hits = await cat.search("alpha", limit=1)
    assert hits and hits[0].code == "A"

    await cat.add([SeriesEntry(namespace="t", code="B", title="beta document")])
    hits = await cat.search("beta", limit=1)
    assert hits and hits[0].code == "B"


# ---------------------------------------------------------------------------
# _ingest regression: rebuild-once
# ---------------------------------------------------------------------------


async def test_ingest_rebuilds_indices_exactly_once(monkeypatch: Any) -> None:
    """Regression guard: a 250-entry ingest used to trigger ~3 rebuilds
    (one per batch of 100). After the add_all refactor it must trigger
    exactly one — this is the load-bearing optimization Phase 1 ships."""

    cat = Catalog("t", embedder=_StubEmbedder())
    calls = {"n": 0}
    original = cat._rebuild_indices

    def _counting_rebuild() -> None:
        calls["n"] += 1
        original()

    monkeypatch.setattr(cat, "_rebuild_indices", _counting_rebuild)

    entries = _make_entries(250)
    # pylint: disable=protected-access
    result = await cat._ingest(entries, batch_size=100)

    assert calls["n"] == 1
    assert result.total == 250
    assert result.indexed == 250
    assert result.skipped == 0
    assert result.errors == 0


async def test_ingest_skips_existing_keys() -> None:
    """_ingest must still filter pre-existing keys before calling add_all."""
    cat = Catalog("t", embedder=_StubEmbedder())
    initial = _make_entries(50)
    await cat.add_all(initial)

    # Re-ingest the same 50 plus 20 new
    combined = initial + _make_entries(20, prefix="NEW")
    # pylint: disable=protected-access
    result = await cat._ingest(combined, batch_size=100)

    assert result.total == 70
    assert result.skipped == 50  # the initial batch was already present
    assert result.indexed == 20
    assert result.errors == 0
    assert len(cat) == 70


async def test_ingest_dry_run_does_not_touch_indices(monkeypatch: Any) -> None:
    cat = Catalog("t", embedder=_StubEmbedder())
    calls = {"n": 0}
    original = cat._rebuild_indices

    def _counting_rebuild() -> None:
        calls["n"] += 1
        original()

    monkeypatch.setattr(cat, "_rebuild_indices", _counting_rebuild)

    entries = _make_entries(30)
    # pylint: disable=protected-access
    result = await cat._ingest(entries, batch_size=10, dry_run=True)

    assert result.total == 30
    assert result.indexed == 30
    assert result.skipped == 0
    assert calls["n"] == 0
    assert len(cat) == 0


async def test_ingest_empty_input_short_circuits() -> None:
    cat = Catalog("t", embedder=_StubEmbedder())
    # pylint: disable=protected-access
    result = await cat._ingest([], batch_size=100)
    assert result.total == 0
    assert result.indexed == 0
    assert result.skipped == 0
