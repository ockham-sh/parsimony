"""Tests for ``parse_catalog_url`` and ``Catalog.load`` / ``Catalog.save``.

Covers the multi-bundle layout: a single ``hf://org/repo`` (or local
directory) holding many namespace subfolders, each loadable via
``hf://org/repo/<sub>``.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import numpy as np
import pytest

from parsimony.catalog import (
    BM25Index,
    Catalog,
    CatalogEntry,
    VectorIndex,
    parse_catalog_url,
)
from parsimony.embedder import EmbedderInfo

# ---------------------------------------------------------------------------
# parse_catalog_url
# ---------------------------------------------------------------------------


class TestParseCatalogURL:
    def test_file_url_root_only(self) -> None:
        parsed = parse_catalog_url("file:///tmp/repo")
        assert parsed.scheme == "file"
        assert parsed.root == "/tmp/repo"
        assert parsed.sub == ""

    def test_file_url_trailing_slash_stripped(self) -> None:
        parsed = parse_catalog_url("file:///tmp/repo/")
        assert parsed.root == "/tmp/repo"

    def test_file_url_keeps_full_path_as_root(self) -> None:
        # No sub-path semantics for file://: the URL points at the
        # snapshot directly. Callers wanting a multi-bundle local layout
        # compose the full path themselves.
        parsed = parse_catalog_url("file:///tmp/repo/bundle_a")
        assert parsed.root == "/tmp/repo/bundle_a"
        assert parsed.sub == ""

    def test_hf_url_root_only(self) -> None:
        parsed = parse_catalog_url("hf://org/repo")
        assert parsed.scheme == "hf"
        assert parsed.root == "org/repo"
        assert parsed.sub == ""

    def test_hf_url_with_sub(self) -> None:
        parsed = parse_catalog_url("hf://org/repo/bundle")
        assert parsed.root == "org/repo"
        assert parsed.sub == "bundle"

    def test_hf_url_with_nested_sub(self) -> None:
        parsed = parse_catalog_url("hf://org/repo/nested/bundle")
        assert parsed.root == "org/repo"
        assert parsed.sub == "nested/bundle"

    def test_hf_url_trailing_slash_stripped(self) -> None:
        parsed = parse_catalog_url("hf://org/repo/bundle/")
        assert parsed.root == "org/repo"
        assert parsed.sub == "bundle"

    def test_hf_url_missing_repo_segment_raises(self) -> None:
        with pytest.raises(ValueError, match="<org>/<repo>"):
            parse_catalog_url("hf://org")

    def test_hf_url_empty_org_raises(self) -> None:
        with pytest.raises(ValueError, match="<org>/<repo>"):
            parse_catalog_url("hf:///repo")

    def test_no_scheme_parses_as_file(self) -> None:
        parsed = parse_catalog_url("/tmp/repo")
        assert parsed.scheme == "file"
        assert parsed.root == "/tmp/repo"
        assert parsed.sub == ""

    def test_empty_path_raises(self) -> None:
        with pytest.raises(ValueError, match="empty path"):
            parse_catalog_url("file://")


# ---------------------------------------------------------------------------
# Catalog.load + save (file://)
# ---------------------------------------------------------------------------


class _StubEmbedder:
    """Deterministic, dependency-free embedder for the round-trip test."""

    DIM = 8

    @property
    def dimension(self) -> int:
        return self.DIM

    def info(self) -> EmbedderInfo:
        return EmbedderInfo(model="stub", dim=self.DIM, normalize=False, package="test")

    async def embed_texts(self, texts: list[str]) -> list[list[float]]:
        # Map each text to a unique unit-ish vector keyed off its hash.
        vectors: list[list[float]] = []
        for text in texts:
            h = abs(hash(text))
            vec = np.zeros(self.DIM, dtype=np.float32)
            vec[h % self.DIM] = 1.0
            vectors.append(vec.tolist())
        return vectors

    async def embed_query(self, query: str) -> list[float]:
        (vector,) = await self.embed_texts([query])
        return vector


def _entry(namespace: str, code: str, title: str) -> CatalogEntry:
    return CatalogEntry(namespace=namespace, code=code, title=title)


@pytest.mark.asyncio
async def test_file_roundtrip_no_sub(tmp_path: Path) -> None:
    catalog = Catalog(name="solo", indexes=[VectorIndex("title_vector", field="title", embedder=_StubEmbedder())])
    catalog.set_entries([_entry("solo", "A", "alpha")])
    await catalog.build()
    await catalog.save(f"file://{tmp_path}/snapshot")
    assert (tmp_path / "snapshot" / "indexes" / "title_vector" / "index.faiss").exists()
    loaded = await Catalog.load(f"file://{tmp_path}/snapshot")
    assert len(loaded) == 1
    assert loaded.entries[0].code == "A"


@pytest.mark.asyncio
async def test_file_url_pointing_at_subdir_loads_directly(tmp_path: Path) -> None:
    """For file://, multi-bundle layouts work via the path itself —
    no special sub semantics, the URL points straight at the bundle."""
    bundle = tmp_path / "multi" / "bundle_a"
    catalog = Catalog(name="bundle_a")
    catalog.set_entries([_entry("bundle_a", "X", "x-title")])
    await catalog.build()
    await catalog.save(f"file://{bundle}")
    assert (bundle / "meta.json").exists()
    loaded = await Catalog.load(f"file://{bundle}")
    assert loaded.entries[0].code == "X"


@pytest.mark.asyncio
async def test_file_roundtrip_preserves_catalog_default_field(tmp_path: Path) -> None:
    catalog = Catalog(
        name="ranked",
        indexes=[
            BM25Index("title_bm25", field="title"),
            BM25Index("description_bm25", field="description"),
        ],
        default_field="description",
    )
    catalog.set_entries(
        [
            CatalogEntry(namespace="ranked", code="A", title="alpha", metadata={"description": "gamma"}),
            CatalogEntry(namespace="ranked", code="B", title="gamma", metadata={"description": "alpha"}),
            CatalogEntry(namespace="ranked", code="C", title="gamma", metadata={"description": "gamma"}),
        ]
    )
    await catalog.build()
    await catalog.save(f"file://{tmp_path}/snapshot")

    loaded = await Catalog.load(f"file://{tmp_path}/snapshot")
    assert loaded.default_field == "description"
    hits = await loaded.search("alpha", limit=1)

    assert hits[0].code == "B"


@pytest.mark.asyncio
async def test_file_missing_path_raises(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        await Catalog.load(f"file://{tmp_path}/does-not-exist")


# ---------------------------------------------------------------------------
# hf:// dispatch — handler call-site contract via monkeypatching
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_hf_load_threads_sub_into_handler(monkeypatch: pytest.MonkeyPatch) -> None:
    """``Catalog.load("hf://org/repo/bundle")`` must reach
    ``_load_hf`` with ``root='org/repo'`` and ``sub='bundle'``."""
    captured: dict[str, Any] = {}

    async def _spy_load_hf(root: str, sub: str) -> Any:
        captured["root"] = root
        captured["sub"] = sub
        return object()  # Catalog isn't actually constructed here.

    from parsimony import catalog as catalog_module

    real_handlers = catalog_module._url_handlers()
    monkeypatch.setattr(
        catalog_module,
        "_url_handlers",
        lambda: {**real_handlers, "hf": (_spy_load_hf, real_handlers["hf"][1])},
    )

    await Catalog.load("hf://org/repo/bundle")

    assert captured == {"root": "org/repo", "sub": "bundle"}


@pytest.mark.asyncio
async def test_hf_load_no_sub(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}

    async def _spy_load_hf(root: str, sub: str) -> Any:
        captured["root"] = root
        captured["sub"] = sub
        return object()

    from parsimony import catalog as catalog_module

    real_handlers = catalog_module._url_handlers()
    monkeypatch.setattr(
        catalog_module,
        "_url_handlers",
        lambda: {**real_handlers, "hf": (_spy_load_hf, real_handlers["hf"][1])},
    )

    await Catalog.load("hf://org/repo")

    assert captured == {"root": "org/repo", "sub": ""}


@pytest.mark.asyncio
async def test_hf_save_threads_sub_into_handler(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}

    async def _spy_save_hf(catalog: Any, root: str, sub: str, *, builder: str | None = None) -> None:
        captured["root"] = root
        captured["sub"] = sub

    from parsimony import catalog as catalog_module

    real_handlers = catalog_module._url_handlers()
    monkeypatch.setattr(
        catalog_module,
        "_url_handlers",
        lambda: {**real_handlers, "hf": (real_handlers["hf"][0], _spy_save_hf)},
    )

    catalog = Catalog(name="x")
    catalog.set_entries([_entry("x", "A", "alpha")])
    await catalog.build()
    await catalog.save("hf://org/repo/bundle")

    assert captured == {"root": "org/repo", "sub": "bundle"}
