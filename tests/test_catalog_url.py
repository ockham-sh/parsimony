"""Tests for ``parse_catalog_url`` and ``Catalog.from_url`` / ``Catalog.push``.

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
    Catalog,
    SeriesEntry,
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

    def test_no_scheme_raises(self) -> None:
        with pytest.raises(ValueError, match="must include a scheme"):
            parse_catalog_url("/tmp/repo")

    def test_empty_path_raises(self) -> None:
        with pytest.raises(ValueError, match="empty path"):
            parse_catalog_url("file://")


# ---------------------------------------------------------------------------
# Catalog.from_url + push (file://)
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


def _entry(namespace: str, code: str, title: str) -> SeriesEntry:
    return SeriesEntry(namespace=namespace, code=code, title=title)


@pytest.mark.asyncio
async def test_file_roundtrip_no_sub(tmp_path: Path) -> None:
    catalog = Catalog(name="solo", embedder=_StubEmbedder())
    await catalog.add([_entry("solo", "A", "alpha")])
    await catalog.push(f"file://{tmp_path}/snapshot")
    loaded = await Catalog.from_url(f"file://{tmp_path}/snapshot", embedder=_StubEmbedder())
    assert len(loaded) == 1
    assert loaded.entries[0].code == "A"


@pytest.mark.asyncio
async def test_file_url_pointing_at_subdir_loads_directly(tmp_path: Path) -> None:
    """For file://, multi-bundle layouts work via the path itself —
    no special sub semantics, the URL points straight at the bundle."""
    bundle = tmp_path / "multi" / "bundle_a"
    catalog = Catalog(name="bundle_a", embedder=_StubEmbedder())
    await catalog.add([_entry("bundle_a", "X", "x-title")])
    await catalog.push(f"file://{bundle}")
    assert (bundle / "meta.json").exists()
    loaded = await Catalog.from_url(f"file://{bundle}", embedder=_StubEmbedder())
    assert loaded.entries[0].code == "X"


@pytest.mark.asyncio
async def test_file_missing_path_raises(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        await Catalog.from_url(f"file://{tmp_path}/does-not-exist", embedder=_StubEmbedder())


# ---------------------------------------------------------------------------
# hf:// dispatch — handler call-site contract via monkeypatching
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_hf_load_threads_sub_into_handler(monkeypatch: pytest.MonkeyPatch) -> None:
    """``Catalog.from_url("hf://org/repo/bundle")`` must reach
    ``_load_hf`` with ``root='org/repo'`` and ``sub='bundle'``."""
    captured: dict[str, Any] = {}

    async def _spy_load_hf(root: str, sub: str, *, embedder: Any = None) -> Any:
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

    await Catalog.from_url("hf://org/repo/bundle")

    assert captured == {"root": "org/repo", "sub": "bundle"}


@pytest.mark.asyncio
async def test_hf_load_no_sub(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}

    async def _spy_load_hf(root: str, sub: str, *, embedder: Any = None) -> Any:
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

    await Catalog.from_url("hf://org/repo")

    assert captured == {"root": "org/repo", "sub": ""}


@pytest.mark.asyncio
async def test_hf_push_threads_sub_into_handler(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}

    async def _spy_push_hf(catalog: Any, root: str, sub: str) -> None:
        captured["root"] = root
        captured["sub"] = sub

    from parsimony import catalog as catalog_module

    real_handlers = catalog_module._url_handlers()
    monkeypatch.setattr(
        catalog_module,
        "_url_handlers",
        lambda: {**real_handlers, "hf": (real_handlers["hf"][0], _spy_push_hf)},
    )

    catalog = Catalog(name="x", embedder=_StubEmbedder())
    await catalog.push("hf://org/repo/bundle")

    assert captured == {"root": "org/repo", "sub": "bundle"}
