"""URL-scheme dispatch on :class:`BaseCatalog` (default behaviour) and
:class:`parsimony.Catalog` (in-process scheme map).

Heavy-dependency loading (FAISS, sentence-transformers) is skipped when
``parsimony-core[standard]`` is not installed.
"""

from __future__ import annotations

import importlib.util

import pytest

from parsimony.catalog.catalog import BaseCatalog

_HAS_STANDARD = (
    importlib.util.find_spec("faiss") is not None
    and importlib.util.find_spec("rank_bm25") is not None
    and importlib.util.find_spec("sentence_transformers") is not None
)


@pytest.mark.asyncio
async def test_base_catalog_from_url_raises_not_implemented() -> None:
    with pytest.raises(NotImplementedError, match="from_url is not implemented"):
        await BaseCatalog.from_url("hf://owner/repo")


@pytest.mark.asyncio
async def test_url_scheme_validation_requires_scheme() -> None:
    if not _HAS_STANDARD:
        pytest.skip("parsimony-core[standard] not installed")
    from parsimony import Catalog

    with pytest.raises(ValueError, match="must include a scheme"):
        await Catalog.from_url("/local/path")


@pytest.mark.asyncio
async def test_unknown_scheme_raises() -> None:
    if not _HAS_STANDARD:
        pytest.skip("parsimony-core[standard] not installed")
    from parsimony import Catalog

    with pytest.raises(ValueError, match="Unsupported catalog URL scheme"):
        await Catalog.from_url("xyz://some/path")


@pytest.mark.asyncio
async def test_s3_scheme_signals_not_implemented() -> None:
    if not _HAS_STANDARD:
        pytest.skip("parsimony-core[standard] not installed")
    from parsimony import Catalog

    with pytest.raises(NotImplementedError, match="not yet implemented"):
        await Catalog.from_url("s3://bucket/key")
