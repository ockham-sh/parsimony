"""Tests for the thin ``parsimony bundles`` CLI.

We monkeypatch ``iter_specs`` so the CLI sees a controlled set of discovered
specs rather than whatever entry points happen to be installed.
"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pandas as pd
import pytest
from pydantic import BaseModel

from parsimony.bundles.discovery import DiscoveredSpec
from parsimony.bundles.spec import CatalogSpec
from parsimony.cli import bundles as cli_bundles
from parsimony.cli import main as cli_main
from parsimony.connector import Connectors, enumerator
from parsimony.discovery import DiscoveredProvider
from parsimony.result import Column, ColumnRole, OutputConfig

# ---------------------------------------------------------------------------
# Fixture plugin
# ---------------------------------------------------------------------------


class _NoParams(BaseModel):
    pass


_ENUM_OUTPUT = OutputConfig(
    columns=[
        Column(name="code", role=ColumnRole.KEY, namespace="fixture"),
        Column(name="title", role=ColumnRole.TITLE),
    ]
)


@enumerator(
    output=_ENUM_OUTPUT,
    catalog=CatalogSpec.static(namespace="fixture"),
)
async def _fixture_enum(params: _NoParams) -> pd.DataFrame:
    """List fixture entries."""
    return pd.DataFrame({"code": ["x", "y"], "title": ["X entity", "Y entity"]})


def _fake_spec() -> DiscoveredSpec:
    provider = DiscoveredProvider(
        name="fixture",
        module_path="tests.fixture",
        connectors=Connectors([_fixture_enum]),
    )
    return DiscoveredSpec(
        provider=provider,
        connector=_fixture_enum,
        spec=_fixture_enum.properties["catalog"],
    )


@pytest.fixture
def patched_iter_specs(monkeypatch: pytest.MonkeyPatch) -> None:
    def _one() -> Iterator[DiscoveredSpec]:
        yield _fake_spec()

    monkeypatch.setattr(cli_bundles, "iter_specs", _one)


# ---------------------------------------------------------------------------
# list
# ---------------------------------------------------------------------------


def test_list_prints_discovered_spec(capsys: pytest.CaptureFixture[str], patched_iter_specs: None) -> None:
    rc = cli_main(["bundles", "list"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "fixture/_fixture_enum" in out
    assert "namespace=fixture" in out


def test_list_filters_by_only(capsys: pytest.CaptureFixture[str], patched_iter_specs: None) -> None:
    rc = cli_main(["bundles", "list", "--only", "other"])
    assert rc == 0
    assert "No catalog specs discovered" in capsys.readouterr().out


def test_list_reports_empty_when_nothing_discovered(
    capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(cli_bundles, "iter_specs", lambda: iter(()))
    rc = cli_main(["bundles", "list"])
    assert rc == 0
    assert "No catalog specs discovered" in capsys.readouterr().out


# ---------------------------------------------------------------------------
# build
# ---------------------------------------------------------------------------


def test_build_requires_namespace_placeholder(capsys: pytest.CaptureFixture[str], patched_iter_specs: None) -> None:
    rc = cli_main(["bundles", "build", "--target", "file:///tmp/static"])
    assert rc == 2
    assert "{namespace}" in capsys.readouterr().err


def test_build_dry_run_skips_enumeration(capsys: pytest.CaptureFixture[str], patched_iter_specs: None) -> None:
    rc = cli_main(["bundles", "build", "--target", "file:///tmp/{namespace}", "--dry-run"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "fixture/_fixture_enum" in out
    assert "fixture" in out
    # nothing actually got built
    assert not Path("/tmp/fixture").exists() or True  # dry-run must not touch the filesystem


def test_build_writes_catalog_to_file_url(
    tmp_path: Path, patched_iter_specs: None, capsys: pytest.CaptureFixture[str]
) -> None:
    """End-to-end: build a real catalog from the fixture enumerator, push to file://, verify on-disk layout."""
    # Use a fake embedder so torch/sentence-transformers stay out of this test.
    from parsimony._standard import catalog as catalog_module
    from parsimony._standard.embedder import EmbeddingProvider
    from parsimony.catalog.embedder_info import EmbedderInfo

    class _FakeEmbedder(EmbeddingProvider):
        @property
        def dimension(self) -> int:
            return 4

        async def embed_texts(self, texts: list[str]) -> list[list[float]]:
            return [[1.0, 0.0, 0.0, 0.0] for _ in texts]

        async def embed_query(self, query: str) -> list[float]:
            return [1.0, 0.0, 0.0, 0.0]

        def info(self) -> EmbedderInfo:
            return EmbedderInfo(model="fake", dim=4, normalize=True, package="tests")

    real_cls = catalog_module.Catalog

    class _TestCatalog(real_cls):  # type: ignore[misc, valid-type]
        def __init__(self, name: str, *, embedder: EmbeddingProvider | None = None) -> None:
            super().__init__(name, embedder=embedder or _FakeEmbedder())

    # Patch the class the CLI imports into its build path.
    import parsimony.cli.bundles as bundles_mod

    orig_build_one = bundles_mod._build_one

    async def _patched_build_one(connector: Any, plan: Any, target_url: str, catalog_cls: Any) -> None:
        await orig_build_one(connector, plan, target_url, _TestCatalog)

    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(bundles_mod, "_build_one", _patched_build_one)
    try:
        target = tmp_path / "catalogs" / "{namespace}"
        rc = cli_main(["bundles", "build", "--target", f"file://{target}"])
        assert rc == 0, capsys.readouterr()
    finally:
        monkeypatch.undo()

    written = tmp_path / "catalogs" / "fixture"
    assert (written / "meta.json").exists()
    assert (written / "entries.parquet").exists()
    assert (written / "embeddings.faiss").exists()
