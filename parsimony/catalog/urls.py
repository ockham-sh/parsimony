"""Catalog URL parsing and snapshot load/save dispatch."""

from __future__ import annotations

import asyncio
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from parsimony.catalog.catalog import Catalog

REPO_TYPE = "dataset"


@dataclass(frozen=True)
class ParsedCatalogURL:
    """Decomposition of a catalog URL."""

    scheme: str
    root: str
    sub: str


def parse_catalog_url(url: str | Path) -> ParsedCatalogURL:
    """Parse ``scheme://...`` into :class:`ParsedCatalogURL`."""
    import os

    url = str(url)
    if "://" not in url:
        path_str = str(Path(url).absolute()) if os.path.isabs(url) else url
        return ParsedCatalogURL(scheme="file", root=path_str, sub="")
    scheme, _, rest = url.partition("://")
    scheme = scheme.lower()
    if not scheme:
        raise ValueError(f"URL has empty scheme: {url!r}")
    if not rest:
        raise ValueError(f"URL has empty path: {url!r}")
    rest = rest.rstrip("/")
    if scheme == "hf":
        parts = rest.split("/")
        if len(parts) < 2 or not parts[0] or not parts[1]:
            raise ValueError(f"hf:// URL needs '<org>/<repo>'; got {url!r}")
        return ParsedCatalogURL(scheme=scheme, root="/".join(parts[:2]), sub="/".join(parts[2:]))
    return ParsedCatalogURL(scheme=scheme, root=rest, sub="")


async def _load_file(root: str, sub: str) -> Catalog:
    from parsimony.catalog.catalog import Catalog

    path = Path(root) / sub if sub else Path(root)
    if not path.exists():
        raise FileNotFoundError(f"Catalog directory does not exist: {path}")
    return await Catalog._load_from_path(path)


async def _save_file(catalog: Catalog, root: str, sub: str, *, builder: str | None = None) -> None:
    target = Path(root) / sub if sub else Path(root)
    await catalog._save_to_path(target, builder=builder)


async def _load_hf(root: str, sub: str) -> Catalog:
    from huggingface_hub import snapshot_download

    from parsimony import cache
    from parsimony.catalog.catalog import Catalog

    cache_dir = cache.catalogs_dir()
    if sub:
        local = await asyncio.to_thread(
            lambda: Path(
                snapshot_download(
                    repo_id=root,
                    repo_type=REPO_TYPE,
                    cache_dir=cache_dir,
                    allow_patterns=[f"{sub}/*"],
                )
            )
        )
        return await Catalog._load_from_path(local / sub)
    local = await asyncio.to_thread(
        lambda: Path(snapshot_download(repo_id=root, repo_type=REPO_TYPE, cache_dir=cache_dir))
    )
    return await Catalog._load_from_path(local)


async def _save_hf(catalog: Catalog, root: str, sub: str, *, builder: str | None = None) -> None:
    from huggingface_hub import HfApi

    with tempfile.TemporaryDirectory() as tmpdir:
        staging = Path(tmpdir) / "snapshot"
        await catalog._save_to_path(staging, builder=builder)

        def _upload() -> None:
            api = HfApi()
            api.create_repo(repo_id=root, repo_type=REPO_TYPE, exist_ok=True)
            api.upload_folder(
                folder_path=str(staging),
                repo_id=root,
                repo_type=REPO_TYPE,
                path_in_repo=sub or None,
            )

        await asyncio.to_thread(_upload)


async def _dispatch_load(url: str) -> Catalog:
    parsed = parse_catalog_url(url)
    if parsed.scheme == "file":
        return await _load_file(parsed.root, parsed.sub)
    if parsed.scheme == "hf":
        return await _load_hf(parsed.root, parsed.sub)
    raise ValueError(f"Unsupported catalog URL scheme {parsed.scheme!r}. Supported: ['file', 'hf']")
