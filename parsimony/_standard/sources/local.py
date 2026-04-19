"""``file://`` scheme: local directory snapshot."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from parsimony._standard.catalog import Catalog


def _strip(url: str) -> str:
    prefix = "file://"
    if not url.startswith(prefix):
        raise ValueError(f"Expected URL to start with {prefix!r}; got {url!r}")
    return url[len(prefix) :]


async def load(url: str) -> Catalog:
    from parsimony._standard.catalog import Catalog

    path = Path(_strip(url))
    if not path.exists():
        raise FileNotFoundError(f"Catalog directory does not exist: {path}")
    return await Catalog.load(path)


async def push(catalog: Catalog, url: str) -> None:
    await catalog.save(Path(_strip(url)))
