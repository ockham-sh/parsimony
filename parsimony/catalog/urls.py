"""Catalog URL parsing primitives.

Pure module — no dependency on :class:`Catalog`. Snapshot load/save helpers
that *do* need ``Catalog`` live in :mod:`parsimony.catalog.catalog` to keep the
import graph acyclic.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

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
