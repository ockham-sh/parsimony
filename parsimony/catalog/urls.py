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
    """Decomposition of a catalog URL.

    ``revision`` is the pinned Hugging Face git revision (branch, tag, or commit
    SHA) parsed from an ``hf://<org>/<repo>@<revision>`` URL, or ``None`` to
    track the dataset's default branch. Pinning a commit SHA makes a remote load
    reproducible and tamper-resistant: the content digest only proves a snapshot
    is self-consistent, not that an upstream re-push hasn't swapped it.
    """

    scheme: str
    root: str
    sub: str
    revision: str | None = None


def parse_catalog_url(url: str | Path) -> ParsedCatalogURL:
    """Parse ``scheme://...`` into :class:`ParsedCatalogURL`.

    For ``hf://`` URLs, an optional ``@<revision>`` suffix on the ``<org>/<repo>``
    segment pins the load to a Hugging Face git revision
    (e.g. ``hf://acme/macro@v1`` or ``hf://acme/macro@<commit-sha>/sub``).
    """
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
        org, repo = parts[0], parts[1]
        revision: str | None = None
        if "@" in repo:
            repo, _, revision = repo.partition("@")
            if not repo or not revision:
                raise ValueError(f"hf:// URL with '@' needs '<org>/<repo>@<revision>'; got {url!r}")
        return ParsedCatalogURL(
            scheme=scheme,
            root=f"{org}/{repo}",
            sub="/".join(parts[2:]),
            revision=revision,
        )
    return ParsedCatalogURL(scheme=scheme, root=rest, sub="")
