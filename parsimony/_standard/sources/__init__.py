"""URL scheme dispatch for the standard catalog.

Schemes are resolved in-process — there is no plugin axis for sources. Adding a
new scheme means adding a handler module here, not registering an entry point.

Currently supported:

* ``file://`` — local directory snapshot.
* ``hf://``  — Hugging Face Hub dataset repository (requires ``huggingface-hub``).
* ``s3://``  — placeholder; raises an actionable ``NotImplementedError``.
"""

from __future__ import annotations

from types import ModuleType
from typing import TYPE_CHECKING, cast

from parsimony.catalog.catalog import _url_scheme

if TYPE_CHECKING:
    from parsimony._standard.catalog import Catalog
    from parsimony._standard.embedder import EmbeddingProvider


async def load_from_url(url: str, *, embedder: EmbeddingProvider | None = None) -> Catalog:
    handler = _handler(_url_scheme(url))
    return cast("Catalog", await handler.load(url, embedder=embedder))


async def push_to_url(catalog: Catalog, url: str) -> None:
    handler = _handler(_url_scheme(url))
    await handler.push(catalog, url)


def _handler(scheme: str) -> ModuleType:
    if scheme == "file":
        from parsimony._standard.sources import local

        return local
    if scheme == "hf":
        from parsimony._standard.sources import hf

        return hf
    if scheme == "s3":
        from parsimony._standard.sources import s3

        return s3
    raise ValueError(f"Unsupported catalog URL scheme {scheme!r}. Supported: 'file', 'hf', 's3' (planned).")
