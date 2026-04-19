"""``s3://`` scheme placeholder.

Implementation is intentionally deferred: lift the ``hf`` handler, swap the
download/upload primitives for ``s3fs`` calls, and add ``s3fs`` to
``parsimony-core[s3]``. Until then we surface a clear error rather than failing on
an opaque import.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from parsimony._standard.catalog import Catalog
    from parsimony._standard.embedder import EmbeddingProvider


async def load(url: str, *, embedder: EmbeddingProvider | None = None) -> Catalog:
    raise NotImplementedError(
        "s3:// catalog loading is not yet implemented. Track support in the parsimony issue tracker."
    )


async def push(catalog: Catalog, url: str) -> None:
    raise NotImplementedError(
        "s3:// catalog publishing is not yet implemented. Track support in the parsimony issue tracker."
    )
