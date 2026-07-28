"""Catalog contracts: row-backend configuration."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


@dataclass(frozen=True, slots=True)
class CatalogBackendConfig:
    """Runtime row-store configuration for a catalog.

    ``kind`` selects the execution model, not a storage hint: ``"memory"``
    means the indexed entities are themselves the rows; ``"parquet"`` means
    rows live in a flat parquet file attached after build. The value is
    persisted into every snapshot's ``meta.json`` (see
    :class:`~parsimony.catalog.storage.BackendMeta`), so the two literals are
    frozen for schema v1.

    ``code_column`` / ``title_column`` map the logical ``code`` and ``title``
    search fields onto the parquet columns that carry them.
    """

    kind: Literal["memory", "parquet"] = "memory"
    rows_path: str | None = None
    namespace: str | None = None
    code_column: str = "code"
    title_column: str = "title"


__all__ = [
    "CatalogBackendConfig",
]
