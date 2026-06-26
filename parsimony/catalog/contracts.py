"""Catalog contracts: filter types and row-backend configuration."""

from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Literal, Protocol, runtime_checkable

FilterSpec = Mapping[str, Sequence[str]]
"""Exact filter: column or entity field name -> allowed values (AND-composed)."""


@dataclass(frozen=True, slots=True)
class CatalogBackendConfig:
    """Persistence hints for a catalog row backend."""

    kind: Literal["memory", "parquet"] = "memory"
    rows_path: str | None = None
    namespace: str | None = None
    code_column: str = "code"
    title_column: str = "title"
    field_links: dict[str, str] = field(default_factory=dict)


@runtime_checkable
class RowBackend(Protocol):
    """Row storage and exact-filter execution for a catalog."""

    def column_names(self) -> list[str]: ...
    def count(self) -> int: ...
    def iter_rows(
        self,
        *,
        filter_spec: FilterSpec | None = None,
        columns: Sequence[str] | None = None,
    ) -> Iterator[dict[str, Any]]: ...
    def match_namespace(self, namespace: str) -> bool: ...


__all__ = [
    "CatalogBackendConfig",
    "FilterSpec",
    "RowBackend",
]
