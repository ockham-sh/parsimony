"""Catalog search models and query errors."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator

from parsimony.entity import Entity, field_text, field_values, normalize_entity_code, normalize_namespace
from parsimony.errors import ConnectorError, InvalidParameterError


class CatalogValueMatch(BaseModel):
    """One distinct indexed value from :meth:`Catalog.search_values`.

    Results order by (coverage desc, score desc): *coverage* is the fraction of
    the query's tokens this value consumes when the value's own tokens are all
    present in the query (1.0 = exact hit), else 0.0; *score* is the honest
    fuzzy relevance within a coverage band.
    """

    model_config = ConfigDict(extra="forbid")

    value: str
    score: float
    coverage: float = 0.0
    linked_value: str | None = None


class UnknownIndexedFieldError(InvalidParameterError):
    """Structured query references a field with no configured index."""

    def __init__(self, message: str) -> None:
        super().__init__("catalog", message)


class BroadSearchUnavailableError(InvalidParameterError):
    """Plain-text query requested but this catalog has no broad-search field."""

    def __init__(self, message: str) -> None:
        super().__init__("catalog", message)


class BroadSearchConfigError(ConnectorError):
    """Catalog default_field is set but no index covers that field."""

    def __init__(self, message: str) -> None:
        super().__init__(message, provider="catalog")


class CatalogMatch(BaseModel):
    """Resolved search result: entity fields plus the ranking evidence.

    Results order by (coverage desc, score desc). *coverage* is lexical
    evidence — the fraction of the query's tokens consumed by the union of the
    row's fully-consumed field values (1.0 = the query is literally this row's
    values; 0.0 = no field value is contained in the query). *score* is
    statistical evidence — the sum over the searched fields of the row's
    normalized per-field relevance (each field contributes 0..1 of its own
    best match, so agreeing evidence accumulates and no single field's raw
    magnitude dominates). A high-score row can rank below a low-score row
    with more coverage; that is the contract, not an anomaly.
    """

    model_config = ConfigDict(extra="forbid")

    namespace: str
    code: str
    title: str
    score: float
    coverage: float = 0.0
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("namespace")
    @classmethod
    def _normalize_namespace(cls, value: str) -> str:
        return normalize_namespace(value)

    @field_validator("code")
    @classmethod
    def _normalize_code_field(cls, value: str) -> str:
        return normalize_entity_code(value)

    @field_validator("title")
    @classmethod
    def _validate_title(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError("title must be non-empty")
        return normalized


def catalog_match_from_entity(entity: Entity, *, score: float, coverage: float = 0.0) -> CatalogMatch:
    """Build a :class:`CatalogMatch` from a stored entity."""

    return CatalogMatch(
        namespace=entity.namespace,
        code=entity.code,
        title=entity.title,
        score=score,
        coverage=coverage,
        metadata=dict(entity.metadata),
    )


__all__ = [
    "BroadSearchConfigError",
    "BroadSearchUnavailableError",
    "CatalogMatch",
    "CatalogValueMatch",
    "UnknownIndexedFieldError",
    "catalog_match_from_entity",
    "field_text",
    "field_values",
    "normalize_entity_code",
    "normalize_namespace",
]
