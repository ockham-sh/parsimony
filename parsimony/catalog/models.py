"""Catalog search models and query errors."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

from parsimony.entity import Entity, field_text, field_values, normalize_entity_code, normalize_namespace


class SearchDiagnostic(BaseModel):
    """Metadata about how a catalog query was executed."""

    model_config = ConfigDict(extra="forbid")

    mode: Literal["broad", "structured"]
    notes: list[str] = Field(default_factory=list)


class UnknownIndexedFieldError(ValueError):
    """Structured query references a field with no configured index."""


class BroadSearchUnavailableError(ValueError):
    """Plain-text query requested but this catalog has no broad-search field."""


class BroadSearchConfigError(ValueError):
    """Catalog default_field is set but no index covers that field."""


class CatalogMatch(BaseModel):
    """Resolved search result: entity fields plus final score."""

    model_config = ConfigDict(extra="forbid")

    namespace: str
    code: str
    title: str
    score: float
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


def catalog_match_from_entity(entity: Entity, *, score: float) -> CatalogMatch:
    """Build a :class:`CatalogMatch` from a stored entity."""

    return CatalogMatch(
        namespace=entity.namespace,
        code=entity.code,
        title=entity.title,
        score=score,
        metadata=dict(entity.metadata),
    )


__all__ = [
    "BroadSearchConfigError",
    "BroadSearchUnavailableError",
    "CatalogMatch",
    "SearchDiagnostic",
    "UnknownIndexedFieldError",
    "catalog_match_from_entity",
    "field_text",
    "field_values",
    "normalize_entity_code",
    "normalize_namespace",
]
