"""Catalog search models and query errors."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

from parsimony.entity import Entity, field_text, field_values, normalize_entity_code, normalize_namespace
from parsimony.errors import InvalidParameterError


class CatalogValueMatch(BaseModel):
    """One distinct indexed value from :meth:`Catalog.search_values`.

    Results order by (coverage desc, score desc): *coverage* is the fraction of
    the query's tokens this value consumes when the value's own tokens are all
    present in the query (1.0 = exact hit), else 0.0; *score* is the honest
    fuzzy relevance within a coverage band. *matched* labels the evidence
    origin, exactly as on :class:`CatalogMatch`.
    """

    model_config = ConfigDict(extra="forbid")

    value: str
    score: float
    coverage: float = 0.0
    matched: Literal["lexical", "semantic", "both"] | None = None
    linked_value: str | None = None


class UnknownIndexedFieldError(InvalidParameterError):
    """Structured query references a field with no configured index."""

    def __init__(self, message: str) -> None:
        super().__init__("catalog", message)


class BroadSearchUnavailableError(InvalidParameterError):
    """Plain-text query requested but this catalog has no broad-search field."""

    def __init__(self, message: str) -> None:
        super().__init__("catalog", message)


class CatalogMatch(BaseModel):
    """Resolved search result: entity fields plus the ranking evidence.

    Two evidence channels plus a label. *coverage* is the fact channel: a
    field value is consumed only when every token it contains appears in the
    query (all-or-nothing per value), and coverage is the fraction of the
    query's tokens covered by the union of the row's consumed values — 1.0
    means the query is literally satisfied by this row's values, 0.0 means no
    value is contained in the query. *score* is the guess channel: per-field
    similarity (lexical + semantic), normalized 0..1 against the field's best
    hit and summed across the searched fields, so agreeing evidence
    accumulates and no single field's raw magnitude dominates; it is relative
    to this query's best hit, never absolute, never comparable across queries
    or catalogs. *matched* labels the evidence origin: "lexical" (token
    overlap), "semantic" (vector proximity), or "both" — an all-"semantic"
    result page means nothing lexically real matched, so rephrase the query
    rather than trust the order. Facts outrank guesses: on a multi-field
    surface rows order by (coverage desc, score desc); on a single-field
    surface only coverage 1.0 outranks the score order. A high-score row
    below a full-coverage row is the contract, not an anomaly.
    """

    model_config = ConfigDict(extra="forbid")

    namespace: str
    code: str
    title: str
    score: float
    coverage: float = 0.0
    matched: Literal["lexical", "semantic", "both"] | None = None
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


def catalog_match_from_entity(
    entity: Entity,
    *,
    score: float,
    coverage: float = 0.0,
    matched: Literal["lexical", "semantic", "both"] | None = None,
) -> CatalogMatch:
    """Build a :class:`CatalogMatch` from a stored entity."""

    return CatalogMatch(
        namespace=entity.namespace,
        code=entity.code,
        title=entity.title,
        score=score,
        coverage=coverage,
        matched=matched,
        metadata=dict(entity.metadata),
    )


__all__ = [
    "BroadSearchUnavailableError",
    "CatalogMatch",
    "CatalogValueMatch",
    "UnknownIndexedFieldError",
    "catalog_match_from_entity",
    "field_text",
    "field_values",
    "normalize_entity_code",
]
