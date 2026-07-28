"""Catalog search models and query errors."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

from parsimony.entity import Entity, field_text, field_values, normalize_entity_code, normalize_namespace
from parsimony.errors import InvalidParameterError


class ComponentSearchDetail(BaseModel):
    """One index component's contribution to a ranked value or field hit.

    ``raw_score`` is component-native (BM25 magnitude, cosine, …) and is **not**
    comparable across kinds. ``rank`` is the 1-based competition rank within that
    component's own candidate table for this query.
    """

    model_config = ConfigDict(extra="forbid")

    kind: Literal["bm25", "vector"]
    raw_score: float
    rank: int


class FieldSearchDetail(BaseModel):
    """How one searched field contributed evidence for a ranked row (or value)."""

    model_config = ConfigDict(extra="forbid")

    field: str
    value: str
    weight: float
    relevance: float
    exact: bool
    fused_rank: int
    components: list[ComponentSearchDetail] = Field(default_factory=list)


class SearchDetail(BaseModel):
    """Inspectable ranking evidence for one match — not a correctness signal.

    Absence of a component means the value was not retained in that component's
    top-k for this query, not that there is no relationship. Commit from provider
    metadata; use this only to explain ordering.
    """

    model_config = ConfigDict(extra="forbid")

    candidate_limit: int
    fields: list[FieldSearchDetail] = Field(default_factory=list)


class CatalogValueMatch(BaseModel):
    """One distinct indexed value from :meth:`Catalog.search_values`.

    Results order by (exact desc, score desc). *exact* is the fact: this value is
    the one the query literally names (case-folded equality), so it outranks every
    fuzzy candidate. *score* is query-relative rank relevance in ``(0, 1]`` for
    this field — ``1.0`` is the best value returned for this query.
    """

    model_config = ConfigDict(extra="forbid")

    value: str
    score: float
    exact: bool = False
    search_detail: SearchDetail | None = None
    linked_value: str | None = None


class UnknownIndexedFieldError(InvalidParameterError):
    """A caller named a field to score that has no configured index.

    Raised for ``field=`` and for a ``fields=`` key on ``multi_field_search``.
    Filter keys name row columns rather than indexes, so an unknown one
    surfaces from the filter layer instead.
    """

    def __init__(self, message: str) -> None:
        super().__init__("catalog", message)


class BroadSearchUnavailableError(InvalidParameterError):
    """Plain-text query requested but this catalog has no broad-search field."""

    def __init__(self, message: str) -> None:
        super().__init__("catalog", message)


class CatalogMatch(BaseModel):
    """Resolved search result: entity fields plus ranking evidence.

    *score* is a guess: independently produced rankings are fused by weighted
    Reciprocal Rank Fusion — ranks, never raw magnitudes — then divided by this
    query's best retained hit so every ranked score sits in ``(0, 1]`` with the
    top row at ``1.0``. It is never absolute, never comparable across queries or
    catalogs. A filter-only (unranked) read leaves *score* and *search_detail*
    as ``None``.

    Ranked rows order by (score desc, namespace, code). Ranking policy beyond
    relevance belongs to the caller: pin what you know with a filter, and commit
    from provider metadata rather than treating *score* or *search_detail* as
    correctness.
    """

    model_config = ConfigDict(extra="forbid")

    namespace: str
    code: str
    title: str
    score: float | None
    search_detail: SearchDetail | None = None
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
    score: float | None,
    search_detail: SearchDetail | None = None,
) -> CatalogMatch:
    """Build a :class:`CatalogMatch` from a stored entity."""

    return CatalogMatch(
        namespace=entity.namespace,
        code=entity.code,
        title=entity.title,
        score=score,
        search_detail=search_detail,
        metadata=dict(entity.metadata),
    )


__all__ = [
    "BroadSearchUnavailableError",
    "CatalogMatch",
    "CatalogValueMatch",
    "ComponentSearchDetail",
    "FieldSearchDetail",
    "SearchDetail",
    "UnknownIndexedFieldError",
    "catalog_match_from_entity",
    "field_text",
    "field_values",
    "normalize_entity_code",
]
