from __future__ import annotations

import json
import re
from abc import ABC, abstractmethod
from typing import Any

from pydantic import BaseModel, Field, field_validator

CODE_PATTERN = re.compile(r"^[a-z][a-z0-9_]*$")


def code_token(value: str) -> str:
    """Normalize a string for use in series codes (provider-side derivation)."""
    token = value.strip().lower()
    token = token.replace("-", "_").replace(" ", "_").replace(".", "_")
    token = re.sub(r"[^a-z0-9_]", "_", token)
    token = re.sub(r"_+", "_", token).strip("_")
    if not token:
        return "unknown"
    if token[0].isdigit():
        return f"v_{token}"
    return token


def normalize_code(value: str) -> str:
    """Normalize catalog namespace strings: lowercase snake_case."""
    normalized = value.strip()
    if not normalized:
        raise ValueError("Value must be non-empty")
    if not CODE_PATTERN.fullmatch(normalized):
        raise ValueError(
            "Value must be lowercase snake_case (letters, numbers, underscores)"
        )
    return normalized


def normalize_entity_code(value: str) -> str:
    """Normalize entity `code` within a namespace: non-empty trimmed string (connector-native)."""
    normalized = value.strip()
    if not normalized:
        raise ValueError("code must be non-empty")
    return normalized


def catalog_key(namespace: str, code: str) -> tuple[str, str]:
    """Canonical in-memory key for (namespace, code)."""
    return (normalize_code(namespace), normalize_entity_code(code))


def normalize_series_catalog_row(row: dict[str, Any]) -> dict[str, Any]:
    """Normalize a series_catalog row from Supabase/Postgres (embedding may be JSON string)."""
    normalized = dict(row)
    embedding = normalized.get("embedding")
    if isinstance(embedding, str):
        stripped = embedding.strip()
        if stripped:
            normalized["embedding"] = [float(v) for v in json.loads(stripped)]
        else:
            normalized["embedding"] = None
    return normalized


class SeriesEntry(BaseModel):
    """Canonical catalog row: indexing input and persisted store shape.

    Identity is ``(namespace, code)``. ``code`` is the connector-native identifier string
    for that namespace (e.g. FRED ``GDPC1``, FMP ``AAPL``).

    ``observable_id`` is reserved for a future knowledge-layer identifier (e.g.
    linking catalog rows to concepts in a graph). The framework persists it but
    does not assign or resolve observables yet.
    """

    namespace: str
    code: str
    title: str
    tags: list[str] = Field(default_factory=list)
    description: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)
    properties: dict[str, Any] = Field(default_factory=dict)
    embedding: list[float] | None = None
    observable_id: str | None = None

    @field_validator("namespace")
    @classmethod
    def _normalize_namespace(cls, value: str) -> str:
        return normalize_code(value)

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


class SeriesMatch(BaseModel):
    """Search projection: catalog row fields needed for display + fetch, plus similarity."""

    namespace: str
    code: str
    title: str
    similarity: float
    tags: list[str] = Field(default_factory=list)
    description: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)
    properties: dict[str, Any] = Field(default_factory=dict)

    @field_validator("namespace")
    @classmethod
    def _normalize_namespace(cls, value: str) -> str:
        return normalize_code(value)

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


def series_match_from_entry(entry: SeriesEntry, *, similarity: float) -> SeriesMatch:
    """Build a :class:`SeriesMatch` from a stored catalog row."""
    return SeriesMatch(
        namespace=entry.namespace,
        code=entry.code,
        title=entry.title,
        similarity=similarity,
        tags=list(entry.tags),
        metadata=dict(entry.metadata),
    )


class IndexResult(BaseModel):
    """Statistics from an indexing run."""

    total: int = 0
    indexed: int = 0
    skipped: int = 0
    errors: int = 0


class EmbeddingProvider(ABC):
    """Text-to-vector embedding for catalog search."""

    @property
    @abstractmethod
    def dimension(self) -> int:
        ...

    @abstractmethod
    async def embed_texts(self, texts: list[str]) -> list[list[float]]:
        """Embeddings for corpus documents (indexing)."""
        ...

    @abstractmethod
    async def embed_query(self, query: str) -> list[float]:
        """Single embedding optimized for retrieval queries."""
        ...
