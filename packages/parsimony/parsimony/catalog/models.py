from __future__ import annotations

__all__ = [
    "IndexResult",
    "SeriesEntry",
    "SeriesMatch",
    "catalog_key",
    "code_token",
    "normalize_code",
    "normalize_entity_code",
    "series_match_from_entry",
]

import re
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
        raise ValueError("Value must be lowercase snake_case (letters, numbers, underscores)")
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


class SeriesEntry(BaseModel):
    """Canonical catalog row: indexing input and persisted store shape.

    Identity is ``(namespace, code)``. ``code`` is the connector-native identifier string
    for that namespace (e.g. FRED ``GDPC1``, FMP ``AAPL``).
    """

    namespace: str
    code: str
    title: str
    tags: list[str] = Field(default_factory=list)
    description: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)
    embedding: list[float] | None = None

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

    def embedding_text(self) -> str:
        """Compose the text an embedder should index for this entry.

        The default joins title, metadata key/value pairs, and tags with a
        ``" | "`` separator. Subclassing or overriding requires reindexing the
        catalog, so this representation is intentionally fixed.
        """
        parts = [self.title]
        if self.metadata:
            meta_parts = [f"{k}: {v}" for k, v in self.metadata.items() if v is not None]
            if meta_parts:
                parts.append(", ".join(meta_parts))
        if self.tags:
            parts.append(f"tags: {', '.join(self.tags)}")
        return " | ".join(parts)


class SeriesMatch(BaseModel):
    """Search projection: catalog row fields needed for display + fetch, plus similarity."""

    namespace: str
    code: str
    title: str
    similarity: float
    tags: list[str] = Field(default_factory=list)
    description: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)

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
        description=entry.description,
        metadata=dict(entry.metadata),
    )


class IndexResult(BaseModel):
    """Statistics from an indexing run."""

    total: int = 0
    indexed: int = 0
    skipped: int = 0
    errors: int = 0
