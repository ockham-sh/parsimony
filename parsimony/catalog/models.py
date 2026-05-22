"""Catalog row models and field normalization helpers."""

from __future__ import annotations

import re
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

CODE_PATTERN = re.compile(r"^[a-z][a-z0-9_]*$")


def code_token(value: str) -> str:
    """Normalize a string for use in provider-derived codes."""

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
    """Normalize entity codes: non-empty trimmed strings."""

    normalized = value.strip()
    if not normalized:
        raise ValueError("code must be non-empty")
    return normalized


def catalog_key(namespace: str, code: str) -> tuple[str, str]:
    """Canonical in-memory key for ``(namespace, code)``."""

    return (normalize_code(namespace), normalize_entity_code(code))


class CatalogEntry(BaseModel):
    """Canonical catalog row."""

    model_config = ConfigDict(extra="forbid")

    namespace: str
    code: str
    title: str
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
    """Resolved search result: catalog entry fields plus final score."""

    model_config = ConfigDict(extra="forbid")

    namespace: str
    code: str
    title: str
    score: float
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


def catalog_match_from_entry(entry: CatalogEntry, *, score: float) -> CatalogMatch:
    """Build a :class:`CatalogMatch` from a stored catalog row."""

    return CatalogMatch(
        namespace=entry.namespace,
        code=entry.code,
        title=entry.title,
        score=score,
        metadata=dict(entry.metadata),
    )


def _field_value(entry: CatalogEntry, field: str) -> Any:
    if field == "namespace":
        return entry.namespace
    if field == "code":
        return entry.code
    if field == "title":
        return entry.title
    return entry.metadata.get(field)


def field_text(entry: CatalogEntry, field: str) -> str:
    """Return searchable text for *field* on *entry*."""

    value = _field_value(entry, field)
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, (list, tuple, set)):
        return " ".join(str(item) for item in value if item is not None)
    if isinstance(value, dict):
        return " ".join(f"{key}: {item}" for key, item in value.items() if item is not None)
    return str(value)
