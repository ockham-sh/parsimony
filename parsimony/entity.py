"""Discoverable identity records for catalog indexing and search."""

from __future__ import annotations

import re
from typing import Any, NamedTuple

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


def normalize_namespace(value: str) -> str:
    """Normalize namespace strings: lowercase snake_case."""

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


class EntityRef(NamedTuple):
    """Canonical in-memory identity key: ``(namespace, code)``.

    A plain tuple subclass — compares and hashes equal to a bare
    ``(namespace, code)`` tuple, so it drops into any dict/mapping key
    position without a migration. Named so ``Result.entities`` and
    ``Result.data`` can share one key type for their parallel views.
    """

    namespace: str
    code: str


def entity_key(namespace: str, code: str) -> EntityRef:
    """Canonical in-memory key for ``(namespace, code)``."""

    return EntityRef(normalize_namespace(namespace), normalize_entity_code(code))


class Entity(BaseModel):
    """Normalized discoverable identity: namespace, code, title, and metadata."""

    model_config = ConfigDict(extra="forbid")

    namespace: str
    code: str
    title: str
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("namespace")
    @classmethod
    def _normalize_namespace_field(cls, value: str) -> str:
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


def field_value(entity: Entity, field: str) -> Any:
    if field == "namespace":
        return entity.namespace
    if field == "code":
        return entity.code
    if field == "title":
        return entity.title
    return entity.metadata.get(field)


def require_scalar_text(
    value: Any,
    *,
    field: str,
    identity: str | None = None,
) -> str | None:
    """Canonicalize a searchable/filterable cell to trimmed text, or ``None``.

    Indexed, ranked, and structured-filter columns are scalar: ``None``,
    ``str``, ``bool``, or a number. Nested values (list/tuple/set/dict) remain
    legal in :attr:`Entity.metadata` for display and storage, but they are not
    a searchable field — operators that need that surface must expose an
    intentional derived scalar (for example ``tags_text="energy prices"``).

    ``math.inf`` / ``NaN`` are rejected: they are not meaningful catalog text.
    """
    if value is None:
        return None
    if isinstance(value, (list, tuple, set, frozenset, dict)):
        where = f" on {identity}" if identity else ""
        raise ValueError(
            f"Searchable field {field!r}{where} must be scalar "
            f"(str/number/bool/null); got {type(value).__name__}. "
            "Keep nested data in metadata for display, or expose a derived "
            "scalar field for search."
        )
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        if isinstance(value, float) and (value != value or value in (float("inf"), float("-inf"))):
            where = f" on {identity}" if identity else ""
            raise ValueError(f"Searchable field {field!r}{where} has non-finite number {value!r}")
        return str(value)
    if isinstance(value, str):
        text = value.strip()
        return text if text else None
    # Arrow / numpy scalars often expose .item(); tolerate that once.
    if hasattr(value, "item") and not isinstance(value, (bytes, bytearray)):
        try:
            return require_scalar_text(value.item(), field=field, identity=identity)
        except (ValueError, TypeError):
            pass
    where = f" on {identity}" if identity else ""
    raise ValueError(
        f"Searchable field {field!r}{where} must be scalar (str/number/bool/null); got {type(value).__name__}"
    )


def field_values(entity: Entity, field: str) -> list[str]:
    """Return searchable scalar values for *field* on *entity* (zero or one string).

    Nested metadata is rejected: searchable fields are scalar. See
    :func:`require_scalar_text`.
    """
    text = require_scalar_text(
        field_value(entity, field),
        field=field,
        identity=f"{entity.namespace}/{entity.code}",
    )
    return [text] if text is not None else []


def field_text(entity: Entity, field: str) -> str:
    """Return searchable text for *field* on *entity*."""

    parts = field_values(entity, field)
    return " ".join(parts)


def _metadata_value(value: Any) -> Any:
    """Normalize a cell value to a plain Python scalar/list for :class:`Entity` metadata."""
    if hasattr(value, "tolist"):
        value = value.tolist()
    elif hasattr(value, "item"):
        value = value.item()
    if isinstance(value, list):
        return [item.item() if hasattr(item, "item") else item for item in value]
    return value


__all__ = [
    "Entity",
    "EntityRef",
    "code_token",
    "entity_key",
    "field_text",
    "field_value",
    "field_values",
    "normalize_entity_code",
    "normalize_namespace",
    "require_scalar_text",
]
