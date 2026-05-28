"""Discoverable identity records for catalog indexing and search."""

from __future__ import annotations

import re
from collections.abc import Sequence
from typing import Any

import pandas as pd
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


def entity_key(namespace: str, code: str) -> tuple[str, str]:
    """Canonical in-memory key for ``(namespace, code)``."""

    return (normalize_namespace(namespace), normalize_entity_code(code))


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


def field_values(entity: Entity, field: str) -> list[str]:
    """Return searchable values for *field* on *entity* (one or more strings)."""

    value = field_value(entity, field)
    if value is None:
        return []
    if isinstance(value, str):
        text = value.strip()
        return [text] if text else []
    if isinstance(value, (list, tuple, set)):
        return [str(item).strip() for item in value if item is not None and str(item).strip()]
    if isinstance(value, dict):
        return [f"{key}: {item}".strip() for key, item in value.items() if item is not None and str(item).strip()]
    text = str(value).strip()
    return [text] if text else []


def field_text(entity: Entity, field: str) -> str:
    """Return searchable text for *field* on *entity* (joined multi-values)."""

    parts = field_values(entity, field)
    return " ".join(parts)


def _metadata_value(value: Any) -> Any:
    if hasattr(value, "tolist"):
        value = value.tolist()
    elif hasattr(value, "item"):
        value = value.item()
    if isinstance(value, list):
        return [item.item() if hasattr(item, "item") else item for item in value]
    return value


def entities_from_dataframe(
    df: pd.DataFrame,
    *,
    namespace: str,
    key_column: str,
    title_column: str | None,
    metadata_columns: Sequence[str],
    namespace_column: str | None = None,
) -> list[Entity]:
    """Build :class:`Entity` rows from a DataFrame with explicit column roles."""

    if df.empty:
        return []
    use_row_namespace = namespace == "__row__" and namespace_column is not None
    static_ns = None if use_row_namespace else normalize_namespace(namespace)
    if use_row_namespace and namespace_column not in df.columns:
        raise ValueError(
            f"DataFrame missing per-row namespace column {namespace_column!r}. Available: {list(df.columns)}"
        )
    if key_column not in df.columns:
        raise ValueError(f"DataFrame missing key column {key_column!r}. Available: {list(df.columns)}")
    title_name = title_column
    if title_name is not None and title_name not in df.columns:
        raise ValueError(f"DataFrame missing title column {title_name!r}. Available: {list(df.columns)}")
    meta_names = [name for name in metadata_columns if name != namespace_column]
    for meta_name in meta_names:
        if meta_name not in df.columns:
            raise ValueError(f"DataFrame missing metadata column {meta_name!r}. Available: {list(df.columns)}")

    key_name = key_column
    needed_cols = {key_name, *meta_names}
    if namespace_column:
        needed_cols.add(namespace_column)
    if title_name:
        needed_cols.add(title_name)
    sub_df = df[list(needed_cols)]
    grouped = sub_df.groupby(key_name, sort=False, dropna=True)

    entries: list[Entity] = []
    for raw_code, sub in grouped:
        code = normalize_entity_code(str(raw_code))
        if title_name and title_name in sub.columns:
            titles = sub[title_name].dropna()
            title = str(titles.iloc[0]) if len(titles) > 0 else code
        else:
            title = code
        metadata: dict[str, Any] = {}
        for meta_name in meta_names:
            vals = sub[meta_name].dropna()
            if len(vals) == 0:
                continue
            normalized = [_metadata_value(v) for v in vals]
            distinct = {repr(v) for v in normalized}
            if len(distinct) > 1:
                raise ValueError(
                    f"Column {meta_name!r} is not entity metadata for code {code!r}: "
                    f"values vary within the entity key. Use ColumnRole.DATA or choose a more specific entity key."
                )
            metadata[meta_name] = normalized[0]
        row_ns: str
        if use_row_namespace:
            assert namespace_column is not None
            ns_vals = sub[namespace_column].dropna()
            if len(ns_vals) == 0:
                raise ValueError(f"Missing namespace value in column {namespace_column!r} for code {code!r}")
            row_ns = normalize_namespace(str(ns_vals.iloc[0]))
        else:
            assert static_ns is not None
            row_ns = static_ns
        entries.append(Entity(namespace=row_ns, code=code, title=title, metadata=metadata))
    return entries


__all__ = [
    "Entity",
    "code_token",
    "entities_from_dataframe",
    "entity_key",
    "field_text",
    "field_value",
    "field_values",
    "normalize_entity_code",
    "normalize_namespace",
]
