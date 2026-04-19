"""Extract catalog (namespace, code) from connector param models using Namespace metadata."""

from __future__ import annotations

from typing import Any, Union, get_args, get_origin

from pydantic import BaseModel

from parsimony.connector import Namespace


def first_namespace_field(model_type: type[BaseModel]) -> tuple[str, str] | None:
    """Return ``(field_name, namespace)`` for the unique field with :class:`Namespace` metadata.

    Raises:
        ValueError: when multiple fields carry ``Namespace`` metadata.
    """
    matches: list[tuple[str, str]] = []
    seen_fields: set[str] = set()
    for name, finfo in model_type.model_fields.items():
        # Pydantic v2 stores ``Annotated`` extras on ``FieldInfo.metadata``.
        for meta in getattr(finfo, "metadata", None) or []:
            if isinstance(meta, Namespace):
                if name not in seen_fields:
                    matches.append((name, meta.name))
                    seen_fields.add(name)
                break
        ann = finfo.annotation
        if ann is None:
            continue
        ann = _strip_optional(ann)
        args = get_args(ann)
        if not args:
            continue
        for meta in args[1:]:
            if isinstance(meta, Namespace):
                if name not in seen_fields:
                    matches.append((name, meta.name))
                    seen_fields.add(name)
                break
    if not matches:
        return None
    if len(matches) > 1:
        fields = [name for name, _ in matches]
        raise ValueError(
            f"{model_type.__name__} has multiple Namespace-annotated fields: {fields}. "
            "Exactly one is required for auto-index identity."
        )
    return matches[0]


def _strip_optional(ann: Any) -> Any:
    origin = get_origin(ann)
    if origin is Union:
        args = [a for a in get_args(ann) if a is not type(None)]
        if len(args) == 1:
            return args[0]
    return ann


def identity_from_params(model_type: type[BaseModel], params: BaseModel) -> tuple[str, str] | None:
    """Return ``(namespace, code)`` from validated params if a Namespace-annotated field exists."""
    found = first_namespace_field(model_type)
    if found is None:
        return None
    field_name, namespace = found
    raw = getattr(params, field_name, None)
    if raw is None:
        return None
    code = str(raw).strip()
    if not code:
        return None
    return (namespace, code)
