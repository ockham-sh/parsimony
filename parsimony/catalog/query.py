"""Structured catalog query parsing."""

from __future__ import annotations

import re
from dataclasses import dataclass

from parsimony.catalog.models import UnknownIndexedFieldError


@dataclass(frozen=True)
class StructuredQuery:
    clauses: list[tuple[str, list[str]]]  # [(field, [value, ...]), ...]


def parse_query(q: str, known_fields: set[str]) -> StructuredQuery | None:
    if not re.search(r"^\s*\w+\s*:", q):
        return None

    clauses: list[tuple[str, list[str]]] = []
    parts = q.split("&&")
    for part in parts:
        part = part.strip()
        if not part:
            continue
        if ":" not in part:
            raise ValueError(f"Malformed clause in structured query: {part!r}")
        field, _, value_str = part.partition(":")
        field = field.strip()
        if not field:
            raise ValueError(f"Empty field in clause: {part!r}")
        values = [val.strip() for val in value_str.split(",") if val.strip()]
        if not values:
            raise ValueError(f"No values provided for field {field!r} in structured query")
        clauses.append((field, values))

    if not clauses:
        return None

    unknown = [field for field, _ in clauses if field not in known_fields]
    if unknown:
        indexed = ", ".join(f"'{f}'" for f in sorted(known_fields))
        bad = ", ".join(f"'{f}'" for f in unknown)
        raise UnknownIndexedFieldError(
            f"Field {bad} is not indexed on this catalog. Indexed fields: [{indexed}]"
        )

    return StructuredQuery(clauses)
