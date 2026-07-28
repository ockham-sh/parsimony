"""Composable, backend-neutral catalog filters.

One filter expression selects rows in either catalog layout. The same tree
compiles to a :mod:`pyarrow.dataset` predicate for parquet pushdown and
evaluates directly against an ordinary Python mapping for in-memory rows, so a
filter never leaks the storage backend into a connector schema.

Three ways to spell the same thing::

    {"geo": "DE", "frequency": ["M", "Q"]}          # mapping shorthand (equality only)
    F("geo").eq("DE") & F("frequency").is_in(["M", "Q"])
    {"all": [{"field": "geo", "eq": "DE"},          # serializable expression
             {"field": "frequency", "in": ["M", "Q"]}]}

Pattern predicates use the typed builder or expression form — not the shorthand::

    F("code").prefix("D.USD.")
    F("code").contains("EUR")
    F("key").matches(r"^D\\.[A-Z]{3}\\.EUR\\.")
    {"field": "code", "prefix": "D.USD."}

The shorthand is exact-only: separate keys are ANDed, and a list of values is
membership (OR) within one field. Nested Boolean logic and pattern ops need the
expression form. ``all`` / ``any`` / ``field`` are reserved at the top of a
mapping, so a column literally named one of those must be filtered through the
typed form.

Field names are *logical*: ``code`` and ``title`` address whatever physical
columns a catalog maps them to. :meth:`Filter.rename` performs that mapping at
the catalog boundary, once, before either backend sees the tree.
"""

from __future__ import annotations

import re
from abc import ABC, abstractmethod
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, TypeAlias

import pyarrow.compute as pc
import pyarrow.dataset as ds

from parsimony.errors import InvalidParameterError

#: Anything accepted where a filter is expected: a built tree, the mapping
#: shorthand, or the serializable expression form.
FilterLike: TypeAlias = "Filter | Mapping[str, Any]"

_RESERVED_KEYS = frozenset({"all", "any", "field"})


class Filter(ABC):
    """A Boolean row predicate that both catalog layouts can execute."""

    @abstractmethod
    def matches(self, row: Mapping[str, Any]) -> bool:
        """Evaluate against one row mapping (missing or null never matches)."""

    @abstractmethod
    def to_arrow(self, columns: Iterable[str]) -> ds.Expression:
        """Compile to a parquet predicate, validating fields against *columns*."""

    @abstractmethod
    def fields(self) -> frozenset[str]:
        """Every field name this filter reads."""

    @abstractmethod
    def rename(self, resolve: Callable[[str], str]) -> Filter:
        """Return an equivalent filter with every field name mapped by *resolve*."""

    def __and__(self, other: Filter) -> Filter:
        return AllOf((self, other))

    def __or__(self, other: Filter) -> Filter:
        return AnyOf((self, other))


def _scalar_cell(row: Mapping[str, Any], field: str) -> str | None:
    cell = row.get(field)
    if cell is None:
        return None
    if isinstance(cell, (list, tuple, set, frozenset, dict)):
        raise InvalidParameterError(
            "catalog",
            f"Filter field {field!r} must be scalar "
            f"(str/number/bool/null); got {type(cell).__name__}. "
            "Nested metadata is display-only — expose a derived scalar field "
            "for structured filtering.",
        )
    return str(cell)


def _require_field(field: str, columns: Iterable[str]) -> None:
    known = set(columns)
    if field not in known:
        raise InvalidParameterError(
            "catalog",
            f"Unknown filter column {field!r}. Available columns: {sorted(known)}",
        )


def _require_field_name(field: str) -> None:
    if not field:
        raise InvalidParameterError("catalog", "A filter condition requires a field name")


def _require_nonempty_text(field: str, kind: str, value: str) -> None:
    if value == "":
        raise InvalidParameterError("catalog", f"Filter {kind} on {field!r} must be non-empty")


@dataclass(frozen=True, slots=True)
class FieldIn(Filter):
    """``field`` holds one of ``values`` — exact equality or membership."""

    field: str
    values: tuple[str, ...]

    def __post_init__(self) -> None:
        _require_field_name(self.field)
        if not self.values:
            raise InvalidParameterError(
                "catalog",
                f"Filter on {self.field!r} has no values. An empty value list would silently "
                "drop the constraint; omit the field instead.",
            )

    def matches(self, row: Mapping[str, Any]) -> bool:
        cell = _scalar_cell(row, self.field)
        if cell is None:
            return False
        return cell in set(self.values)

    def to_arrow(self, columns: Iterable[str]) -> ds.Expression:
        _require_field(self.field, columns)
        return ds.field(self.field).isin(list(self.values))

    def fields(self) -> frozenset[str]:
        return frozenset({self.field})

    def rename(self, resolve: Callable[[str], str]) -> Filter:
        return FieldIn(resolve(self.field), self.values)


@dataclass(frozen=True, slots=True)
class FieldPrefix(Filter):
    """``field`` text starts with ``prefix`` (exact, case-sensitive)."""

    field: str
    prefix: str

    def __post_init__(self) -> None:
        _require_field_name(self.field)
        _require_nonempty_text(self.field, "prefix", self.prefix)

    def matches(self, row: Mapping[str, Any]) -> bool:
        cell = _scalar_cell(row, self.field)
        return cell is not None and cell.startswith(self.prefix)

    def to_arrow(self, columns: Iterable[str]) -> ds.Expression:
        _require_field(self.field, columns)
        return pc.starts_with(ds.field(self.field), self.prefix)

    def fields(self) -> frozenset[str]:
        return frozenset({self.field})

    def rename(self, resolve: Callable[[str], str]) -> Filter:
        return FieldPrefix(resolve(self.field), self.prefix)


@dataclass(frozen=True, slots=True)
class FieldContains(Filter):
    """``field`` text contains ``substring`` (exact, case-sensitive)."""

    field: str
    substring: str

    def __post_init__(self) -> None:
        _require_field_name(self.field)
        _require_nonempty_text(self.field, "contains", self.substring)

    def matches(self, row: Mapping[str, Any]) -> bool:
        cell = _scalar_cell(row, self.field)
        return cell is not None and self.substring in cell

    def to_arrow(self, columns: Iterable[str]) -> ds.Expression:
        _require_field(self.field, columns)
        return pc.match_substring(ds.field(self.field), self.substring)

    def fields(self) -> frozenset[str]:
        return frozenset({self.field})

    def rename(self, resolve: Callable[[str], str]) -> Filter:
        return FieldContains(resolve(self.field), self.substring)


@dataclass(frozen=True, slots=True)
class FieldMatches(Filter):
    """``field`` text matches ``pattern`` as a Python/Arrow regex (``re.search`` semantics)."""

    field: str
    pattern: str

    def __post_init__(self) -> None:
        _require_field_name(self.field)
        _require_nonempty_text(self.field, "match", self.pattern)
        try:
            re.compile(self.pattern)
        except re.error as exc:
            raise InvalidParameterError(
                "catalog",
                f"Invalid regex for filter on {self.field!r}: {exc}",
            ) from exc

    def matches(self, row: Mapping[str, Any]) -> bool:
        cell = _scalar_cell(row, self.field)
        if cell is None:
            return False
        return re.search(self.pattern, cell) is not None

    def to_arrow(self, columns: Iterable[str]) -> ds.Expression:
        _require_field(self.field, columns)
        return pc.match_substring_regex(ds.field(self.field), self.pattern)

    def fields(self) -> frozenset[str]:
        return frozenset({self.field})

    def rename(self, resolve: Callable[[str], str]) -> Filter:
        return FieldMatches(resolve(self.field), self.pattern)


@dataclass(frozen=True, slots=True)
class AllOf(Filter):
    """Every member must match (AND)."""

    filters: tuple[Filter, ...]

    def __post_init__(self) -> None:
        _require_members(self.filters, "all")

    def matches(self, row: Mapping[str, Any]) -> bool:
        return all(item.matches(row) for item in self.filters)

    def to_arrow(self, columns: Iterable[str]) -> ds.Expression:
        known = set(columns)
        return _combine([item.to_arrow(known) for item in self.filters], conjunction=True)

    def fields(self) -> frozenset[str]:
        return frozenset().union(*(item.fields() for item in self.filters))

    def rename(self, resolve: Callable[[str], str]) -> Filter:
        return AllOf(tuple(item.rename(resolve) for item in self.filters))


@dataclass(frozen=True, slots=True)
class AnyOf(Filter):
    """At least one member must match (OR)."""

    filters: tuple[Filter, ...]

    def __post_init__(self) -> None:
        _require_members(self.filters, "any")

    def matches(self, row: Mapping[str, Any]) -> bool:
        return any(item.matches(row) for item in self.filters)

    def to_arrow(self, columns: Iterable[str]) -> ds.Expression:
        known = set(columns)
        return _combine([item.to_arrow(known) for item in self.filters], conjunction=False)

    def fields(self) -> frozenset[str]:
        return frozenset().union(*(item.fields() for item in self.filters))

    def rename(self, resolve: Callable[[str], str]) -> Filter:
        return AnyOf(tuple(item.rename(resolve) for item in self.filters))


class F:
    """Field handle for building conditions: ``F("geo").eq("DE")``."""

    __slots__ = ("_field",)

    def __init__(self, field: str) -> None:
        self._field = field

    def eq(self, value: Any) -> Filter:
        return FieldIn(self._field, (_coerce(value),))

    def is_in(self, values: Sequence[Any]) -> Filter:
        return FieldIn(self._field, normalize_values(values))

    def prefix(self, value: Any) -> Filter:
        return FieldPrefix(self._field, _coerce(value))

    def contains(self, value: Any) -> Filter:
        return FieldContains(self._field, _coerce(value))

    def matches(self, pattern: Any) -> Filter:
        return FieldMatches(self._field, _coerce(pattern))


def all_of(*filters: Filter) -> Filter:
    """AND several filters, collapsing a single argument."""
    return filters[0] if len(filters) == 1 else AllOf(tuple(filters))


def any_of(*filters: Filter) -> Filter:
    """OR several filters, collapsing a single argument."""
    return filters[0] if len(filters) == 1 else AnyOf(tuple(filters))


def as_filter(spec: FilterLike | None) -> Filter | None:
    """Normalize any accepted filter spelling into a filter tree.

    Returns ``None`` for ``None`` and for an empty mapping — "no constraint" —
    so a caller can pass an unset filter through unchanged. Every other input
    must name at least one field with at least one value; a malformed or empty
    condition raises rather than quietly matching every row.
    """
    if spec is None:
        return None
    if isinstance(spec, Filter):
        return spec
    if not isinstance(spec, Mapping):
        raise InvalidParameterError(
            "catalog",
            f"filter must be a mapping or a Filter expression, got {type(spec).__name__}",
        )
    if not spec:
        return None
    if _RESERVED_KEYS & set(spec):
        return _from_expression(spec)
    return all_of(*(FieldIn(field, normalize_values(values)) for field, values in spec.items()))


def _from_expression(spec: Mapping[str, Any]) -> Filter:
    keys = set(spec)
    if keys == {"all"} or keys == {"any"}:
        key = next(iter(keys))
        members = spec[key]
        if not isinstance(members, Sequence) or isinstance(members, (str, bytes)):
            raise InvalidParameterError("catalog", f"filter {key!r} must hold a list of filters")
        parsed = [_expect_filter(member) for member in members]
        _require_members(tuple(parsed), key)
        return all_of(*parsed) if key == "all" else any_of(*parsed)
    if keys == {"field", "eq"}:
        return FieldIn(str(spec["field"]), (_coerce(spec["eq"]),))
    if keys == {"field", "in"}:
        return FieldIn(str(spec["field"]), normalize_values(spec["in"]))
    if keys == {"field", "prefix"}:
        return FieldPrefix(str(spec["field"]), _coerce(spec["prefix"]))
    if keys == {"field", "contains"}:
        return FieldContains(str(spec["field"]), _coerce(spec["contains"]))
    if keys == {"field", "match"}:
        return FieldMatches(str(spec["field"]), _coerce(spec["match"]))
    raise InvalidParameterError(
        "catalog",
        f"Unsupported filter expression keys {sorted(keys)}. Supported: "
        "{'all': [...]}, {'any': [...]}, {'field': ..., 'eq': ...}, {'field': ..., 'in': [...]}, "
        "{'field': ..., 'prefix': ...}, {'field': ..., 'contains': ...}, {'field': ..., 'match': ...}",
    )


def _expect_filter(member: Any) -> Filter:
    parsed = as_filter(member)
    if parsed is None:
        raise InvalidParameterError("catalog", "A Boolean filter group must not contain an empty filter")
    return parsed


def _require_members(filters: tuple[Filter, ...], label: str) -> None:
    if len(filters) < 2:
        raise InvalidParameterError(
            "catalog",
            f"A {label!r} filter group needs at least two members; use the condition directly instead.",
        )


def _coerce(value: Any) -> str:
    if value is None:
        raise InvalidParameterError("catalog", "Filter values must not be null")
    if isinstance(value, (list, tuple, set, frozenset, Mapping)):
        raise InvalidParameterError("catalog", f"Filter value must be a scalar, got {type(value).__name__}")
    return str(value)


def normalize_values(values: Any) -> tuple[str, ...]:
    """Normalize one field's allowed values, keeping a bare string atomic.

    Shared by the filter tree and by ``Catalog.search``'s exact filter so both
    treat ``"DE"`` as one value rather than the characters ``["D", "E"]``.
    """
    if isinstance(values, (str, bytes)) or not isinstance(values, (list, tuple, set, frozenset)):
        return (_coerce(values),)
    # dict.fromkeys: de-duplicate without disturbing the caller's order.
    return tuple(dict.fromkeys(_coerce(value) for value in values))


def _combine(expressions: Sequence[ds.Expression], *, conjunction: bool) -> ds.Expression:
    combined = expressions[0]
    for expression in expressions[1:]:
        combined = combined & expression if conjunction else combined | expression
    return combined


__all__ = [
    "AllOf",
    "AnyOf",
    "F",
    "FieldContains",
    "FieldIn",
    "FieldMatches",
    "FieldPrefix",
    "Filter",
    "FilterLike",
    "all_of",
    "any_of",
    "as_filter",
    "normalize_values",
]
