"""Result primitives: free-form output with optional tabular schema and provenance."""

from __future__ import annotations

__all__ = [
    "Column",
    "ColumnRole",
    "EntityResult",
    "OutputSpec",
    "Provenance",
    "REDACTED",
    "Result",
    "SECRET_NAME_PATTERN",
    "governed_view",
    "shape_descriptor",
]

import json
import logging
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from pathlib import Path
from types import MappingProxyType
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pydantic import BaseModel, ConfigDict, Field, model_validator

from parsimony.entity import (
    Entity,
    _metadata_value,
    normalize_entity_code,
    normalize_namespace,
)

SECRET_NAME_PATTERN = re.compile(r"(?i)(api[_-]?key|token|secret|password|credential|bearer|auth)")

# Oversized values are replaced with a structured marker rather than a
# prefix — a prefix can leak the head of an unredacted secret.
_PROVENANCE_FIELD_BUDGET = 2000

REDACTED = "«redacted»"

#: Key under which Result embeds its schema+provenance payload in Arrow table metadata.
_RESULT_SCHEMA_META_KEY = b"parsimony.result"

#: Reserved KEY namespace value: per-row namespace instead comes from a
#: METADATA column literally named ``entity_namespace``.
_ROW_NAMESPACE = "__row__"

logger = logging.getLogger(__name__)


class ColumnRole(StrEnum):
    """Semantic role of a column in a tabular result."""

    DATA = "data"
    KEY = "key"
    TITLE = "title"
    METADATA = "metadata"


class Column(BaseModel):
    """Declared semantics for one column of a :class:`OutputSpec`.

    Purely declarative: a ``Column`` never inspects, transforms, or renames
    the connector's returned data. It binds semantics (role, namespace,
    description, visibility) to a column name so that a later, explicit
    projection (:attr:`Result.entities`) or presentation (:meth:`Result.to_llm`)
    can interpret the raw payload.
    """

    name: str
    role: ColumnRole = ColumnRole.DATA
    description: str | None = None
    #: Catalog namespace for a KEY column's entity codes. Not meaningful on
    #: any other role — Parsimony enforces no metadata-namespace behavior.
    namespace: str | None = None
    exclude_from_llm_view: bool = False

    @model_validator(mode="after")
    def _validate_exclude_and_namespace(self) -> Column:
        if self.exclude_from_llm_view and self.role in (ColumnRole.DATA, ColumnRole.TITLE):
            raise ValueError(f"exclude_from_llm_view is not allowed for {self.role.value} columns")
        if self.namespace is not None:
            if self.role != ColumnRole.KEY:
                raise ValueError("namespace is only allowed on KEY columns")
            if not str(self.namespace).strip():
                raise ValueError("namespace must be non-empty when set")
        return self

    def llm_annotation(self) -> str:
        """Governed schema token for LLM views: ``(ROLE)`` or ``(ROLE ns:<namespace>)``.

        Single source of truth for how a column's role + namespace is rendered
        into any LLM-facing view (connector card, result preview, fetch log).
        """
        role = self.role.value.upper()
        ns = f" ns:{self.namespace}" if self.namespace else ""
        return f"({role}{ns})"


class Provenance(BaseModel):
    """Where and how tabular data was obtained.

    Framework-only type. Connector code never imports this; the framework
    builds it in :meth:`Connector._wrap_result`. ``properties`` is for
    framework/internal use (e.g. serialization round-trips), not
    connector-authored provider metadata.
    """

    model_config = ConfigDict(extra="forbid")

    source: str
    source_description: str
    params: dict[str, Any] = Field(default_factory=dict)
    fetched_at: datetime | None = None
    properties: dict[str, Any] = Field(default_factory=dict)

    def safe_dump(self) -> dict[str, Any]:
        """Wire-safe JSON projection with oversize field truncation."""
        raw = self.model_dump(mode="json")
        if raw.get("params"):
            raw["params"] = {k: (REDACTED if SECRET_NAME_PATTERN.search(k) else v) for k, v in raw["params"].items()}
        for key in ("params", "properties"):
            if key in raw and raw[key]:
                blob = json.dumps(raw[key], default=str)
                if len(blob) > _PROVENANCE_FIELD_BUDGET:
                    raw[key] = {
                        "truncated": True,
                        "byte_length": len(blob),
                        "field": key,
                    }
        return raw


# ---------------------------------------------------------------------------
# LLM preview helpers
# ---------------------------------------------------------------------------

_PREVIEW_DEFAULT_MAX_ROWS = 10
_PREVIEW_DEFAULT_MAX_CHARS = 2000
_PREVIEW_MAX_CELL_CHARS = 80
_PREVIEW_MAX_KEYS = 20


def _truncate_cell(value: Any, max_len: int) -> str:
    s = str(value)
    if len(s) <= max_len:
        return s
    return s[: max(0, max_len - 1)] + "…"


def _shape_token(value: Any) -> str:
    """One-line type/shape token for a nested value — no payload.

    Scalars (bool/int/float/str/bytes/...) fall through to the type-name
    fallback; only containers and models carry a size annotation.
    """
    if isinstance(value, Mapping):
        return f"dict[{len(value)} keys]"
    if isinstance(value, BaseModel):
        return f"{type(value).__name__}[{len(type(value).model_fields)} fields]"
    if isinstance(value, (list, tuple, set, frozenset)):
        return f"{type(value).__name__}[{len(value)}]"
    if value is None:
        return "None"
    return type(value).__name__


def _homogeneous_elem_type(value: Sequence[Any] | set[Any] | frozenset[Any]) -> str | None:
    types = {type(v).__name__ for v in value}
    return next(iter(types)) if len(types) == 1 else None


def _preview_value(value: Any, *, max_chars: int) -> str:
    """Depth-limited structural preview of an opaque ``Result.data`` payload."""
    tname = type(value).__name__
    if value is None:
        return "Result (NoneType): None"
    if isinstance(value, str):
        return f"Result (str): {len(value)} chars\n{_truncate_cell(value, max_chars)}"
    if isinstance(value, (bytes, bytearray)):
        return f"Result ({tname}): {len(value)} bytes"
    if isinstance(value, bool):
        return f"Result (bool): {value}"
    if isinstance(value, (int, float)):
        return f"Result ({tname}): {value}"
    if isinstance(value, Mapping):
        lines = [f"Result (dict): {len(value)} keys"]
        for i, (k, v) in enumerate(value.items()):
            if i >= _PREVIEW_MAX_KEYS:
                lines.append(f"... ({len(value) - _PREVIEW_MAX_KEYS} more keys)")
                break
            lines.append(f"- {k}: {_shape_token(v)}")
        return "\n".join(lines)
    if isinstance(value, BaseModel):
        fields = list(type(value).model_fields)
        lines = [f"Result ({tname}): {len(fields)} fields"]
        for i, fname in enumerate(fields):
            if i >= _PREVIEW_MAX_KEYS:
                lines.append(f"... ({len(fields) - _PREVIEW_MAX_KEYS} more fields)")
                break
            lines.append(f"- {fname}: {_shape_token(getattr(value, fname, None))}")
        return "\n".join(lines)
    if isinstance(value, (list, tuple, set, frozenset)):
        elem = _homogeneous_elem_type(value)
        suffix = f" of {elem}" if elem else ""
        lines = [f"Result ({tname}): {len(value)} items{suffix}"]
        if isinstance(value, (list, tuple)) and value:
            lines.append(f"[0]: {_shape_token(value[0])}")
        return "\n".join(lines)
    return f"Result ({tname}): {_truncate_cell(repr(value), max_chars)}"


def _frame_csv(frame: pd.DataFrame) -> str:
    truncated = frame.copy()
    # Truncate per-cell by COLUMN POSITION — label-based ``truncated[col] = …``
    # is ambiguous when column names are duplicated (common in SQL joins).
    for i in range(truncated.shape[1]):
        truncated.isetitem(i, truncated.iloc[:, i].map(lambda v: _truncate_cell(v, _PREVIEW_MAX_CELL_CHARS)))
    csv_text: str = truncated.to_csv(index=False)
    return csv_text.strip()


def governed_view(frame: pd.DataFrame, columns: Sequence[Column]) -> tuple[pd.DataFrame, int, list[str]]:
    """Single source of truth for tabular column governance + schema annotation.

    Returns ``(visible_frame, hidden_count, schema_lines)``. Columns flagged
    ``exclude_from_llm_view`` are dropped from ``visible_frame`` and never appear
    in ``schema_lines``. Every LLM-facing tabular view — core preview,
    kernel-output render — applies governance through here, so a hidden column
    is hidden on *every* path.

    Columns are selected by **position** (``iloc``) and dtypes read by zipping
    ``frame.columns`` with ``frame.dtypes`` — robust to non-string column labels
    (a default ``RangeIndex``) and to duplicate column names (common in SQL
    joins), neither of which ``frame[name]`` survives. When ``columns`` covers
    the frame one-to-one (the normal case for a built :class:`Result`) the schema
    is paired to the frame **by position**, so a hidden column does not suppress
    a visible sibling that happens to share its name; otherwise it falls back to
    name-based matching.
    """
    by_name = {c.name: c for c in columns}
    positional = len(columns) == len(frame.columns)
    keep_positions: list[int] = []
    lines: list[str] = []
    hidden_count = 0
    for pos, (name, dtype) in enumerate(zip(frame.columns, frame.dtypes, strict=True)):
        col = columns[pos] if positional else by_name.get(name)
        if col is not None and col.exclude_from_llm_view:
            hidden_count += 1
            continue
        keep_positions.append(pos)
        annot = f" {col.llm_annotation()}" if col is not None else ""
        lines.append(f"- {name}: {dtype}{annot}")
    return frame.iloc[:, keep_positions], hidden_count, lines


def shape_descriptor(frame: pd.DataFrame, hidden_count: int) -> str:
    """Honest ``N rows × M cols [K hidden]`` size token for a tabular view."""
    descriptor = f"{len(frame)} rows × {len(frame.columns)} columns"
    if hidden_count:
        descriptor += f" ({hidden_count} hidden from LLM view)"
    return descriptor


class OutputSpec(BaseModel):
    """Ordered declaration of column semantics for a connector's tabular output.

    An ``OutputSpec`` never sees data. It has no methods that accept a
    DataFrame: no coercion, renaming, matching side effects, or result
    construction. It becomes operational only when a caller asks for an
    entity projection (:attr:`Result.entities`) or a governed presentation
    (:meth:`Result.to_llm`) — both interpret the declaration against
    ``Result.data`` without modifying it.

    ``"*"`` is the sole dynamic rule: a wildcard column assigns its role to
    every returned column not named explicitly elsewhere in the declaration.
    """

    columns: list[Column]

    @model_validator(mode="after")
    def _validate_declaration(self) -> OutputSpec:
        names = [c.name for c in self.columns if c.name != "*"]
        seen: set[str] = set()
        dupes = {n for n in names if n in seen or seen.add(n)}  # type: ignore[func-returns-value]
        if dupes:
            raise ValueError(f"Column names must be unique, found duplicates: {sorted(dupes)}")

        keys = [c for c in self.columns if c.role == ColumnRole.KEY]
        if len(keys) > 1:
            names = [c.name for c in keys]
            raise ValueError(f"OutputSpec must have at most one KEY column, found {len(keys)}: {names}")

        titles = [c.name for c in self.columns if c.role == ColumnRole.TITLE]
        if len(titles) > 1:
            raise ValueError(f"OutputSpec must have at most one TITLE column, found {len(titles)}: {titles}")

        wildcards = [c for c in self.columns if c.name == "*"]
        if len(wildcards) > 1:
            raise ValueError("OutputSpec must have at most one '*' wildcard column")
        if wildcards and wildcards[0].role not in (ColumnRole.DATA, ColumnRole.METADATA):
            raise ValueError("'*' wildcard column must have role DATA or METADATA; it cannot identify an entity")
        return self


@dataclass(frozen=True)
class EntityResult:
    """One entity's slice of a tabular :class:`Result`: identity plus DATA rows.

    Produced only by :attr:`Result.entities`. ``data`` contains exactly the
    columns resolved as ``DATA`` by the parent result's :class:`OutputSpec`,
    in original row order and index. ``provenance`` is the same call-level
    object as the parent :class:`Result` — shared, not copied or mixed into
    metadata.
    """

    entity: Entity
    data: pd.DataFrame
    provenance: Provenance

    @property
    def namespace(self) -> str:
        return self.entity.namespace

    @property
    def code(self) -> str:
        return self.entity.code

    @property
    def title(self) -> str:
        return self.entity.title

    @property
    def metadata(self) -> Mapping[str, Any]:
        return self.entity.metadata


# ---------------------------------------------------------------------------
# Entity projection — the one grouping algorithm shared by Result.entities,
# Result.to_entities(), catalog construction, and data-store loading.
# ---------------------------------------------------------------------------


def _resolve_wildcard_names(frame: pd.DataFrame, claimed: set[str]) -> list[str]:
    return [str(name) for name in frame.columns if str(name) not in claimed]


def _check_no_duplicate_labels(frame: pd.DataFrame, names: Sequence[str]) -> None:
    counts = frame.columns.value_counts()
    ambiguous = sorted({n for n in names if n in counts.index and counts[n] > 1})
    if ambiguous:
        raise ValueError(f"Entity projection columns are ambiguous: DataFrame has duplicate labels for {ambiguous}")


def _consistent_non_null(values: pd.Series, *, label: str, entity_ref: str) -> Any | None:
    """Return the single distinct non-null value in *values*, or ``None``.

    Raises if more than one distinct non-null value is present. Repeated
    equal values and a mix of null plus one distinct non-null value are both
    accepted per the entity-projection contract.
    """
    non_null = [_metadata_value(v) for v in values.dropna()]
    if not non_null:
        return None
    distinct = {repr(v) for v in non_null}
    if len(distinct) > 1:
        raise ValueError(f"{label} has conflicting values for entity {entity_ref}: values vary within the entity key")
    return non_null[0]


def _project_entities(
    data: Any,
    output_spec: OutputSpec | None,
    provenance: Provenance,
) -> Mapping[tuple[str, str], EntityResult]:
    """Group a tabular payload into one :class:`EntityResult` per ``(namespace, code)``.

    See ``Result.entities`` for the full contract. Errors are raised as
    direct ``TypeError``/``ValueError`` with entity and column context.
    """
    if output_spec is None:
        raise ValueError("Result has no OutputSpec; cannot project entities")
    if not isinstance(data, pd.DataFrame):
        raise TypeError(f"Entity projection requires a tabular Result (payload type: {type(data).__name__})")
    frame = data

    columns = output_spec.columns
    key_cols = [c for c in columns if c.role == ColumnRole.KEY]
    if len(key_cols) != 1:
        raise ValueError(f"Entity projection requires exactly one KEY column, found {len(key_cols)}")
    key_col = key_cols[0]
    if not key_col.namespace:
        raise ValueError(f"KEY column {key_col.name!r} must declare namespace=... for entity projection")

    title_cols = [c for c in columns if c.role == ColumnRole.TITLE]
    title_col = title_cols[0] if title_cols else None

    meta_cols = [c for c in columns if c.role == ColumnRole.METADATA and c.name != "*"]
    data_cols = [c for c in columns if c.role == ColumnRole.DATA and c.name != "*"]
    wildcard = next((c for c in columns if c.name == "*"), None)

    per_row_namespace = key_col.namespace == _ROW_NAMESPACE
    namespace_col_name = "entity_namespace" if per_row_namespace else None
    if per_row_namespace and namespace_col_name not in {c.name for c in meta_cols}:
        raise ValueError(
            f'KEY column {key_col.name!r} declares namespace="{_ROW_NAMESPACE}", which requires a '
            'METADATA column named "entity_namespace"'
        )

    required_names = [
        key_col.name,
        *([title_col.name] if title_col else []),
        *(c.name for c in meta_cols),
        *(c.name for c in data_cols),
    ]
    missing = sorted({n for n in required_names if n not in frame.columns})
    if missing:
        raise ValueError(
            f"Entity projection missing declared columns: {missing}. Available: {sorted(map(str, frame.columns))}"
        )
    _check_no_duplicate_labels(frame, required_names)

    claimed = set(required_names)
    wildcard_names = _resolve_wildcard_names(frame, claimed) if wildcard is not None else []

    meta_names = [c.name for c in meta_cols if c.name != namespace_col_name]
    full_data_names = [c.name for c in data_cols]
    if wildcard is not None:
        if wildcard.role == ColumnRole.DATA:
            full_data_names = [*full_data_names, *wildcard_names]
        else:
            meta_names = [*meta_names, *wildcard_names]

    if frame.empty:
        return MappingProxyType({})

    key_series = frame[key_col.name]
    if key_series.isna().any():
        raise ValueError(f"KEY column {key_col.name!r} has null values; entity identity cannot be null")
    codes = key_series.astype(str).map(normalize_entity_code)

    if per_row_namespace:
        assert namespace_col_name is not None
        ns_series = frame[namespace_col_name]
        if ns_series.isna().any():
            raise ValueError(f"Per-row namespace column {namespace_col_name!r} has null values")
        namespaces = ns_series.astype(str).map(normalize_namespace)
    else:
        static_ns = normalize_namespace(key_col.namespace)
        namespaces = pd.Series([static_ns] * len(frame), index=frame.index)

    work = frame.assign(__ns__=namespaces, __code__=codes)

    out: dict[tuple[str, str], EntityResult] = {}
    for (ns, code), sub in work.groupby(["__ns__", "__code__"], sort=False):
        entity_ref = f"({ns}, {code})"

        title = code
        if title_col is not None:
            title_label = f"TITLE column {title_col.name!r}"
            resolved_title = _consistent_non_null(sub[title_col.name], label=title_label, entity_ref=entity_ref)
            if resolved_title is not None:
                title = str(resolved_title)

        metadata: dict[str, Any] = {}
        for name in meta_names:
            value = _consistent_non_null(sub[name], label=f"METADATA column {name!r}", entity_ref=entity_ref)
            if value is not None:
                metadata[name] = value

        entity = Entity(namespace=ns, code=code, title=title, metadata=metadata)
        entity_data = sub[full_data_names].copy() if full_data_names else pd.DataFrame(index=sub.index)
        out[(ns, code)] = EntityResult(entity=entity, data=entity_data, provenance=provenance)
    return MappingProxyType(out)


class Result(BaseModel):
    """Connector output: any payload plus provenance, optionally tabular.

    One result type for every payload. ``data`` is exactly the object a
    connector returned — the framework never copies, coerces, renames, or
    reorders it. When ``data`` is a :class:`~pandas.DataFrame` the result is
    *tabular* (``is_tabular``) and may carry an :class:`OutputSpec` — a
    passive declaration of column roles, namespaces, and
    ``exclude_from_llm_view`` governance. The framework wraps connector
    return values automatically; connectors should not construct this
    directly. Put provider facts in returned tabular columns with
    :class:`ColumnRole` semantics; do not attach provider metadata through
    ``provenance.properties``.
    """

    model_config = {"arbitrary_types_allowed": True}

    data: Any
    provenance: Provenance = Field(default_factory=lambda: Provenance(source="", source_description=""))
    output_spec: OutputSpec | None = Field(default=None)

    # -- payload accessors ------------------------------------------------

    @property
    def is_tabular(self) -> bool:
        """Whether ``data`` is a DataFrame (so frame/schema accessors apply)."""
        return isinstance(self.data, pd.DataFrame)

    @property
    def frame(self) -> pd.DataFrame:
        """The tabular payload. Raises if this result is not tabular."""
        if not self.is_tabular:
            raise TypeError(f"Result payload is not tabular (data type: {type(self.data).__name__})")
        return self.data

    @property
    def text(self) -> str:
        if isinstance(self.data, str):
            return self.data
        return str(self.data)

    @property
    def columns(self) -> list[Column]:
        if self.output_spec is None:
            return []
        return self.output_spec.columns

    # -- entity projection --------------------------------------------------

    @property
    def entities(self) -> Mapping[tuple[str, str], EntityResult]:
        """Lazy, read-only, tuple-keyed view of this result's entities.

        Requires a tabular ``data`` and an ``output_spec`` declaring exactly
        one namespaced KEY column. Groups rows by the normalized
        ``(namespace, code)`` pair (per-row namespace when the KEY declares
        ``namespace="__row__"`` via a METADATA column named
        ``entity_namespace``), in first-appearance order. Each
        :class:`EntityResult` carries only that entity's ``DATA`` columns.

        Intentionally uncached — ``Result.data`` is a mutable raw object, so
        caching would create stale views. Bind the mapping once for repeated
        access: ``entities = result.entities; unrate = entities["fred", "UNRATE"]``.

        Raises ``ValueError``/``TypeError`` (with entity and column context)
        when the declaration or data cannot support a projection: missing
        KEY/namespace, missing declared columns, duplicate DataFrame labels,
        null KEY/namespace values, or conflicting TITLE/METADATA values
        within one entity.
        """
        return _project_entities(self.data, self.output_spec, self.provenance)

    def to_entities(self) -> list[Entity]:
        """Lossy catalog conversion: identity, title, and metadata only."""
        return [item.entity for item in self.entities.values()]

    # -- LLM view ---------------------------------------------------------

    def to_llm(
        self,
        *,
        max_rows: int = _PREVIEW_DEFAULT_MAX_ROWS,
        max_chars: int = _PREVIEW_DEFAULT_MAX_CHARS,
    ) -> str:
        """Governed, bounded string view for LLM context.

        Tabular payloads render an honest header (the *real* row/column
        counts), a governed per-column schema (``exclude_from_llm_view``
        columns dropped), and the first ``max_rows`` rows — never a head/tail
        sample masquerading as the whole. Opaque payloads render a structural
        preview (type + shape), size O(structure + ``max_chars``). ``max_rows``
        is unused for opaque payloads and ``max_chars`` for tabular ones; both
        are accepted for a single uniform signature.
        """
        if not self.is_tabular:
            return _preview_value(self.data, max_chars=max_chars)
        frame = self.frame
        visible_frame, hidden_count, schema_lines = governed_view(frame, self.columns)
        lines = [f"Result (table): {shape_descriptor(frame, hidden_count)}"]
        if visible_frame.shape[1] == 0:
            lines.append("Columns: (all hidden from LLM view)")
            return "\n".join(lines)
        lines.append("Columns:")
        lines.extend(schema_lines)
        n_rows = len(frame)
        shown = min(max_rows, n_rows)
        label = f"Rows (showing {shown} of {n_rows}):" if shown < n_rows else f"Rows ({n_rows}):"
        lines.append(label)
        lines.append(_frame_csv(visible_frame.head(shown)))
        return "\n".join(lines)

    # -- serialization (tabular only) ------------------------------------

    def to_arrow(self) -> pa.Table:
        """Serialize a tabular result to Arrow with embedded provenance and schema."""
        table = pa.Table.from_pandas(self.frame, preserve_index=False)
        payload: dict[str, Any] = {
            "provenance": self.provenance.safe_dump(),
        }
        if self.output_spec is not None:
            payload["columns"] = [c.model_dump(mode="json") for c in self.output_spec.columns]
        meta = dict(table.schema.metadata or {})
        meta[_RESULT_SCHEMA_META_KEY] = json.dumps(payload, default=str).encode("utf-8")
        return table.replace_schema_metadata(meta)

    @classmethod
    def from_arrow(cls, table: pa.Table) -> Result:
        """Deserialize an Arrow table written by :meth:`to_arrow`.

        Retired fields on legacy payloads (``dtype``, ``mapped_name``, the
        ``kind`` role alias) are ignored rather than rejected, so old
        Parquet/Arrow files remain readable.
        """
        df = table.to_pandas()
        raw = (table.schema.metadata or {}).get(_RESULT_SCHEMA_META_KEY)
        if not raw:
            return cls(data=df)
        payload = json.loads(raw.decode("utf-8"))
        provenance = Provenance.model_validate(payload.get("provenance", {}))
        cols_raw = payload.get("columns") or []
        if cols_raw:
            columns = []
            for c in cols_raw:
                role = c.get("role") or c.get("kind") or ColumnRole.DATA
                columns.append(
                    Column.model_validate(
                        {
                            "name": c["name"],
                            "role": role,
                            "description": c.get("description"),
                            "namespace": c.get("namespace") if role == ColumnRole.KEY else None,
                            "exclude_from_llm_view": c.get("exclude_from_llm_view", False),
                        }
                    )
                )
            return cls(
                data=df,
                provenance=provenance,
                output_spec=OutputSpec(columns=columns),
            )
        return cls(data=df, provenance=provenance)

    def to_parquet(self, path: str | Path) -> None:
        """Write a tabular result to Parquet with embedded column schema and provenance."""
        pq.write_table(self.to_arrow(), path)

    @classmethod
    def from_parquet(cls, path: str | Path) -> Result:
        """Read a tabular result from Parquet written by :meth:`to_parquet`."""
        return cls.from_arrow(pq.read_table(path))
