"""Result primitives: free-form output with optional output semantics and provenance."""

from __future__ import annotations

__all__ = [
    "Column",
    "ColumnRole",
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
from datetime import datetime
from enum import StrEnum
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pydantic import AliasChoices, BaseModel, ConfigDict, Field, model_validator

from parsimony.entity import Entity, normalize_entity_code, normalize_namespace

SECRET_NAME_PATTERN = re.compile(r"(?i)(api[_-]?key|token|secret|password|credential|bearer|auth)")

# Oversized values are replaced with a structured marker rather than a
# prefix — a prefix can leak the head of an unredacted secret.
_PROVENANCE_FIELD_BUDGET = 2000

REDACTED = "«redacted»"

#: Key under which Result embeds its schema+provenance payload in Arrow table metadata.
_RESULT_SCHEMA_META_KEY = b"parsimony.result"

logger = logging.getLogger(__name__)


class ColumnRole(StrEnum):
    """Semantic role of a column in a tabular result."""

    DATA = "data"
    KEY = "key"
    TITLE = "title"
    METADATA = "metadata"


class Column(BaseModel):
    """Declared column in an :class:`OutputSpec`.

    Pure annotation: names a column the connector is expected to return and
    states how consumers may interpret it. Declaring a column never renames,
    coerces, or validates the data the connector actually returned.
    """

    model_config = ConfigDict(extra="forbid")

    name: str
    role: ColumnRole = Field(
        default=ColumnRole.DATA,
        validation_alias=AliasChoices("role", "kind"),
    )
    description: str | None = None
    exclude_from_llm_view: bool = False
    #: Catalog namespace for KEY entity codes or METADATA value universes.
    #: Catalog-producing results must set this on their KEY column. On
    #: METADATA columns this is a lightweight annotation only; Parsimony does
    #: not enforce references.
    namespace: str | None = None

    @model_validator(mode="after")
    def _validate_exclude_and_namespace(self) -> Column:
        if self.exclude_from_llm_view and self.role == ColumnRole.DATA:
            raise ValueError("exclude_from_llm_view is not allowed for data columns")
        if self.exclude_from_llm_view and self.role == ColumnRole.TITLE:
            raise ValueError("exclude_from_llm_view is not allowed for title columns")
        if self.namespace is not None:
            if self.role not in (ColumnRole.KEY, ColumnRole.METADATA):
                raise ValueError("namespace is only allowed on KEY or METADATA columns")
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

    The declaration pairs to the frame strictly **by name**: the spec is a
    verbatim annotation, never aligned to the frame's column order, so a
    declared-but-absent column has no effect and an undeclared column renders
    un-annotated. A frame column is hidden when *any* declared column of that
    name is flagged hidden — with duplicate names (common in SQL joins) hiding
    errs on the safe side rather than leaking a sensitive sibling. Columns are
    selected by position (``iloc``) and dtypes read by zipping ``frame.columns``
    with ``frame.dtypes``, robust to non-string labels and duplicates.
    """
    by_name: dict[str, Column] = {}
    hidden_names: set[str] = set()
    for c in columns:
        by_name.setdefault(c.name, c)
        if c.exclude_from_llm_view:
            hidden_names.add(c.name)
    keep_positions: list[int] = []
    lines: list[str] = []
    hidden_count = 0
    for pos, (name, dtype) in enumerate(zip(frame.columns, frame.dtypes, strict=True)):
        if name in hidden_names:
            hidden_count += 1
            continue
        keep_positions.append(pos)
        col = by_name.get(name)
        annot = f" {col.llm_annotation()}" if col is not None else ""
        lines.append(f"- {name}: {dtype}{annot}")
    return frame.iloc[:, keep_positions], hidden_count, lines


def shape_descriptor(frame: pd.DataFrame, hidden_count: int) -> str:
    """Honest ``N rows × M cols [K hidden]`` size token for a tabular view."""
    descriptor = f"{len(frame)} rows × {len(frame.columns)} columns"
    if hidden_count:
        descriptor += f" ({hidden_count} hidden from LLM view)"
    return descriptor


class Result(BaseModel):
    """Connector output: any payload plus provenance, optionally tabular.

    One result type for every payload. ``data`` is exactly what the connector
    returned — the framework never renames, coerces, or validates it. When
    ``data`` is a :class:`~pandas.DataFrame` the result is *tabular*
    (``is_tabular``) and may carry an :class:`OutputSpec` — column roles,
    namespaces, and ``exclude_from_llm_view`` governance; otherwise it is an
    opaque payload rendered as a bounded structural preview. The framework
    wraps connector return values automatically; connectors should not
    construct this directly. Put provider facts in returned tabular columns
    with :class:`ColumnRole` semantics; do not attach provider metadata
    through ``provenance.properties``.
    """

    model_config = {"arbitrary_types_allowed": True}

    data: Any
    provenance: Provenance = Field(default_factory=lambda: Provenance(source="", source_description=""))
    output_spec: OutputSpec | None = Field(default=None)

    # -- construction -----------------------------------------------------

    @classmethod
    def from_dataframe(cls, df: pd.DataFrame | pd.Series) -> Result:
        """Build a tabular result with no output spec."""
        frame = pd.DataFrame(df)
        if frame.empty:
            raise ValueError("Returned an empty DataFrame.")
        return cls(data=frame)

    def _with_properties(self, **properties: Any) -> Result:
        """Merge extras into ``provenance.properties`` (serialization/tests only)."""
        merged = {**self.provenance.properties, **properties}
        new_prov = self.provenance.model_copy(update={"properties": merged})
        return self.model_copy(update={"provenance": new_prov})

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
    def df(self) -> pd.DataFrame:
        return self.frame

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

    @property
    def data_columns(self) -> list[Column]:
        return [c for c in self.columns if c.role == ColumnRole.DATA]

    @property
    def metadata_columns(self) -> list[Column]:
        return [c for c in self.columns if c.role == ColumnRole.METADATA]

    # -- entity projection -------------------------------------------------

    def to_entities(self) -> list[Entity]:
        """Project a role-annotated tabular result into catalog entities.

        Rows sharing the declared KEY value become one
        :class:`~parsimony.entity.Entity`: the KEY column's ``namespace=``
        plus the key value form the identity, TITLE supplies the title
        (falling back to the code when absent), and METADATA columns —
        including a ``"*"`` wildcard claiming every column not otherwise
        declared — become ``metadata``. Role invariants (declared columns
        present, keys non-null, one TITLE/METADATA value per entity) are
        validated here, at projection time — never during connector
        execution. ``data`` itself is left untouched. Raises
        :class:`TypeError` for non-tabular results and :class:`ValueError`
        when the spec is missing or violated. Entities preserve
        first-appearance order: ``catalog.set_entities(result.to_entities())``.
        """
        if not self.is_tabular:
            raise TypeError(f"Entity projection requires a tabular result (data type: {type(self.data).__name__})")
        if self.output_spec is None:
            raise ValueError("Entity projection requires an output spec with role annotations")
        return _project_entities(self.frame, self.output_spec)

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
        """Serialize a tabular result to Arrow with embedded provenance and output spec."""
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
        """Deserialize an Arrow table written by :meth:`to_arrow`."""
        df = table.to_pandas()
        raw = (table.schema.metadata or {}).get(_RESULT_SCHEMA_META_KEY)
        if not raw:
            return cls(data=df)
        payload = json.loads(raw.decode("utf-8"))
        provenance = Provenance.model_validate(payload.get("provenance", {}))
        cols_raw = payload.get("columns") or []
        if cols_raw:
            # Strictness (extra="forbid") exists to catch authoring mistakes; on
            # the read path it would only brick files written before a Column
            # field was removed (e.g. the retired dtype/mapped_name). Drop
            # unknown keys instead of failing.
            known = set(Column.model_fields) | {"kind"}
            columns = [Column.model_validate({k: v for k, v in c.items() if k in known}) for c in cols_raw]
            return cls(
                data=df,
                provenance=provenance,
                output_spec=OutputSpec(columns=columns),
            )
        return cls(data=df, provenance=provenance)

    def to_parquet(self, path: str | Path) -> None:
        """Write a tabular result to Parquet with embedded output spec and provenance."""
        pq.write_table(self.to_arrow(), path)

    @classmethod
    def from_parquet(cls, path: str | Path) -> Result:
        """Read a tabular result from Parquet written by :meth:`to_parquet`."""
        return cls.from_arrow(pq.read_table(path))


class OutputSpec(BaseModel):
    """Declared output semantics for a tabular connector result.

    Pure annotation, never transformation: an ``OutputSpec`` states how
    consumers may interpret columns the connector returns (roles, namespaces,
    descriptions, LLM visibility). It does not rename or coerce data, and
    declaring a column does not make connector execution fail when that
    column is absent. Role-driven operations — the
    :meth:`Result.to_entities` projection, data-store loading — validate
    presence and role invariants themselves when invoked.

    Only the declaration itself is validated here: at most one KEY and one
    TITLE column, and at least one DATA, KEY, or TITLE column. A METADATA
    column named ``"*"`` is a wildcard matching every returned column not
    claimed by another declaration.
    """

    columns: list[Column]

    @model_validator(mode="after")
    def _validate_roles(self) -> OutputSpec:
        keys = [c.name for c in self.columns if c.role == ColumnRole.KEY]
        titles = [c.name for c in self.columns if c.role == ColumnRole.TITLE]
        if len(keys) > 1:
            raise ValueError(f"Output spec must have at most one KEY column, found {len(keys)}: {keys}")
        if len(titles) > 1:
            raise ValueError(f"Output spec must have at most one TITLE column, found {len(titles)}: {titles}")
        if not any(c.role in (ColumnRole.DATA, ColumnRole.KEY, ColumnRole.TITLE) for c in self.columns):
            raise ValueError("Output spec must define at least one data, key, or title column")
        return self


# -- entity projection (Result.to_entities) ---------------------------------


def _scalar(value: Any) -> Any:
    """Coerce numpy scalars/arrays to plain Python values for metadata."""
    if hasattr(value, "tolist"):
        value = value.tolist()
    elif hasattr(value, "item"):
        value = value.item()
    if isinstance(value, list):
        return [item.item() if hasattr(item, "item") else item for item in value]
    return value


def _sole_value(sub: pd.DataFrame, column: str, *, code: str, kind: str) -> Any | None:
    """The single non-null value of *column* within one entity's rows, or None.

    Repeated equal values and null-plus-one-value are accepted; two distinct
    non-null values are a role violation and raise.
    """
    values = sub[column].dropna()
    if len(values) == 0:
        return None
    normalized = [_scalar(v) for v in values]
    distinct = {repr(v) for v in normalized}
    if len(distinct) > 1:
        raise ValueError(
            f"Column {column!r} is not entity {kind} for code {code!r}: values vary within the "
            "entity key. Use ColumnRole.DATA or choose a more specific entity key."
        )
    return normalized[0]


def _declared_roles(spec: OutputSpec) -> tuple[Column, str | None, list[str], list[str], bool]:
    """Split *spec* into (key column, title name, explicit metadata, data names, has wildcard)."""
    key_cols = [c for c in spec.columns if c.role == ColumnRole.KEY]
    if len(key_cols) != 1:
        raise ValueError(f"Entity projection requires exactly one KEY column in the output spec, found {len(key_cols)}")
    key_col = key_cols[0]
    if not key_col.namespace:
        raise ValueError("Entity projection requires the KEY column to declare namespace=...")
    title_cols = [c for c in spec.columns if c.role == ColumnRole.TITLE]
    title_name = title_cols[0].name if title_cols else None
    meta_names = [c.name for c in spec.columns if c.role == ColumnRole.METADATA]
    explicit_meta = [n for n in meta_names if n != "*"]
    data_names = [c.name for c in spec.columns if c.role == ColumnRole.DATA and c.name != "*"]
    return key_col, title_name, explicit_meta, data_names, "*" in meta_names


def _project_entities(frame: pd.DataFrame, spec: OutputSpec) -> list[Entity]:
    """Group *frame* rows by the declared KEY and build catalog entities."""
    key_col, title_name, explicit_meta, data_names, wildcard = _declared_roles(spec)
    try:
        namespace = normalize_namespace(str(key_col.namespace))
    except ValueError as exc:
        raise ValueError(f"Invalid namespace {key_col.namespace!r} on KEY column {key_col.name!r}: {exc}") from exc

    # "No results" tolerance: an empty frame — including the bare
    # ``pd.DataFrame()`` idiom with no columns — projects to no entities
    # rather than tripping the declared-column presence check.
    if frame.empty:
        return []

    if wildcard:
        claimed = {key_col.name, *([title_name] if title_name else []), *explicit_meta, *data_names}
        meta_names = [*explicit_meta, *(str(c) for c in frame.columns if str(c) not in claimed)]
    else:
        meta_names = explicit_meta

    required = [key_col.name, *([title_name] if title_name else []), *explicit_meta, *data_names]
    missing = sorted(set(required) - {str(c) for c in frame.columns})
    if missing:
        raise ValueError(
            f"Result data missing declared columns {missing}. Available: {sorted(str(c) for c in frame.columns)}"
        )

    if frame[key_col.name].isna().any():
        raise ValueError(f"KEY column {key_col.name!r} contains null values; every row must identify an entity")

    entities: list[Entity] = []
    seen: set[str] = set()
    for raw_code, sub in frame.groupby(key_col.name, sort=False):
        try:
            code = normalize_entity_code(str(raw_code))
        except ValueError as exc:
            raise ValueError(f"Invalid entity code from KEY column {key_col.name!r}: {raw_code!r}: {exc}") from exc
        if code in seen:
            raise ValueError(f"Duplicate entity {namespace}:{code} after key normalization")
        seen.add(code)
        title_value = _sole_value(sub, title_name, code=code, kind="title") if title_name else None
        metadata: dict[str, Any] = {}
        for meta_name in meta_names:
            value = _sole_value(sub, meta_name, code=code, kind="metadata")
            if value is not None:
                metadata[meta_name] = value
        entities.append(
            Entity(
                namespace=namespace,
                code=code,
                title=code if title_value is None else str(title_value),
                metadata=metadata,
            )
        )
    return entities


# ``Result.output_spec`` forward-references ``OutputSpec``, defined above but
# only after ``Result``. Resolve the reference explicitly now that both exist, so
# validation never depends on Pydantic's lazy first-use resolution (which would
# break silently if this module were ever split).
Result.model_rebuild()
