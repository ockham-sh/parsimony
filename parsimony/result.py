"""Result primitives: free-form output with optional tabular schema and provenance."""

from __future__ import annotations

__all__ = [
    "Column",
    "ColumnRole",
    "OutputConfig",
    "Provenance",
    "Result",
    "SemanticTableResult",
]

import logging
import re
from collections.abc import Mapping
from datetime import datetime
from enum import StrEnum
from typing import Any

import pandas as pd
from pydantic import AliasChoices, BaseModel, Field, model_validator

logger = logging.getLogger(__name__)

#: Regex matching ``{placeholder}`` segments in a namespace template.
#: Placeholders must be valid Python identifiers (letters, digits, underscore;
#: cannot start with a digit) — this matches the column-name contract and
#: ensures the reverse-resolution regex in ``_find_enumerator`` stays
#: unambiguous (no nested braces, no empty placeholders).
_NAMESPACE_PLACEHOLDER_RE: re.Pattern[str] = re.compile(r"\{([A-Za-z_][A-Za-z0-9_]*)\}")


def namespace_placeholders(template: str) -> list[str]:
    """Return ordered, de-duplicated placeholder names in *template*.

    ``"sdmx-series-{agency}-{dataset_id}"`` → ``["agency", "dataset_id"]``.
    A static namespace with no ``{...}`` segments returns ``[]``.
    """
    seen: dict[str, None] = {}
    for match in _NAMESPACE_PLACEHOLDER_RE.finditer(template):
        seen.setdefault(match.group(1), None)
    return list(seen)


def resolve_namespace_template(template: str, values: Mapping[str, Any]) -> str:
    """Substitute ``{placeholder}`` segments in *template* with *values*.

    Missing keys raise :class:`KeyError` with an actionable message — the
    caller is expected to have validated placeholder columns upstream at
    :class:`OutputConfig` construction time.
    """

    def _sub(match: re.Match[str]) -> str:
        name = match.group(1)
        if name not in values:
            raise KeyError(
                f"namespace template {template!r} references placeholder {name!r} "
                f"not present in row values (available: {sorted(values)})"
            )
        return str(values[name])

    return _NAMESPACE_PLACEHOLDER_RE.sub(_sub, template)


class ColumnRole(StrEnum):
    """Semantic role of a column in a tabular result."""

    DATA = "data"
    KEY = "key"
    TITLE = "title"
    METADATA = "metadata"


class Column(BaseModel):
    """Declared column in an :class:`OutputConfig` schema."""

    name: str
    dtype: str = "auto"
    role: ColumnRole = Field(
        default=ColumnRole.DATA,
        validation_alias=AliasChoices("role", "kind"),
    )
    mapped_name: str | None = None
    param_key: str | None = None
    description: str | None = None
    exclude_from_llm_view: bool = False
    #: Catalog namespace for the entity code when ``role`` is :attr:`ColumnRole.KEY`.
    #: Set when using :meth:`~parsimony.catalog.Catalog.index_result` so the table is self-describing.
    #:
    #: May be a **static** string (``"sdmx-datasets"``) or a **template**
    #: containing ``{placeholder}`` segments that reference sibling columns
    #: in the same :class:`OutputConfig` (``"sdmx-series-{agency}-{dataset_id}"``).
    #: Templates are resolved per row during catalog indexing and reverse-resolved
    #: by :func:`~parsimony.catalog.catalog._find_enumerator` when Catalog.search
    #: needs to locate a live-fallback enumerator for a fully-resolved namespace.
    namespace: str | None = None

    @model_validator(mode="after")
    def _validate_exclude_and_namespace(self) -> Column:
        if self.exclude_from_llm_view and self.role == ColumnRole.DATA:
            raise ValueError("exclude_from_llm_view is not allowed for data columns")
        if self.exclude_from_llm_view and self.role == ColumnRole.TITLE:
            raise ValueError("exclude_from_llm_view is not allowed for title columns")
        if self.namespace is not None:
            if self.role != ColumnRole.KEY:
                raise ValueError("namespace is only allowed on KEY columns")
            if not str(self.namespace).strip():
                raise ValueError("namespace must be non-empty when set")
            # Reject unbalanced braces early — `namespace_placeholders` tolerates
            # malformed input silently, so we check here.
            open_braces = self.namespace.count("{")
            close_braces = self.namespace.count("}")
            if open_braces != close_braces:
                raise ValueError(
                    f"namespace template {self.namespace!r} has unbalanced braces "
                    f"({open_braces} '{{' vs {close_braces} '}}')"
                )
        return self

    @property
    def namespace_placeholders(self) -> list[str]:
        """Placeholder names in :attr:`namespace` (empty when static or unset)."""
        return namespace_placeholders(self.namespace) if self.namespace else []

    @property
    def namespace_is_template(self) -> bool:
        """True iff :attr:`namespace` is set and contains at least one placeholder."""
        return bool(self.namespace_placeholders)


def _coerce_series_dtype(column: Column, series: pd.Series) -> pd.Series:
    match column.dtype:
        case "auto":
            return series
        case "datetime":
            return pd.to_datetime(series)
        case "timestamp":
            if pd.api.types.is_datetime64_any_dtype(series):
                return series
            s = pd.to_numeric(series, errors="coerce")
            s = s.where(s <= 1e11, s / 1000)
            return pd.to_datetime(s, unit="s", errors="coerce")
        case "date":
            return pd.to_datetime(series).dt.normalize()
        case "numeric":
            return pd.to_numeric(series, errors="coerce")
        case "bool":
            return series.astype(bool)
        case _:
            try:
                return series.astype(column.dtype)
            except (TypeError, ValueError) as exc:
                raise ValueError(
                    f"column '{column.name}': unsupported or incompatible dtype '{column.dtype}': {exc}"
                ) from exc


class Provenance(BaseModel):
    """Where and how tabular data was obtained."""

    source: str = ""
    source_description: str = ""
    params: dict[str, Any] = Field(default_factory=dict)
    fetched_at: datetime | None = None
    title: str | None = None
    properties: dict[str, Any] = Field(default_factory=dict)


class Result(BaseModel):
    """Free-form connector output: any data plus provenance and optional tabular schema.

    ``data`` can be anything the connector returns — a pandas DataFrame,
    a string, a dict, a Polars frame, etc.  The framework wraps connector
    return values automatically; connectors never need to construct this
    directly.

    A raw DataFrame/Series can still live in :class:`Result.data`, but it does not
    become a :class:`SemanticTableResult` until a tabular :class:`OutputConfig` is applied.
    :meth:`to_table` is the canonical late schema-application path (same as
    connector ``output=``).
    """

    model_config = {"arbitrary_types_allowed": True}

    data: Any
    provenance: Provenance
    output_schema: OutputConfig | None = Field(default=None)

    # ------------------------------------------------------------------
    # Factory classmethods
    # ------------------------------------------------------------------

    @classmethod
    def from_dataframe(
        cls,
        df: pd.DataFrame | pd.Series,
        provenance: Provenance | None = None,
    ) -> Result:
        """Build a raw :class:`Result` that happens to contain tabular data.

        Preferred over constructing ``Result`` manually when the connector has
        no ``OutputConfig`` and returns a plain DataFrame. This does *not*
        apply semantic table roles; use :meth:`to_table` for that transition.
        """
        frame = pd.DataFrame(df)
        if frame.empty:
            raise ValueError("Returned an empty DataFrame.")
        prov = provenance.model_copy(deep=True) if provenance is not None else Provenance()
        return Result(data=frame, provenance=prov)

    def to_table(
        self,
        output: OutputConfig,
    ) -> SemanticTableResult:
        """Apply an :class:`OutputConfig` to tabular data; same contract as ``@connector(output=...)``.

        Requires ``data`` to be a DataFrame or Series. Unmapped columns become ``DATA`` automatically.
        """
        if not isinstance(self.data, (pd.DataFrame, pd.Series)):
            raise TypeError(f"Result.to_table requires tabular data, got {type(self.data).__name__}")
        return output.build_table_result(
            self.data,
            provenance=self.provenance,
            params=self.provenance.params or None,
            merge_unmapped_as_data=True,
        )

    # ------------------------------------------------------------------
    # Convenience accessors
    # ------------------------------------------------------------------

    @property
    def columns(self) -> list[Column]:
        """Column schema (delegates to ``output_schema.columns``; empty list if no schema)."""
        if self.output_schema is None:
            return []
        return self.output_schema.columns

    @property
    def df(self) -> pd.DataFrame:
        """Return ``data`` as a :class:`~pandas.DataFrame`, raising if incompatible."""
        if isinstance(self.data, pd.DataFrame):
            return self.data
        if isinstance(self.data, pd.Series):
            return self.data.to_frame()
        raise TypeError(
            f"Result.data is {type(self.data).__name__}, not a DataFrame. Use result.data to access the raw value."
        )

    @property
    def text(self) -> str:
        """Return ``data`` as a string."""
        if isinstance(self.data, str):
            return self.data
        return str(self.data)

    @property
    def entity_keys(self) -> pd.DataFrame:
        """Subset of :attr:`data` containing columns with ``role == key``."""
        key_names = [c.mapped_name or c.name for c in self.columns if c.role == ColumnRole.KEY]
        if not key_names:
            return pd.DataFrame()
        df = self.df
        missing = [n for n in key_names if n not in df.columns]
        if missing:
            raise ValueError(f"Result data missing key columns: {missing}")
        return df[key_names].copy()

    @property
    def data_columns(self) -> list[Column]:
        return [c for c in self.columns if c.role == ColumnRole.DATA]

    @property
    def metadata_columns(self) -> list[Column]:
        return [c for c in self.columns if c.role == ColumnRole.METADATA]

class OutputConfig(BaseModel):
    """Declarative schema: maps raw data frames into :class:`SemanticTableResult` instances."""

    columns: list[Column]

    @model_validator(mode="after")
    def _validate_roles(self) -> OutputConfig:
        keys = [c.name for c in self.columns if c.role == ColumnRole.KEY]
        titles = [c.name for c in self.columns if c.role == ColumnRole.TITLE]
        if len(keys) > 1:
            raise ValueError(f"Output config must have at most one KEY column, found {len(keys)}: {keys}")
        if len(titles) > 1:
            raise ValueError(f"Output config must have at most one TITLE column, found {len(titles)}: {titles}")
        if not any(c.role in (ColumnRole.DATA, ColumnRole.KEY, ColumnRole.TITLE) for c in self.columns):
            raise ValueError("Output config must define at least one data, key, or title column")
        # Validate namespace-template placeholders reference sibling columns so that
        # per-row resolution in Catalog.index_result can never KeyError at runtime.
        column_names = {c.name for c in self.columns}
        for col in self.columns:
            if col.namespace_is_template:
                missing = [p for p in col.namespace_placeholders if p not in column_names]
                if missing:
                    raise ValueError(
                        f"namespace template {col.namespace!r} on column {col.name!r} references "
                        f"placeholders not declared as columns in the same OutputConfig: {missing}. "
                        f"Declared columns: {sorted(column_names)}."
                    )
        return self

    def validate_columns(self, df: pd.DataFrame) -> list[str]:
        """Return declared column names absent from *df* (excludes wildcards).

        Useful for diagnosing schema mismatches — a non-empty return value
        means some config columns will be silently skipped during
        :meth:`build_table_result`::

            missing = config.validate_columns(sample_df)
            # missing == ['close'] → typo? API renamed the column?
        """
        declared = {c.name for c in self.columns if c.name != "*"}
        return sorted(declared - set(df.columns))

    def _apply_columns(
        self,
        df: pd.DataFrame,
        params: dict[str, Any],
    ) -> tuple[pd.DataFrame, list[tuple[Column, str]], set[str]]:
        """Match config columns, coerce dtypes, rename.

        Returns processed frame, column info, and consumed input column names.
        """
        processed_series: list[tuple[Column, pd.Series]] = []
        consumed: set[str] = set()

        for column in self.columns:
            matches: list[str] = []
            if column.name in df.columns and column.name not in consumed:
                matches.append(column.name)
                consumed.add(column.name)
            elif column.name == "*":
                for col in df.columns:
                    if col not in consumed:
                        matches.append(col)
                        consumed.add(col)

            for match_name in matches:
                series = df[match_name].copy()
                pre_all_na = series.isna().all()
                series = _coerce_series_dtype(column, series)
                if not series.empty and not pre_all_na:
                    if column.dtype == "timestamp" and series.isna().all():
                        raise ValueError(
                            f"column '{column.name}': all values are NaT after 'timestamp' coercion — "
                            "expected unix epoch (seconds or milliseconds), got non-numeric input"
                        )
                    elif column.dtype == "numeric" and series.isna().all():
                        raise ValueError(
                            f"column '{column.name}': all values are NaN after 'numeric' coercion — "
                            "expected numeric input"
                        )
                new_name = column.mapped_name % params if column.mapped_name else match_name
                series.name = new_name
                processed_series.append((column, series))

        if not processed_series:
            return pd.DataFrame(), [], set()

        new_df = pd.concat([s for _, s in processed_series], axis=1)
        info = [(col, s.name) for col, s in processed_series]
        return new_df, info, consumed

    def build_table_result(
        self,
        df: pd.DataFrame | pd.Series,
        *,
        provenance: Provenance | None = None,
        params: dict[str, Any] | None = None,
        merge_unmapped_as_data: bool = True,
    ) -> SemanticTableResult:
        """Apply column schema; unmapped input columns become DATA when ``merge_unmapped_as_data`` is True.

        Empty tables are allowed when the frame declares column names that match the schema
        (e.g. zero search hits with the expected columns).
        """
        if not isinstance(df, (pd.DataFrame, pd.Series)):
            raise TypeError(f"OutputConfig.build_table_result expected a pandas DataFrame or Series, got {type(df)}")
        frame = pd.DataFrame(df)
        if frame.empty and len(frame.columns) == 0:
            raise ValueError("Returned an empty DataFrame with no columns.")

        p = provenance.model_copy(deep=True) if provenance is not None else Provenance()
        merge_params = params if params is not None else {}
        if merge_params and not p.params:
            p.params = dict(merge_params)

        full_df, columns_info, consumed = self._apply_columns(
            frame,
            merge_params,
        )

        declared = {c.name for c in self.columns if c.name != "*"}
        unmatched = sorted(declared - consumed)
        if unmatched:
            logger.warning(
                "OutputConfig columns not found in DataFrame: %s. Available columns: %s",
                unmatched,
                sorted(frame.columns),
            )

        if not columns_info:
            raise ValueError("Column config matched no input columns.")

        processed_series: list[tuple[Column, pd.Series]] = [
            (col_cfg, full_df[out_name]) for col_cfg, out_name in columns_info
        ]

        if merge_unmapped_as_data:
            for col in frame.columns:
                if col not in consumed:
                    series = frame[col].copy()
                    series.name = str(col)
                    data_col = Column(name=str(col), role=ColumnRole.DATA, dtype="auto")
                    processed_series.append((data_col, series))

        if not processed_series:
            raise ValueError("Column config produced no columns.")

        new_df = pd.concat([s for _, s in processed_series], axis=1)
        resolved_schema: list[Column] = [col_cfg.model_copy(update={"name": s.name}) for col_cfg, s in processed_series]
        resolved_config = OutputConfig(columns=resolved_schema)
        return SemanticTableResult(data=new_df, provenance=p, output_schema=resolved_config)


class SemanticTableResult(Result):
    """Tabular connector output with a required :attr:`output_schema`."""

    output_schema: OutputConfig
