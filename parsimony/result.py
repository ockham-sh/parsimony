"""Result primitives: free-form output with optional tabular schema and provenance."""

from __future__ import annotations

import json
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pydantic import BaseModel, Field, model_validator

_RESULT_SCHEMA_META_KEY = b"parsimony.result"


class ColumnRole(str, Enum):
    """Semantic role of a column in a tabular result."""

    DATA = "data"
    KEY = "key"
    TITLE = "title"
    METADATA = "metadata"


class Column(BaseModel):
    """Declared column in an :class:`OutputConfig` schema."""

    name: str
    dtype: str = "auto"
    role: ColumnRole = ColumnRole.DATA
    mapped_name: str | None = None
    param_key: str | None = None
    description: str | None = None
    exclude_from_llm_view: bool = False
    #: Catalog namespace for the entity code when ``role`` is :attr:`ColumnRole.KEY`.
    #: Set when using :meth:`~parsimony.catalog.Catalog.index_result` so the table is self-describing.
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
        return self


def _coerce_series_dtype(column: Column, series: pd.Series) -> pd.Series:
    match column.dtype:
        case "auto":
            return series
        case "datetime":
            return pd.to_datetime(series)
        case "timestamp":
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
            return series.astype(column.dtype)


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
            raise TypeError(
                f"Result.to_table requires tabular data, got {type(self.data).__name__}"
            )
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
            f"Result.data is {type(self.data).__name__}, not a DataFrame. "
            "Use result.data to access the raw value."
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

    # ------------------------------------------------------------------
    # Arrow / Parquet serialization (tabular data only)
    # ------------------------------------------------------------------

    def to_arrow(self) -> pa.Table:
        """Serialize to Arrow with embedded schema + provenance metadata.

        Requires ``data`` to be a DataFrame and :attr:`output_schema` to be set.
        """
        table = pa.Table.from_pandas(self.df, preserve_index=False)
        payload = {
            "columns": [c.model_dump(mode="json") for c in self.columns],
            "provenance": self.provenance.model_dump(mode="json"),
        }
        meta = dict(table.schema.metadata or {})
        meta[_RESULT_SCHEMA_META_KEY] = json.dumps(payload, default=str).encode("utf-8")
        return table.replace_schema_metadata(meta)

    @classmethod
    def from_arrow(cls, table: pa.Table) -> SemanticTableResult:
        raw = (table.schema.metadata or {}).get(_RESULT_SCHEMA_META_KEY)
        if not raw:
            raise ValueError("Arrow table missing parsimony.result metadata")
        payload = json.loads(raw.decode("utf-8"))
        df = table.to_pandas()
        columns = [Column.model_validate(c) for c in payload["columns"]]
        provenance = Provenance.model_validate(payload["provenance"])
        output_schema = OutputConfig(columns=columns)
        return SemanticTableResult(data=df, provenance=provenance, output_schema=output_schema)

    def to_parquet(self, path: str | Path) -> None:
        """Write Parquet with embedded column schema and provenance."""
        table = self.to_arrow()
        pq.write_table(table, path)

    @classmethod
    def from_parquet(cls, path: str | Path) -> SemanticTableResult:
        table = pq.read_table(path)
        return cls.from_arrow(table)


class OutputConfig(BaseModel):
    """Declarative schema: maps raw data frames into :class:`SemanticTableResult` instances."""

    columns: list[Column]

    @model_validator(mode="after")
    def _validate_roles(self) -> OutputConfig:
        keys = [c.name for c in self.columns if c.role == ColumnRole.KEY]
        titles = [c.name for c in self.columns if c.role == ColumnRole.TITLE]
        if len(keys) > 1:
            raise ValueError(
                f"Output config must have at most one KEY column, found {len(keys)}: {keys}"
            )
        if len(titles) > 1:
            raise ValueError(
                f"Output config must have at most one TITLE column, found {len(titles)}: {titles}"
            )
        if len(keys) == 1 and len(titles) != 1:
            raise ValueError(
                "Output config with a KEY column must define exactly one TITLE column"
            )
        if not any(
            c.role in (ColumnRole.DATA, ColumnRole.KEY, ColumnRole.TITLE)
            for c in self.columns
        ):
            raise ValueError(
                "Output config must define at least one data, key, or title column"
            )
        return self

    def _apply_columns(
        self,
        df: pd.DataFrame,
        params: dict[str, Any],
    ) -> tuple[pd.DataFrame, list[tuple[Column, str]], set[str]]:
        """Match config columns, coerce dtypes, rename; returns processed frame, column info, and consumed input column names."""
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
                series = _coerce_series_dtype(column, series)
                if column.mapped_name:
                    new_name = column.mapped_name % params
                else:
                    new_name = match_name
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
            raise TypeError(
                f"OutputConfig.build_table_result expected a pandas DataFrame or Series, got {type(df)}"
            )
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
        resolved_schema: list[Column] = [
            col_cfg.model_copy(update={"name": s.name})
            for col_cfg, s in processed_series
        ]
        resolved_config = OutputConfig(columns=resolved_schema)
        return SemanticTableResult(data=new_df, provenance=p, output_schema=resolved_config)


class SemanticTableResult(Result):
    """Tabular connector output with a required :attr:`output_schema`."""

    output_schema: OutputConfig  # type: ignore[assignment]
