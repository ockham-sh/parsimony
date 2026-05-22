"""Result primitives: free-form output with optional tabular schema and provenance."""

from __future__ import annotations

__all__ = [
    "Column",
    "ColumnRole",
    "OutputConfig",
    "Provenance",
    "Result",
    "TabularResult",
]

import json
import logging
from datetime import datetime
from enum import StrEnum
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from parsimony.catalog.models import CatalogEntry

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pydantic import AliasChoices, BaseModel, ConfigDict, Field, model_validator

# Oversized values are replaced with a structured marker rather than a
# prefix — a prefix can leak the head of an unredacted secret.
_PROVENANCE_FIELD_BUDGET = 2000

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
    """Where and how tabular data was obtained.

    Framework-only type. Connector code never imports this; the framework
    builds it in :meth:`Connector._wrap_result`. Connectors contribute
    source-specific extras through :meth:`Result.with_properties`.
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


class Result(BaseModel):
    """Opaque connector output: any payload plus provenance.

    Use :class:`TabularResult` when ``data`` is a :class:`~pandas.DataFrame`.
    The framework wraps connector return values automatically; connectors
    should not construct this directly. To attach source-specific extras,
    use :meth:`with_properties`.
    """

    model_config = {"arbitrary_types_allowed": True}

    data: Any
    provenance: Provenance = Field(default_factory=lambda: Provenance(source="", source_description=""))

    def with_properties(self, **properties: Any) -> Result:
        """Merge source-specific extras into ``provenance.properties``."""
        merged = {**self.provenance.properties, **properties}
        new_prov = self.provenance.model_copy(update={"properties": merged})
        return self.model_copy(update={"provenance": new_prov})

    @property
    def text(self) -> str:
        if isinstance(self.data, str):
            return self.data
        return str(self.data)


class TabularResult(Result):
    """Tabular connector output with optional :class:`OutputConfig` schema."""

    data: pd.DataFrame
    output_schema: OutputConfig | None = Field(default=None)

    @classmethod
    def from_dataframe(cls, df: pd.DataFrame | pd.Series) -> TabularResult:
        """Build tabular data with no schema applied."""
        frame = pd.DataFrame(df)
        if frame.empty:
            raise ValueError("Returned an empty DataFrame.")
        return cls(data=frame)

    def with_properties(self, **properties: Any) -> TabularResult:
        merged = {**self.provenance.properties, **properties}
        new_prov = self.provenance.model_copy(update={"properties": merged})
        return self.model_copy(update={"provenance": new_prov})

    def to_table(self, output: OutputConfig) -> TabularResult:
        """Apply *output* to tabular data. Unmapped columns become DATA automatically."""
        result = output.build_table_result(self.data, merge_unmapped_as_data=True)
        return result.model_copy(update={"provenance": self.provenance})

    @property
    def columns(self) -> list[Column]:
        if self.output_schema is None:
            return []
        return self.output_schema.columns

    @property
    def df(self) -> pd.DataFrame:
        return self.data

    @property
    def entity_keys(self) -> pd.DataFrame:
        key_names = [c.mapped_name or c.name for c in self.columns if c.role == ColumnRole.KEY]
        if not key_names:
            return pd.DataFrame()
        missing = [n for n in key_names if n not in self.data.columns]
        if missing:
            raise ValueError(f"Result data missing key columns: {missing}")
        return self.data[key_names].copy()

    @property
    def data_columns(self) -> list[Column]:
        return [c for c in self.columns if c.role == ColumnRole.DATA]

    @property
    def metadata_columns(self) -> list[Column]:
        return [c for c in self.columns if c.role == ColumnRole.METADATA]

    def to_arrow(self) -> pa.Table:
        """Serialize to Arrow with embedded provenance and optional schema metadata."""
        table = pa.Table.from_pandas(self.data, preserve_index=False)
        payload: dict[str, Any] = {
            "provenance": self.provenance.safe_dump(),
        }
        if self.output_schema is not None:
            payload["columns"] = [c.model_dump(mode="json") for c in self.output_schema.columns]
        meta = dict(table.schema.metadata or {})
        meta[_RESULT_SCHEMA_META_KEY] = json.dumps(payload, default=str).encode("utf-8")
        return table.replace_schema_metadata(meta)

    @classmethod
    def from_arrow(cls, table: pa.Table) -> TabularResult:
        """Deserialize an Arrow table written by :meth:`to_arrow`."""
        df = table.to_pandas()
        raw = (table.schema.metadata or {}).get(_RESULT_SCHEMA_META_KEY)
        if not raw:
            return cls(data=df)
        payload = json.loads(raw.decode("utf-8"))
        provenance = Provenance.model_validate(payload.get("provenance", {}))
        cols_raw = payload.get("columns") or []
        if cols_raw:
            columns = [Column.model_validate(c) for c in cols_raw]
            return cls(
                data=df,
                provenance=provenance,
                output_schema=OutputConfig(columns=columns),
            )
        return cls(data=df, provenance=provenance)

    def to_parquet(self, path: str | Path) -> None:
        """Write Parquet with embedded column schema and provenance."""
        pq.write_table(self.to_arrow(), path)

    @classmethod
    def from_parquet(cls, path: str | Path) -> TabularResult:
        """Read Parquet written by :meth:`to_parquet`."""
        return cls.from_arrow(pq.read_table(path))


class OutputConfig(BaseModel):
    """Declarative schema: maps raw data frames into schema-applied :class:`TabularResult` instances."""

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
        return self

    def validate_columns(self, df: pd.DataFrame) -> list[str]:
        """Return declared column names absent from *df* (excludes wildcards)."""
        declared = {c.name for c in self.columns if c.name != "*"}
        return sorted(declared - set(df.columns))

    def _apply_columns(
        self,
        df: pd.DataFrame,
        params: dict[str, Any],
    ) -> tuple[pd.DataFrame, list[tuple[Column, str]], set[str]]:
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
        merge_unmapped_as_data: bool = True,
    ) -> TabularResult:
        """Apply column schema to *df*; unmapped columns become DATA when requested."""
        if not isinstance(df, (pd.DataFrame, pd.Series)):
            raise TypeError(f"OutputConfig.build_table_result expected a pandas DataFrame or Series, got {type(df)}")
        frame = pd.DataFrame(df)
        if frame.empty and len(frame.columns) == 0:
            raise ValueError("Returned an empty DataFrame with no columns.")

        full_df, columns_info, consumed = self._apply_columns(frame, {})

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
        return TabularResult(data=new_df, output_schema=resolved_config)

    def build_entries(self, df: pd.DataFrame) -> list[CatalogEntry]:
        """Apply this schema to *df* to extract a list of :class:`CatalogEntry`.

        The schema must declare exactly one ``KEY`` column with a
        ``namespace``. Optional ``TITLE`` and ``METADATA`` columns populate
        the corresponding fields on each entry. A metadata column named
        ``"*"`` is a wildcard that matches every DataFrame column not
        already claimed by ``KEY``, ``TITLE``, or another explicit
        ``METADATA`` entry.
        """
        from parsimony.catalog.catalog import _entries_from_dataframe

        key_cols = [c for c in self.columns if c.role == ColumnRole.KEY]
        if len(key_cols) != 1:
            raise ValueError(f"Expected exactly one KEY column, found {len(key_cols)}")
        key_col = key_cols[0]
        if not key_col.namespace:
            raise ValueError("KEY column must declare namespace=...")
        title_cols = [c for c in self.columns if c.role == ColumnRole.TITLE]
        title_name = title_cols[0].name if len(title_cols) == 1 else None

        declared_meta = [c.name for c in self.columns if c.role == ColumnRole.METADATA]
        explicit_meta = [name for name in declared_meta if name != "*"]
        if "*" in declared_meta:
            claimed = {key_col.name, *([title_name] if title_name else []), *explicit_meta}
            wildcard_meta = [str(col) for col in df.columns if str(col) not in claimed]
            meta_names = [*explicit_meta, *wildcard_meta]
        else:
            meta_names = explicit_meta

        return _entries_from_dataframe(
            df,
            namespace=key_col.namespace,
            key_column=key_col.name,
            title_column=title_name,
            metadata_columns=meta_names,
        )
