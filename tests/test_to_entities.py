"""Tests for projecting role-annotated results into catalog entities (`result.to_entities()`)."""

from __future__ import annotations

import pandas as pd
import pytest

from parsimony import (
    Column,
    ColumnRole,
    OutputSpec,
    Result,
    connector,
    enumerator,
)
from parsimony.result import Provenance

SPEC = OutputSpec(
    columns=[
        Column(name="series_id", role=ColumnRole.KEY, namespace="fred"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="units", role=ColumnRole.METADATA),
        Column(name="date", role=ColumnRole.DATA),
        Column(name="value", role=ColumnRole.DATA),
    ]
)


def _frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "series_id": ["UNRATE", "UNRATE", "GDP"],
            "title": ["Unemployment", "Unemployment", "GDP"],
            "units": ["Percent", "Percent", "USD"],
            "date": ["2026-01-01", "2026-02-01", "2026-Q1"],
            "value": [4.0, 3.9, 100.0],
        }
    )


def _result(df: pd.DataFrame | None = None, spec: OutputSpec | None = SPEC) -> Result:
    prov = Provenance(source="fred_fetch", source_description="FRED")
    return Result(data=_frame() if df is None else df, provenance=prov, output_spec=spec)


# ---------------------------------------------------------------------------
# Projection semantics
# ---------------------------------------------------------------------------


def test_projection_groups_rows_by_entity() -> None:
    entities = _result().to_entities()
    assert [(e.namespace, e.code, e.title) for e in entities] == [
        ("fred", "UNRATE", "Unemployment"),
        ("fred", "GDP", "GDP"),
    ]
    assert entities[0].metadata == {"units": "Percent"}
    assert entities[1].metadata == {"units": "USD"}


def test_projection_leaves_raw_data_unchanged() -> None:
    r = _result()
    _ = r.to_entities()
    assert list(r.data.columns) == ["series_id", "title", "units", "date", "value"]
    assert len(r.data) == 3


def test_copies_project_from_their_own_data() -> None:
    # Regression tripwire: an earlier cached projection leaked across
    # model_copy; projection must always answer from the copy's own data.
    spec = OutputSpec(
        columns=[
            Column(name="k", role=ColumnRole.KEY, namespace="demo"),
            Column(name="v", role=ColumnRole.DATA),
        ]
    )
    r1 = Result(data=pd.DataFrame({"k": ["A"], "v": [1.0]}), output_spec=spec)
    assert [e.code for e in r1.to_entities()] == ["A"]
    r2 = r1.model_copy(update={"data": pd.DataFrame({"k": ["B"], "v": [2.0]})})
    assert [e.code for e in r2.to_entities()] == ["B"]
    assert [e.code for e in r1.to_entities()] == ["A"]


def test_empty_frame_projects_to_no_entities() -> None:
    assert _result(_frame().iloc[0:0]).to_entities() == []


def test_bare_empty_dataframe_projects_to_no_entities() -> None:
    # The "no results" idiom: a columnless pd.DataFrame() yields no entities
    # rather than tripping the declared-column presence check.
    assert _result(pd.DataFrame()).to_entities() == []


# ---------------------------------------------------------------------------
# Validation at projection time
# ---------------------------------------------------------------------------


def test_non_tabular_result_raises_typeerror() -> None:
    r = Result(data={"a": 1}, output_spec=None)
    with pytest.raises(TypeError, match="tabular"):
        r.to_entities()


def test_missing_spec_raises() -> None:
    with pytest.raises(ValueError, match="output spec"):
        _result(spec=None).to_entities()


def test_spec_without_key_raises() -> None:
    spec = OutputSpec(columns=[Column(name="value", role=ColumnRole.DATA)])
    with pytest.raises(ValueError, match="KEY"):
        _result(spec=spec).to_entities()


def test_key_without_namespace_raises() -> None:
    spec = OutputSpec(
        columns=[
            Column(name="series_id", role=ColumnRole.KEY),
            Column(name="value", role=ColumnRole.DATA),
        ]
    )
    with pytest.raises(ValueError, match="namespace"):
        _result(spec=spec).to_entities()


def test_invalid_namespace_names_the_key_column() -> None:
    spec = OutputSpec(
        columns=[
            Column(name="series_id", role=ColumnRole.KEY, namespace="Not Valid"),
            Column(name="value", role=ColumnRole.DATA),
        ]
    )
    with pytest.raises(ValueError, match="Not Valid.*series_id"):
        _result(spec=spec).to_entities()


def test_missing_declared_columns_raise() -> None:
    df = _frame().drop(columns=["units", "value"])
    with pytest.raises(ValueError, match=r"\['units', 'value'\]"):
        _result(df).to_entities()


def test_null_keys_raise() -> None:
    df = _frame()
    df.loc[1, "series_id"] = None
    with pytest.raises(ValueError, match="null"):
        _result(df).to_entities()


def test_invalid_code_names_the_key_column() -> None:
    df = _frame().iloc[0:1].copy()
    df["series_id"] = "   "
    with pytest.raises(ValueError, match="series_id"):
        _result(df).to_entities()


def test_conflicting_titles_raise() -> None:
    df = _frame()
    df.loc[1, "title"] = "Something else"
    with pytest.raises(ValueError, match="title"):
        _result(df).to_entities()


def test_conflicting_metadata_raises() -> None:
    df = _frame()
    df.loc[1, "units"] = "Ratio"
    with pytest.raises(ValueError, match="units"):
        _result(df).to_entities()


def test_null_plus_one_value_is_accepted() -> None:
    df = _frame()
    df.loc[0, "title"] = None
    df.loc[0, "units"] = None
    unrate = _result(df).to_entities()[0]
    assert unrate.title == "Unemployment"
    assert unrate.metadata == {"units": "Percent"}


def test_all_null_title_falls_back_to_code() -> None:
    df = _frame()
    df["title"] = None
    df["units"] = None
    unrate = _result(df).to_entities()[0]
    assert unrate.title == "UNRATE"
    assert unrate.metadata == {}


def test_empty_string_title_fails_entity_validation() -> None:
    # None means "absent" and falls back to the code; an empty string is a
    # data-quality bug and must fail loudly, not be silently substituted.
    df = _frame()
    df["title"] = ""
    with pytest.raises(ValueError, match="title"):
        _result(df).to_entities()


def test_duplicate_entity_after_normalization_raises() -> None:
    df = _frame()
    df.loc[1, "series_id"] = " UNRATE "
    with pytest.raises(ValueError, match="Duplicate entity"):
        _result(df).to_entities()


# ---------------------------------------------------------------------------
# Wildcard metadata
# ---------------------------------------------------------------------------


def test_wildcard_metadata_claims_undeclared_columns() -> None:
    spec = OutputSpec(
        columns=[
            Column(name="series_id", role=ColumnRole.KEY, namespace="fred"),
            Column(name="title", role=ColumnRole.TITLE),
            Column(name="*", role=ColumnRole.METADATA),
        ]
    )
    df = pd.DataFrame(
        {
            "series_id": ["A"],
            "title": ["Alpha"],
            "frequency": ["M"],
            "seasonal": ["SA"],
        }
    )
    (entity,) = _result(df, spec).to_entities()
    assert entity.metadata == {"frequency": "M", "seasonal": "SA"}


# ---------------------------------------------------------------------------
# Through connectors
# ---------------------------------------------------------------------------


def test_connector_result_projects_entities() -> None:
    @connector(output=SPEC)
    def fetch() -> pd.DataFrame:
        """Fetch demo series."""
        return _frame()

    assert [e.code for e in fetch().to_entities()] == ["UNRATE", "GDP"]


def test_enumerator_projects_entities() -> None:
    @enumerator(
        output=OutputSpec(
            columns=[
                Column(name="series_id", role=ColumnRole.KEY, namespace="fred"),
                Column(name="title", role=ColumnRole.TITLE),
            ]
        )
    )
    def list_series() -> pd.DataFrame:
        """List demo series."""
        return pd.DataFrame({"series_id": ["A", "B"], "title": ["Alpha", "Beta"]})

    entities = list_series().to_entities()
    assert [(e.code, e.title) for e in entities] == [("A", "Alpha"), ("B", "Beta")]
