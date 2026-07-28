"""The composable filter contract: one tree, both backends, no silent weakening."""

from __future__ import annotations

import pyarrow as pa
import pyarrow.dataset as pads
import pytest

from parsimony.catalog.filters import (
    AllOf,
    AnyOf,
    F,
    FieldContains,
    FieldIn,
    FieldMatches,
    FieldPrefix,
    all_of,
    any_of,
    as_filter,
)
from parsimony.errors import InvalidParameterError

SCHEMA = ("geo", "freq", "unit")


def test_mapping_shorthand_ands_keys_and_ors_values() -> None:
    predicate = as_filter({"geo": "DE", "freq": ["M", "Q"]})
    assert predicate == AllOf((FieldIn("geo", ("DE",)), FieldIn("freq", ("M", "Q"))))
    assert predicate.matches({"geo": "DE", "freq": "Q"})
    assert not predicate.matches({"geo": "DE", "freq": "A"})
    assert not predicate.matches({"geo": "FR", "freq": "M"})


def test_bare_string_is_one_value_not_characters() -> None:
    assert as_filter({"geo": "DE"}) == FieldIn("geo", ("DE",))
    assert not as_filter({"geo": "DE"}).matches({"geo": "D"})


def test_single_key_shorthand_needs_no_group() -> None:
    assert as_filter({"geo": ["DE"]}) == FieldIn("geo", ("DE",))


def test_absent_and_null_cells_never_match() -> None:
    predicate = as_filter({"geo": "DE"})
    assert not predicate.matches({})
    assert not predicate.matches({"geo": None})


def test_multi_valued_cell_is_rejected() -> None:
    """Nested cells are display-only; filters require scalar columns."""
    predicate = as_filter({"geo": "DE"})
    with pytest.raises(InvalidParameterError, match="must be scalar"):
        predicate.matches({"geo": ["FR", "DE"]})


def test_non_string_cells_compare_as_text() -> None:
    assert as_filter({"year": 2024}).matches({"year": 2024})
    assert as_filter({"year": "2024"}).matches({"year": 2024})


def test_expression_form_round_trips_nested_or_of_and() -> None:
    predicate = as_filter(
        {
            "any": [
                {"all": [{"field": "geo", "eq": "DE"}, {"field": "freq", "eq": "M"}]},
                {"all": [{"field": "geo", "eq": "FR"}, {"field": "freq", "eq": "A"}]},
            ]
        }
    )
    assert predicate.matches({"geo": "DE", "freq": "M"})
    assert predicate.matches({"geo": "FR", "freq": "A"})
    assert not predicate.matches({"geo": "DE", "freq": "A"})
    assert not predicate.matches({"geo": "IT", "freq": "M"})


def test_builder_operators_match_expression_form() -> None:
    built = (F("geo").eq("DE") & F("freq").is_in(["M", "Q"])) | F("unit").eq("PC")
    assert built.fields() == {"geo", "freq", "unit"}
    assert built.matches({"geo": "DE", "freq": "M"})
    assert built.matches({"unit": "PC"})
    assert not built.matches({"geo": "DE", "freq": "A"})


def test_membership_values_deduplicate_preserving_order() -> None:
    assert as_filter({"freq": ["Q", "M", "Q"]}) == FieldIn("freq", ("Q", "M"))


def test_empty_filter_means_no_constraint() -> None:
    assert as_filter(None) is None
    assert as_filter({}) is None


def test_empty_membership_is_rejected_not_dropped() -> None:
    with pytest.raises(InvalidParameterError, match="silently drop the constraint"):
        as_filter({"geo": []})


def test_null_filter_value_is_rejected() -> None:
    with pytest.raises(InvalidParameterError, match="must not be null"):
        as_filter({"geo": None})


def test_nested_value_is_rejected() -> None:
    with pytest.raises(InvalidParameterError, match="must be a scalar"):
        as_filter({"geo": [["DE"]]})


def test_unsupported_expression_keys_are_rejected() -> None:
    with pytest.raises(InvalidParameterError, match="Unsupported filter expression keys"):
        as_filter({"field": "geo", "like": "DE%"})


def test_prefix_contains_match_ops() -> None:
    assert F("code").prefix("D.").matches({"code": "D.USD.A"})
    assert not F("code").prefix("D.").matches({"code": "M.USD.A"})
    assert F("code").contains("USD").matches({"code": "D.USD.A"})
    assert F("code").matches(r"^D\.[A-Z]{3}\.").matches({"code": "D.USD.A"})
    assert not F("code").matches(r"^D\.[A-Z]{3}\.").matches({"code": "M.USD.A"})
    assert as_filter({"field": "code", "prefix": "D."}) == FieldPrefix("code", "D.")
    assert as_filter({"field": "code", "contains": "USD"}) == FieldContains("code", "USD")
    assert as_filter({"field": "code", "match": r"^D\."}) == FieldMatches("code", r"^D\.")


def test_invalid_regex_is_rejected() -> None:
    with pytest.raises(InvalidParameterError, match="Invalid regex"):
        F("code").matches("(")


def test_empty_pattern_ops_are_rejected() -> None:
    with pytest.raises(InvalidParameterError, match="non-empty"):
        F("code").prefix("")
    with pytest.raises(InvalidParameterError, match="non-empty"):
        F("code").contains("")


@pytest.mark.parametrize(
    "spec",
    [
        {"field": "code", "prefix": "D."},
        {"field": "code", "contains": "USD"},
        {"field": "code", "match": r"^D\.[A-Z]{3}\."},
        {"all": [{"field": "code", "prefix": "D."}, {"field": "code", "contains": "EUR"}]},
    ],
)
def test_pattern_arrow_pushdown_agrees_with_python(spec: dict[str, object]) -> None:
    rows = [
        {"code": "D.USD.EUR.SP00.A"},
        {"code": "M.USD.EUR.SP00.A"},
        {"code": "D.GBP.EUR.SP00.A"},
        {"code": "FXUSDCAD"},
    ]
    predicate = as_filter(spec)
    assert predicate is not None
    dataset = pads.dataset(pa.Table.from_pylist(rows))
    pushed = dataset.to_table(filter=predicate.to_arrow(("code",))).to_pylist()
    evaluated = [row for row in rows if predicate.matches(row)]
    assert pushed == evaluated
    assert pushed, "fixture should not select the empty set"


def test_boolean_group_needs_two_members() -> None:
    with pytest.raises(InvalidParameterError, match="at least two members"):
        AllOf((FieldIn("geo", ("DE",)),))
    with pytest.raises(InvalidParameterError, match="at least two members"):
        AnyOf(())


def test_group_helpers_collapse_a_lone_member() -> None:
    only = FieldIn("geo", ("DE",))
    assert all_of(only) is only
    assert any_of(only) is only


def test_non_mapping_spec_is_rejected() -> None:
    with pytest.raises(InvalidParameterError, match="must be a mapping or a Filter"):
        as_filter(["geo=DE"])  # type: ignore[arg-type]


def test_arrow_compilation_rejects_unknown_column() -> None:
    with pytest.raises(InvalidParameterError, match="Unknown filter column 'nope'"):
        as_filter({"nope": "x"}).to_arrow(SCHEMA)


def test_rename_maps_every_field_in_the_tree() -> None:
    predicate = as_filter({"any": [{"field": "code", "eq": "A"}, {"field": "geo", "eq": "DE"}]})
    renamed = predicate.rename(lambda name: "key" if name == "code" else name)
    assert renamed.fields() == {"key", "geo"}
    assert renamed.matches({"key": "A"})


@pytest.mark.parametrize(
    "spec",
    [
        {"geo": "DE"},
        {"geo": ["DE", "FR"]},
        {"geo": "DE", "freq": ["M", "Q"]},
        {"any": [{"all": [{"field": "geo", "eq": "DE"}, {"field": "freq", "eq": "M"}]}, {"field": "unit", "eq": "PC"}]},
    ],
)
def test_arrow_pushdown_agrees_with_python_evaluation(spec: dict[str, object]) -> None:
    """The same tree must select the same rows through Arrow and in Python."""
    rows = [
        {"geo": g, "freq": f, "unit": u} for g in ("DE", "FR", "IT") for f in ("M", "Q", "A") for u in ("PC", "EUR")
    ]
    predicate = as_filter(spec)
    assert predicate is not None

    dataset = pads.dataset(pa.Table.from_pylist(rows))
    pushed = dataset.to_table(filter=predicate.to_arrow(SCHEMA)).to_pylist()
    evaluated = [row for row in rows if predicate.matches(row)]

    assert pushed == evaluated
    assert pushed, "fixture should not select the empty set"
