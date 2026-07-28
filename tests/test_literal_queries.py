"""A query is text, not a grammar.

``FIELD: value`` clauses joined by ``&&`` and ``,`` used to be parsed into field
scopes and exact constraints. They are not any more, and these tests exist to keep
it that way: the punctuation must be matched as text, never interpreted. The
failure mode being guarded is silent, which is why it is worth its own module — a
caller who believes a filter was applied will trust rows that never satisfied it.
"""

from __future__ import annotations

import pytest

from parsimony.catalog import BM25Index, Catalog, Entity


def _catalog() -> Catalog:
    entries = [
        Entity(namespace="ns", code="A", title="Title A", metadata={"REF_AREA": "Germany", "ICP_ITEM": "energy"}),
        Entity(namespace="ns", code="B", title="Title B", metadata={"REF_AREA": "Italy", "ICP_ITEM": "energy"}),
        Entity(namespace="ns", code="C", title="Title C", metadata={"REF_AREA": "Germany", "ICP_ITEM": "food"}),
        Entity(namespace="ns", code="D", title="Title D", metadata={"REF_AREA": "France", "ICP_ITEM": "food"}),
        Entity(namespace="ns", code="E", title="Title E", metadata={"REF_AREA": "Spain", "ICP_ITEM": "housing"}),
        Entity(namespace="ns", code="F", title="Title F", metadata={"REF_AREA": "Portugal", "ICP_ITEM": "health"}),
        Entity(namespace="ns", code="G", title="Title G", metadata={"REF_AREA": "Greece", "ICP_ITEM": "education"}),
    ]
    catalog = Catalog("test_cat")
    catalog.set_indexes({"title": BM25Index(), "REF_AREA": BM25Index(), "ICP_ITEM": BM25Index()})
    catalog.set_entities(entries)
    catalog.build()
    return catalog


@pytest.mark.parametrize(
    "query",
    ["REF_AREA: Germany", "REF_AREA: Germany, Italy", "REF_AREA: Germany && ICP_ITEM: energy"],
    ids=["single-clause", "comma-or", "ampersand-and"],
)
def test_dsl_spellings_do_not_scope_or_constrain(query: str) -> None:
    """A former DSL query is scored as text against the default field, or misses.

    The decisive property is not what it returns but what it must NOT do: pick up
    ``REF_AREA`` as a scope, or apply ``Germany`` as an exact constraint. Titles
    here carry none of those words, so any interpretation would show up as
    Germany's rows being selected — and text matching cannot select them.
    """
    catalog = _catalog()

    matches = catalog.search(query, limit=10)

    assert {m.code for m in matches} != {"A", "C"}, "the field scope was interpreted"
    assert {m.code for m in matches} != {"A"}, "the clauses were applied as an AND filter"


def test_an_unknown_field_name_in_a_query_is_just_a_word() -> None:
    """``UNKNOWN_FIELD: x`` used to raise; now nothing about it is a field name.

    Rejecting it would mean the parser still exists. Only ``field=`` names a field,
    and only that spelling can be wrong.
    """
    catalog = _catalog()

    assert catalog.search("UNKNOWN_FIELD: Germany", limit=5) == []
    with pytest.raises(Exception, match="UNKNOWN_FIELD"):
        catalog.search("anything", field="UNKNOWN_FIELD", limit=5)


def test_a_colon_bearing_value_is_searchable_as_itself() -> None:
    """Values legitimately contain colons; they must be findable, not parsed.

    The live case that motivated this: a Riksbank policy round is literally named
    ``2026:1``. Under a grammar that string is a clause naming the field ``2026``.
    """
    catalog = Catalog("colon_cat")
    catalog.set_indexes({"title": BM25Index()})
    catalog.set_entities(
        [
            Entity(namespace="ns", code="R1", title="Policy round 2026:1"),
            Entity(namespace="ns", code="R2", title="Policy round 2025:4"),
        ]
    )
    catalog.build()

    matches = catalog.search("2026:1", limit=5)

    assert matches, "a colon-bearing value must be findable by its own text"
    assert matches[0].code == "R1"


def test_filter_is_where_exact_constraints_live() -> None:
    """The replacement for every DSL clause: AND across columns, OR within one."""
    catalog = _catalog()

    both = catalog.search(filter={"REF_AREA": ["Germany"], "ICP_ITEM": ["energy"]}, limit=5)
    assert {m.code for m in both} == {"A"}

    either = catalog.search(filter={"REF_AREA": ["Germany", "France"], "ICP_ITEM": ["food"]}, limit=5)
    assert {m.code for m in either} == {"C", "D"}

    # A scalar is shorthand for a one-element list, so callers never wrap by hand.
    scalar = catalog.search(filter={"REF_AREA": "Germany"}, limit=5)
    assert {m.code for m in scalar} == {"A", "C"}
