"""Guard that public ``parsimony`` export surfaces stay aligned."""

from __future__ import annotations

import ast
from pathlib import Path

import parsimony


def _type_checking_export_names() -> set[str]:
    init_path = Path(parsimony.__file__).resolve()
    tree = ast.parse(init_path.read_text(encoding="utf-8"))
    names: set[str] = set()
    for node in tree.body:
        if isinstance(node, ast.If) and isinstance(node.test, ast.Name) and node.test.id == "TYPE_CHECKING":
            for child in node.body:
                if isinstance(child, ast.ImportFrom) and child.module:
                    for alias in child.names:
                        names.add(alias.asname or alias.name)
    return names


def test_lazy_imports_keys_match_catalog_exports_in_all() -> None:
    lazy_keys = set(parsimony._LAZY_IMPORTS)
    type_checking = _type_checking_export_names()
    catalog_symbols = type_checking & lazy_keys

    assert catalog_symbols, "expected TYPE_CHECKING catalog symbols overlapping _LAZY_IMPORTS"
    for name in sorted(catalog_symbols):
        assert name in parsimony.__all__, f"{name!r} missing from __all__"
        assert name in lazy_keys, f"{name!r} missing from _LAZY_IMPORTS"


def test_namespace_in_all() -> None:
    assert "Namespace" in parsimony.__all__


def test_type_checking_and_lazy_imports_agree_on_heavy_symbols() -> None:
    type_checking = _type_checking_export_names()
    lazy_keys = set(parsimony._LAZY_IMPORTS)
    assert type_checking >= lazy_keys
    assert lazy_keys <= set(parsimony.__all__)
