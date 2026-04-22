"""Structural lint: no provider names in kernel source.

Enforces the light-kernel invariant (DESIGN-distribution-model.md §5):

    Provider names appear in kernel source only in docstrings, examples, and
    conformance test fixtures. This is structurally enforced.

Strategy: walk every Python module under ``parsimony/`` and flag any
**import** that references a known provider name. Imports are the axis that
matters — a string literal that happens to contain ``"fred"`` is not a
structural coupling; ``from parsimony_fred import X`` is.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

# Every provider known to the ecosystem. Additions require conscious review —
# the set is the ground truth for the "kernel knows nothing about providers"
# invariant.
_FORBIDDEN_PROVIDER_NAMES: frozenset[str] = frozenset(
    {
        "alpha_vantage",
        "bde",
        "bdf",
        "bdp",
        "bls",
        "boc",
        "boj",
        "coingecko",
        "destatis",
        "eia",
        "eodhd",
        "finnhub",
        "financial_reports",
        "fmp",
        "fmp_screener",
        "fred",
        "polymarket",
        "rba",
        "riksbank",
        "sdmx",
        "sec_edgar",
        "snb",
        "tiingo",
        "treasury",
    }
)

_KERNEL_ROOT = Path(__file__).parent.parent / "parsimony"


def _forbidden_references(path: Path) -> list[tuple[int, str]]:
    """Return ``[(lineno, diagnostic), ...]`` for every forbidden import in *path*."""
    src = path.read_text()
    tree = ast.parse(src, filename=str(path))

    hits: list[tuple[int, str]] = []

    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            module = node.module or ""
            # parsimony.connectors.<provider>
            if module.startswith("parsimony.connectors."):
                suffix = module.split(".", 2)[2]
                head = suffix.split(".")[0]
                if head in _FORBIDDEN_PROVIDER_NAMES:
                    hits.append(
                        (
                            node.lineno,
                            f"import from parsimony.connectors.{head}",
                        )
                    )
            # parsimony_<provider>
            if module.startswith("parsimony_"):
                head = module[len("parsimony_") :].split(".")[0]
                if head in _FORBIDDEN_PROVIDER_NAMES:
                    hits.append(
                        (
                            node.lineno,
                            f"import from parsimony_{head}",
                        )
                    )
        elif isinstance(node, ast.Import):
            for alias in node.names:
                name = alias.name
                if name.startswith("parsimony_"):
                    head = name[len("parsimony_") :].split(".")[0]
                    if head in _FORBIDDEN_PROVIDER_NAMES:
                        hits.append(
                            (
                                node.lineno,
                                f"import parsimony_{head}",
                            )
                        )
                if name.startswith("parsimony.connectors."):
                    suffix = name.split(".", 2)[2]
                    head = suffix.split(".")[0]
                    if head in _FORBIDDEN_PROVIDER_NAMES:
                        hits.append(
                            (
                                node.lineno,
                                f"import parsimony.connectors.{head}",
                            )
                        )

    return hits


def test_kernel_source_contains_no_provider_imports() -> None:
    """Kernel source references no provider names in imports."""
    violations: list[str] = []
    for module in _KERNEL_ROOT.rglob("*.py"):
        rel = module.relative_to(_KERNEL_ROOT.parent)
        for lineno, diagnostic in _forbidden_references(module):
            violations.append(f"{rel}:{lineno}: {diagnostic}")

    if violations:
        joined = "\n  ".join(violations)
        pytest.fail(
            "Kernel source imports a provider name — violates the 'light kernel' "
            "structural invariant from DESIGN-distribution-model.md §5:\n"
            f"  {joined}\n\n"
            "Fix: move provider-specific code out of the kernel into its own "
            "parsimony_<name> package."
        )
