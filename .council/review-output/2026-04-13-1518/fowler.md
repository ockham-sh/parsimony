# Fowler Review — Structural Quality
**Reviewer:** Martin Fowler (Refactoring)
**Date:** 2026-04-13
**Files reviewed:** `parsimony/connectors/tiers.py`, `parsimony/connectors/__init__.py`, `parsimony/connector.py`, `parsimony/__init__.py`

---

## Summary

The tier infrastructure is structurally clean. Four focused findings below — two that will slow the team down if left unaddressed, two worth noting but not urgent.

---

## Findings

---

FINDING:
- **Title:** `filter_by_tier` uses `object` for both parameter and return type, hiding its real contract
- **File:** `parsimony/connectors/tiers.py:100-136`
- **Principle:** Principle 4 — Names reveal design; bad names mask bad structure (Mysterious Name / names that lie)
- **Severity:** P2
- **What's wrong:** `filter_by_tier` accepts `connectors: object` and returns `object`, yet its body immediately casts the argument with `list(connectors)` and always returns `Connectors`. The signature actively lies about what this function does and what it requires. The comment `# type: ignore[arg-type]` and `# type: ignore[return-value]` on the callers confirm the type system has already given up.
- **Consequence:** Every call site must suppress mypy warnings, and any caller passing the wrong type gets a runtime `TypeError` with no static warning — exactly the class of bug static analysis is supposed to catch.
- **Fix:** Import `Connectors` at the top of `tiers.py` (the circular import that motivated the deferred import no longer exists once you look at the actual import graph — `tiers.py` imports nothing from `parsimony.connector` at module level) and type the signature correctly; if the circular import is genuine, use `TYPE_CHECKING` to keep it static-only.

---

FINDING:
- **Title:** `_TIER_REGISTRY` is a mutable module-level singleton with a lazy-initialisation race
- **File:** `parsimony/connectors/__init__.py:27-44`
- **Principle:** Principle 3 — State is the primary source of bugs; Global Data smell
- **Severity:** P2
- **What's wrong:** `_TIER_REGISTRY` is a module-level `dict | None` mutated by `_get_tier_registry()` via `global`. In an async context with concurrent first-calls (two callers both see `None` before either finishes), both will build and assign the registry — a benign race today but a hidden state dependency that violates the immutability principle the rest of the codebase follows (frozen dataclasses everywhere). The registry is also impossible to override in tests without monkeypatching module state.
- **Consequence:** The pattern is a latent bug factory: any future refactoring that touches the factory init path (e.g. adding a new provider) must remember this global exists and reason about initialisation order.
- **Fix:** Replace the lazy singleton with a pure function that constructs and returns the registry dict each time it is called; the call cost is negligible (three local imports and a dict literal), and the global state disappears entirely.

---

FINDING:
- **Title:** `build_fetch_connectors_from_env` and `build_connectors_from_env` are near-identical, violating the principle that things which change together should live together
- **File:** `parsimony/connectors/__init__.py:97-289`
- **Principle:** Principle 5 — Shotgun Surgery (one change requires edits across many files / duplicated structures)
- **Severity:** P2
- **What's wrong:** Adding a new optional provider (e.g. a new central bank) requires identical edits in both factory functions: the import, the key lookup, the `bind_deps` call, and the `result +` line. The two functions share the same provider sequence and differ only in whether they import `CONNECTORS` or `FETCH_CONNECTORS`. This is the textbook Shotgun Surgery smell applied within a single file.
- **Consequence:** The next provider addition will either be incomplete (one function updated, one forgotten) or will require a careful side-by-side diff of ~200 lines to verify correctness — velocity tax on every provider addition.
- **Fix:** Extract a registry of `(env_var, full_connectors_import, fetch_connectors_import)` entries that both factory functions iterate; the optional/required branching logic is identical and can be shared, leaving each factory to select the right connectors variant per entry.

---

FINDING:
- **Title:** `to_llm` in `Connector` reads `properties["min_tier"]` and `properties["provider"]` inline rather than through the tier module's vocabulary
- **File:** `parsimony/connector.py:364-367`
- **Principle:** Principle 5 — Feature Envy (code in the wrong module; `connector.py` reaching into tier-specific property keys it has no business knowing)
- **Severity:** P3
- **What's wrong:** `connector.py` has no import of `tiers.py` and no declared knowledge of tier concepts, yet it hard-codes the strings `"min_tier"` and `"provider"` — the same keys defined by convention in `tiers.py`. This is a soft coupling: if those key names ever change, `connector.py` silently stops rendering tier information without any error.
- **Consequence:** The tier vocabulary is split across two modules with no single authoritative definition, which compounds over time as more tier-aware rendering is added.
- **Fix:** Define the canonical property key names as constants in `tiers.py` (e.g. `MIN_TIER_KEY`, `PROVIDER_KEY`) and reference them from `connector.py`; or, if the coupling feels wrong, move the tier-aware rendering logic into `tiers.py` as a helper that `to_llm` delegates to.
