# Council Plan: Declarative Dependency Wiring

**Scope:** Replace the `CREDENTIALS` tuple-of-tuples with a simple `dict[str, str]` env-var mapping per provider, and split the factory helper into two intent-revealing functions.
**Context:** The current `CREDENTIALS: tuple[tuple[str, str, bool, str | None], ...]` has four positional fields but the `Connector` already knows which deps are required vs optional from the function signature. The `required` bool and `default` value are derivable state. The only information the factory actually needs is: which env var maps to which dep name.
**Boundaries:** No changes to `bind_deps()`, `Connector`, error hierarchy, or tier gating. Pure refactoring of the credential/dependency declaration and factory wiring.
**Council dispatched:** Fowler (3 recommendations), Dodds (2 recommendations), Collina (3 recommendations). Hunt/Leach/Performance/Willison/Saarinen/Friedman — no recommendations.

---

## Key Design Decision

**Three provider patterns, two factory functions:**

| Pattern | Example | dep_names | Factory behavior |
|---|---|---|---|
| Required provider | FRED, FMP | `api_key` in `dep_names` | **Raise** if env var missing |
| Optional provider | EODHD, EIA, BDF | `api_key` in `dep_names` | **Skip** if env var missing |
| Default-value provider | BLS, Riksbank | `api_key` in `optional_dep_names` | **Always include**, use function default |

The distinction between "required provider" (raise) and "optional provider" (skip) is a **factory-level decision**, not a per-dep decision. Both have `api_key` in `dep_names`. The factory call site makes the intent explicit with two different functions.

Public-data providers (Treasury, BOE, etc.) have `ENV_VARS = {}` — no wiring needed, always included.

---

## Task Sequence

### 1. Replace `CREDENTIALS` with `ENV_VARS: dict[str, str]` across all 21 provider modules

| | |
|---|---|
| **Domain** | Fowler × Carmack — Primitive Obsession (Principle 5) |
| **Ref** | `references/refactoring.md` → Principle 5 |
| **Depends on** | — |

Replace every `CREDENTIALS: tuple[tuple[str, str, bool, str | None], ...]` with `ENV_VARS: dict[str, str]` mapping dep names to env var names. For example, FRED becomes `ENV_VARS = {"api_key": "FRED_API_KEY"}`, Destatis becomes `ENV_VARS = {"username": "DESTATIS_USERNAME", "password": "DESTATIS_PASSWORD"}`, and public-data providers become `ENV_VARS = {}`. Drop the `required` bool and `default` value — they are derivable from the `Connector`'s own `dep_names`/`optional_dep_names`.

---

### 2. Replace `_bind_provider_credentials` + `_add_provider` with two intent-revealing factory helpers

| | |
|---|---|
| **Domain** | Collina × Carmack — Operational vs programmer errors (Principle 1). Cross-ref: Dodds informed the dep_names derivation; Fowler informed the merge of two helpers. |
| **Ref** | `references/quality-backend.md` → Principle 1 |
| **Depends on** | Task 1 |

Replace the current two helpers with two intent-revealing functions: `_require_provider(result, connectors, env_vars, env)` raises `ValueError` if any dep in `dep_names` has no env var value — this is a configuration error, crash immediately. `_include_provider(result, connectors, env_vars, env)` skips the provider if any dep in `dep_names` has no env var value, and skips binding for deps in `optional_dep_names` whose env var is absent (the function signature default kicks in). Both read `dep_names`/`optional_dep_names` from the first connector in the collection to determine behavior — no external metadata needed.

---

### 3. Update both factory functions to use the new helpers

| | |
|---|---|
| **Domain** | Fowler × Carmack — Shotgun Surgery (Principle 5) |
| **Ref** | `references/refactoring.md` → Principle 5 |
| **Depends on** | Tasks 1, 2 |

Rewrite the factory call sites to use `_require_provider` for FRED, FMP, FMP_SCREENER (hard required — crash if missing) and `_include_provider` for everything else (optional providers skip silently, default-value providers always bind). Log at INFO level when an optional provider is skipped, naming the exact env var. The factory's existing comment structure (Required / Optional / Public data) becomes self-documenting through the function names.

---

### 4. Update tests

| | |
|---|---|
| **Domain** | Dodds × Carmack — Test behaviour (Principle 4) |
| **Ref** | `references/quality-frontend.md` → Principle 4 |
| **Depends on** | Tasks 1, 2, 3 |

Update any tests that reference `CREDENTIALS` to use `ENV_VARS`. Verify factory behavior for all three patterns: required provider raises on missing key, optional provider is silently skipped, default-value provider is always included. The existing factory tests should pass with minimal changes since the external behavior is unchanged.

---

## Risks & Watchpoints

- **Collina — Silent skip vs crash:** The distinction between `_require_provider` (crash) and `_include_provider` (skip) must be correct for every provider. If a required provider accidentally uses `_include_provider`, it silently disappears from the collection instead of raising. Review every factory call site.

- **Dodds — dep_names inspection:** The helpers inspect `dep_names`/`optional_dep_names` from the first connector in the collection. All connectors in a provider's collection should have the same dep signature. If a provider mixes connectors with different dep requirements (unlikely but possible), the first connector's deps may not represent all connectors.

---

## External Setup Required

No external setup required. All tasks can be implemented within the codebase.

---

## Summary

| # | Task | Domain | Depends on |
|---|------|--------|------------|
| 1 | Replace CREDENTIALS with ENV_VARS dict | Fowler | — |
| 2 | Two intent-revealing factory helpers | Collina | 1 |
| 3 | Update factory call sites | Fowler | 1, 2 |
| 4 | Update tests | Dodds | 1, 2, 3 |

## Verdict

This is a 4-tuple → dict refactoring that eliminates two redundant fields by letting the Connector's own type information drive policy. The most important decision is Collina's: the "skip vs crash" distinction lives in the factory function name (`_require_provider` vs `_include_provider`), not in per-provider metadata. This makes the factory self-documenting — you can read the call site and know exactly what happens when an env var is missing. Start with Task 1 (mechanical find-and-replace across 21 files), then Task 2 (the two new helpers), then wire them together.
