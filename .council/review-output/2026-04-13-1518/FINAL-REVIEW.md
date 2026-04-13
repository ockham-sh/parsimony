# Council Review: Provider API Tier Awareness

**Scope:** 9 files — `parsimony/connectors/tiers.py` (new), `parsimony/connectors/__init__.py`, `parsimony/connector.py`, `parsimony/__init__.py`, `parsimony/connectors/fmp.py`, `parsimony/connectors/eodhd.py`, `parsimony/connectors/financial_reports.py`, `tests/test_tiers.py` (new), `tests/test_fmp_connectors.py`
**Context:** Parsimony is a Python data connector library. This review covers the new tier awareness feature: ordinal tier ladders per provider, factory-time gating via `gate_by_tier`, typed `TierError`/`RateLimitError` exceptions replacing ad hoc `ValueError`, and `to_llm()` tier annotations.
**Council dispatched:** Hunt (5 findings), Collina (6 findings), Fowler (4 findings), Dodds (6 findings), Beck (8 findings), Willison (no findings), Leach (no findings — no DB), Saarinen (no findings — no UI), Friedman (no findings — no UX), Performance (no findings — negligible cost)

---

## P2 — Fix Soon

### 1. `filter_by_tier` typed as `object` — type system fully erased at the tier boundary

| | |
|---|---|
| **File** | `parsimony/connectors/tiers.py:100-136` |
| **Council** | Dodds x Carmack — Use the type system as armour (Principle 3). Cross-ref: Fowler also flagged this (Names reveal design, Principle 4). |
| **Ref** | `references/quality-frontend.md` → Principle 3 |

**Finding:** `filter_by_tier` accepts `connectors: object` and returns `object`, forcing every call site to use `# type: ignore`. The function knows it operates on `Connectors` and returns `Connectors` but hides this from the type checker to avoid a circular import.

**Fix:** Use `from __future__ import annotations` with `TYPE_CHECKING` to import `Connectors` for type hints only. The runtime import inside the function body can stay. This closes the type hole without any structural change.

---

### 2. `_TIER_REGISTRY` is a mutable module-level singleton

| | |
|---|---|
| **File** | `parsimony/connectors/__init__.py:27-44` |
| **Council** | Fowler x Carmack — State is the primary source of bugs (Principle 3). Cross-ref: Hunt, Collina, Dodds all flagged this independently. |
| **Ref** | `references/refactoring.md` → Principle 3 |

**Finding:** `_TIER_REGISTRY` is a lazily-initialized `dict | None` global. The registry is fully derivable (three imports and a dict literal), yet it's cached in mutable module state. This breaks test isolation (patching tier constants after first call sees stale values) and sets a mutable-global precedent in a codebase that uses frozen dataclasses everywhere.

**Fix:** Replace the lazy singleton with a pure function that builds and returns the dict each time. The cost is negligible — three local imports and a dict literal per factory call. The global disappears entirely.

---

### 3. `TierError` raised with `required_tier="unknown"` — structured exception degraded to string

| | |
|---|---|
| **File** | `parsimony/connectors/fmp.py:78-83`, `parsimony/connectors/eodhd.py:64-69` |
| **Council** | Collina x Carmack — Operational error delivery (Principle 1) |
| **Ref** | `references/quality-backend.md` → Principle 1 |

**Finding:** Both FMP and EODHD raise `TierError` with `required_tier="unknown"` and `configured_tier="unknown"`. The exception class was designed to carry structured tier context so callers can make programmatic decisions. Passing `"unknown"` for both fields makes the structured hierarchy no better than the `ValueError` it replaced.

**Fix:** At the HTTP-402 catch site, the connector's own `min_tier` property is not available (the error comes from the provider, not the framework). At minimum, pass the provider name and the user's configured tier (from the factory env config) if available, or define constants like `TIER_FROM_PROVIDER = "provider-rejected"` to distinguish "I don't know the tier" from "I forgot to fill this in."

---

### 4. `_resolve_tier_config` crashes on typo but silently defaults on absence — inconsistent

| | |
|---|---|
| **File** | `parsimony/connectors/__init__.py:61-73` |
| **Council** | Collina x Carmack — Mixed delivery mechanisms (Principle 1) |
| **Ref** | `references/quality-backend.md` → Principle 1 |

**Finding:** Missing tier env var → silently defaults to lowest tier. Present but unrecognised → `ValueError` crash. The asymmetry surprises operators: forgetting to set `FMP_API_TIER` gives demo access quietly, but misspelling it as `profesional` crashes the entire factory.

**Fix:** Adopt a uniform policy. Either always default to lowest with a `logging.warning` for unrecognised values, or always raise for both missing and invalid. The plan called for fail-closed (default to lowest), so the warning approach is more consistent.

---

### 5. No HTTP-level tier/rate-limit tests for EODHD or financial_reports

| | |
|---|---|
| **File** | `tests/test_tiers.py`, `tests/test_fmp_connectors.py` |
| **Council** | Beck x Carmack — Coverage gaps (tier error path) |

**Finding:** `TierError` on HTTP 402 is only tested for FMP (`test_402_plan_message`). EODHD's 402→`TierError` path and financial_reports' 429→`RateLimitError` (burst vs quota) path have no corresponding HTTP-mock tests. The `RateLimitError` exception is only tested in isolation (constructor), not through the actual retry/raise wiring.

**Fix:** Add at least one HTTP-mock test per provider that exercises the status-code-to-exception mapping: EODHD 402→`TierError`, financial_reports burst-429→`RateLimitError(quota_exhausted=False)`, financial_reports quota-429→`RateLimitError(quota_exhausted=True)`.

---

### 6. `filter_by_tier` tests use `len()` proxy instead of membership assertions

| | |
|---|---|
| **File** | `tests/test_tiers.py:157-179` |
| **Council** | Beck x Carmack — Assertion quality |

**Finding:** `test_enterprise_user_gets_all` and `test_no_ladder_for_provider_passes_through` assert `len(filtered) == 3` without checking which connectors are in the result. A bug that returns the wrong three connectors would not be caught.

**Fix:** Assert membership for each named connector individually, matching the pattern used in the other filter tests.

---

### 7. `to_llm` tests coupled to exact string format

| | |
|---|---|
| **File** | `tests/test_tiers.py:187-199` |
| **Council** | Beck x Carmack — Behavioral vs structure-coupled tests |

**Finding:** Tests assert the exact substring `"[requires fmp professional tier]"`. Any formatting change (capitalization, phrasing, bracket style) breaks all three tests even if the semantic contract (tier info is surfaced) holds.

**Fix:** Test the semantic contract: assert that `to_llm()` output contains both the provider name and tier name, decoupled from exact punctuation.

---

## P3 — Consider

### 8. `filter_by_tier` silently passes connectors with unknown provider

| | |
|---|---|
| **File** | `parsimony/connectors/tiers.py:128-131` |
| **Council** | Collina x Carmack — Fail-closed invariant |

A connector with `min_tier` set but no matching ladder in `tier_ladders` passes through unconditionally. This contradicts the fail-closed philosophy: a developer who adds a tiered connector but forgets to register its ladder gets unrestricted access. Add a `logging.warning` when this occurs so the missing registration is visible at startup.

---

### 9. Tier property keys are magic strings split across modules

| | |
|---|---|
| **File** | `parsimony/connector.py:364-367`, `parsimony/connectors/tiers.py:123-128` |
| **Council** | Fowler x Carmack — Feature Envy (Principle 5). Cross-ref: Dodds flagged the broader issue of untyped `Mapping[str, Any]` for tier metadata. |

`connector.py` hard-codes `"min_tier"` and `"provider"` — the same keys `tiers.py` reads. Neither module defines these strings as constants. A typo in a provider module (e.g., `"min-tier"`) silently bypasses gating with no static analysis catch. Define the canonical key names as constants in `tiers.py` and reference them from both sides.

---

### 10. Comment describes default tier but test doesn't verify it portably

| | |
|---|---|
| **File** | `tests/test_tiers.py:164-171` |
| **Council** | Beck x Carmack — Comments are not assertions |

`test_missing_tier_config_defaults_to_lowest` comments "defaults to demo (lowest)" but only tests that `_starter_only` is excluded — not that the default is specifically `ladder[0]`. Add a test with a ladder whose lowest tier is not named "demo" to verify the fail-closed default is truly positional, not string-matched.

---

## Summary

| # | Finding | Severity | Council | Fix effort |
|---|---------|----------|---------|------------|
| 1 | `filter_by_tier` typed as `object` | P2 | Dodds/Fowler | ~10 lines |
| 2 | `_TIER_REGISTRY` mutable singleton | P2 | Fowler | ~10 lines |
| 3 | `TierError` with `"unknown"` fields | P2 | Collina | ~10 lines |
| 4 | Inconsistent crash vs silent default | P2 | Collina | ~15 lines |
| 5 | Missing HTTP-level tier tests | P2 | Beck | ~40 lines |
| 6 | `len()` proxy assertions | P2 | Beck | ~6 lines |
| 7 | String-coupled `to_llm` tests | P2 | Beck | ~6 lines |
| 8 | Unknown provider passes through | P3 | Collina | ~3 lines |
| 9 | Magic string tier property keys | P3 | Fowler/Dodds | ~10 lines |
| 10 | Default tier test not portable | P3 | Beck | ~5 lines |

## Verdict

No P1s. The tier infrastructure is well-designed — `tier_allows` is correctly fail-closed, the exception hierarchy is clean, the factory gating is in the right place. The most important thing to fix is **#1 + #2**: the `object` type erasure and the mutable singleton. Both undermine the immutability and type-safety guarantees the rest of parsimony is built on, and both are trivial fixes. After that, **#3** (TierError with "unknown") should be addressed — it defeats the purpose of having typed exceptions. Beck's test findings (#5-7) are straightforward and should be fixed to give the tier feature the test coverage it deserves. Collina's domain is most critical: the error handling patterns set here will be copied by every future provider.

---

## Findings Breakdown by Expert

| Expert | P1 | P2 | P3 | Total | Key Areas |
|--------|----|----|----|----|-----------|
| Hunt (Security) | 0 | 0 | 0 | 0 | (findings cut — pre-existing, not tier-specific) |
| Dodds (Architecture) | 0 | 1 | 0 | 1 | type erasure |
| Collina (Backend) | 0 | 2 | 1 | 3 | error handling, fail-closed |
| Leach (Postgres) | 0 | 0 | 0 | 0 | no DB |
| Performance | 0 | 0 | 0 | 0 | negligible |
| Saarinen (UI Quality) | 0 | 0 | 0 | 0 | no UI |
| Friedman (UX Quality) | 0 | 0 | 0 | 0 | no UX |
| Fowler (Refactoring) | 0 | 1 | 1 | 2 | mutable state, magic strings |
| Willison (LLM Pipeline) | 0 | 0 | 0 | 0 | clean |
| Beck (Test Quality) | 0 | 3 | 1 | 4 | coverage gaps, assertion quality |
| **TOTAL** | **0** | **7** | **3** | **10** | |

**Review output written to:** `.council/review-output/2026-04-13-1518/FINAL-REVIEW.md`

**Expert output files:**
- Hunt: `.council/review-output/2026-04-13-1518/hunt.md`
- Dodds: `.council/review-output/2026-04-13-1518/dodds.md`
- Collina: `.council/review-output/2026-04-13-1518/collina.md`
- Leach: `.council/review-output/2026-04-13-1518/leach.md`
- Performance: `.council/review-output/2026-04-13-1518/performance.md`
- Saarinen: `.council/review-output/2026-04-13-1518/saarinen.md`
- Friedman: `.council/review-output/2026-04-13-1518/friedman.md`
- Beck: `.council/review-output/2026-04-13-1518/beck.md`
- Fowler: `.council/review-output/2026-04-13-1518/fowler.md`
- Willison: `.council/review-output/2026-04-13-1518/willison.md`
