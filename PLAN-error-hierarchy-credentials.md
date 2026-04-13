# Council Plan: Connector Error Hierarchy + Declarative Credentials

**Scope:** Replace ~120 ad hoc `ValueError` raises across 21 connector modules with a typed exception hierarchy. Make credential requirements declarative per provider. No retry framework.
**Context:** Parsimony already has `ConnectorError`, `TierError`, and `RateLimitError` in `connectors/tiers.py` from the tier awareness feature. This plan expands the hierarchy with `EmptyDataError`, `UnauthorizedError`, `ProviderError`, `ParseError`, extracts it into its own module, and makes the factory credential-aware through declarative provider declarations.
**Boundaries:** No centralized retry framework. Financial Reports keeps its internal burst retry. No new providers added. Tier gating unchanged.
**Council dispatched:** Fowler (5 recommendations), Collina (6 recommendations), Dodds (6 recommendations), Hunt (no recommendations — feature strengthens security posture), Leach/Performance/Willison/Saarinen/Friedman (no recommendations — no surface).

---

## Key Design Decisions

**Hierarchy shape: flat.** All exception subclasses are direct children of `ConnectorError`. No intermediate abstract classes (no `HttpError` grouping) until shared logic justifies one. Dodds's AHA principle: don't abstract until the duplication screams.

**`provider` is required on all exceptions.** Every `ConnectorError` subclass carries `provider: str` as a required positional argument. This is the observability hook — structured logging and error handling depend on it. Collina and Dodds both independently recommended this.

**`from exc` always, never `from None`.** The chained exception is diagnostic context for debugging. Callers catch `ConnectorError`; the original httpx error is preserved in the chain for engineers investigating provider issues. Fowler: pick one convention and apply it uniformly.

**Input validation stays as `ValueError`/Pydantic.** The ~20 "must be non-empty" checks are programmer errors, not operational errors. They belong in Pydantic field validators, not in the `ConnectorError` hierarchy. Fowler and Collina independently agreed: `ConnectorError` means "something went wrong on the wire or with the data," not "you called this wrong."

**Two factories stay separate.** Dodds: the two factory functions are AHA-compliant duplication — they compose different connector subsets. Don't unify into a parametric function with `if fetch_only` branches. Extract only the shared credential-wiring logic.

**Credentials declared as module-level tuples.** Dodds: a simple `CREDENTIALS` tuple per provider module is the right abstraction at this scale. Not a `CredentialSpec` dataclass (premature). The factory reads these declarations and wires `bind_deps`.

---

## Task Sequence

### 1. Create `connectors/errors.py` with the full exception hierarchy

| | |
|---|---|
| **Domain** | Fowler × Carmack — Divergent Change (Principle 5). Cross-ref: Collina informed structured fields; Dodds informed flat hierarchy. |
| **Ref** | `references/refactoring.md` → Principle 5 |
| **Depends on** | — |

Extract the error hierarchy out of `tiers.py` into a dedicated `connectors/errors.py`. The hierarchy is flat: `ConnectorError` (base, requires `provider: str`) with direct subclasses `TierError`, `RateLimitError`, `UnauthorizedError`, `ProviderError`, `EmptyDataError`, `ParseError`. Each subclass carries structured fields matching the `TierError`/`RateLimitError` pattern — `UnauthorizedError` gets `provider`; `ProviderError` gets `provider` and `status_code`; `EmptyDataError` gets `provider` and `query_params`; `ParseError` gets `provider`. Update `tiers.py` to import error classes from `errors.py` (tier comparison logic stays in `tiers.py`). Update all existing imports across connector modules and tests. Pure move refactoring — no behavioral change, tests pass throughout.

---

### 2. Move input validation checks to Pydantic field validators

| | |
|---|---|
| **Domain** | Fowler × Carmack — Speculative Generality (Principle 5). Cross-ref: Collina confirmed these are programmer errors, not operational errors. |
| **Ref** | `references/refactoring.md` → Principle 5 |
| **Depends on** | — |

The ~20 "must be non-empty" and format validation checks inside connector function bodies (e.g., `if not series_id: raise ValueError(...)`) should be Pydantic `field_validator` decorators on the params models. This moves validation to the boundary where it belongs — callers get a `ValidationError` at call time, not a `ValueError` deep in an async call stack. Work through one provider at a time; verify that existing tests still pass after each migration since some tests may assert on `ValueError`.

---

### 3. Declare credential requirements per provider module

| | |
|---|---|
| **Domain** | Dodds × Carmack — Composition over configuration (Principle 5). Cross-ref: Fowler informed the credential pattern; Collina informed fail-fast validation. |
| **Ref** | `references/quality-frontend.md` → Principle 5 |
| **Depends on** | — |

Each provider module declares a `CREDENTIALS` constant — a tuple of `(env_var_name, bind_dep_name, required)` entries. For example, FRED declares `CREDENTIALS = (("FRED_API_KEY", "api_key", True),)`. BLS declares `CREDENTIALS = (("BLS_API_KEY", "api_key", False),)` with a default empty string. SDMX and other public-data connectors declare no credentials. Destatis declares two: username and password with defaults. This is data, not logic — the factory reads it.

---

### 4. Wire credential declarations into the factory

| | |
|---|---|
| **Domain** | Fowler × Carmack — Shotgun Surgery (Principle 5). Cross-ref: Dodds confirmed two functions stay separate. |
| **Ref** | `references/refactoring.md` → Principle 5 |
| **Depends on** | Tasks 1, 3 |

Extract a shared `_bind_provider_credentials(connectors, credentials, env)` helper that reads env vars per the `CREDENTIALS` declaration, raises `UnauthorizedError` (not `ValueError`) for missing required keys, and calls `bind_deps`. Both factory functions call this helper for each provider instead of repeating the pattern inline. The two public functions stay separate — they still differ in which connector constants they import (`CONNECTORS` vs `FETCH_CONNECTORS`). Adding a new provider becomes: declare `CREDENTIALS` in the module and add one line to each factory function.

---

### 5. Migrate ~120 `ValueError` raises to typed exceptions

| | |
|---|---|
| **Domain** | Collina × Carmack — One delivery mechanism per function (Principle 1). Cross-ref: Fowler informed `from exc` convention; Dodds informed layer-first audit. |
| **Ref** | `references/quality-backend.md` → Principle 1 |
| **Depends on** | Task 1 |

Audit the ~120 `raise ValueError` sites by layer before replacing. Fix shared helpers first (e.g., `_fmp_fetch` covers all 18 FMP connectors in one change), then individual connector functions. The mapping: HTTP 401/403 → `UnauthorizedError`; HTTP 5xx → `ProviderError`; other HTTP errors → `ProviderError`; empty DataFrame after 200 → `EmptyDataError` with `query_params` context; parse failures → `ParseError`. Always use `from exc` to preserve the exception chain. Never catch `CancelledError` — use `except httpx.HTTPStatusError` (not `except Exception`) at the HTTP boundary. Raw `httpx.HTTPStatusError` must never propagate out of a connector function — catch and translate at the provider boundary.

---

### 6. Update public API exports and add integration tests

| | |
|---|---|
| **Domain** | Dodds × Carmack — Test behaviour, not implementation (Principle 4) |
| **Ref** | `references/quality-frontend.md` → Principle 4 |
| **Depends on** | Tasks 1, 4, 5 |

Add all new exception classes to `parsimony/__init__.py` exports (lazy `__getattr__` pattern). Test through the public connector call path: patch `HttpClient.request` at the httpx level, call through `bound = connector.bind_deps(api_key="test"); await bound(...)`, assert the typed exception with its structured fields (`provider`, `status_code`, `query_params`). Cover at least: one 401→`UnauthorizedError` test, one 5xx→`ProviderError` test, one empty-200→`EmptyDataError` test per provider archetype (httpx-based like FMP, SDK-based like Financial Reports, public-data like Treasury).

---

## Risks & Watchpoints

- **Collina — CancelledError discipline:** The Financial Reports connector catches `except Exception` in its retry loop. When migrating its error handling (Task 5), narrow the catch to the SDK's specific exception class. `CancelledError` caught inside a retry loop prevents proper task cancellation and leaks resources.

- **Fowler — Two Hats rule:** Commits that replace `ValueError` with typed errors are refactoring commits (no behavioral change for callers who weren't catching `ValueError`). Commits that add Pydantic validators (Task 2) are behavioral additions. Do not mix them in the same commit.

- **Collina — No retry metadata on exceptions:** Do not add `retryable: bool` or `retry_after` fields to `ProviderError` or `EmptyDataError`. `RateLimitError` already has `retry_after` and `quota_exhausted` — that is sufficient. Adding retry hints to other exceptions is speculative generality that creates a published interface commitment.

- **Dodds — Audit before replacing:** Do not replace ValueError sites in random order. Map the 120 sites by layer (shared helpers vs individual connectors) first. Shared helpers like `_fmp_fetch` fix many connectors in one change. Individual connector raises are module-cohesive and should be converted in place.

---

## External Setup Required

No external setup required. All tasks can be implemented within the codebase.

---

## Summary

| # | Task | Domain | Depends on |
|---|------|--------|------------|
| 1 | Create `errors.py` with full hierarchy | Fowler | — |
| 2 | Move validation to Pydantic | Fowler | — |
| 3 | Declare credentials per provider | Dodds | — |
| 4 | Wire credentials into factory | Fowler | 1, 3 |
| 5 | Migrate ~120 ValueError to typed exceptions | Collina | 1 |
| 6 | Exports + integration tests | Dodds | 1, 4, 5 |

## Verdict

The most important decision in this plan is the error hierarchy shape. Collina's domain is most critical — the operational vs programmer error boundary determines whether callers can write reliable `except` blocks or are forced into string-parsing and catch-all patterns. Start with Task 1 (extract `errors.py`) because it unblocks everything else and is a pure move refactoring with zero risk. Tasks 2, 3, and 5 can then proceed in parallel since they're independent once the hierarchy exists. The factory credential work (Tasks 3→4) and the ValueError migration (Task 5) are the two largest efforts — they touch every provider module but are mechanical once the patterns are established.
