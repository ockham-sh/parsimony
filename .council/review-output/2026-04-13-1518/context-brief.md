## Context Brief for Council Review

### What this code does
Parsimony is a Python data connector library for financial data (FRED, FMP, SDMX, SEC Edgar, EODHD, etc.). This review covers the "Provider API Tier Awareness" feature: a lightweight framework for connectors to declare their required API tier, for the factory to gate availability based on the user's configured tier, and for tier/rate-limit errors to be handled consistently.

### Architecture
- `parsimony/connector.py` — Core `Connector` (frozen dataclass) and `Connectors` (immutable collection) with `tags`, `properties`, `filter()`, `to_llm()`, `bind_deps()`
- `parsimony/connectors/tiers.py` — **NEW** — tier comparison (`tier_allows`), error classes (`ConnectorError`, `TierError`, `RateLimitError`), `filter_by_tier()`
- `parsimony/connectors/__init__.py` — Factory functions `build_connectors_from_env()` and `build_fetch_connectors_from_env()` now with `gate_by_tier` parameter
- `parsimony/connectors/fmp.py` — FMP provider, 18 connectors, tier ladder defined, HTTP 402 now raises `TierError`
- `parsimony/connectors/eodhd.py` — EODHD provider, tier ladder defined, HTTP 402 now raises `TierError`
- `parsimony/connectors/financial_reports.py` — Financial Reports provider, tier ladder defined, burst/quota 429s now raise `RateLimitError`
- `parsimony/__init__.py` — Public API exports, added `ConnectorError`, `TierError`, `RateLimitError`
- `tests/test_tiers.py` — **NEW** — 25 tests for tier comparison, exceptions, filtering, to_llm
- `tests/test_fmp_connectors.py` — Updated 1 test to expect `TierError` instead of `ValueError`

### Stack in use
- Python 3.12+, type hints, frozen dataclasses, async/await
- httpx for HTTP, Pydantic v2 for validation
- pytest for testing
- NO FastAPI, NO SQLAlchemy, NO PostgreSQL, NO web UI — pure library

### Key observations
- The `Connector.properties` field (`Mapping[str, Any]`) is used to carry tier metadata (`min_tier`, `provider`)
- `Connectors.filter(**properties)` already supported arbitrary property filtering before this change
- Tier ladders are provider-specific tuples (e.g., FMP: demo→starter→professional→enterprise)
- `tier_allows()` is fail-closed: returns False if either tier is unrecognised
- Factory defaults to lowest tier when env var is unset (fail-closed)
- `TierError` is terminal (never retry), `RateLimitError` has `quota_exhausted` flag to distinguish burst from quota
- No connectors are actually annotated with `min_tier` yet — just the infrastructure

### Automated check results
- **pytest:** 252 passed, 38 skipped, 0 failures
- **e2e:** No e2e tests defined (pure library, no endpoints)
- **mypy/ruff:** Not available in this environment (no mypy/ruff in venv)
- **Pre-existing:** `test_destatis_fetch` network failure (excluded, unrelated)

### Domain File Assignments

**Hunt (Security):** parsimony/connectors/tiers.py, parsimony/connectors/__init__.py, parsimony/connectors/fmp.py, parsimony/connectors/eodhd.py, parsimony/connectors/financial_reports.py

**Dodds (Architecture):** parsimony/connectors/tiers.py, parsimony/connector.py, parsimony/connectors/__init__.py, parsimony/__init__.py

**Collina (Backend):** parsimony/connectors/tiers.py, parsimony/connectors/fmp.py, parsimony/connectors/eodhd.py, parsimony/connectors/financial_reports.py, parsimony/connectors/__init__.py

**Leach (Postgres):** (no database files)

**Performance:** parsimony/connectors/tiers.py, parsimony/connectors/__init__.py, parsimony/connector.py

**Saarinen (UI Quality):** (no UI files)

**Friedman (UX Quality):** (no user-facing interface)

**Fowler (Refactoring):** parsimony/connectors/tiers.py, parsimony/connectors/__init__.py, parsimony/connector.py, parsimony/__init__.py

**Willison (LLM Pipeline):** parsimony/connector.py (to_llm method)

**Beck (Test Quality):** tests/test_tiers.py, tests/test_fmp_connectors.py, parsimony/connectors/tiers.py
