# Security Review — Troy Hunt
## Parsimony Tiers Feature

---

FINDING:
- Title: API keys passed as default parameter values in connector function signatures
- File: parsimony/connectors/fmp.py:541, parsimony/connectors/fmp.py:558, parsimony/connectors/eodhd.py:36 (and all connector function definitions)
- Principle: Principle 3 — Minimise state; Secrets management
- Severity: P2
- What's wrong: Every connector function in fmp.py and eodhd.py declares `api_key: str` as a keyword-only parameter. While no default value is provided (good), the `base_url` parameter in every fmp connector has a hardcoded production URL as a default (`base_url: str = "https://financialmodelingprep.com/stable"`). If someone were to accidentally pass an API key as a positional argument or if a future maintainer adds a default, there is no type-level barrier. More concretely, the `base_url` default means any test that doesn't override it will hit production endpoints — there is no structural enforcement that tests use a mock URL.
- Consequence: Tests that inadvertently use real API keys against production endpoints leak usage quota and create audit trail noise; a future refactor that adds a default to `api_key` would silently pass an empty string rather than failing at startup.
- Fix: Move `base_url` out of the function signature and into the `_make_http` factory or a module-level constant that tests override via dependency injection; enforce that `api_key` has no default and document that it must be injected via `bind_deps` only.

---

FINDING:
- Title: Tier values from environment variables are lowercased before validation, creating case-normalisation bypass potential
- File: parsimony/connectors/__init__.py:68
- Principle: Principle 2 — Automate defences; Continuous validation
- Severity: P3
- What's wrong: In `_resolve_tier_config`, the raw env var value is stripped and lowercased (`raw = str(raw).strip().lower()`) before being checked against the ladder. The tier ladders themselves are defined in lowercase (e.g. `("demo", "starter", "professional", "enterprise")`), so this is functionally correct. However, this silent normalisation means a misconfigured value like `"PROFESSIONAL "` silently succeeds rather than raising an error — the operator gets no feedback that their configured value was not an exact match.
- Consequence: An operator who fat-fingers the tier value with unexpected casing or whitespace will see it silently accepted rather than receiving a clear `ValueError` pointing to the misconfiguration, making operational debugging harder.
- Fix: Validate the raw value after stripping whitespace but before lowercasing, and emit a warning or raise with a message indicating the exact raw value provided did not match but was normalised; or document the normalisation explicitly in the env var documentation.

---

FINDING:
- Title: Global mutable `_TIER_REGISTRY` cache is not thread-safe
- File: parsimony/connectors/__init__.py:27-44
- Principle: Principle 3 — Minimise state (mutable shared state)
- Severity: P3
- What's wrong: `_TIER_REGISTRY` is a module-level mutable global initialised lazily via `_get_tier_registry()` with a double-check pattern that is not thread-safe in CPython under concurrent async coroutines that happen to call the factory simultaneously on first import. While the GIL makes the assignment atomic in practice, the pattern is fragile and sets a precedent that could be copied into a truly multi-threaded context.
- Consequence: In a multi-threaded deployment (e.g. running the library inside a thread pool), two threads could both observe `_TIER_REGISTRY is None`, both build the registry, and race on assignment — not a data corruption risk today but a maintenance risk as the codebase grows.
- Fix: Use `functools.lru_cache` on the registry builder or initialise `_TIER_REGISTRY` eagerly at module load time rather than lazily; either approach eliminates the mutable global pattern.

---

FINDING:
- Title: `eodhd_fetch` accepts arbitrary extra query parameters via Pydantic `extra="allow"` with no validation
- File: parsimony/connectors/eodhd.py:21-25
- Principle: Principle 2 — Automate defences; Continuous validation (Principle 4 — type system as armour)
- Severity: P2
- What's wrong: `EodhdFetchParams` uses `model_config = ConfigDict(extra="allow")`, which means any caller-supplied field that is not `method` or `path` is forwarded as a query parameter to the EODHD API without any validation or sanitisation. A caller can inject arbitrary query parameters — including `api_token` — into the outgoing request, potentially overriding the injected API key with a different one or sending unexpected fields that the upstream API interprets in unintended ways.
- Consequence: A caller (or an LLM agent using this connector) could supply `api_token=attacker_key` as an extra param, overriding the legitimate injected key and routing requests through an attacker-controlled credential — or leaking query behaviour to an unintended account.
- Fix: Explicitly enumerate all accepted extra query parameters in the model, or add a validator that rejects any extra key matching known credential field names (`api_token`, `apikey`, `token`, `key`); at minimum, strip any extra param whose name matches the credential param name used by `_make_http`.

---

FINDING:
- Title: No static security scanning (bandit/ruff-S) in CI — injection risks go undetected automatically
- File: (project-wide; no pyproject.toml bandit/ruff-S configuration observed)
- Principle: Principle 2 — Automate defences; Linting and type safety as security
- Severity: P2
- What's wrong: The context brief notes mypy and ruff are not available in this environment, and there is no evidence of bandit or ruff security rules (`S`-prefix) being configured. The codebase uses `json.loads` on untrusted API response bodies (financial_reports.py:82-87), constructs dynamic paths via string replacement (fmp.py:57-68), and injects env vars into HTTP clients — all patterns that ruff-S and bandit are designed to flag automatically.
- Consequence: Without continuous automated scanning, any future commit that introduces `eval()`, `shell=True`, or an unparameterised string interpolation into an HTTP request will pass code review undetected — the class of flaws Carmack calls statistically inevitable without mechanical checks.
- Fix: Add `bandit -r parsimony/` and ruff with `select = ["S"]` to the CI pipeline and pre-commit hooks; configure `pyproject.toml` with `[tool.ruff.lint] select = ["S"]` to make security linting a first-class gate.

---

## Summary

No P1 vulnerabilities found. This is a pure Python library with no web endpoints, no SQL, no authentication surface, and no user-facing UI — the traditional OWASP Top 10 attack vectors (SQLi, XSS, broken auth, IDOR) are largely out of scope. The findings cluster around three themes: (1) the EODHD open-parameter design allows credential override by a caller, which is the most concrete exploitable issue (P2); (2) absence of automated security scanning means future regressions will not be caught mechanically; (3) two lower-severity hygiene issues around silent normalisation and a mutable global. The tier-gating logic itself (`tier_allows`, `filter_by_tier`) is correctly fail-closed and requires no remediation.
