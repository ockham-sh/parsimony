# Collina Review — Backend Quality
## Reviewer: Matteo Collina (Backend Quality)
## Date: 2026-04-13

---

FINDING:
- Title: Broad `except Exception` swallows non-429 errors silently in `_with_retry`
- File: parsimony/connectors/financial_reports.py:101-103
- Principle: Principle 1 — Programmer errors are assertion failures (swallowed errors)
- Severity: P2
- What's wrong: `except Exception as exc:` catches everything that isn't a 429 and re-raises it, but the check `if getattr(exc, "status", None) != 429: raise` means any exception without a `.status` attribute (e.g. a `ConnectionError`, `TimeoutError`, or a programmer-error `AttributeError` inside the coroutine) is immediately re-raised — which is correct. However, `AttributeError` and other programmer errors from within the SDK coroutine pass through this same path, making it impossible to distinguish programmer errors from operational ones at the call site. The handler should be narrowed to the SDK's `ApiException` type.
- Consequence: A programmer error inside `coro_factory` (e.g. accessing a missing attribute on the client) is caught, inspected for `.status`, and then re-raised — but the stack trace is contaminated and the classification as "not a 429" is accidental rather than intentional. Debugging becomes harder and future maintainers may narrow the re-raise condition incorrectly.
- Fix: Replace `except Exception` with `except <SdkApiException>` (the specific exception class the SDK raises for HTTP errors) so programmer errors propagate cleanly without passing through the retry logic.

---

FINDING:
- Title: SDK client opened and discarded on every retry iteration — connection not reused
- File: parsimony/connectors/financial_reports.py:97-99
- Principle: Principle 5 — Resource management (unclosed HTTP sessions and connections)
- Severity: P2
- What's wrong: `async with _sdk_client(api_key) as client:` is called inside the `for attempt` loop, so each retry creates a new SDK client (and its underlying HTTP session/connection pool), uses it for a single call, then closes it. On a burst-limit retry sequence of up to 4 attempts, 4 separate connection pools are opened and closed.
- Consequence: Under burst retries, connection overhead is amplified unnecessarily — each attempt pays TCP handshake and TLS negotiation costs rather than reusing an already-warm connection. Under load with many concurrent callers all hitting burst limits simultaneously, this pattern multiplies ephemeral connections.
- Fix: Hoist `async with _sdk_client(api_key) as client:` outside the retry loop so the same client is reused across retry attempts; only re-enter the context if the client itself becomes unusable.

---

FINDING:
- Title: `TierError` raised with `required_tier="unknown"` — loses operational context
- File: parsimony/connectors/fmp.py:78-83 and parsimony/connectors/eodhd.py:64-69
- Principle: Principle 1 — Programmer errors are assertion failures / operational error delivery (mixed delivery, incomplete error context)
- Severity: P3
- What's wrong: Both FMP and EODHD raise `TierError` on HTTP 402 with `required_tier="unknown"` and `configured_tier="unknown"`. The `TierError` class was designed to carry structured tier context (`provider`, `required_tier`, `configured_tier`) so callers can make programmatic decisions (e.g. prompt the user to upgrade to a specific tier). Passing `"unknown"` for both fields degrades the exception to a string message container — no better than a plain `ValueError`.
- Consequence: Callers who catch `TierError` and inspect `.required_tier` to determine the upgrade path receive `"unknown"`, making the structured exception hierarchy useless for those providers. The tier gating framework's value proposition is undermined for the two primary commercial providers.
- Fix: Pass the connector's `min_tier` property (available at call time via the connector's `properties` dict) into the `TierError`, or at minimum pass the user's `configured_tier` from the injected API tier config so the error carries the provider's known ladder position.

---

FINDING:
- Title: `_resolve_tier_config` raises `ValueError` for invalid tier env var — inconsistent with fail-closed default
- File: parsimony/connectors/__init__.py:69-73
- Principle: Principle 1 — Mixed delivery mechanisms (operational errors delivered inconsistently)
- Severity: P2
- What's wrong: When an env var is absent, `_resolve_tier_config` silently defaults to the lowest tier (fail-closed). When an env var is present but unrecognised, it raises `ValueError`. These two code paths deliver the error differently: absence = silent default, typo = hard crash. The docstring calls this "fail-fast, matching the API-key pattern" but the API-key pattern (`raise ValueError("FRED_API_KEY is not configured")`) only fires when the key is completely absent — not when the value is subtly wrong.
- Consequence: A misconfigured `FMP_API_TIER=profesional` (typo) crashes the factory at startup with no connectors available, while a missing `FMP_API_TIER` silently limits the user to demo tier. The asymmetry surprises operators and makes the failure mode depend on whether the user set the variable at all.
- Fix: Either document the asymmetry explicitly in the function docstring and raise a more informative error (including the env var name, the bad value, and the valid tier list), or adopt a uniform policy: always default to lowest tier and warn via `logging.warning` when an unrecognised value is encountered, treating misconfiguration as a non-fatal operational error consistent with the fail-closed philosophy.

---

FINDING:
- Title: `filter_by_tier` silently passes through connectors with unknown provider — not fail-closed
- File: parsimony/connectors/tiers.py:128-131
- Principle: Principle 1 — Programmer errors are assertion failures (invariant violation not flagged)
- Severity: P3
- What's wrong: When a connector has a `min_tier` property but its `provider` value has no entry in `tier_ladders`, the connector is passed through unconditionally (lines 128-131). This is explicitly documented as the intended behaviour, but a connector with `min_tier` set implies it has access requirements — silently granting access when the ladder is missing contradicts the fail-closed principle applied elsewhere.
- Consequence: If a developer adds a new provider connector with `min_tier` but forgets to register its ladder in the tier registry, all connectors for that provider become available to all users regardless of their tier. The fail-closed logic in `tier_allows` is bypassed entirely by the missing-ladder path.
- Fix: Emit a `logging.warning` (or raise `ValueError` during factory construction) when a connector carries `min_tier` for a provider not present in `tier_ladders`, so the missing registration is surfaced at startup rather than silently passing through as unrestricted access.

---

FINDING:
- Title: `_TIER_REGISTRY` global mutable singleton — not safe for test isolation
- File: parsimony/connectors/__init__.py:27-44
- Principle: Principle 1 — Programmer errors / shared mutable state
- Severity: P3
- What's wrong: `_TIER_REGISTRY` is a module-level mutable singleton populated lazily on first call. Once populated, it is never invalidated. Tests that patch provider tier constants (`FMP_TIERS`, `EODHD_TIERS`, `FR_TIERS`) after the registry has been built will see stale values because the cached registry is not re-evaluated.
- Consequence: Test ordering can affect results — a test that builds the registry early will cache the real tier ladders, causing later tests that patch those constants to operate against stale cached state. This is a latent test isolation hazard that compounds as the test suite grows.
- Fix: Either remove the caching (the function body is cheap — three imports and a dict literal) or expose a `_reset_tier_registry()` helper for test teardown, and document the caching behaviour so test authors know to reset it when patching tier constants.
