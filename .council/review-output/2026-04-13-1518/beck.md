# Beck — Test Quality Review

---

## FINDING 1

- **Title:** `test_enterprise_user_gets_all` uses `len()` instead of asserting membership
- **File:** `tests/test_tiers.py:157-162`
- **Principle:** Assertion quality — test what matters, not a proxy metric
- **Severity:** P2
- **What's wrong:** The test asserts `len(filtered) == 3` rather than asserting that each expected connector is present. A bug that passes through the wrong three connectors (e.g., duplicates, wrong objects) would not be caught.
- **Consequence:** Tests can pass while the filtering logic returns incorrect connectors, masking a correctness bug.
- **Fix:** Assert membership for each of the three named connectors (`_starter_only`, `_pro_only`, `_no_tier`) individually, the same pattern used in the other `filter_by_tier` tests.

---

## FINDING 2

- **Title:** `test_no_ladder_for_provider_passes_through` also uses `len()` proxy assertion
- **File:** `tests/test_tiers.py:173-179`
- **Principle:** Assertion quality — assert the specific behavior under test
- **Severity:** P2
- **What's wrong:** Asserting `len(filtered) == 3` does not confirm *which* connectors passed through; it only confirms count. The semantics being tested — that connectors with an unrecognised provider pass through unconditionally — are not validated at the connector-identity level.
- **Consequence:** A regression that substitutes different connectors in the output would not be caught.
- **Fix:** Assert that all three named connectors are present in the filtered result by name, matching the assertion style used in the surrounding tests.

---

## FINDING 3

- **Title:** `TestToLlmTierAnnotation` tests string literals, not semantic contract
- **File:** `tests/test_tiers.py:187-199`
- **Principle:** Behavioral vs structure-coupled tests — test observable contracts, not implementation details
- **Severity:** P2
- **What's wrong:** The tests assert the exact substring `"[requires fmp professional tier]"`. If the rendering format changes for a legitimate UX reason (e.g., capitalisation, phrasing), all three tests break despite the behaviour being correct.
- **Consequence:** Tests become a friction cost on benign refactors and give false confidence about the semantic contract (that tier metadata is surfaced in LLM output), not just the current string format.
- **Fix:** Test the semantic contract: assert that `to_llm()` contains the provider name and tier name somewhere in the output, decoupled from exact punctuation and bracket format.

---

## FINDING 4

- **Title:** No test exercises `tier_allows` with a single-element ladder
- **File:** `tests/test_tiers.py:55-83`
- **Principle:** Coverage gaps — boundary conditions
- **Severity:** P3
- **What's wrong:** The ladder boundary tests only cover the empty ladder case (`test_empty_ladder_returns_false`) and the standard 4-element FMP ladder. A single-element ladder is a meaningful degenerate case (`tier_allows(("enterprise",), "enterprise", "enterprise")`) that exercises the boundary between index 0 and 0 correctly but is not covered.
- **Consequence:** Low impact given the simple implementation, but the boundary is untested and a future change to the comparison logic could silently break this edge.
- **Fix:** Add one test for a single-element ladder where user tier equals the required tier (should return `True`).

---

## FINDING 5

- **Title:** `test_402_plan_message` in `test_fmp_connectors.py` is the only HTTP-level tier test and it is connector-specific rather than transport-level
- **File:** `tests/test_fmp_connectors.py:481-487`
- **Principle:** Coverage gaps — tier error path coverage across providers
- **Severity:** P2
- **What's wrong:** The TierError-on-402 behaviour is only tested for `fmp_company_profile`. The context brief states `eodhd` and `financial_reports` also raise `TierError` on 402, but there are no corresponding tests for those providers.
- **Consequence:** Regressions in tier-error handling for EODHD or financial_reports would not be caught by the test suite.
- **Fix:** Add a parallel HTTP-402-raises-TierError unit test for at least one connector from each provider that implements the 402 handler (EODHD, financial_reports).

---

## FINDING 6

- **Title:** `RateLimitError` burst-vs-quota distinction is not tested through a real HTTP-path mock
- **File:** `tests/test_tiers.py:110-127`
- **Principle:** Coverage gaps — integration between error contract and HTTP layer
- **Severity:** P2
- **What's wrong:** The `RateLimitError` tests in `TestExceptions` only test the exception constructor in isolation. No test exercises the HTTP-429-to-`RateLimitError` path through a provider connector mock, meaning the wiring between an actual 429 response and the `quota_exhausted` flag is untested.
- **Consequence:** The `quota_exhausted` discrimination logic inside the provider could be broken or missing entirely and the unit tests would still pass.
- **Fix:** Add at least one mock-HTTP test per provider that raises 429 and asserts the resulting `RateLimitError` has the correct `quota_exhausted` value.

---

## FINDING 7

- **Title:** `test_missing_tier_config_defaults_to_lowest` comment asserts "demo" but does not verify it
- **File:** `tests/test_tiers.py:164-171`
- **Principle:** Assertion quality — comments are not assertions
- **Severity:** P3
- **What's wrong:** The comment `# defaults to "demo" (lowest)` describes the expected behavior but there is no assertion that the default tier used is actually the lowest element of the ladder. The test only checks that `_starter_only` is excluded, not that the fail-closed default is specifically the first ladder element.
- **Consequence:** If `filter_by_tier` were changed to default to a different tier (e.g., hardcoded `"demo"` string instead of `ladder[0]`), the test would not detect the regression for a ladder where the lowest tier is not named `"demo"`.
- **Fix:** Add a second test that uses a ladder whose lowest tier is not `"demo"` and confirms the same fail-closed behavior, or assert the specific tier used in the default path.

---

## FINDING 8

- **Title:** Module-level connector fixtures (`_starter_only`, `_pro_only`, `_no_tier`) are shared mutable state across tests
- **File:** `tests/test_tiers.py:29-44`
- **Principle:** Mock discipline — test isolation
- **Severity:** P3
- **What's wrong:** The three test connectors are defined at module scope and reused across all test classes. While the connectors themselves appear to be immutable frozen dataclasses, the `Connectors` collection is reconstructed per test via `self._collection()`, so the risk is low but the pattern invites problems if state ever accumulates on the connector objects.
- **Consequence:** If a future change makes connectors stateful (e.g., hit counters, caching), shared module-level fixtures would cause inter-test contamination.
- **Fix:** Move connector definitions into a pytest fixture with function scope, or document explicitly that they are intentionally immutable and safe to share.
