# Architecture Review — Kent C. Dodds (Architecture Quality)

Reviewer: Kent C. Dodds (Architecture Quality)
Date: 2026-04-13
Files reviewed: parsimony/connectors/tiers.py, parsimony/connector.py, parsimony/connectors/__init__.py, parsimony/__init__.py

---

## Findings

---

FINDING:
- **Title:** `filter_by_tier` typed as `object` on both input and output, losing the type system entirely at the module boundary
- **File:** `parsimony/connectors/tiers.py:100-136`
- **Principle:** Principle 3 — Use the type system as armour
- **Severity:** P2
- **What's wrong:** `filter_by_tier` accepts `connectors: object` and returns `object`, with a `# type: ignore` cast on the `Connectors(out)` return. The function knows it is working with `Connectors` but deliberately erases that from the type signature to avoid a circular import, pushing the problem to every call site.
- **Consequence:** Every caller must either trust the return type silently or cast it themselves; mypy sees `object` flowing through the tier-gating path, meaning type errors in that flow are invisible to static analysis.
- **Fix:** Move the circular import to a `TYPE_CHECKING` guard and use a quoted forward reference `"Connectors"` in the signature, or restructure so `tiers.py` depends only on the protocol (an iterable of connectors with `properties`), not the concrete class. Either approach closes the hole without the `object` erasure.

---

FINDING:
- **Title:** Module-level mutable singleton `_TIER_REGISTRY` treats registry state as a global
- **File:** `parsimony/connectors/__init__.py:27-44`
- **Principle:** Principle 2 — Eliminate state that can be derived, then colocate what remains
- **Severity:** P3
- **What's wrong:** `_TIER_REGISTRY` is a module-level `None`-initialized mutable global that is lazily populated on first call and then reused. The registry is fully derivable from the provider modules each time it is needed — caching it at the global level adds a statefulness that survives across test runs and process-level reuse.
- **Consequence:** Tests that import provider modules in a different order, or that mock `FMP_TIERS`, may get stale registry state from a previous test's initialization, producing false passes or hard-to-diagnose failures.
- **Fix:** Either make `_get_tier_registry()` a pure function that builds and returns the dict on every call (acceptable given it is called at most once per factory invocation), or use `functools.cache` to memoize it in a way that is clearly reset-able in tests. The global mutable sentinel is the wrong tool for a derivable value.

---

FINDING:
- **Title:** Tier metadata carried as untyped `Mapping[str, Any]` properties instead of a typed structure
- **File:** `parsimony/connector.py:165`, `parsimony/connectors/tiers.py:123-128`
- **Principle:** Principle 3 — Use the type system as armour
- **Severity:** P2
- **What's wrong:** `min_tier` and `provider` are stored as arbitrary string keys in `Connector.properties: Mapping[str, Any]` and accessed via `.get("min_tier")` / `.get("provider", "")`. There is no type-level enforcement that these keys exist, have the right type, or are spelled correctly. This is the `Any`-dict anti-pattern at a data boundary.
- **Consequence:** A typo in a provider module (e.g. `"min-tier"` vs `"min_tier"`) silently causes the connector to pass through the tier gate unchecked — no static analysis tool catches it, and the runtime behaviour is wrong-but-quiet rather than a clear error.
- **Fix:** Introduce a typed `ConnectorTierMetadata` dataclass or `TypedDict` (with `provider: str` and `min_tier: str | None`) and add a dedicated field on `Connector` — or at minimum a validated factory helper that enforces the keys when tier metadata is present. The existing `properties` bag is the right escape hatch for arbitrary registry metadata, but tier-gating logic should not read from unvalidated strings.

---

FINDING:
- **Title:** `build_fetch_connectors_from_env` and `build_connectors_from_env` are two near-duplicate 60-line functions with identical structure
- **File:** `parsimony/connectors/__init__.py:97-289`
- **Principle:** Principle 1 — Duplication is local damage; wrong abstractions are systemic damage
- **Severity:** P3
- **What's wrong:** Both factory functions follow the same pattern: read env, raise on missing required keys, compose `result` with `.bind_deps()` and `+`, then call `_apply_tier_gate`. The only difference is which connector constants are imported (`CONNECTORS` vs `FETCH_CONNECTORS`). Any new provider requires the same change in both places.
- **Consequence:** Adding or removing a provider requires editing both factories in parallel; a future developer will inevitably update one and forget the other, causing the full and fetch-only surfaces to diverge silently.
- **Fix:** Extract a single `_build_connector_surface(env, *, fetch_only: bool) -> Connectors` private function that accepts a flag (or a per-provider selector callable) to choose between full and fetch-only imports. The two public functions become thin wrappers that apply `_apply_tier_gate` on top.

---

FINDING:
- **Title:** `client` convenience export builds a full `Connectors` instance on every attribute access via `__getattr__`
- **File:** `parsimony/__init__.py:178-181`
- **Principle:** Principle 2 — Eliminate state that can be derived, then colocate what remains
- **Severity:** P2
- **What's wrong:** `from parsimony import client` triggers `__getattr__("client")`, which calls `build_connectors_from_env()` inline and returns the result without caching. Each attribute access to `parsimony.client` re-invokes the factory from scratch, re-reading env vars and re-binding all dependencies.
- **Consequence:** Any code that accesses `parsimony.client` twice (e.g. in a module with `import parsimony; parsimony.client["a"](...); parsimony.client["b"](...)`) silently builds the connector graph twice. This is expensive and surprising for what looks like a stable module-level object.
- **Fix:** Cache the built `Connectors` instance at the module level after first construction — either via a private `_client: Connectors | None = None` sentinel with the same lazy-init pattern used for `_TIER_REGISTRY`, or by using `functools.cached_property`-equivalent semantics. The `__getattr__` hook is the right mechanism for lazy loading; the missing piece is memoisation.

---

FINDING:
- **Title:** `Connectors.__iter__` has no return type annotation, breaking the type contract of the collection protocol
- **File:** `parsimony/connector.py:627`
- **Principle:** Principle 3 — Use the type system as armour
- **Severity:** P3
- **What's wrong:** `def __iter__(self):` is missing a return type annotation. Every other public method on `Connectors` is annotated. The unannotated `__iter__` means mypy infers the element type as `Any` when iterating over a `Connectors` instance, silently defeating type checking for all iteration call sites.
- **Consequence:** Code that iterates over a `Connectors` collection and accesses `.name` or `.properties` on each element will type-check as `Any.name`, making it impossible for static analysis to catch attribute errors or incorrect method calls on the yielded elements.
- **Fix:** Annotate the return type as `Iterator[Connector]` and add `from collections.abc import Iterator` to the imports. This is a one-line change that restores full type coverage over iteration.
