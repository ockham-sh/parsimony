# Simon Willison — LLM Pipeline Quality Review

## to_llm() Tier Annotation: Clear and Correct

The tier annotation in `Connector.to_llm()` (lines 364-368) is well-designed for LLM consumption:

```python
min_tier = self.properties.get("min_tier")
if min_tier is not None:
    provider = self.properties.get("provider", "")
    desc += f" [requires {provider} {min_tier} tier]"
```

**Strengths:**

1. **Token efficiency** — Appends to description inline rather than creating separate sections; preserves compact format mandate
2. **Fail-safe defaults** — Uses `properties.get()` with fallback; if `provider` is missing, produces `[requires  tier]` (blank provider) rather than crashing
3. **Clear format** — Square brackets signal constraint metadata; human-readable phrasing ("requires fmp professional tier")
4. **Correct placement** — After output columns but before parameters; tier is a connector-level capability constraint, not a parameter
5. **Test coverage** — Three tests validate: presence when set, absence when unset, collection-level inclusion

**Minor observation:**

The fallback `provider=""` produces slightly odd output if `provider` is missing (e.g., "[requires  professional tier]"). However, this is fail-safe behavior and unlikely in practice since tier/provider should be paired by convention.

**No issues found.** The annotation is crisp, LLM-friendly, and properly tested.
