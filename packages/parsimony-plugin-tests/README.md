# parsimony-plugin-tests

Shared pytest conformance suite for [parsimony](https://parsimony.dev) plugins.

Inherit one class, set one attribute, and your plugin gets the full
parsimony provider contract checked at test time.

## Install (as a dev dependency of your plugin)

```toml
# packages/parsimony-mybank/pyproject.toml
[project.optional-dependencies]
dev = [
    "parsimony-plugin-tests>=0.1.0a1",
]
```

## Use

```python
# packages/parsimony-mybank/tests/test_conformance.py
from parsimony_plugin_tests import ProviderTestSuite
from parsimony_mybank import PROVIDER


class TestMyBankProvider(ProviderTestSuite):
    provider = PROVIDER
    entry_point_name = "mybank"  # optional; checks installation via entry points
```

That's it. Pytest's standard discovery picks up the inherited methods.

## What it checks

Structural — every check runs without network, credentials, or environment
configuration.

| Check | Why |
|---|---|
| `provider` is a `ProviderSpec` | Plugin author exported the right object |
| `provider.name` is a non-empty lowercase slug | Discoverable, no shell-quoting issues |
| `provider.resolve()` returns at least one connector | Empty providers are bugs |
| Connector names are unique | Connectors compose by name |
| Connector names start with `<provider>_` | Naming convention |
| Each connector wraps an `async def` | Required by the executor |
| Each connector has a non-empty description | MCP/CLI surface needs it |
| `param_type` is a Pydantic `BaseModel` | Required for validation |
| `ENV_VARS` keys are `UPPER_SNAKE_CASE` | Convention |
| `ENV_VARS` keys match a real `dep_name` | Catches typos that silently fail to bind |
| Entry point resolves to the same `ProviderSpec` | Catches missing/wrong `[project.entry-points]` block |

## What it does *not* check

Behavioural correctness against live APIs, result schemas, retry/rate-limit
semantics. Those belong in plugin-specific tests (with cassettes or live
integration tests gated behind environment flags).

## License

Apache-2.0
