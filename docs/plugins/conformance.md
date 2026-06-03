# Conformance testing

`parsimony.testing` is the toolkit you use to prove that your provider plugin exports a
surface the kernel accepts: a module-level `CONNECTORS` constant holding a non-empty
`Connectors` instance, whose connectors satisfy a small set of integrity rules. Run the
checks procedurally with `assert_plugin_valid`, or wire them into pytest with the
`ProviderTestSuite` base class.

These checks are the same ones the [CLI](../cli.md) runs under `parsimony list --strict`, so
a plugin that passes its own conformance test will list as `pass` for any consumer.

## The supported surface

`parsimony.testing` exports exactly four public names:

| Name | Kind | Purpose |
| --- | --- | --- |
| `assert_plugin_valid` | function | Run every check against a module; raise on the first failure. |
| `ProviderTestSuite` | class | A pytest base class that collects conformance tests automatically. |
| `ConformanceError` | exception | Raised by a failing check; subclasses `AssertionError`. |
| `iter_check_names` | function | Enumerate the registered check names. |

```python
from parsimony.testing import (
    assert_plugin_valid,
    ProviderTestSuite,
    ConformanceError,
    iter_check_names,
)
```

!!! note "Helpers outside `__all__`"
    The module also defines `connector_count(module)` and `iter_connectors(module)`. They are
    convenience helpers that return `0` / an empty iterator when `CONNECTORS` is missing or is
    not a `Connectors`. They are deliberately omitted from `__all__` — treat them as internal
    and rely only on the four names above.

## `assert_plugin_valid`

```python
def assert_plugin_valid(module: ModuleType) -> None
```

Pass it an already-imported plugin module. It runs the five registered checks in order and
raises `ConformanceError` on the **first** failure; it returns `None` when the module passes
everything. It is fail-fast, not fail-collecting — later checks do not run once one fails.

```python
# tests/test_conformance.py in your plugin distribution
import my_plugin  # the module that exports CONNECTORS
from parsimony.testing import assert_plugin_valid


def test_plugin_is_conformant() -> None:
    assert_plugin_valid(my_plugin)  # raises ConformanceError on the first failure
```

!!! warning "It inspects, it does not import"
    `assert_plugin_valid` takes a module object you already imported. It does **not** import
    the module for you and does **not** resolve entry points. Verifying that your distribution
    is actually registered under the `parsimony.providers` group is a separate step — use
    `ProviderTestSuite` with `entry_point_name` set (below).

## The five checks

Each check reads the module's `CONNECTORS` export and inspects the connectors. The export
check runs first as a gatekeeper; the rest assume `CONNECTORS` is present (which it is, when
run through `assert_plugin_valid`).

| Check name | Enforces |
| --- | --- |
| `check_connectors_exported` | Module has a `CONNECTORS` attribute that is a `Connectors` with at least one connector. |
| `check_descriptions_non_empty` | Every connector's stripped description is non-empty and 20–800 characters. |
| `check_enumerator_decorator` | Every `enumerator`-tagged connector was built with `@enumerator`, not a faked tag. |
| `check_enumerator_return_type` | Every `enumerator`-tagged connector declares `output=` and a `pd.DataFrame`/`Series` return. |
| `check_flat_public_params` | No connector exposes a public `params:` parameter annotated as a Pydantic `BaseModel`. |

### check_connectors_exported

The gatekeeper. It requires that the module defines `CONNECTORS`, that the value is a
[`Connectors`](../connectors/calling-binding-composing.md) instance, and that it holds at
least one connector. The three failure reasons are:

```text
module 'my_plugin' must export CONNECTORS
CONNECTORS must be a parsimony.Connectors instance; got <type>
CONNECTORS must contain at least one connector
```

### check_descriptions_non_empty

For every connector it computes `(description or "").strip()` and rejects:

- an empty (or whitespace-only) description,
- a description shorter than 20 characters,
- a description longer than 800 characters.

The bounds are inclusive: `[20, 800]` characters, measured on the **stripped** string, not on
words. A connector's description defaults to the stripped docstring of its function (see
[defining connectors](../connectors/defining-connectors.md)), and that description is required
at decoration time — so this check mainly guards against descriptions that exist but are too
terse or too verbose for an agent prompt.

### check_enumerator_decorator and check_enumerator_return_type

Two checks that only apply to connectors carrying the `enumerator` tag. `check_enumerator_decorator`
confirms the underlying function was decorated with [`@enumerator`](../connectors/loaders-and-enumerators.md)
(it carries the role marker that decorator sets) rather than `@connector(..., tags=["enumerator"])`.
`check_enumerator_return_type` confirms the connector declares `output=` and that its return
annotation names `pd.DataFrame`/`Series` and is **not** a `list[Entity]`.

!!! note "These two are a backstop, not your first line of defence"
    `@enumerator(output=...)` validates the same constraints at decoration time and raises a
    plain `ValueError` the moment you decorate a non-conformant enumerator. A real enumerator
    therefore cannot reach `assert_plugin_valid` in a state that fails these two checks — they
    exist to catch hand-rolled connectors that fake the `enumerator` tag through the bare
    `@connector` decorator.

### check_flat_public_params

Connectors expose **flat, top-level parameters** — that flatness is what lets the framework
render a connector's call surface into an LLM prompt and bind individual parameters. This check
forbids the `params: SomeModel` bundling idiom: for each connector it looks for a public
parameter literally named `params`, resolves its type hint (unwrapping `typing.Annotated`), and
rejects it if the annotation is a subclass of Pydantic's `BaseModel`.

```python
from pydantic import BaseModel
import pandas as pd
from parsimony.connector import connector


class _BundledParams(BaseModel):
    country: str


@connector()
async def bundled(params: _BundledParams) -> pd.DataFrame:  # FAILS check_flat_public_params
    """Toy connector with bundled params, for conformance demonstration only."""
    return pd.DataFrame({"country": [params.country]})


@connector()
async def flat(country: str) -> pd.DataFrame:  # PASSES — flat top-level parameter
    """Toy connector with a flat top-level parameter, the required shape."""
    return pd.DataFrame({"country": [country]})
```

!!! warning "Only the parameter named `params` is inspected"
    The check triggers solely on a public parameter literally named `params`. A bundled
    `BaseModel` passed under any other name is not detected. Pydantic models are fine for your
    connector's *internal* validation — just do not expose one as the public `params`
    parameter.

### Enumerating the check names

`iter_check_names()` yields the registered check names in execution order — useful for
building a report or asserting the set in your own meta-test.

```python
from parsimony.testing import iter_check_names

assert set(iter_check_names()) == {
    "check_connectors_exported",
    "check_descriptions_non_empty",
    "check_enumerator_decorator",
    "check_enumerator_return_type",
    "check_flat_public_params",
}
```

## ConformanceError

```python
class ConformanceError(AssertionError):
    def __init__(
        self,
        check: str,
        reason: str,
        *,
        module_path: str | None = None,
        next_action: str | None = None,
    ) -> None
```

`ConformanceError` subclasses `AssertionError`, so it surfaces as an ordinary pytest assertion
failure and is caught by `except AssertionError`. Its string form is `[{check}] {reason}`, and
it carries structured attributes for richer reporting:

| Attribute | Meaning |
| --- | --- |
| `check` | The failing check's name (e.g. `"check_flat_public_params"`). |
| `reason` | A human-readable explanation. |
| `module_path` | The plugin module path, when known. |
| `next_action` | A suggested fix, when the check provides one. |

`to_report_dict()` returns those four fields as a dict for structured output (for example, to
feed a JSON report).

```python
from parsimony.testing import ConformanceError, assert_plugin_valid
import my_plugin

try:
    assert_plugin_valid(my_plugin)
except ConformanceError as exc:
    print(exc.check)             # e.g. 'check_descriptions_non_empty'
    print(exc.reason)            # human-readable reason
    print(exc.to_report_dict())  # {'check', 'module_path', 'reason', 'next_action'}
```

## ProviderTestSuite

For a pytest-native test, subclass `ProviderTestSuite` in your plugin's test file and set the
class attributes:

```python
class ProviderTestSuite:
    module: ClassVar[ModuleType | None] = None
    module_path: ClassVar[str | None] = None
    entry_point_name: ClassVar[str | None] = None
```

Pytest collects two methods from any subclass:

- `test_plugin_conforms` — resolves the module and runs `assert_plugin_valid` (all five checks).
- `test_entry_point_resolves` — verifies the plugin is installed and registered under
  `parsimony.providers`; **skipped** unless `entry_point_name` is set.

Set either `module` (an already-imported module) or `module_path` (a dotted import string);
`module` wins when both are set. If neither is set, `_resolve_module` raises `TypeError`.

```python
# tests/test_suite.py in your plugin distribution
from parsimony.testing import ProviderTestSuite


class TestMyPlugin(ProviderTestSuite):
    module_path = "my_plugin.connectors"   # dotted path to the CONNECTORS-exporting module
    entry_point_name = "my_provider"        # also verify pyproject entry-point registration
```

When `entry_point_name` is set, `test_entry_point_resolves` enumerates installed providers via
[`iter_providers`](discovery.md) and raises:

- `ConformanceError("check_entry_point_registered")` if no provider with that name is
  installed under the `parsimony.providers` group, or
- `ConformanceError("check_entry_point_matches")` if the registered name resolves to a
  different module than the one the suite is configured against.

!!! tip "Test methods are plain functions"
    The two test methods are ordinary `def` (not `async`) and need no fixtures or markers.
    Subclassing `ProviderTestSuite` in a `test_*.py` module is enough for pytest to discover
    them. `pytest` itself is imported lazily inside the suite (for `pytest.skip`), so the
    procedural `assert_plugin_valid` path has no hard dependency on pytest.

!!! warning "Duplicate provider names raise during the entry-point test"
    If two installed distributions register the same provider name, `iter_providers` raises
    `RuntimeError` — so a name collision surfaces as a `RuntimeError` from
    `test_entry_point_resolves`, not as a `ConformanceError`. Uninstall one of the conflicting
    distributions.

## A conformant module to test against

The smallest module that passes all five checks exports a `CONNECTORS` built from
[`@connector`](../connectors/defining-connectors.md) and
[`@enumerator`](../connectors/loaders-and-enumerators.md) connectors with adequate
descriptions and flat parameters:

```python
import pandas as pd
from parsimony.connector import Connectors, connector, enumerator
from parsimony.result import Column, ColumnRole, OutputConfig

FETCH_OUTPUT = OutputConfig(
    columns=[
        Column(name="key", role=ColumnRole.KEY, namespace="synth"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="value", dtype="numeric", role=ColumnRole.DATA),
    ]
)
ENUM_OUTPUT = OutputConfig(
    columns=[
        Column(name="key", role=ColumnRole.KEY, namespace="synth"),
        Column(name="title", role=ColumnRole.TITLE),
    ]
)


@connector(output=FETCH_OUTPUT, tags=["tool"])
async def synth_fetch(key: str) -> pd.DataFrame:
    """Fetch a synthetic observation series. Returns a small example table."""
    return pd.DataFrame([{"key": key, "title": key, "value": 1.0}])


@enumerator(output=ENUM_OUTPUT, tags=["synth"])
async def enumerate_synth(limit: int = 10) -> pd.DataFrame:
    """Enumerate up to ``limit`` synthetic catalog entries for discovery."""
    return pd.DataFrame([{"key": f"k{i}", "title": f"Item {i}"} for i in range(limit)])


CONNECTORS = Connectors([synth_fetch, enumerate_synth])
```

Drop this in a module, point a test at it, and `assert_plugin_valid` returns cleanly. See
[authoring a provider plugin](authoring.md) for the full distribution layout and the
`pyproject.toml` entry-point registration this conformance suite assumes.

## See also

- [Authoring a provider plugin](authoring.md) — build the distribution these checks validate.
- [Discovering installed providers](discovery.md) — the `iter_providers` API the entry-point test uses.
- [Defining connectors](../connectors/defining-connectors.md) — the description and flat-params rules these checks enforce.
- [Command-line interface](../cli.md) — `parsimony list --strict` reuses `assert_plugin_valid`.
