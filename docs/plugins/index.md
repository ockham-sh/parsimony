# Plugins and providers

Parsimony's core package — `parsimony-core` — ships **zero connectors**. The framework, the
data carriers, the typed errors, and the [Catalog](../catalog/index.md) all live in core, but
every actual data source is published separately as its own `parsimony-<name>` distribution
and discovered at runtime. This page explains that plugin model: how a provider registers
itself, what it must export, and how you load installed providers into a usable
[`Connectors`](../connectors/calling-binding-composing.md) collection.

## Why connectors live outside core

A connector is a small piece of integration code bound to one upstream API — its auth scheme,
its pagination, its quirks. Keeping those out of the core package means the framework can be
small, stable, and dependency-light while the connector ecosystem grows independently. You
install only the sources you need, and a bug in one provider can never destabilize the rest.

A bare `pip install parsimony-core` therefore discovers nothing. To get any connectors you
install one or more provider distributions:

```bash
pip install parsimony-core parsimony-fred
```

!!! note
    `parsimony-fred` is used throughout these docs as a representative provider. The
    framework imposes no naming beyond the `parsimony-<name>` convention; providers are
    ordinary PyPI distributions that you install like any other dependency.

## The provider contract

A provider is a distribution that registers an **entry point** under the well-known group
`parsimony.providers`. The entry point's *name* becomes the provider's public name; its
*value* is the dotted path of a module that, when imported, exposes a module-level attribute
named exactly `CONNECTORS` that is an instance of `Connectors`.

That is the entire contract — two halves:

| Half | Where it lives | What it must be |
| --- | --- | --- |
| Registration | the plugin's `pyproject.toml`, under `[project.entry-points."parsimony.providers"]` | `provider-name = "dotted.module.path"` |
| Export | the module at that dotted path | a top-level `CONNECTORS: Connectors` |

A minimal plugin `pyproject.toml` declares its entry point like this:

```toml
[project.entry-points."parsimony.providers"]
fred = "parsimony_fred"
```

And the module it points at exports `CONNECTORS`:

```python
# parsimony_fred/__init__.py
from parsimony import Connectors, connector


@connector()
def fred_fetch(series_id: str) -> dict:
    """Fetch a single FRED economic series by its identifier."""
    # real plugins use parsimony.transport to call the upstream API
    return {"series_id": series_id, "value": 42}


CONNECTORS = Connectors([fred_fetch])
```

When you ask the framework to load that provider, it imports the module, reads the `CONNECTORS`
attribute, and rejects anything that is not a `Connectors` instance — the validation is
equivalent to this:

```python
import importlib

from parsimony import Connectors

mod = importlib.import_module("parsimony_fred")  # the plugin's module path
obj = getattr(mod, "CONNECTORS", None)
if not isinstance(obj, Connectors):
    raise TypeError("parsimony_fred must export CONNECTORS: Connectors")
```

!!! warning
    The export name is case-sensitive and fixed: it must be `CONNECTORS`, and it must be a
    `Connectors` instance — not a bare list of connectors. A module that exports a list, a
    differently-named attribute, or nothing at all raises `TypeError` the moment it is loaded.

For the full walkthrough of building a conformant distribution, see
[authoring a provider plugin](authoring.md).

## Loading installed providers

The `parsimony.discover` module turns installed entry points into connectors. It has exactly
three public functions and one record type, with no cache, no singleton, and no import-time
side effects — callers cache the result themselves if they want to. The module is also
re-exported at the top level as `parsimony.discover`.

| Function | Strictness | Behavior |
| --- | --- | --- |
| `iter_providers()` | metadata-only | yields one `Provider` per installed entry point; imports no plugin code |
| `load(*names)` | strict | imports the named providers and concatenates their connectors; raises `LookupError` if any name is absent |
| `load_all()` | forgiving | imports every installed provider; logs and skips any that fail |

### Discover what is installed (no imports)

`iter_providers()` reads distribution metadata only — it never imports a plugin's module, so
it is cheap and side-effect-free. Each `Provider` is a frozen record carrying the provider
`name`, the `module_path` to import, and the owning distribution's `dist_name` and `version`
(each `None` if unresolvable). A `homepage` property is computed lazily from the
distribution's metadata on each access.

```python
from parsimony import discover

for provider in discover.iter_providers():
    print(provider.name, provider.module_path, provider.dist_name, provider.version)
    print("  homepage:", provider.homepage)
```

With only `parsimony-core` installed this prints nothing — there are no providers to find.

!!! warning
    `iter_providers()` refuses to guess between conflicting plugins. If two installed
    distributions register the *same* provider name, it raises `RuntimeError` telling you to
    uninstall one. Because `load` and `load_all` both iterate it, that check fires for them too.

### Load by name (strict)

`load(*names)` is the way to compose exactly the providers you depend on. It imports each
named provider and returns a single `Connectors` concatenating their connectors, in the order
you passed the names. If any requested name is not installed, it raises `LookupError` listing
both the missing names and the sorted set of available ones — so a typo or a forgotten
`pip install` fails loudly rather than silently producing an empty bundle.

```python
from parsimony import discover


connectors = discover.load("fred")  # LookupError if "fred" is not installed
print(connectors.names())           # connector names contributed by the provider
result = connectors["fred_fetch"](series_id="GDP")
print(result.data)  # the connector's payload (a DataFrame for a tabular fetch)
```

!!! note
    This example needs the `parsimony-fred` distribution installed and a FRED API key, so it
    will not run on a bare `parsimony-core` install. `result.data` holds the payload the
    framework wrapped in a [`Result`](../connectors/results.md). Because the real `fred_fetch`
    returns a DataFrame, `result.is_tabular` is `True` and the tabular accessors
    (`result.frame` / `result.df`) apply; a connector that returns a scalar or dict yields a
    `Result` whose `data` is that value and for which the tabular accessors raise.

A loaded `Connectors` is a normal collection: index it by connector name with `[]`, check
membership with `in`, list `.names()`, `bind` shared parameters across it, and call any
connector. See
[calling, binding, and composing](../connectors/calling-binding-composing.md) for the full
surface.

### Load everything (forgiving)

`load_all()` imports every installed provider and returns their combined connectors. Unlike
`load`, it is exception-tolerant: if one plugin fails to import — a missing transitive
dependency, a broken module, a contract violation — it logs a warning on the
`parsimony.discover` logger and skips that plugin, so one bad provider never breaks discovery
of the rest.

```python
import logging

from parsimony import discover

logging.getLogger("parsimony.discover").setLevel(logging.WARNING)

connectors = discover.load_all()
print(len(connectors), "connectors from all installed providers")
print(connectors.names())
```

!!! warning
    Because `load_all()` swallows every exception from a plugin's load step, a provider that
    violates the contract (no `CONNECTORS` export) is silently skipped with only a logged
    warning. The same broken provider passed to `load("its_name")` raises `TypeError` loudly.
    Use `load(...)` when you want missing or malformed dependencies to fail hard.

### Composing providers explicitly

`load` already concatenates the providers you name, but you can also load bundles separately
and merge them with the `+` operator — the same composition idiom used everywhere for
`Connectors`. There is no `.merge` method.

```python
from parsimony import discover

fred = discover.load("fred")
sdmx = discover.load("sdmx")
both = fred + sdmx          # one collection over both providers
print(both.names())
```

!!! warning
    Merging two providers that each export a connector of the *same* name raises `ValueError`
    from the `Connectors` constructor — provider-name collisions are caught by
    `iter_providers`, but connector-name collisions are caught when the collections are
    combined.

## Inspecting plugins from the command line

The [`parsimony` CLI](../cli.md) wraps the same discovery layer. `parsimony list` walks the
`parsimony.providers` group and prints each plugin, its version, its connector count, and a
conformance status:

```bash
parsimony list
```

```text
No parsimony plugins discovered (0 plugins).
Install one to get started, e.g. `pip install parsimony-fred`.
```

Add `--json` for machine-readable output, or `--strict` to run the
[conformance suite](conformance.md) against each plugin and exit non-zero on any failure. See
the [CLI reference](../cli.md) for the full command surface.

## Where to go next

- [Discovering installed providers](discovery.md) — the full `parsimony.discover` API:
  `iter_providers`, `Provider`, `load`, `load_all`.
- [Authoring a provider plugin](authoring.md) — build and package your own
  `parsimony-<name>` distribution.
- [Conformance testing](conformance.md) — validate a plugin with `parsimony.testing` before
  you publish it.

## See also

- [Discovering installed providers](discovery.md) — load and inspect what is installed
- [Authoring a provider plugin](authoring.md) — package your own provider
- [Conformance testing](conformance.md) — the `parsimony.testing` validation toolkit
- [Calling, binding, and composing](../connectors/calling-binding-composing.md) — working with the `Connectors` you load
- [Command-line interface](../cli.md) — `parsimony list` and the rest of the CLI
