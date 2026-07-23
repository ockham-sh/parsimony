# Discovering installed providers

`parsimony` ships no connectors of its own — every connector lives in a separate
`parsimony-<name>` distribution that registers itself under the `parsimony.providers`
entry-point group (see [Plugins and providers](index.md)). The `parsimony.discover` module is
how you find those installed providers and load their connectors at runtime. It is a small,
deliberately stateless layer: three functions plus one frozen dataclass, with no cache, no
singleton, and no import-time side effects.

```python
from parsimony import discover
```

The top-level `discover` name re-exports the module. For the individual symbols, import from
`parsimony.discover` directly:

```python
from parsimony.discover import Provider, iter_providers, load, load_all
```

## The provider model in one paragraph

A provider is a `parsimony-<name>` distribution that declares an entry point under the group
`parsimony.providers`. The entry point's **name** becomes the provider's public name; the
entry point's **value** is the dotted module path that, when imported, must export a
module-level `CONNECTORS` attribute that is a [`Connectors`](../connectors/calling-binding-composing.md)
collection. Discovery reads provider metadata (distribution name, version, homepage) entirely
from `importlib.metadata` — plugins do not export a `__version__` or a metadata dict.

!!! note "A bare install discovers nothing"
    `parsimony-core` declares no `parsimony.providers` entry points and ships no in-tree
    connectors. After `pip install parsimony-core`, `iter_providers()` yields zero providers
    and `load_all()` returns an empty collection. You must install at least one
    `parsimony-<name>` distribution for discovery to return anything.

## `iter_providers()` — enumerate without importing

`iter_providers() -> Iterator[Provider]` enumerates every installed provider by reading the
`parsimony.providers` entry points. It is **metadata-only**: it imports no plugin modules, so
it is cheap and side-effect-free. Each item is a `Provider` record.

```python
from parsimony.discover import iter_providers

for p in iter_providers():
    print(p.name, p.module_path, p.dist_name, p.version, p.homepage)
```

If two installed distributions register the **same provider name**, `iter_providers()` raises
`RuntimeError` rather than guessing which one wins — the message names both distributions and
tells you to uninstall one. Because the check lives inside `iter_providers()`, it fires for
`load()` and `load_all()` too, since both iterate it. The error is raised mid-iteration as the
generator is consumed.

!!! warning "Duplicate *provider* names vs duplicate *connector* names"
    There are two distinct collision checks at two layers. `iter_providers()` raises
    `RuntimeError` when two distributions claim the same **provider** name. Separately, the
    `Connectors` constructor raises `ValueError` when a single collection ends up with two
    connectors of the same **connector** name. A `load()` call that concatenates two distinct
    providers that each export a connector named `fetch` raises `ValueError` from the
    `Connectors` constructor, not `RuntimeError`.

## `Provider` — an installed plugin record

`Provider` is a frozen dataclass holding metadata only. It does not hold a reference to the
plugin module; importing the module is deferred until you call `.load()`.

| Field / member | Type | Meaning |
| --- | --- | --- |
| `name` | `str` | The entry-point name — the provider's public name. |
| `module_path` | `str` | The entry-point value — the dotted module to import. |
| `dist_name` | `str \| None` | Owning distribution's name, `None` if unresolvable. |
| `version` | `str \| None` | Owning distribution's version, `None` if unresolvable. |
| `homepage` | `str \| None` (property) | Homepage URL from distribution metadata, computed lazily. |
| `load()` | `-> Connectors` | Import `module_path`; return its `CONNECTORS` export. |

`homepage` is a computed `@property`, not a stored field: it re-reads
`importlib.metadata` on every access. It accepts either the core-metadata `Home-page` field
(rejecting an empty value and the literal `"UNKNOWN"`) or a PEP 621 `[project.urls]` entry
whose key, case-insensitively, equals `homepage`. It returns `None` when nothing is declared
or the distribution cannot be found.

### `Provider.load()` and the plugin contract

`Provider.load()` imports `module_path` via `importlib.import_module`, reads its module-level
`CONNECTORS` attribute, and returns it. If the module does not export `CONNECTORS`, or exports
something that is not a `Connectors` instance, it raises `TypeError`:

```text
TypeError: <module_path> must export CONNECTORS: Connectors
```

This is the plugin contract. A conformant provider module looks like this (see
[Authoring a provider plugin](authoring.md) for the full distribution layout):

```python
# parsimony_demo/__init__.py — the module a provider's entry point points at
from parsimony import connector, Connectors


@connector()
def demo_fetch(series_id: str) -> dict:
    """Fetch a demo series by id."""
    return {"series_id": series_id}


CONNECTORS = Connectors([demo_fetch])  # Provider.load() reads this exact name
```

!!! tip "Repeated `Provider.load()` is idempotent"
    Because Python's import system caches modules, calling `Provider.load()` more than once
    returns the same `Connectors` object — the import happens at most once per process.

## `load(*names)` — strict load by name

`load(*names: str) -> Connectors` loads the named providers and concatenates their connectors
into one `Connectors` collection, in the order you pass the names. It is **strict**: if any
requested name is not installed, it raises `LookupError` before importing anything, listing
both the missing names and the sorted set of available names.

```python
from parsimony.discover import load

connectors = load("fred", "sdmx")   # LookupError if either name is not installed
print(connectors.names())            # sorted connector names across both providers
series = connectors["fred_fetch"]    # KeyError if that connector is absent
```

If a named provider violates the contract (no `CONNECTORS`), `load()` propagates the
`TypeError` loudly — it does not swallow it.

## `load_all()` — forgiving load of everything

`load_all() -> Connectors` loads every installed provider and returns the union of their
connectors. It is **forgiving**: each provider's `.load()` runs inside a `try`/`except
Exception`, so one broken plugin — say, a missing transitive dependency that fails to import —
is logged at `WARNING` on the `parsimony.discover` logger and skipped, while the rest still
load.

```python
import logging
from parsimony.discover import load_all

logging.getLogger("parsimony.discover").setLevel(logging.WARNING)

connectors = load_all()              # a broken plugin won't break the others
print(len(connectors), connectors.names())
```

!!! warning "`load_all()` swallows contract violations too"
    The `except Exception` in `load_all()` catches the `TypeError` from a contract-violating
    plugin just like any other failure: a provider that fails to export `CONNECTORS` is
    silently skipped with a `WARNING`. The same provider passed to `load(name)` would raise
    that `TypeError` to the caller. Use `load(name)` when you want a missing or malformed
    provider to fail fast; use `load_all()` when partial discovery is acceptable.

## Composing loaded collections

`load()` and `load_all()` each return a single `Connectors`. To merge collections from
separate calls, use the `+` operator — there is no `.merge` method.

```python
from parsimony.discover import load

macro = load("fred")
official = load("sdmx")
everything = macro + official       # one Connectors with all connectors from both
print(everything.names())
```

Both operands must be `Connectors`; merging is how you assemble exactly the providers you want
under one call surface. See [Calling, binding, and composing](../connectors/calling-binding-composing.md)
for the full collection API (`get`, `[]`, `in`, `names`, `bind`, `filter`, `search`).

## Inspecting metadata before loading code

Because `iter_providers()` imports nothing, you can inspect a provider's metadata and only
import its module when you decide to use it.

```python
from parsimony.discover import iter_providers

by_name = {p.name: p for p in iter_providers()}
p = by_name["fred"]
print(p.dist_name, p.version, p.homepage)   # all from importlib.metadata, no plugin import
connectors = p.load()                       # only now is the plugin module imported
```

!!! tip "Cache at your own level"
    `parsimony.discover` keeps no internal cache and runs no work at import time. If you call
    discovery on a hot path, store the result yourself — the module will re-enumerate entry
    points and re-import metadata on every call otherwise.

The [`parsimony list`](../cli.md) command is a thin consumer of this same API: it enumerates
providers for a table view and can validate them against the
[conformance checks](conformance.md).

## Installed versus installable

`parsimony.discover` and `parsimony.registry` answer two different questions, and neither
substitutes for the other:

| | `parsimony.discover` | `parsimony.registry` |
|---|---|---|
| Question | What's already installed in **this** environment? | What official `parsimony-<name>` package **could** I install to cover a source? |
| Reads | `parsimony.providers` entry points via `importlib.metadata` | The versioned manifest at `https://parsimony.dev/connectors.json`, falling back to a read-only snapshot bundled in the `parsimony-core` wheel on any failure |
| Returns | `Provider` records — `iter_providers()`, `load()`, `load_all()` | `ConnectorRegistry` — `list_available()` |
| CLI | `parsimony list` | `parsimony list --available` |

Check what's installed first; only query the registry — and only then consider installing a
new package — for a source that discovery doesn't already cover:

```python
from parsimony import discover, registry

installed = {p.name for p in discover.iter_providers()}
if "sdmx" not in installed:
    available = registry.list_available()  # ConnectorRegistry(generated_at, connectors, source)
    match = next((c for c in available.connectors if c.entry_point == "sdmx"), None)
    # ... `pip install match.package`, then re-run discover.iter_providers() to confirm
```

`registry.list_available()` never installs anything itself — it only reports what exists.
Installing (`pip install <package>` / `uv pip install <package>`) and re-discovering
(`discover.load()` / `discover.load_all()`) afterward are the caller's responsibility; see the
[agent skill's routing order](https://github.com/ockham-sh/parsimony/tree/main/skills/parsimony)
(installed/local → registry → author) for a worked end-to-end example, or
[the registry API](../reference/api.md) for the full `list_available()` contract, including
the bundled-fallback behavior and `ConnectorRegistry.source`.

## See also

- [Plugins and providers](index.md) — the entry-point group, the `CONNECTORS` contract, and the provider model overview.
- [Authoring a provider plugin](authoring.md) — build and register your own `parsimony-<name>` distribution.
- [Conformance testing](conformance.md) — validate a provider module against the five plugin checks.
- [Calling, binding, and composing](../connectors/calling-binding-composing.md) — work with the `Connectors` collections that discovery returns.
- [Command-line interface](../cli.md) — `parsimony list` lists and validates installed providers; `parsimony list --available` queries the registry.
- [Public API & import map](../reference/api.md) — `parsimony.registry.list_available()`, the typed API behind the registry.
