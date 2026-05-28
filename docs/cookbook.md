# Cookbook

## Bind a Credential

```python
fred = FRED.bind(api_key="YOUR_FRED_KEY")
result = await fred["fred_fetch"](series_id="GDP")
```

## Merge Sources

```python
bundle = fred + sdmx
```

## Inspect exposed parameters

```python
fred["fred_search"].exposed_signature
```

Bind secrets before passing connectors to untrusted callers.

## Search a catalog

```python
from parsimony.catalog import Catalog

catalog = await Catalog.load("hf://parsimony-dev/riksbank")
matches, diagnostic = await catalog.search("policy rate", limit=5)
for match in matches:
    print(match.code, match.title, match.score)
```

- Plain text (no `field:` prefix) runs **broad** search against `default_field` (usually `title`).
- Structured queries use `FIELD: value` syntax. Combine filters with `&&` (AND across fields). Use commas for OR within one field: `frequency: M, Q`.

Examples:

```python
await catalog.search("yield curve")                    # broad
await catalog.search("code: SEKEURPMI")                # structured, exact code lookup
await catalog.search("agency: ECB && REF_AREA: France")  # structured AND
```

If a structured query names an unindexed field, search raises `UnknownIndexedFieldError` listing the indexed fields.

## DisMax: one search surface over multiple Entity fields

When the same concept appears in multiple Entity columns (e.g. `short_title` and `long_title`), bind one DSL surface with `DisMaxIndex`:

```python
from parsimony.catalog import Catalog, DisMaxIndex, BM25Index, Entity

catalog = Catalog(
    name="demo",
    indexes={
        "title": DisMaxIndex(
            fields=["short_title", "long_title"],
            component_factory=BM25Index,
        ),
    },
    default_field="title",
)
catalog.set_entities([
    Entity(
        namespace="demo",
        code="gdp",
        title="unused",
        metadata={
            "short_title": "World Bank GDP",
            "long_title": "Gross domestic product, World Bank WDI",
        },
    ),
])
await catalog.build()

matches, _ = await catalog.search("title: World Bank GDP", limit=5)
# Scores short_title and long_title; returns the per-row max (Lucene DisMax).
```

The dict key (`"title"`) is what users type in queries. The `fields` list names the Entity attributes or metadata keys read internally.
