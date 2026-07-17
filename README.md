<div align="center">
<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/assets/parsimony-brand-dark.png" />
  <img src="docs/assets/parsimony-brand-light.png" alt="parsimony" width="460" />
</picture>

**An agent-friendly data access layer for financial and economic data.**

[![PyPI](https://img.shields.io/pypi/v/parsimony-core.svg)](https://pypi.org/project/parsimony-core/)
[![License: Apache-2.0](https://img.shields.io/badge/License-Apache--2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Python](https://img.shields.io/pypi/pyversions/parsimony-core.svg)](https://pypi.org/project/parsimony-core/)

</div>


<p align="center">
  <img src="docs/assets/parsimony-hero.gif" alt="Defining and calling a Parsimony connector" width="900" />
</p>

---

[Parsimony](https://parsimony.dev) is a Python framework for keeping your financial data sources organized, searchable, and accessible through a single interface for people and agents. 

We try to keep it lightweight, simple, and [parsimonious](https://en.wikipedia.org/wiki/Occam's_razor).

## Quickstart

You can define connectors for internal data or use community plugins with prebuilt connectors. For official statistics, `parsimony-sdmx` is a good starting point. See [parsimony-connectors](https://github.com/ockham-sh/parsimony-connectors) for other financial and economic data sources.

Each plugin provides a `CONNECTORS` collection with its available connectors.

```bash
pip install parsimony-sdmx
```

```python
from parsimony_sdmx import CONNECTORS

print(CONNECTORS.describe())

datasets = CONNECTORS["sdmx_datasets_search"](
    query="euro area inflation",
    agency="ESTAT",
    limit=5,
)
print(datasets.raw[["flow_id", "title"]])

dataset_id = datasets.raw.iloc[0]["dataset_id"]
series = CONNECTORS["sdmx_series_search"](
    agency="ESTAT",
    dataset_id=dataset_id,
    query="euro area all items annual rate",
    limit=5,
)
print(series.raw[["key", "title"]])

series_ref = series.raw.iloc[0]["key"]
result = CONNECTORS["sdmx_fetch"](
    dataset_ref=f"ESTAT-{dataset_id}",
    series_ref=series_ref,
    start_period="2020",
)
print(result.raw.tail())
```

## Defining a connector

A connector is slightly more than a Python function.

```bash
pip install parsimony-core
```

```python
import pandas as pd

from parsimony import Connectors, connector

@connector
def latest_price(symbol: str) -> pd.DataFrame:
    """Fetch the latest price for a symbol."""
    return pd.DataFrame({"symbol": [symbol], "currency": ["USD"], "price": [101.50]})

result = latest_price(symbol="ACME")
print(result.raw)

connectors = Connectors([latest_price])  # bundle several connectors; `+` combines bundles
```

## Data catalogs

Some sources can list their identifiers but cannot search them. A `Catalog` turns that list into something you can search.

```bash
pip install 'parsimony-core[catalog]'
```

```python
import pandas as pd

from parsimony import Catalog, Column, ColumnRole, OutputSpec, connector, enumerator

SERIES_OUTPUT = OutputSpec(
    columns=[
        Column(name="series_id", role=ColumnRole.KEY, namespace="demo"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="frequency", role=ColumnRole.METADATA),
    ]
)

@enumerator(output=SERIES_OUTPUT)
def list_series() -> pd.DataFrame:
    """List all series available from a provider."""
    # Replace this sample with a call to the provider's listing endpoint.
    return pd.DataFrame(
        {
            "series_id": ["UNRATE", "GDPC1"],
            "title": ["Unemployment Rate", "Real Gross Domestic Product"],
            "frequency": ["monthly", "quarterly"],
        }
    )

listed = list_series()             # -> Result, carrying SERIES_OUTPUT as its output_spec
catalog = Catalog("macro")
catalog.set_entities(listed.entities.values())
catalog.build()

@connector(tags=["search"])
def macro_search(query: str) -> pd.DataFrame:
    """Search the macroeconomic series catalog."""
    matches = catalog.search(query, limit=10)
    return pd.DataFrame(
        [{"code": match.code, "title": match.title, "score": match.score} for match in matches]
    )

hits = macro_search(query="unemployment")
print(hits.raw)
```

Some plugins use published catalogs from [Hugging Face](https://huggingface.co/parsimony-dev) to make sources such as Eurostat, ECB, and IMF searchable. See [Catalog](docs/catalog/index.md) for indexes and saved catalogs.

## Using Parsimony with agents

Parsimony becomes particularly useful for knowledge work when it's used alongside a coding agent. An agent that can run Python can use connectors as tools. It can list the available connectors, check their parameters, search for identifiers, and fetch data from public or internal sources, all using a single access pattern.

The included [Agent Skill](skills/parsimony/SKILL.md) gives Cursor, Claude Code, Codex, and other compatible agents these instructions:

```bash
npx skills add ockham-sh/parsimony
```

Without a global Node installation, use `uvx npx-skills add ockham-sh/parsimony`.

## Documentation

- [Quickstart](docs/getting-started/quickstart.md)
- [Connectors](docs/connectors/index.md)
- [Catalog](docs/catalog/index.md)
- [API reference](docs/reference/api.md) — public imports

The complete documentation is published at [docs.parsimony.dev](https://docs.parsimony.dev).

## Development

```bash
make install
make check
```

See the [development guide](docs/development.md) and [contribution guidelines](CONTRIBUTING.md).

## License

Apache-2.0. See [LICENSE](LICENSE).
