<div align="center">
<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/assets/parsimony-brand-dark.png" />
  <img src="docs/assets/parsimony-brand-light.png" alt="parsimony" width="460" />
</picture>

**Financial data access skill for coding agents.**

[![PyPI](https://img.shields.io/pypi/v/parsimony-core.svg)](https://pypi.org/project/parsimony-core/)
[![License: Apache-2.0](https://img.shields.io/badge/License-Apache--2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Python](https://img.shields.io/pypi/pyversions/parsimony-core.svg)](https://pypi.org/project/parsimony-core/)

</div>


---

Agents + code execution is the best way to automate financial workflows.

[Parsimony](https://parsimony.dev) is a coding-agent skill, backed by an eponymous [Python framework](https://docs.parsimony.dev), that gives your agent one interface to find, fetch, organize, and navigate financial data across sources.


## Install

Install the [Parsimony skill](skills/parsimony/SKILL.md) for Cursor, Claude Code, Codex, and other compatible agents:

```bash
npx skills add ockham-sh/parsimony
```

Or, without Node, run the same CLI via uv:

```bash
uvx npx-skills add ockham-sh/parsimony
```

Then ask your agent for the data you need:

> Find Eurostat's harmonised index of consumer prices for the euro area, fetch the annual rate from 2020 onward, and show the source and parameters used.

Behind the scenes, the agent uses the Parsimony framework in your own Python environment to navigate and fetch data, or build custom connectors.


## The Framework

The skill is backed by the [Parsimony framework](https://docs.parsimony.dev), which provides:

1. A thin layer for building, organizing, and using *data connectors* -- functions for searching, navigating, and fetching data.
2. Purpose-built components for writing custom connectors efficiently, such as data catalogs and semantic search indexes.


## The Ecosystem

Connectors are distributed as plugins. A curated selection of [prebuilt plugins](https://parsimony.dev/connectors) is maintained in a [dedicated repository](https://github.com/ockham-sh/parsimony-connectors).

For sources that don't provide search and discovery endpoints, such as Eurostat, Parsimony provides custom search functions built on Parsimony catalogs.

You and your agent can also publish your own plugins. Public plugins follow the [`parsimony-<name>` convention](https://docs.parsimony.dev/plugins/authoring/), which lets Parsimony discover them automatically once installed.


## Documentation

The complete framework documentation is published at [docs.parsimony.dev](https://docs.parsimony.dev).


## Development

```bash
make install
make check
```

See the [development guide](docs/development.md) and [contribution guidelines](CONTRIBUTING.md).


## The Stack

- [parsimony-core](https://github.com/ockham-sh/parsimony) provides the framework and agent skill.
- [parsimony-connectors](https://github.com/ockham-sh/parsimony-connectors) provides a curated set of connector plugins.
- [parsimony-agents](https://github.com/ockham-sh/parsimony-agents) provides a framework for building data retrieval and analysis agents on Parsimony.
- Open-format catalogs are hosted on [Hugging Face](https://huggingface.co/parsimony-dev). They power functions for sources without native discovery APIs.
- The upcoming [Ockham Terminal](https://ockham.sh) is an open-source, self-hostable IDE for data-related work, built on `parsimony-agents`.


## License

Apache-2.0. See [LICENSE](LICENSE).
