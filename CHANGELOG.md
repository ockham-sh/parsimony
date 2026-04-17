# Changelog

All notable changes to parsimony will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/).

## [Unreleased]

### Changed

- **Distribution name is now `parsimony-core` on PyPI.** The `parsimony` name on PyPI
  is currently held by an unrelated squatted project. Import path is unchanged
  (`from parsimony import ...`). The distribution will migrate to the bare `parsimony`
  name once it becomes available; a shim `parsimony-core` will then depend on `parsimony`
  for backwards compatibility.
- `pyproject.toml`: explicit `[tool.hatch.build.targets.wheel]` and `sdist` sections added
  so the `parsimony/` package is unambiguously included in the built distributions.

### Added

- `@loader` decorator for observation-persistence connectors
- `DataStore` abstract class and `InMemoryDataStore` implementation
- `LoadResult` statistics model for data loading
- MCP (Model Context Protocol) server integration (`parsimony.mcp`)
- Provider registry pattern (`ProviderSpec`) for declarative connector wiring
- `Connectors.filter()` for tag/property-based connector filtering
- `Connectors.to_llm()` and `Connector.to_llm()` for LLM-ready descriptions
- `Connector.describe()` for human-readable documentation
- `Result.to_table()` for late schema application
- `Result.entity_keys`, `Result.data_columns`, `Result.metadata_columns` accessors
- Arrow/Parquet serialization (`Result.to_arrow()`, `Result.to_parquet()`)
- `Namespace` annotation for catalog-aware parameter fields
- `Catalog` lazy namespace population (GitHub download + live enumerator fallback)
- `Catalog.embed_pending()` for backfilling embeddings
- Hybrid search (FTS5 BM25 + vec0 cosine via Reciprocal Rank Fusion) in `SQLiteCatalogStore`
- New connectors: CoinGecko, Finnhub, Tiingo, Alpha Vantage, EIA, BLS, US Treasury
- Central bank connectors: SNB, RBA, Riksbank, BDE, BOJ, BOC, BDP, BDF, Destatis
- Financial reports connector
- `Makefile` with common development commands
- `CLAUDE.md` for AI-assisted development conventions
- `py.typed` marker for PEP 561

### Changed

- `build_connectors_from_env()` now driven by declarative `PROVIDERS` registry
- Improved error messages for missing dependencies and validation failures
- `RateLimitError.retry_after` now validates against epoch timestamp misuse

### Fixed

- `from parsimony import client` no longer crashes without API keys (uses `lenient=True`)
- Bare `Exception` catches narrowed to specific operational types (EC-1 convention)
- Fixed import ordering in riksbank, snb, coingecko, tiingo modules
- All ruff linting issues resolved (E501, E402, F841, B904, B905, SIM)

## [0.1.0a1] - 2026-04-10

### Added

- `@connector` and `@enumerator` decorators for typed data source wrappers
- `Connectors` composition with `+` operator and `bind_deps()`
- `Result` and `SemanticTableResult` with provenance tracking
- `Catalog` with optional vector-searchable catalog
- `SQLiteCatalogStore` implementation with FTS5 and optional vector search
- `LiteLLMEmbeddingProvider` for catalog embeddings
- `OutputConfig` with `Column` roles (KEY, TITLE, METADATA, DATA)
- Built-in connectors: FRED, SDMX, FMP, FMP Screener, SEC Edgar, Polymarket, EODHD
- `with_callback()` for post-fetch hooks on connectors and collections
- Typed error hierarchy: `ConnectorError`, `UnauthorizedError`, `PaymentRequiredError`,
  `RateLimitError`, `ProviderError`, `EmptyDataError`, `ParseError`
- `HttpClient` with credential redaction in logs
