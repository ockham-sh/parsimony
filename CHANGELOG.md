# Changelog

All notable changes to ockham will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/).

## [0.1.0] - Unreleased

### Added

- `@connector` and `@enumerator` decorators for typed data source wrappers
- `Connectors` composition with `+` operator and `bind_deps()`
- `Result` and `SemanticTableResult` with provenance tracking
- `SeriesCatalog` with optional vector-searchable catalog
- `InMemoryCatalogStore` and `SupabaseCatalogStore` implementations
- `LiteLLMEmbeddingProvider` for catalog embeddings
- `OutputConfig` with `Column` roles (KEY, TITLE, METADATA, DATA) and `Namespace` annotation
- Built-in connectors: FRED, SDMX, FMP, SEC Edgar, Polymarket, EODHD, IBKR
- `with_callback()` for post-fetch hooks on connectors and collections
