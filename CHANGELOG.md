# Changelog

All notable changes to parsimony will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/).

## [Unreleased]

## [0.1.0] - 2026-04-10

### Added

- `@connector` and `@enumerator` decorators for typed data source wrappers
- `Connectors` composition with `+` operator and `bind_deps()`
- `Result` and `SemanticTableResult` with provenance tracking
- `Catalog` with optional vector-searchable catalog
- `SQLiteCatalogStore` implementation with FTS5 and optional vector search
- `LiteLLMEmbeddingProvider` for catalog embeddings
- `OutputConfig` with `Column` roles (KEY, TITLE, METADATA, DATA) and `Namespace` annotation
- Built-in connectors: FRED, SDMX, FMP, SEC Edgar, Polymarket, EODHD, and more
- `with_callback()` for post-fetch hooks on connectors and collections
