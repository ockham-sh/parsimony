# API Reference

This is the concise public surface for `parsimony-core`.

## Connectors

### `@connector`

Decorates an async callable and returns a `Connector`.

```python
@connector(output=OUTPUT, tags=["tool"])
async def fetch(symbol: str, api_key: str = "") -> pd.DataFrame:
    """Fetch data for a symbol."""
```

Keyword arguments:

- `name`: override the function name.
- `description`: override the docstring.
- `output`: optional `OutputConfig` for tabular result shaping.
- `tags`: labels used by callers such as agent runtimes or apps.
- `properties`: exact-match metadata for filtering.
- `secrets`: parameter names omitted from provenance.

The callable signature is the connector parameter surface.

### `Connector`

Important attributes and methods:

- `name: str`
- `description: str`
- `signature: inspect.Signature`
- `exposed_signature: inspect.Signature`
- `output_config: OutputConfig | None`
- `tags: tuple[str, ...]`
- `properties: Mapping[str, Any]`
- `bind(**kwargs) -> Connector`
- `with_callback(callback) -> Connector`
- `describe() -> str`
- `to_llm() -> str`
- `await connector(*args, **kwargs) -> Result`

`bind` is partial application. Bound values are fixed on the returned connector and removed from its exposed signature.

### `Connectors`

Immutable collection of `Connector` values.

- `Connectors([...])`
- `a + b` — combine two collections (duplicate names raise `ValueError`)
- `connectors.bind(**kwargs)`
- `connectors.with_callback(callback)`
- `connectors.filter(predicate)`
- `connectors.search(query, *, tags=None, **properties)`
- `connectors.names()`
- keyed lookup with `connectors["name"]`

## Specialized Decorators

`@enumerator(output=...)` decorates async functions that return `pd.DataFrame`. The framework wraps them as `TabularResult`. Entity projection happens in catalog build code via `OutputConfig.build_entities()`.

`@loader(output=...)` validates observation-loading output: exactly one namespaced `KEY`, at least one `DATA`, and no `TITLE` or `METADATA` columns.

## Results

`Result` wraps arbitrary connector output with framework-built provenance. `TabularResult` is the tabular subtype; use `TabularResult.from_dataframe`, `TabularResult.from_arrow`, and `TabularResult.from_parquet` for tabular construction. `OutputConfig` maps DataFrame columns into semantic roles.

`Provenance.params` records call-time connector arguments after binding. Bound values are omitted. Parameters listed in `Connector.secrets` are also omitted.

## Catalogs

`Entity(namespace, code, title, metadata={...})` is the normalized discoverable identity record (`parsimony.entity`).

`Catalog(name, indexes=None, default_field="title")` is the catalog lifecycle object.

- Constructing it is cheap and does not build indexes.
- `indexes=None` uses the default index policy: at `build()`, BM25 indexes are created for `code`, `title`, and each metadata key on the entries.
- `default_field` is the field name targeted by broad queries, defaults to `"title"`.
- `catalog.set_entities(entities)` replaces rows without rebuilding indexes.
- `catalog.set_indexes(indexes)` changes indexing channels without rebuilding indexes.
- `catalog.index_for(field)` returns the index configured for a given field (raises `KeyError` if missing).
- `await catalog.build()` builds configured indexes over the current entries.
- `await catalog.save(url_or_path)` writes a portable snapshot.
- `await Catalog.load(url_or_path)` loads a built, searchable snapshot. Caching loaded catalogs is the caller's responsibility.

`Catalog.name` is the artifact name used for snapshots. Entity namespaces live on `Entity.namespace` or on a tabular `Result`'s `KEY` column.

Built-in index types:

- `BM25Index()` — BM25 over one Entity field (field name supplied at build time via the catalog dict key).
- `VectorIndex(embedder=...)` — vector similarity over one Entity field.
- `HybridIndex(components=[...], fusion=None)` — fuses multiple indexing methods (e.g. BM25 + Vector) over the **same** Entity field using a `Ranker` policy (defaults to `ZScoreFusion()`).
- `DisMaxIndex(fields=[...], component_factory=BM25Index, tie_breaker=0.0)` — DisMax fusion across **multiple** Entity fields using the same component type (BM25 or Vector). The catalog dict key is the DSL surface name; `fields` lists the Entity fields read internally.

Example — one search surface over short and long titles:

```python
Catalog(
    name="provider",
    indexes={
        "title": DisMaxIndex(
            fields=["short_title", "long_title"],
            component_factory=BM25Index,
        ),
    },
    default_field="title",
)
```

Users query `title: World Bank GDP`; the index scores `short_title` and `long_title` and returns the per-row maximum.

`OutputConfig.build_entities(df)` projects tabular rows into `list[Entity]` using `KEY`/`TITLE`/`METADATA` columns. A metadata column named `"*"` is a wildcard that captures every column not already claimed. Catalog build helpers call this after enumerator `TabularResult`s are fetched — connectors do not return entities directly.

### Local catalog search

`make_local_search_connector(...)` builds a standard catalog-search connector for provider packages. Related helpers: `CatalogLRU`, `resolved_catalog_url`, `CatalogSearchParams`.

## Stores

- `InMemoryDataStore` — observation tables keyed by `(namespace, code)`
- `LoadResult` — statistics from a data load run

## Ranking

`Ranking` is one index's evidence. `RankingSet` is named evidence across indexes.

`ZScoreFusion(weights={...})` is the default score-based fusion policy with per-index Z-score normalization.

`RRF(weights={...}, k=60)` is rank-based fusion (Reciprocal Rank Fusion).

`MinMaxScoreFusion(weights={...})` is score-based fusion with per-index min-max normalization.

## Discovery

`parsimony.discover.iter_providers()` lists installed provider metadata.

`parsimony.discover.load(*names)` loads named providers strictly.

`parsimony.discover.load_all()` loads every installed provider forgivingly.

## Testing

`assert_plugin_valid(module)` and `ProviderTestSuite` check the plugin contract: exported non-empty `CONNECTORS`, non-empty connector descriptions, enumerator return types, and flat public parameters.

## Errors

`InvalidParameterError` is raised for call-time parameter validation failures before an upstream request is made.
