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
- `Connectors.merge(a, b, ...)`
- `connectors.bind(**kwargs)`
- `connectors.with_callback(callback)`
- `connectors.filter(predicate=None, *, name=None, tags=None, **properties)`
- `connectors.replace(name, connector)`
- `connectors.names()`
- keyed lookup with `connectors["name"]`

## Specialized Decorators

`@enumerator(output=...)` validates catalog-enumeration output: no `DATA` columns, exactly one `KEY`, exactly one `TITLE`.

`@loader(output=...)` validates observation-loading output: exactly one namespaced `KEY`, at least one `DATA`, and no `TITLE` or `METADATA` columns.

## Results

`Result` wraps arbitrary connector output with framework-built provenance. `TabularResult` is the tabular subtype; use `TabularResult.from_dataframe`, `TabularResult.from_arrow`, and `TabularResult.from_parquet` for tabular construction. `OutputConfig` maps DataFrame columns into semantic roles.

`Provenance.params` records call-time connector arguments after binding. Bound values are omitted. Parameters listed in `Connector.secrets` are also omitted.

## Catalogs

`CatalogEntry(namespace, code, title, metadata={...})` is the canonical catalog row.

`Catalog(name, indexes=None, default_field="title")` is the catalog lifecycle object.

- Constructing it is cheap and does not build indexes.
- `indexes` is a list of index objects, or `None` for default BM25 index on title.
- `default_field` is the field name targeted by broad queries, defaults to `"title"`.
- `catalog.set_entries(entries)` replaces rows without rebuilding indexes.
- `catalog.set_indexes(indexes)` changes indexing channels without rebuilding indexes.
- `catalog.index_for(field)` returns the index configured for a given field (raises `KeyError` if missing).
- `await catalog.build()` builds configured indexes over the current entries.
- `await catalog.save(url_or_path)` writes a portable snapshot.
- `await Catalog.load(url_or_path)` loads a built, searchable snapshot. Caching loaded catalogs is the caller's responsibility.

`Catalog.name` is the artifact name used for snapshots. Entry namespaces live on `CatalogEntry.namespace` or on a catalog `Result`'s `KEY` column.

Built-in index types:

- `BM25Index(name, field="title")`
- `VectorIndex(name, field="title", embedder=...)`
- `HybridIndex(name, field, indexes, fusion=None)` wraps multiple leaf indexes (e.g. BM25 + Vector) for the same field and fuses their results using a `Ranker` policy (defaults to `ZScoreFusion()`).

`OutputConfig.build_entries(df)` is the catalog counterpart of `OutputConfig.build_table_result(df)`: the schema applies itself to *df*, reading the `KEY`/`TITLE`/`METADATA` columns to produce `list[CatalogEntry]`. A metadata column named `"*"` is a wildcard that captures every column not already claimed. Enumerators return the resulting list directly.

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

`assert_plugin_valid(module)` and `ProviderTestSuite` check the minimal plugin contract: exported non-empty `CONNECTORS` and non-empty connector descriptions.
