# Snapshots and persistence

A built [catalog](index.md) is an in-memory object. To ship it, reuse it across
processes, or rebuild it lazily, you persist it as a **snapshot** — a portable directory of
the entries plus their serialized indexes. Parsimony writes snapshots atomically, stamps each
one with an integrity digest, and can load them back into a fully searchable `Catalog`.
Snapshots live behind a small URL scheme: a local `file://` path or a Hugging Face
`hf://` dataset.

## Saving a catalog

`Catalog.save` writes a snapshot to disk. The catalog must be built first — saving a dirty
catalog raises a plain `ValueError`, the same gate that guards [search](search.md).

```python
from parsimony.catalog import BM25Index, Catalog, Entity

catalog = Catalog("solo", indexes={"title": BM25Index()}, default_field="title")
catalog.set_entities(
    [
        Entity(namespace="solo", code="A", title="alpha title"),
        Entity(namespace="solo", code="B", title="beta title"),
    ]
)
catalog.build()
catalog.save("file:///tmp/parsimony/snapshot", builder="my-job")
```

The optional `builder` keyword is a free-form identifier of the script or job that produced
the snapshot. It is recorded in the manifest's `build.builder` field and is otherwise inert.

!!! warning "Build before you save"
    `save()` calls the same dirty-state check as `search()`. After any mutation
    (`set_entities`, `set_index`, `set_indexes`, `update_indexes`, `delete_many`) the catalog
    is marked dirty and `save()` raises `ValueError("Catalog entries or indexes changed — call
    catalog.build() before it can be saved")` until you re-run `catalog.build()`.

!!! note "BM25 / vector backends are an optional extra"
    The `BM25Index` example above builds and searches only with the `catalog` extra installed
    (it pulls in `rank-bm25`). The snapshot machinery itself — atomic writes, the manifest, the
    integrity digest, URL dispatch — is pure `parsimony-core`. See
    [Installation](../getting-started/installation.md) for the extras matrix.

## Loading a catalog

`Catalog.load` is a classmethod. It returns a non-dirty, immediately searchable catalog —
there is no `build()` call needed after a load.

```python
from parsimony.catalog import Catalog

catalog = Catalog.load("file:///tmp/parsimony/snapshot")
hits, diag = catalog.search("alpha", limit=5)
print(diag.mode, [m.code for m in hits])
```

A loaded catalog is reconstructed exactly from what was serialized: its `default_field` and
index set come from the manifest, and the framework's default-index policy is forced off. That
means calling `build()` on a loaded catalog will **not** re-derive metadata-key indexes — the
serialized indexes are authoritative. (See [Indexes](indexes.md) for the default policy.)

## The snapshot layout

A snapshot is a directory with three parts:

```text
snapshot/
├── entries.parquet      # entities as rows; zstd-compressed when non-empty
├── indexes/             # one subdirectory per index field
│   ├── code/
│   └── title/
└── meta.json            # the CatalogMeta manifest
```

| Path | Contents |
| --- | --- |
| `entries.parquet` | PyArrow table with columns `namespace`, `code`, `title`, `metadata_json`. Each entity's `metadata` is JSON-encoded into `metadata_json`. Non-empty tables use `zstd` compression; an empty catalog writes the four-column schema with no rows. |
| `indexes/<field>/` | One directory per configured index, written by that index's own `save()`. The field name is the logical search surface. |
| `meta.json` | The `CatalogMeta` manifest (see below). |

!!! warning "Only JSON-serializable metadata survives a round-trip"
    `metadata` is stored as a JSON string in the `metadata_json` column. Anything not
    JSON-encodable will fail to serialize, and non-JSON-native types are reconstructed as their
    JSON equivalents on load.

### The manifest

`meta.json` deserializes to a `CatalogMeta` model. Import it (and the nested `BuildInfo`) from
`parsimony.catalog.storage`; neither is a top-level name.

```python
from parsimony.catalog.storage import read_meta

meta = read_meta("/tmp/parsimony/snapshot")
print(meta.name, meta.entry_count, meta.index_fields)
```

`CatalogMeta` fields:

| Field | Type | Meaning |
| --- | --- | --- |
| `schema_version` | `Literal[1]` | Snapshot format version. Pinned to `1`; loading any other value raises `ValueError`. |
| `name` | `str` | The catalog's normalized name. |
| `namespaces` | `list[str]` | Sorted distinct namespaces across the entries. |
| `entry_count` | `int` | Number of entities (`>= 0`). |
| `index_fields` | `dict[str, str]` | Maps each index field to its kind string: `"bm25"`, `"vector"`, `"hybrid"`, or `"dis_max"`. |
| `default_field` | `str \| None` | The broad-search surface, if one was configured. |
| `build` | `BuildInfo` | Provenance for this snapshot (see below). |

`BuildInfo` fields:

| Field | Type | Meaning |
| --- | --- | --- |
| `built_at` | `datetime` | UTC timestamp, defaulted at construction. |
| `parsimony_version` | `str \| None` | Optional library version stamp; not auto-populated. |
| `builder` | `str \| None` | The free-form identifier passed to `save(..., builder=...)`. |
| `content_sha256` | `str` | Integrity digest of every snapshot file except `meta.json`. Empty string when not computed. |

## Atomic writes and integrity

Saving never leaves a half-written snapshot in place. The catalog is written to a sibling temp
directory (`<name>.<pid>.<uuid>.tmp` next to the target), the manifest is written last, any
existing target is removed, and the temp directory is renamed onto the target. If anything
fails mid-write, the temp directory is cleaned up and the original target is untouched. Because
the temp directory is a sibling of the target, the target's **parent** directory must be
writable.

The manifest's `content_sha256` is computed over every file in the snapshot except `meta.json`
itself: each file is hashed, the `relpath:hexdigest` lines are sorted and concatenated, and
that is hashed again. On load, if `content_sha256` is non-empty, the digest is recomputed and a
mismatch raises `ValueError`:

```text
Catalog snapshot integrity check failed for /tmp/parsimony/snapshot2:
  expected sha256: ...
  actual sha256:   ...
```

!!! note "Integrity is opt-in by presence"
    The check runs only when `build.content_sha256` is non-empty. `save()` always populates it,
    so snapshots Parsimony writes are always verified on load. A hand-written manifest that
    leaves the digest empty skips the check.

## Which indexes are serializable

`save()` serializes only the four built-in index types — `BM25Index`, `VectorIndex`,
`HybridIndex`, and `DisMaxIndex`. Any other object satisfying the `CatalogIndex` protocol is
treated as runtime-only:

```text
TypeError: Catalog index for field 'custom' is runtime-only and cannot be serialized
```

On load, indexes are reconstructed by the `kind` string recorded in `meta.index_fields`; an
unrecognized kind raises `ValueError`. See [Indexes](indexes.md) for the index types and their
`kind` values.

## URL schemes

Both `save()` and `load()` dispatch on the URL scheme. Two schemes are supported; any other
raises `ValueError("Unsupported catalog URL scheme '<scheme>'. Supported: ['file', 'hf']")`.

| Scheme | Form | Behavior |
| --- | --- | --- |
| `file://` (or a bare path) | `file:///abs/path/snapshot`, `data/cat` | Reads/writes a local directory. A bare path with no `://` is treated as `file`. |
| `hf://` | `hf://<org>/<repo>[/<sub>...]` | A Hugging Face **dataset** repo. Lazily imports `huggingface_hub`; not needed for `file://`. |

!!! warning "`s3://` is not supported by snapshots"
    Only `file` and `hf` are recognized by `save()` / `load()`. Passing `s3://...` raises the
    unsupported-scheme `ValueError` regardless of any installed extra.

### Parsing URLs

The pure parser lives in `parsimony.catalog.urls`. It splits a URL into a `ParsedCatalogURL`
(`scheme`, `root`, `sub`):

```python
from parsimony.catalog.urls import parse_catalog_url

p = parse_catalog_url("hf://org/repo/nested/bundle")
print(p.scheme, p.root, p.sub)   # -> hf org/repo nested/bundle

q = parse_catalog_url("file:///tmp/repo/bundle_a")
print(q.scheme, q.root, q.sub)   # -> file /tmp/repo/bundle_a (sub is empty)

r = parse_catalog_url("data/cat")
print(r.scheme, r.root, r.sub)   # -> file data/cat (bare path -> file)
```

The semantics differ between schemes:

- **`file://`** has no sub-path semantics. The entire path goes into `root` and `sub` stays
  empty; the snapshot directory is addressed directly. A bare absolute path is absolutized; a
  bare relative path is left as the raw string. Multi-bundle local layouts are composed by the
  caller.
- **`hf://`** decomposes into `root = "<org>/<repo>"` and `sub` = the remaining (possibly
  nested) path. A missing org or repo segment raises `ValueError`.

For `hf://` loads, `huggingface_hub.snapshot_download` pulls the dataset into the
[catalogs cache directory](../caching.md) with `repo_type="dataset"`; a `sub` restricts the
download to that bundle. Saving to `hf://` stages the snapshot locally, then creates the repo
(`exist_ok=True`) and uploads the folder.

## Lazy load-or-build

When a catalog might be remote, missing, or cheap to rebuild, `load_or_build_catalog` resolves
it through a three-step fallback. Import it from `parsimony.catalog.search`.

```python
from pathlib import Path

from parsimony.catalog import BM25Index, Catalog, Entity
from parsimony.catalog.search import load_or_build_catalog


def build() -> Catalog:
    c = Catalog("demo", indexes={"title": BM25Index()}, default_field="title")
    c.set_entities([Entity(namespace="demo", code="a", title="alpha widget")])
    c.build()
    return c


tmp = Path("/tmp/parsimony")
cache = tmp / "lazy-cache"
# url is absent -> build() runs and the result is saved to the lazy cache
cat = load_or_build_catalog(f"file://{tmp}/missing", cache_path=cache, build=build)
# second call hits the lazy cache; build() does not run again
again = load_or_build_catalog(f"file://{tmp}/missing", cache_path=cache, build=build)
print(len(again), (cache / "meta.json").is_file())  # -> 1 True
```

The resolution order is:

1. **Remote URL** — try `Catalog.load(url)`. If it succeeds, return it.
2. **Local lazy cache** — if the load failed because the catalog is *missing*, and
   `<cache_path>/meta.json` exists, load `file://<cache_path>`. If that cached snapshot is
   unreadable, log a warning and continue to step 3.
3. **Build callback** — if `build` is supplied, call it, save the result to the lazy cache with
   `builder="lazy"`, and return it. If `build` is `None`, raise `CatalogNotFoundError`.

Only *missing* errors trigger the fallback: `CatalogNotFoundError`, `FileNotFoundError`,
Hugging Face's `RepositoryNotFoundError`, or a `ConnectorError` whose message says "not found",
"not present", or "does not exist". Any other load failure propagates unchanged.

!!! note "`CatalogNotFoundError` is not a top-level name"
    Import it from `parsimony.errors`. It subclasses `ConnectorError`, so it carries the same
    agent-facing message convention as the rest of the [typed errors](../connectors/errors.md)
    — its default message ends with a "DO NOT retry." directive.

### Caching hydrated catalogs

`CatalogLRU` keeps already-loaded `Catalog` instances in memory, keyed by URL, so repeat
lookups for the same snapshot reuse one object instead of re-reading from disk. It is bounded
(default size 4, must be `>= 1`) and `threading.Lock`-guarded.

```python
from parsimony.catalog.search import CatalogLRU

lru = CatalogLRU(size=2)
a = lru.get_or_load("file:///tmp/parsimony/snapshot")
b = lru.get_or_load("file:///tmp/parsimony/snapshot")
assert a is b  # same in-memory instance
lru.clear()
```

When both `cache_path` and `build` are passed, `get_or_load` delegates to
`load_or_build_catalog`; otherwise it calls `Catalog.load(url)` directly, mapping a missing
catalog to `CatalogNotFoundError`. Entries beyond the configured size are evicted
oldest-first.

## See also

- [The Catalog](index.md) — the lifecycle (build → search → save) these snapshots persist.
- [Building and searching](search.md) — the dirty/build gate that `save()` shares.
- [Indexes](indexes.md) — which index kinds are serializable and how `kind` drives reload.
- [Caching](../caching.md) — the cache root that backs `hf://` downloads and lazy caches.
- [Errors](../connectors/errors.md) — `CatalogNotFoundError` and the agent-facing taxonomy.
