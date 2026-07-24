# Caching

Parsimony keeps an on-disk cache for the artefacts it downloads and computes — catalog
snapshots, embedder model files, and per-connector scratch — under a single root that you can
relocate or inspect. The `parsimony.cache` module gives you the path helpers that locate (and
create) those directories, read-only introspection, a wipe utility, and `TTLDiskCache`, a
small JSON key/value store with a caller-specified time-to-live. The module is re-exported at
the top level, so `from parsimony import cache` works.

```python
from parsimony import cache

print(cache.root())   # the resolved cache root, created with safe permissions
```

The same surface is wrapped by the [`parsimony cache` CLI subcommands](cli.md) for operators
who want to inspect or clear the cache without writing Python.

## The cache root

The root is resolved from one source of truth:

| Source | Behavior |
| --- | --- |
| `PARSIMONY_CACHE_DIR` (if set) | Used verbatim, with `~` expansion via `Path.expanduser()`. |
| Otherwise | `platformdirs.user_cache_dir("parsimony")` — platform-correct by default. |

The platform defaults are:

| Platform | Default root |
| --- | --- |
| Linux | `~/.cache/parsimony` |
| macOS | `~/Library/Caches/parsimony` |
| Windows | `%LOCALAPPDATA%\parsimony\Cache` |

`cache.root()` returns the resolved path **and ensures it exists** with safe permissions
(`0o700` on POSIX — see [Security hardening](#security-hardening)). Calling it is therefore a
write: it creates `~/.cache/parsimony` (or your override) if it is missing.

```python
import os
os.environ["PARSIMONY_CACHE_DIR"] = "~/my-cache"   # ~ is expanded

from parsimony import cache
print(cache.root())   # /home/<user>/my-cache  (created on first call)
```

!!! warning "`PARSIMONY_CACHE_DIR` is taken literally"
    Only `~` is expanded. A relative path is **not** resolved to an absolute one, and the path
    is not checked for writability until a helper actually creates a directory. See the
    [environment variable reference](reference/environment.md) for the full list of tunables.

## The four subdirectories

The root holds four named subdirectories, each carrying a distinct class of artefact. Each has
a dedicated helper that returns the path and creates it on the way out:

| Helper | Path | Purpose |
| --- | --- | --- |
| `catalogs_dir()` | `$ROOT/catalogs` | [Catalog snapshots](catalog/snapshots.md) downloaded from Hugging Face for search. |
| `models_dir(slug=None)` | `$ROOT/models` or `$ROOT/models/<slug>` | [Embedder](catalog/embedders.md) model artefacts (ONNX, tokenizer files). |
| `connectors_dir(provider=None)` | `$ROOT/connectors` or `$ROOT/connectors/<provider>` | Connector-owned scratch, opt-in per provider. |
| `staging_dir(provider)` | `$ROOT/staging/<provider>` | Per-provider staging area used by catalog publish drivers. |

```python
from parsimony import cache

root = cache.root()                          # $ROOT          (created, chmod 0o700 on POSIX)
scratch = cache.connectors_dir("sdmx")       # $ROOT/connectors/sdmx
(scratch / "listing.json").write_text("[]")

models_root = cache.models_dir()                                          # $ROOT/models
slug_dir = cache.models_dir("sentence-transformers__all-MiniLM-L6-v2")    # $ROOT/models/<slug>
```

!!! note "These helpers create directories"
    Every one of `catalogs_dir`, `models_dir`, `connectors_dir`, and `staging_dir` creates the
    directory it returns (with the same `0o700` safe-mkdir treatment as `root()`). They are not
    pure path builders. The only read-only entry point is [`info()`](#inspecting-the-cache).

`models_dir` and `connectors_dir` take an **optional** sub-key (`slug` / `provider`); calling
them with no argument returns the parent directory, which is useful for callers that own an
internal layout underneath. `staging_dir` is the exception: it **requires** a `provider`
argument and has no parent form.

### Sub-key validation

Any caller-supplied path component (`slug` or `provider`) is validated to defend against
path traversal. A valid component matches `^[A-Za-z0-9_][A-Za-z0-9_\-.]*$`:

- ASCII letters, digits, `_`, `-`, and `.` are allowed.
- A **leading** dot is forbidden, which blocks `..` and `.hidden`. Trailing and embedded dots
  are fine — `v1.2.3` and `model.v2` are accepted, but `.git` is rejected.
- Slashes, backslashes, spaces, and shell metacharacters (`;`, `$`, newlines, …) are rejected.
- The empty string is rejected.

A violation raises `ValueError` at the API boundary, so bad input fails fast:

```python
from parsimony import cache

cache.connectors_dir("../etc")   # ValueError: invalid cache subkey '../etc'
cache.staging_dir(".hidden")     # ValueError: invalid cache subkey '.hidden'
```

## Inspecting the cache

`info()` returns cache occupancy as a JSON-shaped dict. It is strictly **read-only**: it
resolves the root without creating it, never creates any subdirectory, and walks each subdir
once (tolerating per-file `OSError` so the walk continues). A subdir that does not exist
reports `exists=False, size_bytes=0, files=0`.

```python
from parsimony import cache

report = cache.info()
print(report["root"])                                  # str path to the root
print(report["subdirs"]["models"]["size_bytes"])       # int bytes
print(report["subdirs"]["models"]["files"])            # int file count
print(report["subdirs"]["models"]["exists"])           # bool
```

The shape is:

```text
{
  "root": "/home/<user>/.cache/parsimony",
  "subdirs": {
    "catalogs":   {"path": "...", "size_bytes": 0, "files": 0, "exists": False},
    "models":     {"path": "...", "size_bytes": 0, "files": 0, "exists": True},
    "connectors": {"path": "...", "size_bytes": 0, "files": 0, "exists": False},
    "staging":    {"path": "...", "size_bytes": 0, "files": 0, "exists": False}
  }
}
```

!!! note "`staging` is always reported"
    `info()` always lists all four subdirectories, including `staging`. The module docstring
    and some CLI help text describe only catalogs, models, and connectors, but `staging` is a
    real, clearable subdir.

### Per-repo catalog breakdown

`catalog_repos()` breaks `$ROOT/catalogs` down by Hugging Face repo instead of reporting one
aggregate size — read-only, largest first, and it never creates the `catalogs` dir:

```python
from parsimony import cache

for entry in cache.catalog_repos():
    print(entry["repo_id"], entry["files"], entry["size_bytes"])
# parsimony-dev/sdmx 97 4404020
# acme/fred          12 389120
```

Use this before clearing anything to see *which* provider's cached catalog snapshots dominate
disk, then evict just that one with `clear_catalog_repo()` (below) instead of the whole
`catalogs` subdir.

## Clearing the cache

`clear(subdir=None)` removes a single named subdirectory, or all of them when called with no
argument. It is idempotent — a missing target is a safe no-op — and it does **not** recreate
the emptied directory (the next helper call will). An unknown subdir name raises `ValueError`.

```python
from parsimony import cache

cache.clear("models")   # wipe just $ROOT/models
cache.clear()           # wipe catalogs, models, connectors, and staging
cache.clear("bogus")    # ValueError: unknown cache subdir 'bogus'; expected one of (...)
```

`clear_catalog_repo(repo_id)` is the targeted counterpart: it removes only the cached snapshots
for one Hugging Face repo under `catalogs`, leaving every other provider's cache warm. Returns
`True` when something was cached and removed, `False` when there was nothing to clear:

```python
from parsimony import cache

cache.clear_catalog_repo("parsimony-dev/sdmx")   # wipe just that provider's catalogs
```

!!! tip "Prefer a targeted clear, and use the CLI for interactive/scripted clears"
    Reach for `clear_catalog_repo()` / `parsimony cache clear --repo ORG/NAME` before the
    broader `clear("catalogs")` / `--subdir catalogs` — it drops exactly the provider you meant
    to refresh instead of every provider's cached catalogs. `parsimony cache clear` prompts
    before deleting and shows how many files and bytes will go; pass `--yes` to skip the prompt
    for scripted or agentic use — it is **required** whenever stdin is not a terminal, since a
    prompt there can never be answered (`clear` fails fast with exit `2` rather than blocking
    on it). See the [CLI page](cli.md#parsimony-cache-clear) for the exact flags and output.

## Security hardening

The cache is a classic poisoning target — if another user can write into the directory tree,
they can swap in a malicious snapshot or model file. On POSIX, `cache.root()` and the four
directory helpers therefore refuse to use a group- or world-writable directory (or ancestor)
**unless** the sticky bit is set:

- A directory (or existing ancestor) that is group-writable (`S_IWGRP`) or world-writable
  (`S_IWOTH`) raises `RuntimeError`, since either lets another user rename or replace your
  cache subtree.
- The sticky bit (`S_ISVTX`) is the exception: it restricts rename/unlink to the file owner, so
  the canonical `/tmp` case (mode `0o1777`) is allowed.
- A path that exists but is not a directory raises `RuntimeError`.
- After the check, the directory is created with `mkdir(parents=True, exist_ok=True)` and
  `chmod 0o700` (best-effort; an `OSError` from the chmod is suppressed).

!!! warning "Shared hosts and the env override"
    If `PARSIMONY_CACHE_DIR` (or one of its ancestors) points at a group- or world-writable
    non-sticky directory, every helper raises `RuntimeError`. The fix is to pick a user-private
    directory or unset the variable — the error message says exactly that. This commonly bites
    shared CI runners and multi-user hosts.

!!! note "POSIX only"
    On non-POSIX platforms (`os.name != "posix"`), the writable-bits check and the `chmod 0o700`
    are both skipped, because `Path.stat().st_mode` does not reliably reflect Windows ACLs. The
    security guarantee is POSIX-only.

## `TTLDiskCache`

`TTLDiskCache` is a JSON-backed key/value store for small payloads — one file per key under a
root directory you supply. It is the right tool for caching ad-hoc JSON (a provider listing, a
small metadata blob); for large binary or vector payloads, build a purpose-specific cache
instead.

The defining design choice is that **the TTL is supplied by the caller on every `get()`** via
the keyword-only `max_age_s`. The cache stores no expiry metadata — only the file's mtime — so
a single directory can serve callers with different freshness needs.

```python
from pathlib import Path
from parsimony.cache import TTLDiskCache

c = TTLDiskCache(Path("/tmp/parsimony-ttl"))

c.put("datasets-ESTAT", {"agency": "ESTAT", "n": 7661})
fresh = c.get("datasets-ESTAT", max_age_s=3600)   # dict if younger than 1h, else None
stale = c.get("datasets-ESTAT", max_age_s=0)       # None: nothing is younger than 0s
```

### Constructor and methods

| Member | Signature | Behavior |
| --- | --- | --- |
| `TTLDiskCache(root)` | `__init__(self, root: Path) -> None` | Stores the root. Does **not** create it — `put()` creates it lazily. |
| `get` | `get(self, key, *, max_age_s) -> Any \| None` | Returns the parsed value if the file exists and `time.time() - mtime <= max_age_s`; otherwise `None`. |
| `put` | `put(self, key, value) -> None` | Persists `value` (which must be JSON-serializable) atomically. |

`get` is forgiving by design: it returns `None` — never raising — for a missing file, a stale
entry, or corrupt JSON. The corrupt case additionally logs a warning. A poisoned or truncated
cache file therefore degrades to a cache miss rather than an exception, so callers can always
fall through to the live computation.

`put` writes atomically: it writes a `<name>.tmp` sibling and then `os.replace`s it into place,
so a crashed writer never leaves a partial file visible to a concurrent reader, and no `.tmp`
files survive a successful write. The parent directory is created if missing.

!!! warning "Staleness is mtime-based"
    Freshness is measured against the file's modification time. Touching or copying a cache file
    (changing its mtime) effectively resets its age; conversely, backdating the mtime is how you
    force staleness. The cache holds no per-entry TTL, so the same file can look fresh to one
    caller (large `max_age_s`) and stale to another (small `max_age_s`) in the same run.

### Key-to-filename mapping

Filenames are derived so that distinct keys never collide:

- A key matching `^[A-Za-z0-9_\-]+$` maps to a human-readable `{key}.json`.
- Any other key is sanitized (each non-matching character becomes `_`, truncated to 60
  characters) and suffixed with the first 8 hex characters of the key's SHA-256. This keeps keys
  like `"a:b"` and `"a/b"` — both sanitizing to `a_b` — in separate files.

```python
from pathlib import Path
from parsimony.cache import TTLDiskCache

c = TTLDiskCache(Path("/tmp/parsimony-ttl"))
c.put("datasets:ESTAT", [1, 2])   # -> datasets_ESTAT_<8hex>.json
c.put("datasets/ESTAT", [3, 4])   # -> a different file, despite the same sanitized stem
assert c.get("datasets:ESTAT", max_age_s=3600) == [1, 2]
```

## See also

- [Command-line interface](cli.md) — the `parsimony cache path|info|clear` operator front-end.
- [Environment variables](reference/environment.md) — `PARSIMONY_CACHE_DIR` and other tunables.
- [Snapshots and persistence](catalog/snapshots.md) — what lands in `$ROOT/catalogs`.
- [Embedders](catalog/embedders.md) — what lands in `$ROOT/models`.
