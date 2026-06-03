# Command-line interface

`parsimony-core` installs a single console script, `parsimony`, for inspecting your
environment from the shell: which provider plugins are installed and conformant, and what the
on-disk cache holds. It is a thin operator front-end — there is no connector-calling or
catalog-building verb. Use the Python API for that.

The script is wired as `parsimony = parsimony.cli:main`, so installing the package puts a
`parsimony` executable on your `PATH`. Every command returns a process exit code; the
`--help` output and successful data commands exit `0`.

```text
usage: parsimony [-h] {list,cache} ...

Parsimony CLI — connector framework for financial data.

positional arguments:
  {list,cache}
    list        List discovered plugins and their connectors.
    cache       Inspect or clear the parsimony cache.

options:
  -h, --help    show this help message and exit
```

The top-level subcommand is required. Running `parsimony` with no verb makes argparse print
usage to stderr and exit `2` before any command runs.

## `parsimony list`

`parsimony list` walks the `parsimony.providers` entry-point group — the same discovery
mechanism the Python API uses (see [discovering installed providers](plugins/discovery.md)) —
and reports each installed [provider plugin](plugins/index.md).

```text
usage: parsimony list [-h] [--json] [--strict]

Inspects the 'parsimony.providers' entry-point group. Shows each plugin's
connectors and env-var status. With --strict, runs the conformance suite
against each plugin and exits non-zero on any failure.

options:
  -h, --help  show this help message and exit
  --json      Emit JSON instead of a table.
  --strict    Run conformance checks; exit non-zero on any failure.
```

Without `--strict`, `list` is **metadata-only**: it reads each provider's entry-point
metadata and never imports the plugin module. Every row therefore reports its connector count
as `?` and its conformance status as `skipped`. This keeps a bare `parsimony list` fast and
side-effect-free even when plugins have heavy imports.

A typical table with one plugin installed looks like this:

```text
NAME    VERSION  CONNECTORS  CONFORMANCE
------  -------  ----------  -----------
fred    0.1.0    ?           skipped

1 plugin(s) discovered.
```

The columns are `NAME`, `VERSION`, `CONNECTORS`, and `CONFORMANCE`. A missing version renders
as `?`, and a zero (or not-yet-counted) connector count renders as `?`. A `N plugin(s)
discovered.` footer follows the table.

When no plugins are installed — core ships zero connectors — `list` prints a hint instead of
an empty table:

```text
No parsimony plugins discovered (0 plugins).
Install one to get started, e.g. `pip install parsimony-fred`.
```

### `--json`

`--json` emits a single dict with one `plugins` key instead of the table. Each entry carries
`name`, `module`, `distribution`, `version`, `connector_count`, `conformance`, and
`conformance_detail`. With no plugins installed:

```text
{
  "plugins": []
}
```

### `--strict` and conformance

`--strict` folds in the [conformance suite](plugins/conformance.md). For each provider it
imports the module, calls the provider's `load()` to count connectors, and runs
`assert_plugin_valid(module)`. A passing plugin gets `conformance: pass` and the real
connector count; a `ConformanceError` flips the row to `conformance: fail` with the error
message recorded in `conformance_detail`. Any other exception during import or load (for
example an `ImportError`, or a missing `CONNECTORS` export raising `TypeError`) is also caught
and recorded as a failure rather than crashing the command. Failed rows are echoed after the
table as `! <name>: <detail>` lines.

```text
$ parsimony list --json --strict
{
  "plugins": [
    {
      "name": "foo",
      "module": "pkg_foo_cli",
      "distribution": "parsimony-foo",
      "version": "0.1.0",
      "connector_count": 1,
      "conformance": "pass",
      "conformance_detail": null
    }
  ]
}
```

`list` exits `1` only when `--strict` is set **and** at least one plugin's conformance is
`fail`. In every other case — including a clean `--strict` run, a metadata-only run, or no
plugins at all — it exits `0`.

!!! tip "Use `--strict` in CI"
    `parsimony list --strict` is a single-command smoke test for a deployment: it confirms
    every installed provider imports cleanly, exports a non-empty `Connectors`, and passes the
    same five conformance checks plugin authors run locally. Wire it into a release pipeline
    and let the non-zero exit fail the build.

## `parsimony cache`

`parsimony cache` inspects and clears the global on-disk cache that [embedders](catalog/embedders.md),
[catalog snapshots](catalog/snapshots.md), and connector scratch share. It delegates to the
[caching](caching.md) module — the CLI is just an operator wrapper over `parsimony.cache`.

```text
usage: parsimony cache [-h] {path,info,clear} ...

Manage the global parsimony cache. The root resolves through
PARSIMONY_CACHE_DIR (defaulting to platformdirs.user_cache_dir('parsimony'))
and contains three named subdirectories: catalogs, models, connectors.

positional arguments:
  {path,info,clear}
    path             Print the resolved cache root.
    info             Show occupancy of each cache subdirectory.
    clear            Remove a cache subdirectory (or all of them).

options:
  -h, --help         show this help message and exit
```

The cache action is required; `parsimony cache` with no action exits `2`.

!!! note "There are four subdirectories, not three"
    The help text above names three subdirectories (`catalogs`, `models`, `connectors`), but
    the cache actually has a fourth, `staging`. `cache info` lists all four and
    `cache clear --subdir staging` is a valid target. See [caching](caching.md) for what each
    one holds.

The cache root resolves through the `PARSIMONY_CACHE_DIR` environment variable (with `~`
expansion); when it is unset, the root falls back to `platformdirs.user_cache_dir("parsimony")`
— `~/.cache/parsimony` on Linux, `~/Library/Caches/parsimony` on macOS, and
`%LOCALAPPDATA%\parsimony\Cache` on Windows.

### `parsimony cache path`

Print the resolved cache root, one line, no decoration:

```text
$ parsimony cache path
/home/user/.cache/parsimony
```

!!! warning "`cache path` creates the root directory"
    Unlike `cache info`, `cache path` ensures the root exists, creating it (with `0o700`
    permissions on POSIX) as a side effect. On POSIX it also raises a `RuntimeError` — surfaced
    as an unhandled traceback, exit `1` — if the resolved cache directory is group- or
    world-writable without the sticky bit, a cache-poisoning guard. Point `PARSIMONY_CACHE_DIR`
    at a user-private directory to avoid this on shared hosts.

### `parsimony cache info`

Show how much each subdirectory holds. The command is strictly read-only: it never creates
the root or any subdirectory, so a subdir that does not yet exist on disk renders its `FILES`
and `SIZE` as `-`.

```text
$ parsimony cache info
SUBDIR      FILES  SIZE    PATH
----------  -----  ------  ------------------------------------------
catalogs    -      -       /home/user/.cache/parsimony/catalogs
models      1      2.0 KB  /home/user/.cache/parsimony/models
connectors  1      100 B   /home/user/.cache/parsimony/connectors
staging     -      -       /home/user/.cache/parsimony/staging

root: /home/user/.cache/parsimony
```

Pass `--json` for the raw dict — useful for scripting. Each subdir entry carries `path`,
`size_bytes` (raw bytes), `files`, and `exists`:

```text
$ parsimony cache info --json
{
  "root": "/home/user/.cache/parsimony",
  "subdirs": {
    "catalogs": {
      "path": "/home/user/.cache/parsimony/catalogs",
      "size_bytes": 0,
      "files": 0,
      "exists": false
    },
    "models": {
      "path": "/home/user/.cache/parsimony/models",
      "size_bytes": 2048,
      "files": 1,
      "exists": true
    },
    "connectors": {
      "path": "/home/user/.cache/parsimony/connectors",
      "size_bytes": 100,
      "files": 1,
      "exists": true
    },
    "staging": {
      "path": "/home/user/.cache/parsimony/staging",
      "size_bytes": 0,
      "files": 0,
      "exists": false
    }
  }
}
```

!!! note "Table sizes are binary-rounded; JSON is raw bytes"
    The `SIZE` column rounds to one decimal in `KB`/`MB`/`GB`/`TB` steps of 1024 (so 2048
    bytes shows as `2.0 KB`), while `--json` reports the exact byte count in `size_bytes`.
    Script against the JSON when you need precision.

### `parsimony cache clear`

Remove a single subdirectory with `--subdir NAME`, or every subdirectory when `--subdir` is
omitted.

```text
usage: parsimony cache clear [-h] [--subdir NAME] [--yes]

options:
  -h, --help     show this help message and exit
  --subdir NAME  Clear only this subdir (catalogs, models, connectors).
  --yes          Skip the confirmation prompt.
```

By default `clear` is interactive — it computes the file count and total size of the targets,
then prompts before deleting. Only `y` or `yes` (case-insensitive, surrounding whitespace
stripped) proceeds; anything else prints `Aborted.` and exits `0` without deleting:

```text
$ parsimony cache clear
Remove all subdirs (1 file(s), 2.0 KB)? [y/N] n
Aborted.
```

If the targets are already empty, `clear` short-circuits without prompting:

```text
$ parsimony cache clear --subdir catalogs --yes
Nothing to clear (subdir 'catalogs' are empty).
```

Pass `--yes` to skip the prompt for unattended use:

```text
$ parsimony cache clear --subdir models --yes
Cleared subdir 'models' (1 file(s), 2.0 KB).
```

An unknown subdir name is rejected: the command prints an error to stderr (with the valid
names listed) and exits `2`. The known names are sorted alphabetically in the message.

```text
$ parsimony cache clear --subdir bogus
error: unknown cache subdir 'bogus'; expected one of ['catalogs', 'connectors', 'models', 'staging']
```

!!! tip "Closed stdin is treated as a decline"
    When stdin is not a terminal — a non-interactive shell, a CI step, a redirected
    `/dev/null` — the confirmation prompt receives an `EOFError`, which `clear` treats as
    "no". The cache is preserved and the command exits `0`. This means `parsimony cache clear`
    without `--yes` can never destroy your cache by accident in an automated context; pass
    `--yes` deliberately when you do want an unattended wipe.

## Exit codes

| Code | When |
|------|------|
| `0`  | Any successful command, `--help`, a metadata-only or clean `--strict` list, an aborted or empty `cache clear`. |
| `1`  | `parsimony list --strict` when at least one plugin's conformance is `fail`. |
| `2`  | Argparse errors — missing or invalid subcommand, missing cache action — and `cache clear --subdir` with an unknown subdir name. |

```text
$ parsimony
usage: parsimony [-h] {list,cache} ...
parsimony: error: the following arguments are required: command
# exit 2

$ parsimony bogus
usage: parsimony [-h] {list,cache} ...
parsimony: error: argument command: invalid choice: 'bogus' (choose from 'list', 'cache')
# exit 2
```

## See also

- [Caching](caching.md) — the cache root, the four subdirectories, and `TTLDiskCache` behind the `cache` verb.
- [Plugins and providers](plugins/index.md) — how `parsimony.providers` entry points and the `CONNECTORS` export work.
- [Discovering installed providers](plugins/discovery.md) — the `parsimony.discover` API that `parsimony list` is built on.
- [Conformance testing](plugins/conformance.md) — the checks `parsimony list --strict` runs.
