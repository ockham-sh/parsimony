# Command-line interface

`parsimony` installs a single console script, `parsimony`, for inspecting your
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

`parsimony list` has two modes, matching the two questions `parsimony.discover` and
`parsimony.registry` each answer (see [installed versus installable](plugins/discovery.md#installed-versus-installable)):
by default it walks the `parsimony.providers` entry-point group — the same discovery mechanism
the Python API uses — and reports each **installed** [provider plugin](plugins/index.md).
`--available` instead reports what you could `pip install`, from the official registry.

```text
usage: parsimony list [-h] [--json] [--strict | --available]

Inspects the 'parsimony.providers' entry-point group. Shows each plugin's
name, version, connector count, and conformance status. With --strict, imports
each plugin to run the conformance suite and to list the env vars its
connectors declare via requires=; exits non-zero on any failure. With
--available, lists installable parsimony-<name> packages from the parsimony-
connectors manifest instead of what's installed.

options:
  -h, --help    show this help message and exit
  --json        Emit JSON instead of a table.
  --strict      Run conformance checks; exit non-zero on any failure.
  --available   List installable parsimony-<name> packages from the
                connectors.json manifest (fetched over HTTPS; falls back to
                a bundled offline snapshot on any failure).
```

`--strict` and `--available` are mutually exclusive — they answer different questions (what's
installed and conformant here, vs. what could be installed) and combining them is a `2`-exit
argparse error.

Without `--strict`, `list` is **metadata-only**: it reads each provider's entry-point
metadata and never imports the plugin module. Every row therefore reports its connector count
as `?` and its conformance status as `skipped`. This keeps a bare `parsimony list` fast and
side-effect-free even when plugins have heavy imports.

A typical table with one plugin installed looks like this:

```text
NAME    VERSION  CONNECTORS  CONFORMANCE  REQUIRES
------  -------  ----------  -----------  --------
fred    0.1.0    ?           skipped      ?

1 plugin(s) discovered.
```

The columns are `NAME`, `VERSION`, `CONNECTORS`, `CONFORMANCE`, and `REQUIRES`. A missing
version renders as `?`, and a zero (or not-yet-counted) connector count renders as `?`. The
`REQUIRES` column lists the env vars the plugin's connectors declare via `requires=` — names
like `FRED_API_KEY`, never values. It is only inspected under `--strict`: `?` marks "not
inspected", `-` marks "inspected, declares none". A `N plugin(s) discovered.` footer follows
the table.

When no plugins are installed — core ships zero connectors — `list` prints a hint instead of
an empty table:

```text
No parsimony plugins discovered (0 plugins).
Run `parsimony list --available` to see installable packages, e.g. `pip install parsimony-fred`.
```

### `--json`

`--json` emits a single dict with one `plugins` key instead of the table. Each entry carries
`name`, `module`, `distribution`, `version`, `connector_count`, `conformance`,
`conformance_detail`, and `requires` (the sorted env-var names the plugin's connectors
declare via `requires=`; empty unless `--strict` inspected the plugin). With no plugins
installed:

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
      "conformance_detail": null,
      "requires": ["FOO_API_KEY"]
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

### `--available`

`--available` switches `list` from "what's installed" to "what could I install": it queries the
official registry of `parsimony-<name>` packages — a thin CLI wrapper over
[`parsimony.registry.list_available()`](reference/api.md) — instead of the
`parsimony.providers` entry-point group. Use it when you need a source that isn't installed
here yet.

```text
$ parsimony list --available
PACKAGE           PROVIDER                              ENTRY POINT  CONNECTORS  KEYLESS
----------------  ------------------------------------  -----------  ----------  -------
parsimony-fred     FRED (Federal Reserve Economic Data)  fred         3           no
parsimony-sdmx     SDMX protocol (ECB, Eurostat, ...)     sdmx         4           yes

2 installable package(s) (remote registry, generated 2026-07-22).
Install a match with `pip install <package>`.
```

The footer names the load `source`: `remote` when the live
`https://parsimony.dev/connectors.json` endpoint answered, or `bundled` when it didn't and the
CLI fell back to the read-only snapshot shipped in this release. A bundled fallback also logs a
warning to stderr naming the canonical URL, since a bundled snapshot may predate newly published
connectors — pass `-v`/`--verbose` to see it, or watch stderr directly. `--json` includes the
same `source` field plus `generated_at` and a `connectors` array (`package`, `provider`,
`entry_point`, `connector_count`, `keyless`):

```text
$ parsimony list --available --json
{
  "source": "remote",
  "generated_at": "2026-07-22",
  "connectors": [
    {
      "package": "parsimony-fred",
      "provider": "FRED (Federal Reserve Economic Data)",
      "entry_point": "fred",
      "connector_count": 3,
      "keyless": false
    }
  ]
}
```

If neither the live endpoint nor the bundled snapshot can be loaded, `list --available` prints
`error: ...` to stderr and exits `1` — see
[Discovering installed providers § installed versus installable](plugins/discovery.md#installed-versus-installable)
for how a caller (or the [agent skill](https://github.com/ockham-sh/parsimony/tree/main/skills/parsimony))
is expected to handle that.

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

Pass `--repos` to break the `catalogs` subdir down by Hugging Face repo (largest first) —
useful for finding which provider's cached catalog snapshots dominate disk before clearing one:

```text
$ parsimony cache info --repos
...
CATALOG REPO         FILES  SIZE
--------------------  -----  ------
parsimony-dev/sdmx    97     4.2 MB
acme/fred             12     380 KB

2 repo(s) cached.
```

`--repos` adds a `catalog_repos` key to the `--json` payload (a list of `{repo_id, dirname,
path, size_bytes, files}`, largest first); it does not change what `cache info` alone reports.

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

Remove a single subdirectory with `--subdir NAME`, every subdirectory when `--subdir` is
omitted, or just one Hugging Face catalog repo with `--repo ORG/NAME`.

```text
usage: parsimony cache clear [-h] [--subdir NAME | --repo ORG/NAME] [--yes]

options:
  -h, --help       show this help message and exit
  --subdir NAME    Clear only this subdir (catalogs, models, connectors, staging).
  --repo ORG/NAME  Clear only the cached catalogs for this Hugging Face repo
                    (e.g. parsimony-dev/sdmx).
  --yes            Skip the confirmation prompt.
```

`--subdir` and `--repo` are mutually exclusive — combining them is a `2`-exit argparse error.
Prefer `--repo` when you only need to evict one provider's catalog snapshots: it targets exactly
the cached repo (found with `cache info --repos`) and leaves every other provider's cache warm,
where `--subdir catalogs` wipes all of them.

By default `clear` is interactive — it computes the file count and total size of the targets,
then prompts before deleting. Only `y` or `yes` (case-insensitive, surrounding whitespace
stripped) proceeds; anything else prints `Aborted.` and exits `0` without deleting:

```text
$ parsimony cache clear
Remove all subdirs (1 file(s), 2.0 KB)? [y/N] n
Aborted.
```

If the targets are already empty (or, for `--repo`, nothing is cached for that repo), `clear`
short-circuits without prompting:

```text
$ parsimony cache clear --subdir catalogs --yes
Nothing to clear (subdir 'catalogs' are empty).

$ parsimony cache clear --repo never/cached --yes
Nothing to clear (no cached catalogs for repo 'never/cached').
```

Pass `--yes` to skip the prompt for unattended use — **required** whenever `clear` runs
somewhere with no attached terminal (a script, a CI step, an agent loop):

```text
$ parsimony cache clear --subdir models --yes
Cleared subdir 'models' (1 file(s), 2.0 KB).

$ parsimony cache clear --repo parsimony-dev/sdmx --yes
Cleared catalogs for repo 'parsimony-dev/sdmx' (97 file(s), 4.2 MB).
```

An unknown subdir name is rejected: the command prints an error to stderr (with the valid
names listed) and exits `2`. The known names are sorted alphabetically in the message.

```text
$ parsimony cache clear --subdir bogus
error: unknown cache subdir 'bogus'; expected one of ['catalogs', 'connectors', 'models', 'staging']
```

!!! warning "Non-interactive stdin without `--yes` fails fast, it does not prompt"
    `clear` checks whether stdin is a terminal *before* it would call `input()`. When it is not
    — a non-interactive shell, a CI step, a redirected `/dev/null`, an agent's backgrounded
    invocation — and `--yes` was not passed, `clear` never prompts: it prints
    `error: refusing to prompt for confirmation on non-interactive stdin; pass --yes to clear
    <label> unattended.` to stderr and exits `2`, leaving the cache untouched. This is
    deliberately loud rather than silent: a prompt with no attached terminal cannot ever be
    answered, so blocking on `input()` there is indistinguishable from a hang. Pass `--yes`
    whenever you intend an unattended clear.

## Exit codes

| Code | When |
|------|------|
| `0`  | Any successful command, `--help`, a metadata-only or clean `--strict` list, a successful `--available` list (remote or bundled), an aborted or empty `cache clear`. |
| `1`  | `parsimony list --strict` when at least one plugin's conformance is `fail`; `parsimony list --available` when neither the live registry nor the bundled snapshot could be loaded. |
| `2`  | Argparse errors — missing or invalid subcommand, missing cache action, `--strict`/`--available` passed together, `--subdir`/`--repo` passed together — `cache clear --subdir` with an unknown subdir name — and `cache clear` on non-interactive stdin without `--yes`. |

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
- [Discovering installed providers](plugins/discovery.md) — the `parsimony.discover` API that `parsimony list` is built on, and how it differs from `parsimony.registry`.
- [Conformance testing](plugins/conformance.md) — the checks `parsimony list --strict` runs.
- [Public API & import map](reference/api.md) — `parsimony.registry.list_available()`, the typed API behind `parsimony list --available`.
