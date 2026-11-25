# Recording and replay

Every connector call saves its `Result` in a folder for the current Python process. Setting
`PARSIMONY_REPLAY` to one of those folders makes the same script run again on the saved results
instead of the live sources. Your code doesn't change: recording happens inside the connector call.

## What is saved, and where

A run is one Python process: one execution of a script, or a notebook kernel until it restarts.
Its folder is created at the first connector call, under
`platformdirs.user_data_dir("parsimony") / "runs"` (`~/.local/share/parsimony/runs` on Linux).
That is outside the [cache root](caching.md), so `parsimony cache clear` leaves runs alone.

```text
~/.local/share/parsimony/runs/20261116T135412Z-0ea389/
  fred_fetch-cee49eabd27e.parquet
  fred_fetch-f12b6e8a6a95.parquet
  coingecko_coin_detail-1a2b3c4d5e6f.json
  run.json
```

- The folder name is the UTC start time and six random hex characters, so runs sort by time and
  two processes never share a folder.
- Each file is `<connector>-<key>`, where the key is a hash of the connector name and the call
  parameters (declared `secrets=` left out). The same call gets the same file name in every run.
  Calling it again in the same process overwrites the file with the latest result.
- A table is written with `Result.to_parquet`, which embeds the provenance (source, parameters,
  fetch time). Any other result is written as JSON: `{"provenance": ..., "raw": ...}`.
- `run.json` names the script or notebook that made the calls and the SHA-256 of its bytes:
  `{"entry": "/home/me/project/analysis.py", "sha256": "..."}`, or `{"entry": null}` when there
  is none (`python -c`, an interactive shell).
- A connector that calls other connectors saves one file per call, the inner ones included. A
  failed call saves nothing.

Recording never breaks a fetch: if saving fails, you get one line on stderr,
`parsimony: call not recorded (...)`, and the result as usual.

## Settings

| Variable | Effect |
| --- | --- |
| unset | Runs go to `user_data_dir("parsimony")/runs` |
| `PARSIMONY_RECORD=off` | Nothing is saved |
| `PARSIMONY_RECORD=<folder>` | Runs go to that folder, which is created if missing |
| `PARSIMONY_REPLAY=<run folder>` | Connector calls return the saved results of that run and never contact the source |

Both are read on every call. Set `PARSIMONY_RECORD=off` for test suites, CI, catalog rebuilds and
long-running services.

## Using runs

**What is a chart made of?** Find the runs of the script, then read the saved calls:

```bash
grep -l analysis.py ~/.local/share/parsimony/runs/*/run.json
```

```python
from parsimony import Result

r = Result.from_parquet("~/.local/share/parsimony/runs/20261116T135412Z-0ea389/fred_fetch-cee49eabd27e.parquet")
r.provenance  # source, params, fetched_at
```

**What changed since last month?** The same call has the same file name in every run, so load it
from two run folders and compare in pandas:

```python
old = Result.from_parquet("runs/20261016T091200Z-3f1a2b/fred_fetch-cee49eabd27e.parquet").frame
new = Result.from_parquet("runs/20261116T135412Z-0ea389/fred_fetch-cee49eabd27e.parquet").frame
old.compare(new)
```

**Rebuild a figure** from the data it was first made with:

```bash
PARSIMONY_REPLAY=~/.local/share/parsimony/runs/20261116T135412Z-0ea389 python analysis.py
```

The first replayed call prints `parsimony: replaying <run folder>` to stderr. Results carry their
original fetch time. Replay saves nothing and creates no run folder. A call the run doesn't hold
raises `LookupError`; unset `PARSIMONY_REPLAY` to fetch live. Recorded and live data are never
mixed.

**What used a wrong source?** List the runs that called a connector:

```bash
ls ~/.local/share/parsimony/runs/*/fred_fetch-*
```

## Keeping and deleting runs

Parsimony never deletes a run folder. Delete folders by hand when you no longer need them, for
example:

```bash
rm -r ~/.local/share/parsimony/runs/202610*
```

To keep a run with a project, copy its folder into the project.

Run folders hold a copy of everything fetched, licensed and internal data included. Give them the
same permissions as the data itself.

## Limits

- Results JSON can't hold (bytes, a Series, custom objects) are not saved.
- Matching is exact: a script that computes dates from today makes different calls on a later day
  and misses on replay. Write dates explicitly.
- A repeated call keeps only its latest result.
- Calls made in worker processes may land in other run folders.
