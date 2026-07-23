# Bundled data

`connectors.json` is a snapshot of `ockham-sh/parsimony-connectors`' generated manifest
(`scripts/gen_roster.py` there, tracked in `ockham-sh/parsimony-connectors#83`). It backs
`parsimony.registry.list_available()` (and the `parsimony list --available` CLI verb built on
it), which fetches the canonical live registry at `https://parsimony.dev/connectors.json`
first. This file is only the offline / network-failure floor, read via
`importlib.resources.files("parsimony") / "_data" / "connectors.json"` — never written,
refreshed, or invalidated after install. It is not a runtime cache.

**Refresh procedure (once per core release):** copy the connectors repo's committed
`connectors.json` over this file verbatim before tagging a `parsimony-core` release:

```bash
cp ../parsimony-connectors/connectors.json parsimony/_data/connectors.json
```

**This is enforced, not just documented.** The `verify-bundled-manifest` job in
`.github/workflows/publish.yml` fetches `ockham-sh/parsimony-connectors@main`'s `connectors.json`
at release time and fails the build if it differs byte-for-byte from this file — a stale
snapshot cannot ship. `tests/test_registry.py` additionally asserts this file parses as a valid
manifest at the current `schema_version` on every test run, so a malformed edit fails locally
and in CI too.

A stale-but-valid snapshot only affects the offline fallback path (the live, network-available
path is unaffected), but "stale" itself is no longer possible to ship silently.
