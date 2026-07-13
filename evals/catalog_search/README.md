# Catalog search-surface evals

Empirical relevance battery behind the `(coverage, score)` ranking contract shipped in
`Catalog.search(fields=...)` (issue #69). These are not unit tests — they run real
cached SDMX catalogs through the full search path and score rankings against
metadata-predicate targets (MRR, hit@10). Unit tests live in
`tests/test_catalog_search_surface.py`; this directory is the evidence that the design
beats the alternatives and the gate that future ranking changes must clear.

## Requirements

- A venv with **this repo** and **parsimony-connectors** (`parsimony-sdmx` package)
  installed editable — the battery loads catalogs through the connector's own
  resolution path (`flow_loader.py`).
- Cached SDMX catalogs under `~/.cache/parsimony/catalogs/` for: ECB `BOP`, `EXR`,
  `ICP`, `YC`; ESTAT `une_rt_m`, `prc_hicp_aind`, `prc_hicp_manr`, `namq_10_gdp`,
  `bop_eu6_q`, `teibp050`. Any `sdmx_series_search` call against a flow populates its
  cache entry.
- The embedder model used by the catalogs' vector indexes (downloaded on first use).

Run everything from this directory: `python full_eval.py`, etc.

## Files

| File | Purpose |
|---|---|
| `flow_loader.py` | Loads a flow's catalog + declared search surface via connector internals. |
| `full_eval.py` | The battery: 37 scored cases (query taxonomy Q1–Q17) + 4 sentinel cases (Q19–Q22, documented limits). Defines `CASES`, targets as metadata predicates, and the reference rankers (`title_only`, `auto_filter`, `cov_pool`, `cov_nsum`). |
| `native_battery.py` | Runs the battery through the real `Catalog.search(fields=...)` API (not a simulation) — the acceptance gate. |
| `design_limits_probe.py` | Red-team attacks L1–L10 on the shipped design (morphology, diacritics, negation, typos, number tokens, ...). |
| `q18_probe.py` | Standalone synthetic probe for the cross-band limit (a fuzzy-strong row outranked by a coverage-band row). |
| `juraj_probe.py` | Simulation of the per-facet-document embedding architecture ("Concept: Value" docs, pure cosine) for comparison. |
| `juraj_complement.py` | Hybrid variants keeping the coverage band but varying the within-band vote (norm_sum vs cosine votes vs both). |

## Acceptance gate

`native_battery.py` on the 37 non-sentinel cases must score **≥ 0.798 MRR** and
**≥ 31/37 hit@10**, and no sentinel case (Q19–Q22) may get *worse* than its documented
rank. Reference results at the time the gate was frozen:

| Ranker | MRR | hit@10 | Notes |
|---|---|---|---|
| `title_only` | 0.448 | — | pre-#69 behavior (bare query → title index only) |
| `auto_filter` | 0.596 | — | rejected: exact-value probe hijacks short identifiers |
| `cov_pool` | 0.687 | — | coverage band + pooled raw scores |
| **`cov_nsum` (shipped)** | **0.812** | **32/37** | coverage band + per-field top-normalized sum |

Rejected along the way (implementations deleted, results reproducible from
`full_eval.py` history): RRF and min-max/z-score rank fusion (plateau truncation —
per-field ranks carry no containment signal), BM25 k1=0, Jaccard similarity,
coverage/score blends, hard auto-filtering, `DisMaxIndex`.

Comparison against the pure-embedding architecture (`juraj_probe.py`, same stock
encoder): per-facet cosine sum scores 0.607 MRR vs 0.812 — the gap is the measured
value of lexical containment. Its wins concentrate exactly on the sentinel cases
(typos, codes), which is why a fine-tuned "projected similarity" embedder is logged as
a candidate *within-band voter* (see `juraj_complement.py`: `cov_both` reaches 0.844
with zero regressions) rather than a replacement — blocked on a held-out case batch
and on persisting DSD concept names at catalog build time.
