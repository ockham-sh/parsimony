# Ranking and fusion

Every match a `Catalog` returns carries a fused `score` and optional `search_detail`
evidence. There is no categorical `matched` label and no third, more authoritative
channel. This page explains how `score` is computed — hierarchical Reciprocal Rank
Fusion (RRF) at two levels — and how to read `search_detail` without mistaking it for
correctness.

One fact underlies all of it: a `Catalog` scores a field's **distinct** indexed values,
not one score per row — see [value deduplication](indexes.md#value-deduplication) —
then builds a candidate row set from those values (`FieldIn` + one scan) and ranks rows by
the relevance of the value they carry. Everything below is in terms of *values*, with row
scores built from them.

## The contract

| | Kind | Answers | Computed from |
|---|---|---|---|
| `score` | estimate or absent | How relevant does this row look for this query? | Weighted RRF over per-field row rankings (`rrf` top-normalizes; best = `1.0`). `None` on filter-only reads. |
| `search_detail` | evidence or absent | Why did this row rank here? | Per-field value, weight, relevance, fused rank, and per-component native raw score + competition rank; `None` when unranked |

`score` is a guess and the model says so. Every *ranked* public score sits in `(0, 1]` with this
query's best retained hit at `1.0` — that top-normalization is part of
[`rrf`](../reference/api.md) itself, not a search-layer afterthought. It is never
probability, never calibrated similarity, and never comparable across queries or
catalogs. A filter-only read leaves `score` and `search_detail` null — there was nothing to rank.

`search_detail` does not score anything; it preserves the evidence behind the number
you're reading. Native BM25 magnitudes and vector cosines are **not** comparable across
kinds. Absence of a component means the value was not retained in that component's top-k
for this query (given `candidate_limit`), not that there is no relationship.

Three consequences follow, and they are the whole ranking contract:

- **`filter` is what you want enforced; `query` only orders what survives.** A constraint
  expressed as query text is a hint the ranker is free to outweigh; the same constraint as
  a [`filter`](search.md#filters) excludes non-matching rows outright. If it matters, filter
  on it.
- **Rows order by `(score desc, namespace, code)`** — deterministic, with no tier the
  caller did not ask for. Two rows with equal scores always come back in the same order,
  so a result page is reproducible and diffable.
- **Commit from provider metadata, not ranking evidence.** Ranked rows are a shortlist.
  Titles, codes, dimension labels, and filters decide; `search_detail` is optional
  debugging when explaining *why* a row ranked where it did.
- **Ranking policy above relevance belongs to the caller.** Only the caller knows what its
  fields mean, so if a domain has a fact worth ranking above relevance, that connector
  tiers on it itself rather than every provider carrying a generic column for it. See
  [Tiering above relevance](#tiering-above-relevance).

## Score, step by step

`Catalog.multi_field_search` is the whole ranking algorithm, and `Catalog.search` is the
one-field case of it. Four steps:

1. **Score each field's distinct values.** For every declared field, the index returns
   index-native scores — BM25, vector neighbours, or their Level-1 RRF fusion inside a
   hybrid field (see [Fusion inside a hybrid field](#fusion-inside-a-hybrid-field)) —
   plus component traces (kind, raw score, competition rank).
2. **Standardize the field ranking.** `search_index_values` pins exact matches first, then
   calls `rrf` so the best value is `1.0`. Raw magnitudes die here for the public
   relevance number; component raw scores stay in the evidence. Exact reinjection that
   fuzzy retrieval missed carries empty `components`.
   `Catalog.search_values` exposes the same relevance plus value-level `search_detail`.
3. **Pool candidates and scan once.** Take the disjunction of "any row carrying any of
   those scored values", AND it with the caller's `filter`, and scan the matching rows
   exactly once — recording, per field, each row's value hit (text, weight, relevance,
   components).
4. **Fuse across fields (Level-2 RRF).** Each field is a ranking of those rows; `rrf`
   fuses them with the field weights and top-normalizes again so the best row reports
   `1.0`. Each contributing field keeps its fused competition rank in `search_detail`.

This is **hierarchical RRF**, not accidental double-normalization. Level 1 fuses lexical
and semantic evidence over the same values into one field ranking. Level 2 fuses independent
field rankings into one row ranking — consuming only each field's order, so a hybrid field
does not get twice the vote merely because it has two internal components. Both levels use
the same `rrf` / `rrf_traced` primitive, including its top-normalize contract.

### Weighted fusion across fields

Four rows over three metadata facets, each weighted equally:

```python
matches = catalog.multi_field_search(
    "quarterly services germany",
    fields={"freq": 1.0, "item": 1.0, "geo": 1.0},
    limit=4,
)
```

| Row | `freq` | `item` | `geo` | Fields hit | `score` (after Level-2 `rrf`) |
|---|---|---|---|---|---|
| A | Quarterly | Services | Germany | 3 of 3 | `1.0` |
| B | Quarterly | Services | France | 2 of 3 | `≈ 0.67` |
| C | Quarterly | Goods | Italy | 1 of 3 | `≈ 0.33` |
| D | Annual | Goods | Germany | 1 of 3 | `≈ 0.33` |

When every hit is exact (or otherwise tied at the top of its field), Level-2 `rrf`
is driven by how many fields the row appears in (scaled by weights); the fused best
is `1.0`. C and D tie and are ordered by `(namespace, code)`.

A field weight means **the contribution of this field's ranking to row order**. Because RRF
discards within-field magnitude, it does *not* mean "scale this field's BM25/cosine scores."
`fields={"title": 3.0, "description": 1.0}` says a title hit is worth three description
hits. Every weight must be a positive, finite number.

### Value-level exactness

Within one field, the value the query literally names is that field's best possible match
by definition, so it reports relevance `1.0`. Exact means case-folded, whitespace-trimmed
string **equality** — nothing softer. No token containment, no grading of how nearly a
value matched.

Query `"M"` against a `freq` field holding `"M"` and `"M-2"`:

| `freq` value | Exact? | Relevance |
|---|---|---|
| `M` | yes | `1.0` |
| `M-2` | no | `< 1.0` |

A tokenless exact value (a bare `"-"`, a short code with no BM25 tokens) still reports
`1.0`: exactness is reinjected even when fuzzy retrieval scored nothing
(`components` empty in that case).

`Catalog.search_values` surfaces the same fact as
[`CatalogValueMatch.exact`](#resolving-a-value-before-you-rank).

## Reading a result page

- **A high `score`** — this row is the most relevant thing found *for this query*. It is
  not a claim that the row is correct. Treat the page as a shortlist; commit from
  titles, codes, and other provider metadata.
- **`search_detail.fields[*].components`** — which index components retained this value
  in their top-k, with native raw scores and competition ranks. Empty components mean
  exact reinjection without a fuzzy hit, or that this field contributed only through
  another path — never "no relationship" as a global claim.
- **Missing a component kind** — the value fell outside that component's candidate
  limit for this query. Raise `candidate_values` / `top_k` if you need deeper recall
  evidence; do not infer unrelatedness.
- **`search_detail is None`** — a filter-only read. There was nothing to rank (every row
  satisfies the filter equally), so every match reports `score=None`.

On discovery-connector pages, verbose evidence is projected as a `search_detail` column
(canonical JSON of `SearchDetail`) with `role=None` and `exclude_from_llm_view=True`, so
`to_llm()` keeps `score` but hides the nested traces. Rehydrate with
`SearchDetail.model_validate_json(row["search_detail"])`. See
[the discovery-connector surface](search.md#the-discovery-connector-surface).

## Resolving a value before you rank

When you know what a value *means* but not how it is spelled in the data, resolve it
instead of hoping the ranker guesses right. `Catalog.search_values` ranks a field's
distinct values by `(exact desc, score desc)`; read the value off the result and then
filter exactly on it:

```python
candidates = catalog.search_values("Germany", "geo", limit=5)
print(candidates[0].value, candidates[0].exact)   # -> Germany True

matches = catalog.search(
    "unemployment", filter={"geo": candidates[0].value}, limit=10,
)
```

This is the sequence to prefer over pushing `"germany"` into the query text and hoping. The
filter *enforces* the country; the query only orders what survives it. If the field is
declared in the catalog's `field_links`, each result also carries `linked_value` — the
canonical code for that label — so the next step can filter on the code rather than the
prose.

## Fusion inside a hybrid field

A [`HybridIndex`](indexes.md#hybridindex) field holds two components — a `BM25Index` and a
`VectorIndex` — and fuses them at the value level with tie-aware, unweighted **Reciprocal
Rank Fusion** (`k=60`): BM25's positive scores against the vector's top-*k* candidates.
Each retained component hit is recorded in `search_detail` with its native `raw_score` and
competition `rank`.

```text
fused(value) = Σ 1 / (60 + rank)   summed over every component that surfaced value
```

`rank` is a **1-based competition rank over that component's own scores** — tied scores
share a rank, so a plateau of equally-scored lexical hits contributes identically to every
value in it, and the other component decides the order within the tie. A value surfaced by
only one component gets no contribution from the other; a value both components agree on
roughly doubles a single-component hit, so agreement stays visible in both the fused
relevance and the dual component traces.

There is nothing to configure: `k` is a fixed module constant (`RRF_K = 60` in
`parsimony.indexes`), and both components contribute equally. RRF is rank-based
rather than score-based precisely so a component with naturally larger raw scores cannot
dominate the other. Component `raw_score` values in `search_detail` remain native and are
not comparable across kinds.

This is **one regime**, whatever the caller composes on top. A field is scored the same way
whether it is searched alone or as one of six weighted fields, because a field being scored
knows nothing about how many other fields the caller is about to combine it with — and a
scoring mode that shifted underneath it could not be reasoned about field by field. Each
field therefore carries its own semantic recall.

!!! note "There is no configurable fusion"
    The `parsimony.ranking` module — `RRF`, `ZScoreFusion`, `MinMaxScoreFusion`,
    `Ranking`/`RankingSet`, weights, custom `Ranker` implementations — has been removed.
    Fusion is one fixed, unweighted algorithm. There is nothing to tune, subclass, or supply
    to a `HybridIndex` constructor. See [Snapshots](#snapshots-fusion-is-native-not-stored)
    below for what that means for old saved catalogs.

## The candidate-value cap: a noise floor, not just a cost cap

Per field, only the top scored values feed the fuzzy band — `top_k_values` on
`Catalog.search`, `candidate_values` on `Catalog.multi_field_search`, both defaulting to
`50`. The same bound caps the vector candidate pool inside a hybrid field and is recorded
on each match as `search_detail.candidate_limit`. This is deliberate: values past the
cutoff would otherwise contribute weak positives — a stray token match three hundred
candidates deep — that add noise without adding signal.

The cap applies to **distinct values, never to rows**, and that distinction is what makes it
safe. Thousands of rows can share one scored value, so a bounded value table is not a
bounded row set: candidates are pooled at the value level and the matching rows are scanned
exactly once, in full. If the pool were built by taking a page of rows per field and
joining, a row that matched on a field where it happened to fall off page two would lose
that evidence.

An exactly-named value is added back regardless of where it fell (empty `components` when
fuzzy retrieval missed it), so the cap can never cost you the value the caller literally
typed. Truncation costs only fuzzy recall on distant, low-confidence values. A missing
component in `search_detail` means "not retained in this top-k," not "no relationship."

## Tiering above relevance

`score` deliberately encodes relevance and nothing else, and rows order by it alone. When a
domain has a fact worth ranking above relevance — a series is still being updated, a dataset
is the official release rather than a mirror — that ranking belongs to the caller, who is the
only one who knows what the fact means. It is an ordinary sort over the returned matches:

```python
hits = catalog.search("unemployment", limit=50)
tiered = sorted(hits, key=lambda m: (m.metadata.get("freq") != "M", -(m.score or 0.0)))
```

The alternative — a generic "fact" column on every result, computed by the kernel — was
tried and removed. A kernel-computed fact has to be domain-neutral to be computable at all,
and a domain-neutral fact is not the one any particular caller wanted: it ends up
proxying something incidental (value brevity, token counts) while presenting itself as
authoritative, and it pins rows above better matches for reasons the caller never asked
for. Exactness at the *value* level survives because it is the one claim that needs no
domain knowledge to state: the string is equal, or it is not.

## Snapshots: fusion is native, not stored

A `HybridIndex` snapshot's `meta.json` still writes a `fusion` key — frozen so pre-0.0.2
readers that expect one keep parsing successfully — but it is inert: `HybridIndex.load()`
ignores it entirely. Fusion is computed natively at query time, not from a serialized
policy. A snapshot written by an older Parsimony version loads and searches unchanged;
there is nothing to migrate.

## Known limits

- **RRF is magnitude-blind.** Hybrid fusion is rank-based by design, which is exactly what
  keeps one component's raw score scale from dominating the other. The tradeoff is that RRF
  also throws away *how much* better one candidate is than the next: a landslide winner and
  a candidate that barely edges out its neighbour contribute identically to the fused rank
  as long as their relative order is the same. A concrete symptom: a short generic title
  ("SWESTR Index") can out-rank the intended row ("SWESTR — Swedish Krona Short-Term Rate")
  on BM25 length normalization alone, since RRF turns that sliver of an edge into a full
  rank step. The mitigation is compositional — declare a second field with its own weight,
  so one field's rank artefact is outvoted rather than decisive.
- **Facet-only queries with no concept word.** A query built entirely from facet words —
  `"annual index germany"`, with no word naming what is actually being searched for — scores
  a row that matches on facets alone as highly as the intended one, because there is no term
  left to separate them. `score` measures relevance to the words given; it cannot know that
  "index" alone is too generic to anchor the query. The mitigation is qualitative: include
  at least one word naming the concept, not just its facets — or move the facets into
  `filter`, where they are enforced rather than scored, and leave the query for the concept.

## See also

- [Indexes](indexes.md) — `BM25Index`, `VectorIndex`, `HybridIndex`, and the discovery index policy.
- [Building and searching](search.md) — `Catalog.search`, `multi_field_search`, filters, and `CatalogMatch`.
- [Entities](entities.md) — the `CatalogMatch` model these scores populate.
- [Snapshots and persistence](snapshots.md) — the legacy `fusion` key in a `HybridIndex` manifest.
