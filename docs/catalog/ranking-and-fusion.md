# Ranking and fusion

Every match a `Catalog` returns carries two kinds of evidence and a label: `coverage`, a
**fact**; `score`, a **guess**; and `matched`, which engine produced the evidence. This
page starts with that model and a handful of worked examples, then works down into the
mechanics that compute the three numbers.

One fact underlies all of it: a `Catalog` scores a field's **distinct** indexed values,
not one score per row — see [value deduplication](indexes.md#value-deduplication-and-row-postings)
— and every row that carries a scored value inherits that evidence through the field's
postings. Everything below is in terms of *values* (the docs also call them **cells**: one
indexed field value, e.g. one row's `title` or one row's `region`), with row-level
`coverage`/`score`/`matched` built from them.

## The two channels

| | Kind | Answers | Computed from |
|---|---|---|---|
| `coverage` | fact | How much of the query is exactly satisfied by this row's values? | Token containment only |
| `score` | guess | How similar does the row look overall? | Lexical BM25 + semantic vector |
| `matched` | label | Which engine produced the evidence — `"lexical"`, `"semantic"`, or `"both"`? | Which component(s) contributed |

`coverage` is verifiable: it only ever credits a row for query tokens some field value
of that row provably contains, nothing more. `score` is a similarity estimate — useful,
but never provably correct. `matched` doesn't score anything; it just says which of the
two engines is behind the evidence you're looking at, so a `None` (filter-only, no
query), `"lexical"` (token overlap), `"semantic"` (vector proximity), or `"both"` tells
you where to trust and where to double-check.

`CatalogValueMatch`, the result type from [`Catalog.search_values`](search.md), carries
the same trio per value — `coverage`, `score`, and `matched` — so value-level consumers
report evidence exactly like row search does.

**The one-sentence contract: verified facts outrank guesses; guesses stay visible,
labeled as guesses.** Nothing with a `coverage` you can trust more than a `coverage` you
can't is ever ranked below it, and nothing with only a `score` is ever hidden — it just
ranks by how good a guess it is.

## Coverage, step by step

Coverage is computed in three steps:

1. **Tokenize the query.** `"annual rate of change"` → `{annual, rate, of, change}`.
2. **Consume cells.** A cell (one indexed field value) is *consumed* only if **every**
   token the cell contains also appears in the query — all-or-nothing per cell. A cell
   that claims anything the query didn't ask for is a different concept, not a weaker
   match, so it isn't partially credited.
3. **Union and divide.** A row's `coverage` is the fraction of the *query's* tokens
   covered by the union of that row's consumed cells, across whichever fields the
   search touched.

Consumption is binary per cell (a cell either qualifies or it doesn't); `coverage` itself
is graded per query, because a row can consume several cells and each contributes
whichever of its own tokens the query asked for.

The five examples below are all run against a live catalog to pin down the exact
numbers; each query tokenizes to lowercase words.

### False friend

Query `"annual rate of change"` over a `title` field:

| Row title | Consumed? | `coverage` | `score` | Rank |
|---|---|---|---|---|
| "Annual rate of change" | yes — every cell token is in the query | `1.0` | `1.0` | pinned, #1 |
| "Annual average rate of change" | no — `average` is not in the query | `0.0` | `1.0` | visible, #2 |

Both rows land on the same `score` here (BM25 favors the longer title's extra term
overlap just as much as the shorter one's exact match), but only the first row's title
is *entirely* contained in the query. The second is a different, more specific concept
("average rate of change" vs. "rate of change") and is graded accordingly — still
returned, still visible, just not pinned above the literal match.

### Term repetition

Query `"current account"` over a `title` field:

| Row title | Consumed? | `coverage` | `score` | Rank |
|---|---|---|---|---|
| "Current account" | yes | `1.0` | `0.667` | pinned, #1 |
| "Current account, Current transfers" | no — `transfers` is extra | `0.0` | `1.0` | visible, #2 |

The repeated tokens in the second title actually earn it the *higher* BM25 score. It
still can't outbid the first row's coverage pin — score alone never beats a verified
fact.

### Facet AND across fields (the main payoff)

Query `"quarterly services germany"` scored across three metadata fields at once:

```python
matches = catalog.search(
    "quarterly services germany", fields=["freq", "item", "geo"], limit=10,
)
```

| Row | `freq` | `item` | `geo` | Cells consumed | `coverage` | `score` |
|---|---|---|---|---|---|---|
| A | Quarterly | Services | Germany | 3 of 3 | `1.0` | `3.0` |
| B | Quarterly | Services | France | 2 of 3 | `0.667` | `2.0` |
| C | Quarterly | Goods | Italy | 1 of 3 | `0.333` | `1.0` |

This is coverage doing its real job: an AND across independently-scored facets. Row A
fully satisfies all three query terms and ranks first; B satisfies two of three and
ranks second regardless of how close its raw `score` gets to A's — a row that consumes
more of the query outranks one that merely looks similar.

### Title degeneration (why partial coverage doesn't rank on a single field)

Query `"unemployment rate monthly"` over a `title` field:

| Row title | Consumed? | `coverage` | `score` | Rank |
|---|---|---|---|---|
| "Unemployment rate (%) - monthly data" | no — extra tokens | `0.0` | `1.0` | #1 (score order) |
| "Unemployment rate" | yes, but only 2 of 3 query tokens | `0.667` | `0.667` | #2 (score order) |

Neither row hits `coverage == 1.0`, so neither pins — both rank by `score` alone, and
the longer title wins because it actually shares more terms with the query. If a
single-field surface ranked by raw `coverage` instead, the short title's higher fraction
(`0.667` vs. `0.0`) would put it first — rewarding brevity, not relevance, since a short
value "contains" more of a long query than a long value does by construction. That's why
[row ranking](#row-ranking-coverage-tiers-by-surface-arity) on a single field only lets a
full `1.0` pin outrank the fuzzy order; every other `coverage` value is reported but does
not participate in ordering.

### The known limit

Query `"annual index germany"` — three facet words, no word naming the actual concept —
scored across the same three fields:

| Row | `freq` | `item` | `geo` | Cells consumed | `coverage` | `score` |
|---|---|---|---|---|---|---|
| "Administrative index series" (wrong concept) | Annual | Index | Germany | 3 of 3 | `1.0` | `3.0` |
| "Consumer price index" (intended) | Annual | Consumer price index | Germany | 2 of 3 | `0.667` | `3.0` |

The wrong-concept row's `item` value is the bare word "Index," which is fully contained
in the query; the intended row's `item` value, "Consumer price index," carries two extra
tokens (`consumer`, `price`) the query never asked for, so it isn't consumed. Both rows
tie on `score`, but the wrong-concept row's full coverage pins it above the row an
analyst actually wants. See [Known limits](#known-limits) below — this is a documented,
accepted tradeoff, not a bug.

## Score

`score` sums, over the searched fields, each field's **normalized** contribution: the
row's best-matching value in that field, divided by that field's own top score for this
query. Every field therefore contributes `0.0`–`1.0`, so agreeing evidence across fields
accumulates and no single field's raw magnitude — BM25 and cosine similarity live on
entirely different scales — can dominate the row's total.

The corollary: `score` is relative to *this query's* best hit. It is never an absolute
relevance measure and is never comparable across different queries or different
catalogs — `0.6` on one search says nothing about `0.6` on another.

## Row ranking: coverage tiers by surface arity

Value-level fusion produces one score per distinct field value; `Catalog.search` then
grades every row that carries a scored or fully-consumed value into a `(coverage, score,
matched)` triple and orders rows by how many fields the search touched — a **facet
surface** (`fields=[a, b, ...]`, several fields) or a **title surface** (broad search, a
DSL clause, or `fields=["one_field"]`, exactly one field):

- **Facet surface**: rows rank `(coverage desc, score desc)` throughout. `coverage`
  counts provably-satisfied constraints — a row that fully consumes three of three query
  facets outranks one that consumes two of three, regardless of raw `score`. This is the
  facet-AND example above.
- **Title surface**: there is no cross-field union to accumulate, so ranking on raw
  `coverage` here would just proxy value *brevity* (a short title "contains" more of a
  long query than a long title does, which says nothing about relevance — the title
  degeneration example above). Only a full-consumption hit — `coverage == 1.0`, a
  literal pin — outranks the fuzzy order; every other row ranks by `score` alone.
  `coverage` is still reported on the match (the raw measurement); it just doesn't
  participate in ordering below the `1.0` tier.

This is a deliberate contract, not an anomaly: on a title surface a high-`score` row can
rank below a `coverage=1.0` row with a lower score.

## Reading a result page

- **`coverage == 1.0`** — your query is literally satisfied by this row's values (an
  exact or subset containment hit).
- **`0 < coverage < 1`** — that fraction of your query's tokens is provably satisfied by
  this row; the rest is unverified.
- **`coverage == 0` and a high `score`** — the row *looks* similar but nothing in it is
  provably a match. Check `matched`: `"semantic"` means the resemblance is entirely a
  vector guess.
- **A whole page of `matched == "semantic"`** — nothing lexically real matched anywhere
  in the result set; the vector index is guessing at meaning with no literal anchor.
  Rephrase the query rather than trust the ranking order as-is.

Row-level containment (`coverage > 0`) is always lexical evidence by construction — a
token-subset test, not an embedding — so a row with positive `coverage` always carries
at least `"lexical"` in its evidence, even when its positive `score` came only from the
vector component.

On discovery-connector pages the lexical evidence may live in the `description` field —
searched, but deliberately not a result column — so `matched` cannot be reconstructed
from the visible rows: it is the engine's receipt for text it read on the agent's
behalf. See [the discovery-connector surface](search.md#the-discovery-connector-surface).

## Under the hood

The sections below are the mechanics that produce the `coverage`/`score`/`matched`
numbers above: how a hybrid field's BM25 and vector components combine at the value
level (two regimes, picked automatically), the noise floor on the fuzzy-score table, and
what a snapshot's legacy `fusion` key means today.

!!! note "There is no configurable fusion"
    The `parsimony.ranking` module — `RRF`, `ZScoreFusion`, `MinMaxScoreFusion`,
    `Ranking`/`RankingSet`, weights, custom `Ranker` implementations — has been
    removed. Fusion is now two fixed, unweighted algorithms, chosen automatically by
    how many fields a search touches. There is nothing to tune, subclass, or supply to
    a `HybridIndex` constructor. See [Snapshots](#snapshots-fusion-is-native-not-stored)
    below for what that means for old saved catalogs.

### Two regimes, picked by surface arity

A **surface** is the set of fields one `Catalog.search` call scores against: a single
field (broad search against the `title` index, a resolved `FIELD: value` DSL clause, or
`fields=["title"]`), or several (`fields=["title", "region"]`). A `HybridIndex`
field's two components — a `BM25Index` and a `VectorIndex` — combine differently
depending on which kind of surface it is being searched under. The regime is picked by
the *call*, never by counting a field's distinct values; a field with ten values and a
field with a million follow the exact same rule.

#### Multi-field surfaces: lexical-first, semantic void-fill

When `fields=[...]` names more than one field, every hybrid component in scope is
lexical-first: BM25 ranks the field whenever it has any positive score at all. The
vector component only steps in when lexical evidence **abstains entirely** for that
field — the semantic bridge for query phrasing that shares no vocabulary with the
indexed values (`"young people"` → `"less than 25 years"`), without letting the
never-abstaining vector perturb an order lexical evidence has already decided.

```python
matches = catalog.search("young people", fields=["title", "age_group"], limit=10)
```

Here, if `title`'s BM25 finds any positive hit, `age_group`'s vector component is
still free to void-fill *if `age_group`'s own BM25 comes back empty* — void-fill is
decided per field, not once for the whole surface.

#### Single-field surfaces: tie-aware Reciprocal Rank Fusion (RRF, k=60)

When a query scores exactly one field, that field carries all the recall alone, so
both components get a vote: BM25's positive scores and the vector's top-*k* candidates
fuse with unweighted Reciprocal Rank Fusion, `k=60`.

```text
fused(value) = Σ 1 / (60 + rank)   summed over every component that surfaced value
```

`rank` is a **1-based competition rank over that component's own scores** — tied
scores share a rank, so a plateau of equally-scored lexical hits contributes
identically to every value in it, and the other component decides the order within
the tie. A value surfaced by only one component gets no contribution from the other; a
value both components agree on roughly doubles a single-component hit, so agreement
stays visible even after the per-field score normalization above.

There is no `weights=` or `k=` to configure. `k` is a fixed module constant
(`RRF_K = 60` in `parsimony.catalog.indexes`), and both components contribute equally
— RRF is rank-based, not score-based, so a component with naturally larger raw scores
never dominates the other.

### `top_k_values`: a noise floor, not just a cost cap

Per field, only the top `top_k_values` scored values feed the fuzzy-score band
(default `50`; the same value also bounds the vector candidate pool in both fusion
regimes). This is deliberate: values past the cutoff would otherwise contribute weak
positives — a stray token match three hundred candidates deep — that add noise
without adding signal.

Fully-consumed values are gated separately from the score table and always count
toward `coverage`, no matter where they would have fallen in the fuzzy ranking.
Truncation can only cost you fuzzy recall on distant, low-confidence values; it never
drops an exact or subset-consumed match.

### Snapshots: fusion is native, not stored

A `HybridIndex` snapshot's `meta.json` still writes a `fusion` key — frozen so
pre-0.0.2 readers that expect one keep parsing successfully — but it is inert:
`HybridIndex.load()` ignores it entirely. Fusion is computed natively at query time
from the two regimes above, not from a serialized policy. A snapshot written by an
older Parsimony version loads and searches unchanged; there is nothing to migrate.

## Known limits

- **Facet-only queries with no concept word.** A query built entirely from facet words —
  `"annual index germany"`, with no word naming what's actually being searched for — can
  be fully consumed by a row that happens to match on facets alone but is conceptually
  wrong, which then out-ranks the intended row (the *Known limit* worked example above).
  Coverage rewards provable containment; it has no way to know that "index" alone is too
  generic a concept word to anchor the query. The mitigation is qualitative, not
  structural: include at least one word that names the concept, not just its facets.
- **RRF is magnitude-blind.** Single-field fusion (RRF, `k=60`) is rank-based by design —
  see [the two regimes](#two-regimes-picked-by-surface-arity) — which is exactly what
  keeps one component's raw score scale from dominating the other. The tradeoff is that
  RRF also throws away *how much* better one candidate is than the next: a landslide
  winner in one component and a candidate that barely edges out its neighbor contribute
  identically to the fused rank as long as their relative order is the same. When two
  single-field results have close `score`s, that closeness may understate or overstate
  how differentiated the underlying evidence actually was. A concrete symptom: a short
  generic title ("SWESTR Index") can out-rank the intended row ("SWESTR — Swedish Krona
  Short-Term Rate") on BM25 length normalization alone, since RRF turns that sliver of
  an edge into a full rank step. This is why the discovery-connector factory declares
  `fields=["title", "description"]` — a two-field surface takes the lexical-first facet
  regime instead — leaving RRF to deliberately-narrowed single-field surfaces (e.g.
  SDMX dataset titles, where a flow's identity *is* its title).

## See also

- [Indexes](indexes.md) — `BM25Index`, `VectorIndex`, `HybridIndex`, and the discovery index policy.
- [Building and searching](search.md) — `Catalog.search`, `fields=`, and `CatalogMatch`.
- [Entities](entities.md) — the `CatalogMatch` model these scores populate.
- [Snapshots and persistence](snapshots.md) — the legacy `fusion` key in a `HybridIndex` manifest.
