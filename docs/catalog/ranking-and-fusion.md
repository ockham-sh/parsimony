# Ranking and fusion

The `parsimony.ranking` module is the small, dependency-light layer (standard library plus
pydantic only) that turns per-index candidate lists into one final ranked list. It models
retrieval results as immutable dataclasses and combines them with pure-policy *fusion
rankers* — Reciprocal Rank Fusion, min-max score fusion, or z-score score fusion. The
[catalog's hybrid index](indexes.md) uses this layer to merge BM25 and vector results, and
the [search API](search.md) uses it to assemble `CatalogMatch` results. You can also use it
standalone to fuse any set of named rankings.

## The data model

Three frozen dataclasses describe a single ranked identity and two collections of them. All
are immutable, so you build a new instance rather than mutating an existing one.

| Type | Fields | Meaning |
|------|--------|---------|
| `RankedItem` | `namespace`, `code`, `rank`, `score` | One ranked catalog identity. |
| `RankedSetItem` | `index`, `namespace`, `code`, `rank`, `score` | The same, plus the name of the index that produced it. |
| `Ranking` | `items: tuple[RankedItem, ...] = ()` | An ordered ranked list. |
| `RankingSet` | `items: tuple[RankedSetItem, ...] = ()` | A flat collection of per-index ranked rows. |

`rank` is a zero-based integer (rank `0` is the top result), and `score` is the fused or
raw relevance score. The collections validate their contents at construction:

- `Ranking` requires its items to be unique by `(namespace, code)`. A duplicate raises
  `ValueError("Ranking entries must be unique by (namespace, code)")`.
- `RankingSet` requires uniqueness by `(index, namespace, code)`, raising
  `ValueError("RankingSet entries must be unique by (index, namespace, code)")` otherwise.
- Both reject any item with `rank < 0`, raising a `ValueError` about
  "zero-based non-negative integers".

Both collections expose a `.empty()` classmethod that returns the empty instance
(`Ranking(())` / `RankingSet(())`).

```python
from parsimony.ranking import RankedItem, Ranking

ranking = Ranking((
    RankedItem(namespace="us_state", code="CA", rank=0, score=0.9),
    RankedItem(namespace="us_state", code="NY", rank=1, score=0.7),
))
print(len(ranking.items))          # 2
print(Ranking.empty().items)       # ()
```

!!! note "Import paths"
    Only `RRF`, `Ranker`, `Ranking`, `ZScoreFusion`, and `MinMaxScoreFusion` are re-exported
    from the top-level package (`from parsimony import RRF`). Everything else in this
    module — `RankedItem`, `RankedSetItem`, `RankingSet`, `concat`, `ranking_from_scores`,
    the `*Spec` models, `RankerSpec`, and `ranker_to_spec` / `ranker_from_spec` — lives only
    under `parsimony.ranking`. The clearest convention for ranking-heavy code is to import
    the whole surface from there: `from parsimony.ranking import ...`.

## Building a RankingSet with `concat`

You rarely build a `RankingSet` by hand. The `concat` function takes a mapping of
index-name to `Ranking` and flattens it into one `RankingSet`, stamping every item with its
source index name. The dict's iteration order is preserved in the resulting tuple.

```python
from parsimony.ranking import RankedItem, Ranking, concat

bm25 = Ranking((
    RankedItem(namespace="indicator", code="gdp", rank=0, score=12.0),
    RankedItem(namespace="indicator", code="cpi", rank=1, score=8.0),
))
vector = Ranking((
    RankedItem(namespace="indicator", code="cpi", rank=0, score=0.82),
    RankedItem(namespace="indicator", code="gdp", rank=1, score=0.61),
))

rankings = concat({"bm25": bm25, "vector": vector})
print(len(rankings.items))  # 4 — two rows from each index, each tagged with its index name
```

## The `Ranker` protocol

A ranker is a *pure policy*: a callable that takes a `RankingSet` and a keyword-only `limit`
and returns one fused `Ranking`. The `Ranker` protocol captures exactly that contract.

```python
from typing import Protocol
from parsimony.ranking import Ranking, RankingSet

class Ranker(Protocol):
    def __call__(self, rankings: RankingSet, *, limit: int) -> Ranking: ...
```

Three concrete rankers ship in the box (`RRF`, `MinMaxScoreFusion`, `ZScoreFusion`), all
frozen dataclasses. Any object matching the protocol — including your own implementation —
is accepted at runtime, for example as the `fusion` argument of a
[`HybridIndex`](indexes.md). A custom ranker cannot be serialized, however; see
[serialization](#serializing-rankers) below.

## Reciprocal Rank Fusion (`RRF`)

`RRF` fuses on *rank position*, not raw score. For every `RankedSetItem` it accumulates
`weight / (k + rank + 1.0)` into a per-`(namespace, code)` total, where `weight` is the
index's weight (default `1.0`) and `k` defaults to `60`. Because contributions accumulate
across indexes, an identity that appears in several indexes is rewarded over one that
appears in only one — even if its raw scores are small.

| Parameter | Type | Default | Notes |
|-----------|------|---------|-------|
| `weights` | `Mapping[str, float]` | `{}` (every index `1.0`) | Per-index multipliers; finite and non-negative. |
| `k` | `int` | `60` | Smoothing constant; must be positive. |

```python
from parsimony.ranking import RRF, RankedSetItem, RankingSet

rankings = RankingSet((
    RankedSetItem(index="a", namespace="n", code="A", rank=0, score=100.0),
    RankedSetItem(index="a", namespace="n", code="B", rank=1, score=90.0),
    RankedSetItem(index="b", namespace="n", code="B", rank=0, score=0.1),
))

final = RRF()(rankings, limit=2)
print([item.code for item in final.items])  # ['B', 'A'] — B wins by appearing in both
print([item.rank for item in final.items])  # [0, 1]
```

!!! tip "RRF is rank-based, not score-based"
    A tiny raw score at rank 0 beats a huge raw score at rank 1. To bias toward one index
    despite that, weight it: `RRF(weights={"strong": 10.0})` promotes the `strong` index's
    rows.

`RRF(k=0)` raises `ValueError("RRF k must be positive")`.

## Score fusion: `MinMaxScoreFusion` and `ZScoreFusion`

The two score-fusion rankers normalize each index's raw scores onto a common scale before
combining them, so an index with naturally large scores does not dominate one with small
scores. Both take only a `weights` mapping (no `k`).

### MinMaxScoreFusion

For each index, `MinMaxScoreFusion` finds the min and max score and adds
`((score - min) / (max - min)) * weight` for each item. If an index's scores are all equal
(`max == min`) it is **skipped entirely** — a constant-score index contributes nothing.

```python
from parsimony.ranking import MinMaxScoreFusion, RankedSetItem, RankingSet

rankings = RankingSet((
    RankedSetItem(index="bm25", namespace="n", code="A", rank=0, score=10.0),
    RankedSetItem(index="bm25", namespace="n", code="B", rank=1, score=0.0),
    RankedSetItem(index="vector", namespace="n", code="B", rank=0, score=0.9),
    RankedSetItem(index="vector", namespace="n", code="A", rank=1, score=0.8),
))

final = MinMaxScoreFusion(weights={"bm25": 0.2, "vector": 1.0})(rankings, limit=2)
print([item.code for item in final.items])  # ['B', 'A']
```

### ZScoreFusion

`ZScoreFusion` normalizes by the *standard score*: for each index it computes the mean and
the **sample** standard deviation (variance divided by `n - 1`, Bessel's correction) and
adds `((score - mean) / std) * weight` for each item. This is the default fusion policy for
[`HybridIndex`](indexes.md).

A single-row index, or any index whose standard deviation collapses to `0.0` (or `NaN`),
contributes `0.0` for every one of its items — the items still appear, just with no
contribution from that index.

```python
from parsimony.ranking import RankedItem, Ranking, ZScoreFusion, concat

r1 = Ranking((
    RankedItem(namespace="n", code="A", rank=0, score=10.0),
    RankedItem(namespace="n", code="B", rank=1, score=20.0),
    RankedItem(namespace="n", code="C", rank=2, score=30.0),
))
r2 = Ranking((
    RankedItem(namespace="n", code="A", rank=0, score=1.0),
    RankedItem(namespace="n", code="B", rank=1, score=2.0),
    RankedItem(namespace="n", code="C", rank=2, score=3.0),
))

rs = concat({"idx1": r1, "idx2": r2})
final = ZScoreFusion(weights={"idx1": 2.0, "idx2": 0.5})(rs, limit=5)
print([item.code for item in final.items])   # ['C', 'B', 'A']
print([item.score for item in final.items])  # [2.5, 0.0, -2.5]
```

With three equally-spaced scores (10 / 20 / 30) the sample-std normalization gives exactly
`+1 / 0 / -1` per index; weighting `idx1` by `2.0` and `idx2` by `0.5` sums those to
`+2.5 / 0.0 / -2.5`.

### Weights

All three rankers accept a `weights` mapping keyed by index name. Any index not listed
defaults to `1.0`. Weights are validated and coerced at construction:

- Each value is coerced to `float`; a non-finite value (`inf` / `nan`) or a negative value
  raises `ValueError("Ranker weights must be finite non-negative numbers")`.
- Keys are coerced with `str(...)`, so non-string keys are stringified rather than rejected.

```python
from parsimony.ranking import RRF, MinMaxScoreFusion

RRF(weights={"bad": -1.0})                  # ValueError: ...finite non-negative...
MinMaxScoreFusion(weights={"bad": float("nan")})  # ValueError: ...finite non-negative...
```

!!! note "Frozen dataclasses still normalize their input"
    `RRF`, `MinMaxScoreFusion`, and `ZScoreFusion` are frozen, but each rewrites `weights`
    during construction (via `object.__setattr__`) to the validated `dict[str, float]`. The
    stored `weights` is always a plain dict, not the `Mapping` you passed in.

## Final ordering, ranks, and ties

All three rankers feed their accumulated `(namespace, code, score)` rows to
`ranking_from_scores`, which is also usable on its own. It sorts by **descending score**,
breaking ties by the rows' original insertion order, then assigns **0-based competition
ranks**: every item in a tie group shares the position of the group's first member.

```python
from parsimony.ranking import ranking_from_scores

rows = [("n", "A", 5.0), ("n", "B", 5.0), ("n", "C", 5.0), ("n", "D", 1.0)]
final = ranking_from_scores(rows, limit=2)
print([(item.code, item.rank) for item in final.items])
# [('A', 0), ('B', 0), ('C', 0)]
```

!!! warning "A tie group can push the output past `limit`"
    `limit` caps the *rank* of a tie group's first member, not the output length. A tie
    group whose rank is `>= limit` is dropped, but a tie group that *starts* below the limit
    is emitted in full — even if that yields more than `limit` items. Above, `limit=2`
    returns three items because A, B, and C all sit at rank `0`. `D` is dropped because the
    next rank position (`3`) is `>= limit`.

An empty `RankingSet`, a `limit <= 0`, or (for the score fusions) a state where every index
turned out constant all yield `Ranking.empty()`.

```python
from parsimony.ranking import RRF, RankingSet

print(RRF()(RankingSet.empty(), limit=10).items)  # ()
```

## Serializing rankers

To persist a fusion policy in a [catalog snapshot](snapshots.md), each built-in ranker has a
matching pydantic spec model. The specs use `extra="forbid"` (unknown keys are rejected) and
a `kind` literal discriminator.

| Ranker | Spec | `kind` discriminator |
|--------|------|----------------------|
| `RRF` | `RRFSpec` | `"rrf"` (also carries `k`) |
| `MinMaxScoreFusion` | `MinMaxScoreFusionSpec` | `"min_max_score_fusion"` |
| `ZScoreFusion` | `ZScoreFusionSpec` | `"z_score_fusion"` |

`RankerSpec` is the discriminated-union type alias over the three, keyed on `kind`. Convert
in either direction with `ranker_to_spec` and `ranker_from_spec`.

```python
from parsimony.ranking import RRF, RRFSpec, ranker_from_spec, ranker_to_spec

spec = ranker_to_spec(RRF(weights={"title_bm25": 0.2}, k=42))
assert spec == RRFSpec(weights={"title_bm25": 0.2}, k=42)

ranker = ranker_from_spec(spec)
assert isinstance(ranker, RRF)
assert ranker.weights == {"title_bm25": 0.2}
assert ranker.k == 42
```

Because `RankerSpec` is a pydantic discriminated union, you can validate an arbitrary raw
config (for example from a JSON snapshot) and build a ranker from it:

```python
from pydantic import TypeAdapter
from parsimony.ranking import RankerSpec, ZScoreFusion, ranker_from_spec

raw = {"kind": "z_score_fusion", "weights": {"bm25": 1.0, "vector": 2.0}}
spec = TypeAdapter(RankerSpec).validate_python(raw)
ranker = ranker_from_spec(spec)
assert isinstance(ranker, ZScoreFusion)
assert ranker.weights == {"bm25": 1.0, "vector": 2.0}
```

!!! warning "Only the three built-ins are serializable"
    `ranker_to_spec` raises `TypeError` for any ranker that is not `RRF`,
    `MinMaxScoreFusion`, or `ZScoreFusion` — including a perfectly valid custom `Ranker`
    protocol implementation (the message ends "is runtime-only and cannot be serialized").
    `ranker_from_spec` raises `TypeError` for an unrecognized spec type. If you supply a
    custom fusion ranker to a `HybridIndex`, that index cannot be saved to a snapshot.

## Where this fits

You usually do not call this layer directly. The [catalog](index.md) builds it for you: a
[`HybridIndex`](indexes.md) ranks its BM25 and vector components separately with
`ranking_from_scores`, joins them with `concat`, and fuses with its `fusion` ranker (default
`ZScoreFusion()`); the hybrid-search [policy](indexes.md) constructs a weighted
`ZScoreFusion`; and [`Catalog.search`](search.md) turns the resulting `Ranking` into the
`CatalogMatch` list it returns. Reach for `parsimony.ranking` directly when you are building
a custom index, tuning fusion weights, or fusing rankings produced outside the catalog.

## See also

- [Indexes](indexes.md) — the BM25, vector, and hybrid indexes whose results these rankers fuse.
- [Building and searching](search.md) — how `Catalog.search` produces and consumes rankings.
- [Snapshots and persistence](snapshots.md) — where serialized ranker specs are stored.
- [The Catalog](index.md) — the discovery layer that ties indexes, search, and ranking together.
