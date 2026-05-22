"""Ranking primitives for catalog retrieval."""

from __future__ import annotations

import math
from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Annotated, Literal, Protocol, TypeAlias

from pydantic import BaseModel, ConfigDict, Field, field_validator

RRF_K = 60


@dataclass(frozen=True)
class RankedItem:
    """One ranked catalog identity."""

    namespace: str
    code: str
    rank: int
    score: float


@dataclass(frozen=True)
class RankedSetItem:
    """One ranked identity within a named index."""

    index: str
    namespace: str
    code: str
    rank: int
    score: float


@dataclass(frozen=True)
class Ranking:
    """One ranked list of catalog identities."""

    items: tuple[RankedItem, ...] = ()

    def __post_init__(self) -> None:
        seen: set[tuple[str, str]] = set()
        for item in self.items:
            key = (item.namespace, item.code)
            if key in seen:
                raise ValueError("Ranking entries must be unique by (namespace, code)")
            seen.add(key)
            if item.rank < 0:
                raise ValueError("Ranking rank values must be zero-based non-negative integers")

    @classmethod
    def empty(cls) -> Ranking:
        return cls(())


@dataclass(frozen=True)
class RankingSet:
    """Named collection of per-index rankings."""

    items: tuple[RankedSetItem, ...] = ()

    def __post_init__(self) -> None:
        seen: set[tuple[str, str, str]] = set()
        for item in self.items:
            key = (item.index, item.namespace, item.code)
            if key in seen:
                raise ValueError("RankingSet entries must be unique by (index, namespace, code)")
            seen.add(key)
            if item.rank < 0:
                raise ValueError("RankingSet rank values must be zero-based non-negative integers")

    @classmethod
    def empty(cls) -> RankingSet:
        return cls(())


class Ranker(Protocol):
    """Pure policy for collapsing index rankings into one final ranking."""

    def __call__(self, rankings: RankingSet, *, limit: int) -> Ranking:
        """Rank candidate identities from *rankings*."""
        ...


class RRFSpec(BaseModel):
    """Serializable snapshot form for :class:`RRF`."""

    model_config = ConfigDict(extra="forbid")

    kind: Literal["rrf"] = "rrf"
    weights: dict[str, float] = Field(default_factory=dict)
    k: int = RRF_K

    @field_validator("weights")
    @classmethod
    def _validate_weights(cls, value: Mapping[str, float]) -> dict[str, float]:
        return _validated_weights(value)

    @field_validator("k")
    @classmethod
    def _validate_k(cls, value: int) -> int:
        if value <= 0:
            raise ValueError("RRF k must be positive")
        return value


class MinMaxScoreFusionSpec(BaseModel):
    """Serializable snapshot form for :class:`MinMaxScoreFusion`."""

    model_config = ConfigDict(extra="forbid")

    kind: Literal["min_max_score_fusion"] = "min_max_score_fusion"
    weights: dict[str, float] = Field(default_factory=dict)

    @field_validator("weights")
    @classmethod
    def _validate_weights(cls, value: Mapping[str, float]) -> dict[str, float]:
        return _validated_weights(value)


class ZScoreFusionSpec(BaseModel):
    """Serializable snapshot form for :class:`ZScoreFusion`."""

    model_config = ConfigDict(extra="forbid")

    kind: Literal["z_score_fusion"] = "z_score_fusion"
    weights: dict[str, float] = Field(default_factory=dict)

    @field_validator("weights")
    @classmethod
    def _validate_weights(cls, value: Mapping[str, float]) -> dict[str, float]:
        return _validated_weights(value)


RankerSpec: TypeAlias = Annotated[RRFSpec | MinMaxScoreFusionSpec | ZScoreFusionSpec, Field(discriminator="kind")]


def concat(rankings: dict[str, Ranking]) -> RankingSet:
    """Build a :class:`RankingSet` from named rankings."""

    items: list[RankedSetItem] = []
    for name, ranking in rankings.items():
        for item in ranking.items:
            items.append(
                RankedSetItem(
                    index=name,
                    namespace=item.namespace,
                    code=item.code,
                    rank=item.rank,
                    score=item.score,
                )
            )
    return RankingSet(tuple(items))


def _rank_rows_with_ties(rows: Sequence[tuple[int, float]], *, limit: int) -> list[tuple[int, int, float]]:
    """Assign competition ranks, preserving complete equal-score groups."""

    if limit <= 0 or not rows:
        return []
    ordered = sorted(rows, key=lambda row: (-row[1], row[0]))
    ranked: list[tuple[int, int, float]] = []
    position = 0
    while position < len(ordered):
        _idx, score = ordered[position]
        rank = position
        group_end = position + 1
        while group_end < len(ordered) and ordered[group_end][1] == score:
            group_end += 1
        if rank >= limit:
            break
        ranked.extend((group_idx, rank, group_score) for group_idx, group_score in ordered[position:group_end])
        position = group_end
    return ranked


def ranking_from_scores(rows: Sequence[tuple[str, str, float]], *, limit: int) -> Ranking:
    """Build a ranking from raw ``(namespace, code, score)`` rows."""

    ranked_rows = _rank_rows_with_ties([(idx, score) for idx, (_ns, _code, score) in enumerate(rows)], limit=limit)
    items = tuple(
        RankedItem(
            namespace=rows[idx][0],
            code=rows[idx][1],
            rank=rank,
            score=score,
        )
        for idx, rank, score in ranked_rows
    )
    return Ranking(items)


def _validated_weights(weights: Mapping[str, float]) -> dict[str, float]:
    out: dict[str, float] = {}
    for name, weight in weights.items():
        value = float(weight)
        if not math.isfinite(value) or value < 0:
            raise ValueError("Ranker weights must be finite non-negative numbers")
        out[str(name)] = value
    return out


def _group_by_index(items: Sequence[RankedSetItem]) -> dict[str, list[RankedSetItem]]:
    grouped: dict[str, list[RankedSetItem]] = defaultdict(list)
    for item in items:
        grouped[item.index].append(item)
    return grouped


@dataclass(frozen=True)
class RRF:
    """Reciprocal Rank Fusion over index-local ranks, optionally weighted."""

    weights: Mapping[str, float] = field(default_factory=dict)
    k: int = RRF_K

    def __post_init__(self) -> None:
        if self.k <= 0:
            raise ValueError("RRF k must be positive")
        object.__setattr__(self, "weights", _validated_weights(self.weights))

    def __call__(self, rankings: RankingSet, *, limit: int) -> Ranking:
        if not rankings.items:
            return Ranking.empty()
        scores: dict[tuple[str, str], float] = defaultdict(float)
        for item in rankings.items:
            weight = float(self.weights.get(item.index, 1.0))
            key = (item.namespace, item.code)
            scores[key] += weight / (self.k + item.rank + 1.0)
        rows = [(namespace, code, score) for (namespace, code), score in scores.items()]
        return ranking_from_scores(rows, limit=limit)


@dataclass(frozen=True)
class MinMaxScoreFusion:
    """Weighted score fusion with per-index min-max normalization."""

    weights: Mapping[str, float] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "weights", _validated_weights(self.weights))

    def __call__(self, rankings: RankingSet, *, limit: int) -> Ranking:
        if not rankings.items:
            return Ranking.empty()
        scores: dict[tuple[str, str], float] = defaultdict(float)
        for _index_name, group in _group_by_index(rankings.items).items():
            min_score = min(item.score for item in group)
            max_score = max(item.score for item in group)
            if max_score == min_score:
                continue
            weight = float(self.weights.get(_index_name, 1.0))
            span = max_score - min_score
            for item in group:
                norm = ((item.score - min_score) / span) * weight
                key = (item.namespace, item.code)
                scores[key] += norm
        if not scores:
            return Ranking.empty()
        rows = [(namespace, code, score) for (namespace, code), score in scores.items()]
        return ranking_from_scores(rows, limit=limit)


@dataclass(frozen=True)
class ZScoreFusion:
    """Weighted score fusion with per-index Z-score normalization."""

    weights: Mapping[str, float] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "weights", _validated_weights(self.weights))

    def __call__(self, rankings: RankingSet, *, limit: int) -> Ranking:
        if not rankings.items:
            return Ranking.empty()
        scores: dict[tuple[str, str], float] = defaultdict(float)
        for index_name, group in _group_by_index(rankings.items).items():
            values = [item.score for item in group]
            mean = sum(values) / len(values)
            if len(values) < 2:
                std = 0.0
            else:
                variance = sum((value - mean) ** 2 for value in values) / (len(values) - 1)
                std = math.sqrt(variance)
            weight = float(self.weights.get(index_name, 1.0))
            for item in group:
                norm = 0.0 if std == 0.0 or math.isnan(std) else ((item.score - mean) / std) * weight
                key = (item.namespace, item.code)
                scores[key] += norm
        if not scores:
            return Ranking.empty()
        rows = [(namespace, code, score) for (namespace, code), score in scores.items()]
        return ranking_from_scores(rows, limit=limit)


def ranker_from_spec(spec: RankerSpec) -> Ranker:
    """Build a runtime ranker from a serializable ranker spec."""

    if isinstance(spec, RRFSpec):
        return RRF(weights=spec.weights, k=spec.k)
    if isinstance(spec, MinMaxScoreFusionSpec):
        return MinMaxScoreFusion(weights=spec.weights)
    if isinstance(spec, ZScoreFusionSpec):
        return ZScoreFusion(weights=spec.weights)
    raise TypeError(f"Unsupported ranker spec: {type(spec).__name__}")


def ranker_to_spec(ranker: Ranker) -> RankerSpec:
    """Return the serializable spec for a built-in ranker."""

    if isinstance(ranker, RRF):
        return RRFSpec(weights=dict(ranker.weights), k=ranker.k)
    if isinstance(ranker, MinMaxScoreFusion):
        return MinMaxScoreFusionSpec(weights=dict(ranker.weights))
    if isinstance(ranker, ZScoreFusion):
        return ZScoreFusionSpec(weights=dict(ranker.weights))
    raise TypeError(f"Catalog ranker {type(ranker).__name__!r} is runtime-only and cannot be serialized")


__all__ = [
    "MinMaxScoreFusion",
    "MinMaxScoreFusionSpec",
    "RRF",
    "RRFSpec",
    "RankedItem",
    "RankedSetItem",
    "Ranker",
    "RankerSpec",
    "Ranking",
    "RankingSet",
    "ZScoreFusion",
    "ZScoreFusionSpec",
    "concat",
    "ranker_from_spec",
    "ranker_to_spec",
    "ranking_from_scores",
]
