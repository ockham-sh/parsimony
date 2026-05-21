"""Ranking primitives for catalog retrieval."""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Annotated, Literal, Protocol, TypeAlias

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field, field_validator

RANKING_COLUMNS = ("namespace", "code", "rank", "score")
RANKING_SET_COLUMNS = ("index", "namespace", "code", "rank", "score")
RRF_K = 60


def _require_columns(df: pd.DataFrame, columns: Sequence[str], *, label: str) -> None:
    missing = [column for column in columns if column not in df.columns]
    if missing:
        raise ValueError(f"{label} missing required columns: {missing}")


@dataclass(frozen=True)
class Ranking:
    """One ranked list of catalog identities."""

    df: pd.DataFrame

    def __post_init__(self) -> None:
        _require_columns(self.df, RANKING_COLUMNS, label="Ranking")
        key_dupes = self.df.duplicated(["namespace", "code"])
        if key_dupes.any():
            raise ValueError("Ranking entries must be unique by (namespace, code)")
        if (self.df["rank"] < 0).any():
            raise ValueError("Ranking rank values must be zero-based non-negative integers")
        object.__setattr__(self, "df", self.df.loc[:, RANKING_COLUMNS].copy())

    def to_table(self) -> pd.DataFrame:
        """Return a copy of the validated ranking table."""

        return self.df.copy()


@dataclass(frozen=True)
class RankingSet:
    """Named collection of per-index rankings."""

    df: pd.DataFrame

    def __post_init__(self) -> None:
        _require_columns(self.df, RANKING_SET_COLUMNS, label="RankingSet")
        key_dupes = self.df.duplicated(["index", "namespace", "code"])
        if key_dupes.any():
            raise ValueError("RankingSet entries must be unique by (index, namespace, code)")
        if (self.df["rank"] < 0).any():
            raise ValueError("RankingSet rank values must be zero-based non-negative integers")
        object.__setattr__(self, "df", self.df.loc[:, RANKING_SET_COLUMNS].copy())

    def to_table(self) -> pd.DataFrame:
        """Return a copy of the validated ranking-set table."""

        return self.df.copy()


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

    frames: list[pd.DataFrame] = []
    for name, ranking in rankings.items():
        table = ranking.to_table()
        if table.empty:
            continue
        table.insert(0, "index", name)
        frames.append(table)
    if not frames:
        return RankingSet(pd.DataFrame(columns=RANKING_SET_COLUMNS))
    return RankingSet(pd.concat(frames, ignore_index=True))


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
    return Ranking(
        pd.DataFrame(
            [
                {
                    "namespace": rows[idx][0],
                    "code": rows[idx][1],
                    "rank": rank,
                    "score": score,
                }
                for idx, rank, score in ranked_rows
            ],
            columns=RANKING_COLUMNS,
        )
    )


def _validated_weights(weights: Mapping[str, float]) -> dict[str, float]:
    out: dict[str, float] = {}
    for name, weight in weights.items():
        value = float(weight)
        if not math.isfinite(value) or value < 0:
            raise ValueError("Ranker weights must be finite non-negative numbers")
        out[str(name)] = value
    return out


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
        table = rankings.to_table()
        if table.empty:
            return Ranking(pd.DataFrame(columns=RANKING_COLUMNS))
        table["_weight"] = table["index"].map(self.weights).fillna(1.0).astype(float)
        scored = (
            table.assign(_rrf=table["_weight"] / (self.k + table["rank"].astype(float) + 1.0))
            .groupby(["namespace", "code"], as_index=False)["_rrf"]
            .sum()
            .rename(columns={"_rrf": "score"})
            .reset_index(drop=True)
        )
        return ranking_from_scores(
            [(row.namespace, row.code, float(row.score)) for row in scored.itertuples()],
            limit=limit,
        )


@dataclass(frozen=True)
class MinMaxScoreFusion:
    """Weighted score fusion with per-index min-max normalization."""

    weights: Mapping[str, float] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "weights", _validated_weights(self.weights))

    def __call__(self, rankings: RankingSet, *, limit: int) -> Ranking:
        table = rankings.to_table()
        if table.empty:
            return Ranking(pd.DataFrame(columns=RANKING_COLUMNS))
        normalized: list[pd.DataFrame] = []
        for index_name, group in table.groupby("index", sort=False):
            min_score = float(group["score"].min())
            max_score = float(group["score"].max())
            if max_score == min_score:
                continue
            weight = float(self.weights.get(str(index_name), 1.0))
            normalized.append(group.assign(_score=((group["score"] - min_score) / (max_score - min_score)) * weight))
        if not normalized:
            return Ranking(pd.DataFrame(columns=RANKING_COLUMNS))
        scored = (
            pd.concat(normalized, ignore_index=True)
            .groupby(["namespace", "code"], as_index=False)["_score"]
            .sum()
            .rename(columns={"_score": "score"})
            .reset_index(drop=True)
        )
        return ranking_from_scores(
            [(row.namespace, row.code, float(row.score)) for row in scored.itertuples()],
            limit=limit,
        )


@dataclass(frozen=True)
class ZScoreFusion:
    """Weighted score fusion with per-index Z-score normalization."""

    weights: Mapping[str, float] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "weights", _validated_weights(self.weights))

    def __call__(self, rankings: RankingSet, *, limit: int) -> Ranking:
        table = rankings.to_table()
        if table.empty:
            return Ranking(pd.DataFrame(columns=RANKING_COLUMNS))
        normalized: list[pd.DataFrame] = []
        for index_name, group in table.groupby("index", sort=False):
            scores = group["score"].astype(float)
            mean = float(scores.mean())
            std = float(scores.std())
            norm_series = pd.Series(0.0, index=scores.index) if std == 0.0 or math.isnan(std) else (scores - mean) / std
            weight = float(self.weights.get(str(index_name), 1.0))
            normalized.append(group.assign(_score=norm_series * weight))
        if not normalized:
            return Ranking(pd.DataFrame(columns=RANKING_COLUMNS))
        scored = (
            pd.concat(normalized, ignore_index=True)
            .groupby(["namespace", "code"], as_index=False)["_score"]
            .sum()
            .rename(columns={"_score": "score"})
            .reset_index(drop=True)
        )
        return ranking_from_scores(
            [(row.namespace, row.code, float(row.score)) for row in scored.itertuples()],
            limit=limit,
        )


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
    "RANKING_COLUMNS",
    "RANKING_SET_COLUMNS",
    "MinMaxScoreFusion",
    "MinMaxScoreFusionSpec",
    "RRF",
    "RRFSpec",
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
