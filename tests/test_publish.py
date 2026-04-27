"""Tests for :func:`parsimony.publish.publish` phase-separated pipeline.

The publisher runs in two phases per batch:

* **Phase 1** — up to ``fetch_concurrency`` parallel enumerators write
  their :class:`Result` to a parquet on disk via :meth:`Result.to_parquet`
  and drop the in-memory DataFrame. After ``gather`` returns, the parent
  holds zero ``Result`` references — only file paths.
* **Phase 2** — strictly sequential embed/index/push. No fetch subprocess
  is alive while a Catalog is in flight, so a mega-flow embed peak cannot
  collide with subprocess RAM (which is what previously OOM-killed the
  host on ESTAT mega-flows).

These tests cover the contracts:

* ``fetch_concurrency=1`` reproduces the strictly-sequential baseline.
* ``fetch_concurrency=2`` correctly publishes a mix of slow and fast
  enumerators and the ephemeral staging dir is cleaned up on exit.
* A single producer-side ``ValidationError`` does not stop the rest
  of the batch; it lands in :attr:`PublishReport.failed`.
* An explicit ``staging_dir`` is caller-owned (never deleted by publish).
* Phase 2 (embed) never overlaps with Phase 1 (fetch) — ``gather``
  completes before the for-loop starts.
"""

from __future__ import annotations

import asyncio
import tempfile
import time
from pathlib import Path
from types import ModuleType
from typing import Any
from unittest.mock import patch

import pandas as pd
from pydantic import ValidationError

from parsimony.publish import publish
from parsimony.result import Column, ColumnRole, OutputConfig, Provenance, Result


def _module(**attrs: Any) -> ModuleType:
    """Build a throw-away module object with the given plugin surface."""
    m = ModuleType("fake_publish_plugin")
    for k, v in attrs.items():
        setattr(m, k, v)
    return m


def _make_result(ns: str, n: int = 3) -> Result:
    """Build a tiny Result with a KEY column scoped to *ns*.

    ``namespace=ns`` on the KEY column lets ``Catalog.add_from_result``
    honor the per-flow namespace from the schema (which is what the
    real connectors do).
    """
    df = pd.DataFrame(
        {
            "code": [f"item_{i}" for i in range(n)],
            "title": [f"Title {ns} {i}" for i in range(n)],
        }
    )
    cfg = OutputConfig(
        columns=[
            Column(name="code", role=ColumnRole.KEY, namespace=ns),
            Column(name="title", role=ColumnRole.TITLE),
        ]
    )
    return Result(data=df, output_schema=cfg, provenance=Provenance(source="test"))


def _async_fn(ns: str, *, delay: float = 0.0):
    """Return an async callable that yields a Result for *ns* after *delay*."""

    async def _impl() -> Result:
        if delay:
            await asyncio.sleep(delay)
        return _make_result(ns)

    _impl.__name__ = f"enumerate_{ns}"
    return _impl


# ---------------------------------------------------------------------------
# K=1 reproduces sequential baseline
# ---------------------------------------------------------------------------


async def test_k1_publishes_all_in_input_order(tmp_path: Path) -> None:
    target = f"file://{tmp_path}/{{namespace}}"
    namespaces = ["alpha", "beta", "gamma"]
    mod = _module(CATALOGS=[(ns, _async_fn(ns)) for ns in namespaces])

    report = await publish(mod, target=target, fetch_concurrency=1)

    assert report.published == namespaces  # input order preserved at K=1
    assert report.skipped == []
    assert report.failed == []
    # Each namespace produced an on-disk catalog
    for ns in namespaces:
        assert (tmp_path / ns / "meta.json").exists()


# ---------------------------------------------------------------------------
# K=2 with mixed delays publishes everything; ephemeral staging cleaned up
# ---------------------------------------------------------------------------


async def test_k2_publishes_mixed_delays_and_cleans_ephemeral_staging(
    tmp_path: Path,
) -> None:
    target = f"file://{tmp_path}/{{namespace}}"
    # slow first forces 'fast' to overtake when K=2
    mod = _module(
        CATALOGS=[
            ("slow_a", _async_fn("slow_a", delay=0.20)),
            ("fast_b", _async_fn("fast_b", delay=0.0)),
            ("slow_c", _async_fn("slow_c", delay=0.20)),
        ]
    )

    # Snapshot tmp before so we can verify nothing is left behind
    tmproot = Path(tempfile.gettempdir())
    before = {p.name for p in tmproot.iterdir() if p.name.startswith("parsimony-publish-")}

    report = await publish(mod, target=target, fetch_concurrency=2)

    assert sorted(report.published) == ["fast_b", "slow_a", "slow_c"]
    assert report.failed == []
    for ns in ("slow_a", "fast_b", "slow_c"):
        assert (tmp_path / ns / "meta.json").exists()

    # Ephemeral staging dir is wiped on context exit
    after = {p.name for p in tmproot.iterdir() if p.name.startswith("parsimony-publish-")}
    assert (after - before) == set(), f"ephemeral staging leaked: {after - before}"


# ---------------------------------------------------------------------------
# Producer-side ValidationError isolated; rest of batch still publishes
# ---------------------------------------------------------------------------


async def test_validation_error_in_one_flow_does_not_abort_batch(tmp_path: Path) -> None:
    async def _bad() -> Result:
        # Real pydantic ValidationError — exact same path as ESTAT $DV_*
        # rejections in the SDMX connector. Missing required ``name``
        # field on Column triggers the expected ValidationError.
        Column.model_validate({"role": "key"})
        raise AssertionError("unreachable")  # pragma: no cover

    target = f"file://{tmp_path}/{{namespace}}"
    mod = _module(
        CATALOGS=[
            ("ok_first", _async_fn("ok_first")),
            ("bad", _bad),
            ("ok_last", _async_fn("ok_last")),
        ]
    )

    report = await publish(mod, target=target, fetch_concurrency=2)

    assert sorted(report.published) == ["ok_first", "ok_last"]
    assert len(report.failed) == 1
    assert report.failed[0][0] == "bad"
    # The recorded message preserves the underlying pydantic error.
    assert "validation error" in report.failed[0][1].lower()


# ---------------------------------------------------------------------------
# Explicit staging_dir is caller-owned (not wiped)
# ---------------------------------------------------------------------------


async def test_explicit_staging_dir_persists_after_run(tmp_path: Path) -> None:
    staging = tmp_path / "my_staging"
    target = f"file://{tmp_path}/out/{{namespace}}"
    mod = _module(CATALOGS=[("only", _async_fn("only"))])

    report = await publish(
        mod,
        target=target,
        fetch_concurrency=1,
        staging_dir=staging,
    )

    assert report.published == ["only"]
    # Caller-owned staging dir is never removed by the publisher. The
    # parquet inside is unlinked post-consume; the directory itself
    # survives so a future --resume can use it.
    assert staging.exists() and staging.is_dir()
    # Per-flow parquet was cleaned after successful consume
    assert not (staging / "only.parquet").exists()


# ---------------------------------------------------------------------------
# Phase 1 (fetch) finishes before Phase 2 (embed) starts
# ---------------------------------------------------------------------------


async def test_phase_separation_no_fetch_during_embed(tmp_path: Path) -> None:
    """The for-loop must only start after every fetch has staged its parquet.

    Records ``time.monotonic()`` snapshots:
      * ``fetch_done_ts[ns]`` — set by each fetch right before returning.
      * ``embed_started_ts`` — first observed call to ``Catalog.add_from_result``.

    Phase separation ⇒ ``max(fetch_done_ts.values()) <= embed_started_ts``.
    A regression to a pipelined design would let the embed start before
    the slowest fetch finishes, breaking this invariant.
    """
    fetch_done_ts: dict[str, float] = {}
    embed_started_ts: list[float] = []

    def _fn_with_timing(ns: str, *, delay: float):
        async def _impl():
            await asyncio.sleep(delay)
            result = _make_result(ns)
            fetch_done_ts[ns] = time.monotonic()
            return result

        return _impl

    # Wrap Catalog.add_from_result to capture the first-call timestamp.
    from parsimony.catalog import Catalog as _Catalog

    real_add_from_result = _Catalog.add_from_result

    async def _spy_add_from_result(self, *args, **kwargs):  # noqa: ANN001, ANN002, ANN003
        embed_started_ts.append(time.monotonic())
        return await real_add_from_result(self, *args, **kwargs)

    target = f"file://{tmp_path}/{{namespace}}"
    # Mix of delays so the gather has a real spread to observe.
    mod = _module(
        CATALOGS=[
            ("slow", _fn_with_timing("slow", delay=0.30)),
            ("medium", _fn_with_timing("medium", delay=0.15)),
            ("fast", _fn_with_timing("fast", delay=0.05)),
        ]
    )

    with patch.object(_Catalog, "add_from_result", _spy_add_from_result):
        report = await publish(mod, target=target, fetch_concurrency=2)

    assert sorted(report.published) == ["fast", "medium", "slow"]
    assert len(fetch_done_ts) == 3
    assert len(embed_started_ts) == 3

    last_fetch = max(fetch_done_ts.values())
    first_embed = min(embed_started_ts)
    # Strict invariant: phase 2 cannot start before phase 1 finishes.
    assert first_embed >= last_fetch, (
        f"embed started at {first_embed:.3f} before last fetch finished at "
        f"{last_fetch:.3f}; phase separation broken"
    )
