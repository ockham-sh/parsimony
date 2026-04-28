"""Publish catalog snapshots from plugin modules.

A plugin declares its catalogs by exporting one of two shapes on the
module:

* ``CATALOGS: list[tuple[str, Callable[[], Awaitable[Result]]]]`` — static
  list of ``(namespace, async_fn)`` pairs. Use when the namespace set is
  known at import time (e.g. FRED publishes one catalog called ``fred``).
* ``async def CATALOGS() -> AsyncIterator[tuple[str, Callable[[], Awaitable[Result]]]]``
  — async generator. Use when namespaces are fetched at build time (e.g.
  SDMX discovers agencies/dataflows on the wire).

An optional ``RESOLVE_CATALOG(namespace: str) -> Callable | None`` function
produces a single catalog on demand — used by ``--only`` when the caller
knows the namespace without iterating the full generator.

The publisher runs each enumerator once per namespace, ingests rows into a
fresh :class:`~parsimony.Catalog`, and pushes to
``target_template.format(namespace=...)``. No resume, no manifest, no
content hashing — if a publish fails, re-run it.
"""

from __future__ import annotations

import asyncio
import contextlib
import ctypes
import ctypes.util
import gc
import inspect
import logging
import os
import shutil
import sys
import tempfile
import time
from collections.abc import AsyncIterator, Awaitable, Callable, Iterable, Iterator, Mapping
from dataclasses import dataclass
from pathlib import Path
from types import ModuleType
from typing import Any

from pydantic import ValidationError

from parsimony.catalog import Catalog
from parsimony.discover import Provider, iter_providers
from parsimony.embedder import EmbeddingProvider, FragmentEmbeddingCache
from parsimony.result import Result

logger = logging.getLogger(__name__)

__all__ = [
    "PublishReport",
    "collect_catalogs",
    "publish",
    "publish_provider",
]


CatalogFn = Callable[[], Awaitable[Result]]
CatalogEntry = tuple[str, CatalogFn]


@dataclass(frozen=True)
class PublishReport:
    """Outcome of a publish run for one provider."""

    provider: str
    target_template: str
    published: list[str]  # namespaces successfully published
    skipped: list[str]  # namespaces that had no rows
    failed: list[tuple[str, str]]  # (namespace, error message)

    @property
    def ok(self) -> bool:
        return not self.failed


# ---------------------------------------------------------------------------
# Collect catalogs from a plugin module
# ---------------------------------------------------------------------------


async def collect_catalogs(
    module: ModuleType,
    *,
    only: Iterable[str] | None = None,
) -> list[CatalogEntry]:
    """Resolve a plugin module's declared catalogs.

    Accepts the following shapes on *module*:

    * ``CATALOGS = [("ns", fn), ...]`` — static list.
    * ``async def CATALOGS(): yield "ns", fn`` — async generator function.
    * ``RESOLVE_CATALOG(namespace) -> Callable | None`` — on-demand lookup.

    Dispatch rules:

    * When *only* is ``None``: iterate all of ``CATALOGS`` to fan out the
      full publish set. ``RESOLVE_CATALOG`` is unused (the caller didn't
      name anything specific).
    * When *only* is a set: try ``RESOLVE_CATALOG`` FIRST for each
      requested namespace. Only fall back to walking ``CATALOGS`` for
      namespaces the resolver didn't recognise. This matters when
      ``CATALOGS`` itself is expensive (e.g. a plugin that live-queries
      an upstream API to enumerate what exists) — a targeted
      ``--only ns`` shouldn't pay the full fan-out cost if the plugin
      can short-circuit the lookup.
    """
    wanted = None if only is None else {n for n in only}
    catalogs: list[CatalogEntry] = []
    seen: set[str] = set()

    resolve = getattr(module, "RESOLVE_CATALOG", None)
    if wanted is not None and resolve is not None:
        for ns in wanted:
            fn = resolve(ns)
            if fn is not None:
                catalogs.append((ns, fn))
                seen.add(ns)
        # Everything the caller asked for resolved directly — skip the
        # CATALOGS walk entirely. This is the common case when a plugin
        # pairs a targetable RESOLVE_CATALOG with an expensive CATALOGS.
        if wanted == seen:
            return catalogs

    raw = getattr(module, "CATALOGS", None)
    if raw is not None:
        async for ns, fn in _iter_catalogs(raw):
            if ns in seen:
                continue
            if wanted is not None and ns not in wanted:
                continue
            catalogs.append((ns, fn))
            seen.add(ns)

    return catalogs


async def _iter_catalogs(raw: Any) -> AsyncIterator[CatalogEntry]:
    """Yield (namespace, fn) pairs from whatever shape *raw* takes."""
    if inspect.isasyncgenfunction(raw):
        async for item in raw():
            yield _validate_entry(item)
        return

    if inspect.isasyncgen(raw):
        async for item in raw:
            yield _validate_entry(item)
        return

    if callable(raw):
        result = raw()
        if inspect.isasyncgen(result):
            async for item in result:
                yield _validate_entry(item)
            return
        if inspect.isawaitable(result):
            result = await result
        if isinstance(result, Iterable):
            for item in result:
                yield _validate_entry(item)
            return
        raise TypeError(
            f"CATALOGS callable must return an async iterator or iterable of (ns, fn); got {type(result).__name__}"
        )

    if isinstance(raw, Iterable):
        for item in raw:
            yield _validate_entry(item)
        return

    raise TypeError(
        f"CATALOGS must be an iterable of (ns, fn) or an async generator function; got {type(raw).__name__}"
    )


def _validate_entry(item: Any) -> CatalogEntry:
    if not isinstance(item, tuple) or len(item) != 2:
        raise TypeError(f"CATALOGS entry must be a (namespace, fn) tuple; got {item!r}")
    ns, fn = item
    if not isinstance(ns, str) or not ns:
        raise ValueError(f"CATALOGS namespace must be a non-empty string; got {ns!r}")
    if not callable(fn):
        raise TypeError(f"CATALOGS fn for namespace {ns!r} must be callable; got {type(fn).__name__}")
    return ns, fn


# ---------------------------------------------------------------------------
# Bind connector-style dependencies from environment variables
# ---------------------------------------------------------------------------


def _bind_fn(fn: CatalogFn, env: Mapping[str, str] | None) -> CatalogFn:
    """Bind environment-sourced deps to *fn* if it's a :class:`~parsimony.Connector`.

    The connector's decorator-declared ``env_map`` drives binding. Plain
    async functions pass through unchanged — the plugin author is expected
    to have captured credentials via closure or module state.
    """
    from parsimony.connector import Connector, Connectors

    if not isinstance(fn, Connector):
        return fn
    if not fn.env_map:
        return fn
    single = Connectors([fn]).bind_env(env)
    return single[fn.name]


# ---------------------------------------------------------------------------
# Core publish primitives
# ---------------------------------------------------------------------------


async def publish(
    module: ModuleType,
    *,
    target: str,
    only: Iterable[str] | None = None,
    dry_run: bool = False,
    env: Mapping[str, str] | None = None,
    provider_name: str | None = None,
    embedder: EmbeddingProvider | None = None,
    fragment_cache: FragmentEmbeddingCache | None = None,
    fetch_concurrency: int = 1,
    staging_dir: Path | None = None,
) -> PublishReport:
    """Publish every declared catalog in *module* to ``target.format(namespace=...)``.

    Parameters
    ----------
    module:
        The plugin module exporting ``CATALOGS`` (and optionally
        ``RESOLVE_CATALOG``).
    target:
        URL template — must contain ``{namespace}``. Examples:
        ``"file:///tmp/catalogs/{namespace}"``, ``"hf://ockham/catalog-{namespace}"``.
    only:
        When set, limit the publish to these namespaces.
    dry_run:
        Collect catalogs and log targets, but skip enumeration + push.
    env:
        Override dict layered on top of ``os.environ``; passed to
        :meth:`Connectors.bind_env` for catalog entries that are connectors.
    provider_name:
        Label used in the report; defaults to ``module.__name__``.
    embedder:
        Override the default :class:`SentenceTransformerEmbedder`. Use this
        to plug in :class:`OnnxEmbedder` (int8, 2-3× faster on CPU) or a
        custom provider. Shared across every catalog in the batch — the
        model loads once and amortizes over the whole publish run.
    fragment_cache:
        Optional :class:`~parsimony.FragmentEmbeddingCache` wired onto
        every catalog in the batch. Providers whose enumerators declare
        a ``FRAGMENTS`` column use the compose path (mean-pool of
        per-fragment vectors) instead of embedding the full
        :meth:`SeriesEntry.semantic_text`. Sharing one cache across the
        batch lets the second dataflow reuse vectors embedded while
        processing the first.
    fetch_concurrency:
        Number of catalog enumerators allowed to run in parallel. Default
        1 reproduces the strictly-sequential path. Values >1 spawn a
        bounded pool of fetch tasks; each writes its :class:`Result` to
        a parquet on disk via :meth:`Result.to_parquet` and exits memory.
        A single consumer drains the staging dir one flow at a time and
        runs the existing embed/index/push path. Memory profile in the
        parent is unchanged because at most one ``Result`` is hydrated
        at a time. Useful when fetch is network-bound (e.g. SDMX).
    staging_dir:
        Where parquet handoff files live. ``None`` (default) creates an
        ephemeral ``tempfile.mkdtemp`` that is wiped on exit — correct
        for one-shot REPL or library use. Passing an explicit path makes
        the staging dir persistent: a future enhancement can resume by
        skipping fetches whose staged parquet already exists.
    """
    if "{namespace}" not in target:
        raise ValueError(f"target {target!r} must contain '{{namespace}}'")

    report_name = provider_name or module.__name__

    catalogs = await collect_catalogs(module, only=only)
    published: list[str] = []
    skipped: list[str] = []
    failed: list[tuple[str, str]] = []

    total = len(catalogs)
    if dry_run:
        for idx, (namespace, _fn) in enumerate(catalogs, start=1):
            logger.info(
                "[%d/%d] publishing %s/%s → %s",
                idx,
                total,
                report_name,
                namespace,
                target.format(namespace=namespace),
            )
            published.append(namespace)
        return PublishReport(
            provider=report_name,
            target_template=target,
            published=published,
            skipped=skipped,
            failed=failed,
        )

    # Phase-separated fetch/embed:
    #
    # * Phase 1 — up to ``fetch_concurrency`` enumerators run in parallel.
    #   Each writes its ``Result`` to a parquet in the staging dir via
    #   ``Result.to_parquet`` (which embeds provenance + output_schema
    #   in the Arrow table metadata), then drops the in-memory ``Result``.
    #   When ``gather`` returns, every flow is on disk and the parent
    #   holds only ``(namespace, path)`` tuples — no ``DataFrame`` and
    #   no live subprocess.
    # * Phase 2 — strictly sequential embed/index/push. With Phase 1
    #   already done, no fetch subprocess is alive while a Catalog is
    #   in flight. The mega-flow embed peak (~25 GB on Spain-class
    #   ESTAT flows) cannot collide with subprocess memory pressure.
    #
    # ``MemoryError`` is intentionally NOT caught: if a single flow
    # doesn't fit, propagate so the wrapper recycles the process and
    # ``--resume`` picks up the rest.
    sem = asyncio.Semaphore(max(1, fetch_concurrency))

    async def _fetch_one(
        namespace: str,
        fn: CatalogFn,
        staging: Path,
    ) -> tuple[str, Path | None, str | None]:
        """Fetch + stage one catalog. Returns ``(ns, path|None, err|None)``."""
        async with sem:
            # ``starting`` log is emitted only after the semaphore is
            # acquired, so at most ``fetch_concurrency`` "starting"
            # lines appear without matching "done" lines at any time —
            # an honest progress signal during Phase 1.
            t0 = time.monotonic()
            logger.info("[fetch] %s/%s starting", report_name, namespace)
            try:
                result = await _invoke(_bind_fn(fn, env))
            except ValidationError as exc:
                # Input-shape rejection (e.g. ESTAT $DV_* pseudo-flows
                # that slipped past upstream filtering): single-line log,
                # no traceback. Surface in the report under ``failed``.
                logger.warning(
                    "[fetch] %s/%s skipped: invalid input — %s",
                    report_name,
                    namespace,
                    _summarize_validation_error(exc),
                )
                return namespace, None, str(exc)
            except Exception as exc:
                logger.exception("[fetch] %s/%s failed", report_name, namespace)
                return namespace, None, str(exc)

            path = staging / f"{namespace}.parquet"
            try:
                await asyncio.to_thread(result.to_parquet, path)
            except Exception as exc:
                logger.exception("[fetch] %s/%s staging failed", report_name, namespace)
                return namespace, None, f"staging: {exc}"
            finally:
                # Drop the in-memory Result before releasing the
                # semaphore so the next fetcher doesn't run alongside
                # a still-resident DataFrame.
                del result
                _release_memory()

            elapsed = time.monotonic() - t0
            try:
                size_mb = path.stat().st_size / (1024 * 1024)
            except OSError:
                size_mb = 0.0
            logger.info(
                "[fetch] %s/%s done (%.1fs, %.0f MB staged)",
                report_name,
                namespace,
                elapsed,
                size_mb,
            )
            return namespace, path, None

    log_rss = os.environ.get("PARSIMONY_LOG_RSS") == "1"
    rss_reader = _make_rss_reader() if log_rss else None

    with _staging_root(staging_dir) as staging:
        # PHASE 1 — parallel fetch (K-bounded). After ``gather`` returns,
        # every Result is on disk and the parent holds only file paths.
        logger.info(
            "=== fetch phase: %d flows, K=%d ===",
            total,
            max(1, fetch_concurrency),
        )
        phase1_t0 = time.monotonic()
        staged = await asyncio.gather(*[_fetch_one(ns, fn, staging) for ns, fn in catalogs])
        n_staged = sum(1 for _, p, _ in staged if p is not None)
        n_failed = total - n_staged
        logger.info(
            "=== fetch phase done: %d/%d staged in %.1fs (%d failed) ===",
            n_staged,
            total,
            time.monotonic() - phase1_t0,
            n_failed,
        )

        # PHASE 2 — strictly sequential embed/index/push. No fetch is
        # running. Parent peak = 1 hydrated Result + 1 Catalog, exactly
        # as in the K=1 baseline.
        logger.info("=== publish phase: indexing %d staged flows ===", n_staged)
        for processed, (namespace, parquet_path, err) in enumerate(staged, start=1):
            if err is not None:
                failed.append((namespace, err))
                continue
            assert parquet_path is not None  # _fetch_one invariant: (path, err) — exactly one is None
            url = target.format(namespace=namespace)
            logger.info("[%d/%d] publishing %s/%s", processed, total, report_name, namespace)
            catalog: Catalog | None = None
            try:
                t_load = time.monotonic()
                try:
                    result = await asyncio.to_thread(Result.from_parquet, parquet_path)
                except Exception as exc:
                    logger.exception("publish failed for %s/%s (parquet load)", report_name, namespace)
                    failed.append((namespace, f"parquet load: {exc}"))
                    continue
                logger.info(
                    "[%d/%d] %s/%s parquet load: %.2fs",
                    processed,
                    total,
                    report_name,
                    namespace,
                    time.monotonic() - t_load,
                )

                try:
                    catalog = Catalog(namespace, embedder=embedder, fragment_cache=fragment_cache)
                    t_ingest = time.monotonic()
                    index = await catalog.add_from_result(result)
                    logger.info(
                        "[%d/%d] %s/%s add_from_result: indexed=%d skipped=%d in %.2fs",
                        processed,
                        total,
                        report_name,
                        namespace,
                        index.indexed,
                        index.skipped,
                        time.monotonic() - t_ingest,
                    )
                    # Drop the staged DataFrame before push so the upload
                    # doesn't carry it.
                    del result
                    if index.indexed == 0 and index.total == 0:
                        skipped.append(namespace)
                        continue
                    t_push = time.monotonic()
                    await catalog.push(url)
                    logger.info(
                        "[%d/%d] %s/%s push: %.2fs",
                        processed,
                        total,
                        report_name,
                        namespace,
                        time.monotonic() - t_push,
                    )
                    published.append(namespace)
                except Exception as exc:  # broad — one bad flow shouldn't abort the batch
                    logger.exception("publish failed for %s/%s", report_name, namespace)
                    failed.append((namespace, str(exc)))
            finally:
                # Deterministic per-flow release: drop in-memory indices,
                # delete the staged parquet, then ``gc.collect()`` +
                # ``malloc_trim``. ``MALLOC_ARENA_MAX`` in the wrapper
                # environment is what makes ``malloc_trim`` actually
                # reclaim across all arenas.
                if catalog is not None:
                    catalog.release_index()
                parquet_path.unlink(missing_ok=True)
                _release_memory()
                # Persist the fragment cache after every flow so a kill
                # (OOM, SIGKILL, host crash) cannot drop the in-memory
                # vectors accumulated across this batch. Atomic write
                # under the hood — safe to call on every iteration.
                if fragment_cache is not None:
                    try:
                        fragment_cache.persist()
                    except Exception:
                        logger.exception(
                            "[%d/%d] %s/%s fragment_cache.persist failed",
                            processed,
                            total,
                            report_name,
                            namespace,
                        )
                    stats = fragment_cache.stats()
                    refs = stats["hits"] + stats["misses"]
                    hit_rate = stats["hits"] / refs if refs else 0.0
                    logger.info(
                        "[%d/%d] %s/%s cache: hits=%d misses=%d unique=%d hit_rate=%.1f%%",
                        processed,
                        total,
                        report_name,
                        namespace,
                        stats["hits"],
                        stats["misses"],
                        stats["unique_fragments"],
                        hit_rate * 100,
                    )
                if rss_reader is not None:
                    rss_gb = rss_reader()
                    if rss_gb is not None:
                        logger.info(
                            "[%d/%d] %s/%s done — rss=%.2f GB",
                            processed,
                            total,
                            report_name,
                            namespace,
                            rss_gb,
                        )

    return PublishReport(
        provider=report_name,
        target_template=target,
        published=published,
        skipped=skipped,
        failed=failed,
    )


@contextlib.contextmanager
def _staging_root(staging_dir: Path | None) -> Iterator[Path]:
    """Yield a directory for parquet staging.

    ``None`` → ephemeral ``tempfile.mkdtemp`` wiped on context exit (the
    library default; correct for one-shot REPL or test use). An explicit
    ``Path`` is treated as caller-owned: the directory is created if
    missing but never deleted, so a crash mid-publish leaves staged
    parquets for a future ``--resume`` to skip.
    """
    if staging_dir is None:
        td = Path(tempfile.mkdtemp(prefix="parsimony-publish-"))
        try:
            yield td
        finally:
            shutil.rmtree(td, ignore_errors=True)
    else:
        staging_dir.mkdir(parents=True, exist_ok=True)
        yield staging_dir


def _resolve_malloc_trim() -> ctypes._FuncPointer | None:
    """Bind ``malloc_trim`` from libc, or return None on non-glibc systems."""
    if not sys.platform.startswith("linux"):
        return None
    libc_name = ctypes.util.find_library("c")
    if libc_name is None:
        return None
    try:
        libc = ctypes.CDLL(libc_name, use_errno=False)
    except OSError:
        return None
    fn = getattr(libc, "malloc_trim", None)
    if fn is None:
        return None
    fn.argtypes = [ctypes.c_size_t]
    fn.restype = ctypes.c_int
    return fn  # type: ignore[no-any-return]  # ctypes getattr returns Any


_MALLOC_TRIM = _resolve_malloc_trim()


def _release_memory() -> None:
    """Force a Python GC pass and ask glibc to return arenas to the OS.

    CPython does not return free memory to the OS — glibc's allocator
    keeps freed arenas in its own pool. For a long-running publish that
    processes flows of hugely variable size (ESTAT has sub-1 k series
    flows interleaved with 800 k+ series flows), the high-water mark
    only ever grows. After processing one of the giants, the parent's
    RSS stays near its peak even though the catalog object is gone.

    Calling ``malloc_trim(0)`` after each flow asks glibc to release
    unused arenas back to the OS. Combined with an explicit
    ``gc.collect()`` this brings RSS back down to baseline between
    flows and prevents the slow OOM-by-fragmentation that kills WSL
    hosts. Linux/glibc-specific; other platforms get the GC pass only.

    The wrapper script must export ``MALLOC_ARENA_MAX=2`` for this to
    actually reclaim — otherwise glibc creates per-thread arenas that
    ``malloc_trim`` cannot reach. Errors from libc are swallowed —
    this is a best-effort hint, not a correctness primitive.
    """
    gc.collect()
    if _MALLOC_TRIM is None:
        return
    try:
        _MALLOC_TRIM(0)
    except Exception:  # pragma: no cover — defensive only
        logger.debug("malloc_trim failed; ignoring", exc_info=True)


def _make_rss_reader() -> Callable[[], float | None] | None:
    """Return a callable that yields current process RSS in GB, or None.

    Tries ``psutil`` first (cross-platform); falls back to ``/proc/self/status``
    on Linux. Returns ``None`` from the reader on failure rather than raising.
    """
    try:
        import psutil

        proc = psutil.Process()

        def _read_psutil() -> float | None:
            try:
                return float(proc.memory_info().rss) / 1e9
            except Exception:  # pragma: no cover — defensive
                return None

        return _read_psutil
    except ImportError:
        pass

    if not sys.platform.startswith("linux"):
        return None

    def _read_proc() -> float | None:
        try:
            with open("/proc/self/status") as fh:
                for line in fh:
                    if line.startswith("VmRSS:"):
                        kb = int(line.split()[1])
                        return kb / (1024 * 1024)
        except OSError:
            return None
        return None

    return _read_proc


def _summarize_validation_error(exc: ValidationError) -> str:
    """Render a pydantic ValidationError as a single short string.

    The default ``str(exc)`` produces a multi-line dump that ends in a
    "For further information visit https://errors.pydantic.dev/..." footer
    — fine for interactive debugging, terrible for batch logs. This pulls
    the first error's ``loc`` + ``msg`` + ``input`` (truncated) into one
    grep-friendly line.
    """
    errors = exc.errors()
    if not errors:
        return "validation failed"
    first = errors[0]
    loc = ".".join(str(p) for p in first.get("loc", ()))
    msg = first.get("msg", "validation failed")
    inp = first.get("input")
    extra = f" (got {inp!r})" if isinstance(inp, str) and len(inp) <= 80 else ""
    return f"{loc}: {msg}{extra}"


async def _invoke(fn: CatalogFn) -> Result:
    """Call *fn* with no arguments; if the callable is a Connector, use its empty params."""
    from parsimony.connector import Connector

    if isinstance(fn, Connector):
        return await fn(fn.param_type())
    raw: Any = fn()
    resolved = await raw if inspect.isawaitable(raw) else raw
    if not isinstance(resolved, Result):
        raise TypeError(f"catalog callable must return Result; got {type(resolved).__name__}")
    return resolved


# ---------------------------------------------------------------------------
# Discovery-integrated entry point (used by the CLI)
# ---------------------------------------------------------------------------


async def publish_provider(
    provider_name: str,
    *,
    target: str,
    only: Iterable[str] | None = None,
    dry_run: bool = False,
    env: Mapping[str, str] | None = None,
    embedder: EmbeddingProvider | None = None,
    fragment_cache: FragmentEmbeddingCache | None = None,
    fetch_concurrency: int = 1,
    staging_dir: Path | None = None,
) -> PublishReport:
    """Publish *provider_name*'s catalogs — resolves the module via discovery."""
    import importlib

    provider = _find_provider(provider_name)
    module = importlib.import_module(provider.module_path)
    # Trigger the contract check: p.load() raises TypeError if CONNECTORS is missing.
    provider.load()
    return await publish(
        module,
        target=target,
        only=only,
        dry_run=dry_run,
        env=env,
        provider_name=provider.name,
        embedder=embedder,
        fragment_cache=fragment_cache,
        fetch_concurrency=fetch_concurrency,
        staging_dir=staging_dir,
    )


def _find_provider(name: str) -> Provider:
    providers = list(iter_providers())
    for p in providers:
        if p.name == name:
            return p
    available = sorted(p.name for p in providers)
    raise ValueError(f"no parsimony provider named {name!r}. Available: {available}")


# ---------------------------------------------------------------------------
# Sync CLI entry point
# ---------------------------------------------------------------------------


def run_cli(
    *,
    provider: str,
    target: str,
    only: Iterable[str] | None = None,
    dry_run: bool = False,
) -> int:
    """CLI-friendly wrapper: returns process exit code."""
    try:
        report = asyncio.run(publish_provider(provider, target=target, only=only, dry_run=dry_run))
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    for ns in report.published:
        print(f"  published: {ns}")
    for ns in report.skipped:
        print(f"  skipped (no rows): {ns}")
    for ns, err in report.failed:
        print(f"  FAILED: {ns}: {err}", file=sys.stderr)
    return 0 if report.ok else 1
