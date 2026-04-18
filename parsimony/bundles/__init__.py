"""Plugin-owned catalog bundle pipeline.

A bundle is a Parquet rowstore + FAISS vector index + JSON manifest packaged
under a single namespace and published to one HuggingFace dataset repo. The
package owns the entire lifecycle:

* Spec types plugins attach to ``@enumerator(catalog=...)``
  (:class:`CatalogSpec`, :class:`CatalogPlan`). Use ``CatalogSpec.static(namespace=...)``
  for the one-namespace-one-bundle pattern; ``CatalogSpec(plan=...)`` for
  dynamic plans that yield N items.
* The wire format (:class:`BundleManifest`, :data:`ENTRIES_PARQUET_SCHEMA`,
  filename + size constants).
* The async build orchestrator (:func:`~parsimony.bundles.build.build_bundle_dir`).
* The HF Hub publish target (:class:`~parsimony.bundles.targets.HFBundleTarget`).
* The read-side store (:class:`~parsimony.stores.hf_bundle.HFBundleCatalogStore`).
* Discovery and recall@K eval primitives.

Plan callables must be async generator functions (``async def`` returning
:class:`AsyncIterator` of :class:`CatalogPlan`). Sync sources should wrap
themselves with :func:`to_async` — the build pipeline does not branch on
async vs sync.
"""

from __future__ import annotations

from parsimony.bundles.discovery import (
    DiscoveredSpec,
    iter_specs,
)
from parsimony.bundles.errors import (
    BundleError,
    BundleIntegrityError,
    BundleNotFoundError,
    BundleSpecError,
    BundleStaleError,
    BundleTooLargeError,
)
from parsimony.bundles.eval import (
    EvalQuery,
    EvalResult,
    aggregate_recall,
    load_queries,
    per_query_failures,
    run_eval,
)
from parsimony.bundles.format import (
    BUNDLE_FILENAMES,
    ENTRIES_FILENAME,
    ENTRIES_PARQUET_SCHEMA,
    HF_ORG,
    INDEX_FILENAME,
    MANIFEST_FILENAME,
    MAX_INDEX_BYTES,
    MAX_MANIFEST_BYTES,
    MAX_PARQUET_BYTES,
    BundleManifest,
    hf_repo_id,
)
from parsimony.bundles.safety import (
    fetch_published_entry_count,
    shrink_guard,
)
from parsimony.bundles.spec import (
    DEFAULT_TARGET,
    CatalogPlan,
    CatalogSpec,
    PlanCallable,
    materialize,
    to_async,
)

__all__ = [
    "BUNDLE_FILENAMES",
    "BundleError",
    "BundleIntegrityError",
    "BundleManifest",
    "BundleNotFoundError",
    "BundleSpecError",
    "BundleStaleError",
    "BundleTooLargeError",
    "CatalogPlan",
    "CatalogSpec",
    "DEFAULT_TARGET",
    "DiscoveredSpec",
    "ENTRIES_FILENAME",
    "ENTRIES_PARQUET_SCHEMA",
    "EvalQuery",
    "EvalResult",
    "HF_ORG",
    "INDEX_FILENAME",
    "MANIFEST_FILENAME",
    "MAX_INDEX_BYTES",
    "MAX_MANIFEST_BYTES",
    "MAX_PARQUET_BYTES",
    "PlanCallable",
    "aggregate_recall",
    "fetch_published_entry_count",
    "hf_repo_id",
    "iter_specs",
    "load_queries",
    "materialize",
    "per_query_failures",
    "run_eval",
    "shrink_guard",
    "to_async",
]
