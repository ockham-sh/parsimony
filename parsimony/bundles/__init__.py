"""Plugin-owned catalog bundle pipeline.

A bundle is a Parquet rowstore + FAISS vector index + JSON manifest packaged
under a single namespace and published to one HuggingFace dataset repo.

Plugin authors import the decorator-facing primitives from this namespace:

* :class:`CatalogSpec` / :class:`CatalogPlan` — attach via ``@enumerator(catalog=...)``.
  Use ``CatalogSpec.static(namespace=...)`` for the one-namespace-one-bundle
  pattern; ``CatalogSpec(plan=...)`` for dynamic plans yielding N items.
* :func:`to_async` — wrap a sync source so the build pipeline does not branch.
* :class:`BundleError` / :class:`BundleNotFoundError` / :class:`BundleSpecError` —
  errors plugin code may catch. ``BundleNotFoundError`` is distinct because
  callers legitimately catch it as a "no bundle published yet" control-flow
  signal rather than a failure.

Everything else (format constants, discovery, eval, safety, targets) lives in
the submodules and is consumed by the kernel CLI directly.
"""

from __future__ import annotations

from parsimony.bundles.errors import BundleError, BundleNotFoundError, BundleSpecError
from parsimony.bundles.spec import CatalogPlan, CatalogSpec, to_async

__all__ = [
    "BundleError",
    "BundleNotFoundError",
    "BundleSpecError",
    "CatalogPlan",
    "CatalogSpec",
    "to_async",
]
