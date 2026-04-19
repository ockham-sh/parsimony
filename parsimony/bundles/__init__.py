"""Plugin-owned catalog bundle pipeline.

A bundle is a published snapshot of a :class:`~parsimony.Catalog` under one
namespace. The on-disk layout (``meta.json`` + ``entries.parquet`` +
``embeddings.faiss``) is owned by the standard catalog implementation; this
package provides the **declarative layer** plugin authors use to attach a
publishable catalog to their ``@enumerator`` functions.

Public surface:

* :class:`CatalogSpec` / :class:`CatalogPlan` — attach via ``@enumerator(catalog=...)``.
  Use :meth:`CatalogSpec.static` for the one-namespace-one-bundle pattern;
  ``CatalogSpec(plan=...)`` for dynamic plans yielding N items.
* :func:`to_async` — wrap a sync source so the build pipeline does not branch.
* :class:`LazyNamespaceCatalog` — opt-in wrapper that populates missing
  namespaces by fetching a published bundle or falling back to a live
  enumerator. Composes any :class:`~parsimony.BaseCatalog`.

The one bundle-specific error, :class:`~parsimony.BundleNotFoundError`,
lives on :mod:`parsimony.errors` with the other public exceptions.
"""

from __future__ import annotations

from parsimony.bundles.spec import CatalogPlan, CatalogSpec, to_async

__all__ = [
    "CatalogPlan",
    "CatalogSpec",
    "to_async",
]
