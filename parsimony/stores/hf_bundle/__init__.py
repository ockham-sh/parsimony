"""HFBundleCatalogStore package — thin barrel over the split modules.

Public API: :class:`HFBundleCatalogStore` and :class:`LoadedNamespace`. The
class lives in :mod:`.store`; helpers are split across
:mod:`.cache_layout`, :mod:`.hf_download`, :mod:`.integrity`.
"""

from __future__ import annotations

from parsimony.stores.hf_bundle.store import HFBundleCatalogStore, LoadedNamespace

__all__ = [
    "HFBundleCatalogStore",
    "LoadedNamespace",
]
