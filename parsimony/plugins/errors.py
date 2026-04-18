"""Plugin-discovery errors.

Separate from :mod:`parsimony.errors` because these surface at discovery time,
not during connector execution.
"""

from __future__ import annotations


class PluginError(Exception):
    """Base class for plugin discovery/loading errors."""


class PluginImportError(PluginError):
    """Raised when a plugin entry-point target cannot be imported."""

    def __init__(self, module_path: str, original: BaseException) -> None:
        self.module_path = module_path
        self.original = original
        super().__init__(f"Failed to import plugin module {module_path!r}: {original}")


class PluginContractError(PluginError):
    """Raised when a plugin module does not satisfy the export contract.

    The contract is documented in ``docs/plugin-contract.md``. In summary:
    the target module must export ``CONNECTORS: Connectors`` (required) and
    may export ``ENV_VARS: dict[str, str]`` and
    ``PROVIDER_METADATA: dict[str, Any]``.
    """

    def __init__(self, module_path: str, reason: str) -> None:
        self.module_path = module_path
        self.reason = reason
        super().__init__(f"Plugin {module_path!r} violates contract: {reason}")
