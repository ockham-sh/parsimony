"""Plugin-discovery errors.

Separate from :mod:`parsimony.errors` because these surface at discovery time,
not during connector execution.

All plugin-contract failures — discovery-time (:class:`PluginImportError`,
:class:`PluginContractError`) and runtime conformance
(:class:`~parsimony.testing.ConformanceError`) — expose the same
``to_report_dict()`` shape so the CLI builds a uniform JSON report
regardless of which class raised.
"""

from __future__ import annotations

from typing import Any


class PluginError(Exception):
    """Base class for plugin discovery/loading errors.

    Subclasses set :attr:`check` so downstream renderers can group errors
    by contract aspect; :attr:`next_action` is surfaced next to the error
    in operator output.
    """

    check: str = "plugin"

    def __init__(
        self,
        message: str,
        *,
        module_path: str | None = None,
        reason: str | None = None,
        next_action: str | None = None,
    ) -> None:
        super().__init__(message)
        self.module_path = module_path
        self.reason = reason or message
        self.next_action = next_action

    def to_report_dict(self) -> dict[str, Any]:
        """Structured fields for JSON-report consumers (CLI, CI)."""
        return {
            "check": self.check,
            "module_path": self.module_path,
            "reason": self.reason,
            "next_action": self.next_action,
        }


class PluginImportError(PluginError):
    """Raised when a plugin entry-point target cannot be imported."""

    check = "import"

    def __init__(self, module_path: str, original: BaseException) -> None:
        self.original = original
        super().__init__(
            f"Failed to import plugin module {module_path!r}: {original}",
            module_path=module_path,
            reason=str(original),
            next_action=(
                "run `python -c 'import " + module_path + "'` locally to see the full traceback"
            ),
        )


class PluginContractError(PluginError):
    """Raised when a plugin module does not satisfy the export contract.

    The contract is documented in ``docs/contract.md``. In summary the target
    module must export ``CONNECTORS: Connectors`` (required) and may export
    ``ENV_VARS: dict[str, str]`` and ``PROVIDER_METADATA: dict[str, Any]``.
    """

    check = "contract"

    def __init__(
        self,
        module_path: str,
        reason: str,
        *,
        next_action: str | None = None,
    ) -> None:
        super().__init__(
            f"Plugin {module_path!r} violates contract: {reason}",
            module_path=module_path,
            reason=reason,
            next_action=next_action,
        )
