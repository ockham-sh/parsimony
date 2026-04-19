"""Catalog spec types declared by plugins on ``@enumerator``.

A plugin attaches a :class:`CatalogSpec` to its enumerator via the
``catalog=`` kwarg. The ``CatalogSpec`` dataclass IS the contract — typed
instances give plugin authors IDE auto-complete and mypy coverage on the
boundary.

The unified concept: a plan generator yields :class:`CatalogPlan` items.
**Coalescing rule (load-bearing invariant):** items with the same
``namespace`` value are coalesced into one bundle (rows concatenated
before embedding); items with distinct ``namespace`` values produce
distinct bundles. This rule covers all three patterns:

- **Static 1:1** (treasury): one plan item, one bundle. Use
  ``CatalogSpec.static(namespace="...")``.
- **Aggregate N:1** (SDMX datasets): N plan items, all sharing
  ``namespace="sdmx_datasets"``, one aggregated bundle. Use
  ``CatalogSpec(plan=...)`` with an async generator.
- **Fan-out 1:N** (SDMX series): N plan items each with a distinct
  ``namespace`` (e.g. ``"sdmx_series_ECB_EXR"``), N bundles. Use
  ``CatalogSpec(plan=...)`` with an async generator.
"""

from __future__ import annotations

import inspect
import re
from collections.abc import AsyncIterator, Callable, Iterable, Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any, Final, TypeAlias

from parsimony.bundles.errors import BundleSpecError

DEFAULT_TARGET: Final[str] = "hf_bundle"
"""Default catalog target name. The HF bundle target ships in
:mod:`parsimony.bundles.targets`.
"""

# Namespaces become HuggingFace dataset repo IDs (``parsimony-dev/<namespace>``)
# and on-disk cache paths. Restrict to a strict allowlist so a hostile or
# typo'd plugin can't redirect publishes/reads via ``../`` or alternate
# org names.
_NAMESPACE_RE: Final[re.Pattern[str]] = re.compile(r"^[a-z0-9][a-z0-9_-]{0,63}$")


def _validate_namespace(value: str, *, field_name: str) -> None:
    if not isinstance(value, str) or not value:
        raise BundleSpecError(f"{field_name} must be a non-empty string")
    if not _NAMESPACE_RE.match(value):
        raise BundleSpecError(
            f"{field_name}={value!r} contains characters outside the allowed set "
            "(lowercase letters, digits, hyphen, underscore; must start with a letter or digit; max 64 chars)",
            next_action="rename the namespace to match ^[a-z0-9][a-z0-9_-]{0,63}$",
        )


@dataclass(frozen=True, slots=True)
class CatalogPlan:
    """One invocation of an enumerator within a build plan.

    Yielded by a :class:`CatalogSpec`'s ``plan`` callable; the build pipeline
    runs the enumerator with ``params`` and tags the result with
    ``namespace``. Items sharing a ``namespace`` aggregate into one bundle.
    """

    namespace: str
    params: Mapping[str, Any] = field(default_factory=lambda: MappingProxyType({}))

    def __post_init__(self) -> None:
        _validate_namespace(self.namespace, field_name="CatalogPlan.namespace")
        # Freeze params so downstream code can't mutate the planner's output.
        if not isinstance(self.params, MappingProxyType):
            object.__setattr__(self, "params", MappingProxyType(dict(self.params)))


PlanCallable: TypeAlias = Callable[[], AsyncIterator[CatalogPlan]]
"""Type of a plan callable — an async generator function.

Sync plan sources should wrap themselves with :func:`to_async`; the
decorator rejects sync callables to keep the build pipeline single-shape.
"""


@dataclass(frozen=True, slots=True)
class CatalogSpec:
    """Describes how to enumerate a plugin's catalog content.

    The required ``plan`` callable is an async generator factory yielding
    :class:`CatalogPlan` items. For the static one-namespace case, use the
    :meth:`static` classmethod which builds a one-shot plan generator.

    ``static_namespace`` is set when the spec was constructed via
    :meth:`static`; tools (like the CLI ``list``/``plan`` verbs) read it to
    surface the namespace without running the plan generator. Dynamic specs
    leave it ``None``.
    """

    plan: PlanCallable
    target: str = DEFAULT_TARGET
    embed: bool = True
    static_namespace: str | None = None

    def __post_init__(self) -> None:
        if not callable(self.plan):
            raise BundleSpecError(
                "CatalogSpec.plan must be a callable (no string/dotted-path lookup)",
                next_action="pass the function object directly, not its name",
            )
        if isinstance(self.plan, str):
            raise BundleSpecError("CatalogSpec.plan must not be a string")
        if not isinstance(self.target, str) or not self.target:
            raise BundleSpecError("CatalogSpec.target must be a non-empty string")
        if not isinstance(self.embed, bool):
            raise BundleSpecError("CatalogSpec.embed must be a bool")
        if self.static_namespace is not None:
            _validate_namespace(self.static_namespace, field_name="CatalogSpec.static_namespace")

    @classmethod
    def static(
        cls,
        namespace: str,
        *,
        target: str = DEFAULT_TARGET,
        embed: bool = True,
    ) -> CatalogSpec:
        """Build a one-namespace spec whose plan yields a single bare ``CatalogPlan``.

        The plan callable is synthesised in this kernel module; because
        ``static_namespace`` is set, the plan-provenance check at the
        decorator boundary skips this (the synthesised plan is trusted).
        """
        _validate_namespace(namespace, field_name="CatalogSpec.static.namespace")
        ns = namespace

        async def _one_shot() -> AsyncIterator[CatalogPlan]:
            yield CatalogPlan(namespace=ns)

        return cls(plan=_one_shot, target=target, embed=embed, static_namespace=namespace)


# ---------------------------------------------------------------------------
# Decorator-boundary normalization
# ---------------------------------------------------------------------------


def from_decorator_kwargs(
    value: CatalogSpec,
    *,
    connector_module: str,
) -> CatalogSpec:
    """Validate a ``catalog=`` decorator kwarg.

    Requires a typed :class:`CatalogSpec` instance. For user-supplied plan
    callables (non-static), verifies the plan originates from the same
    plugin top-level package as the connector (eliminates cross-plugin
    plan substitution). Kernel-synthesised plans (from
    :meth:`CatalogSpec.static`) are trusted by construction.
    """
    if not isinstance(value, CatalogSpec):
        raise BundleSpecError(
            "catalog= must be a CatalogSpec instance "
            f"(got {type(value).__name__}); construct one via CatalogSpec(plan=...) "
            "or CatalogSpec.static(namespace=...)",
            next_action="construct the typed dataclass at the @enumerator call site",
        )
    if value.static_namespace is None:
        _validate_plan_provenance(value.plan, connector_module=connector_module)
    return value


def _validate_plan_provenance(
    plan: PlanCallable,
    *,
    connector_module: str,
) -> None:
    """Refuse plans defined outside the connector's plugin package.

    A plan callable that lives in a different package than the connector
    itself is a substitution attack vector (a malicious plugin declaring
    ``plan=victim.victim_plan``). We compare the plan callable's
    ``__module__`` against the connector module's top-level package and
    require the same root.
    """
    plan_module = getattr(plan, "__module__", None)
    if not isinstance(plan_module, str):
        raise BundleSpecError(
            "CatalogSpec.plan callable has no __module__ — cannot verify origin",
        )
    plan_root = plan_module.split(".", 1)[0]
    connector_root = connector_module.split(".", 1)[0]
    if plan_root != connector_root:
        raise BundleSpecError(
            f"CatalogSpec.plan defined in {plan_module!r} but the connector "
            f"lives in {connector_module!r} — plan must originate from the same plugin "
            f"(top-level package: {connector_root!r})",
            next_action="move the plan function into the connector's package",
        )


# ---------------------------------------------------------------------------
# Sync → async plan adapter
# ---------------------------------------------------------------------------


def to_async(
    source: Iterable[CatalogPlan] | Callable[[], Iterable[CatalogPlan]],
) -> PlanCallable:
    """Wrap a sync iterable (or zero-arg callable returning one) as a plan callable.

    The single accepted plan-generator shape is async (pick one code path).
    Plugins with trivially synchronous plans use this adapter::

        from parsimony.bundles import to_async, CatalogPlan, CatalogSpec

        def _agencies():
            for a in ("ECB", "ESTAT", "IMF_DATA", "WB_WDI"):
                yield CatalogPlan(namespace="sdmx_datasets", params={"agency": a})

        @enumerator(catalog=CatalogSpec(plan=to_async(_agencies)))
        async def enumerate_sdmx_datasets(...): ...
    """

    async def _async_plan() -> AsyncIterator[CatalogPlan]:
        iterable = source() if callable(source) else source
        for item in iterable:
            if not isinstance(item, CatalogPlan):
                raise BundleSpecError(
                    f"to_async source yielded {type(item).__name__}, expected CatalogPlan",
                )
            yield item

    # Preserve module of the original source so provenance check succeeds.
    src_module = getattr(source, "__module__", None)
    if isinstance(src_module, str):
        _async_plan.__module__ = src_module
    return _async_plan


# ---------------------------------------------------------------------------
# Materialization (used by build orchestrator)
# ---------------------------------------------------------------------------


async def materialize(spec: CatalogSpec) -> list[CatalogPlan]:
    """Resolve a spec to its concrete list of :class:`CatalogPlan` items.

    Runs the plan callable and collects every yielded item. Used by the
    build CLI to enumerate work before kicking off bundles.
    """
    plans: list[CatalogPlan] = []
    iterator = spec.plan()
    if not inspect.isasyncgen(iterator):
        raise BundleSpecError(
            f"CatalogSpec.plan must return an async iterator; "
            f"got {type(iterator).__name__}. Wrap sync sources with to_async().",
        )
    async for item in iterator:
        if not isinstance(item, CatalogPlan):
            raise BundleSpecError(
                f"plan generator yielded {type(item).__name__}, expected CatalogPlan",
            )
        plans.append(item)
    return plans


__all__ = [
    "DEFAULT_TARGET",
    "CatalogPlan",
    "CatalogSpec",
    "PlanCallable",
    "from_decorator_kwargs",
    "materialize",
    "to_async",
]
