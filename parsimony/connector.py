"""Connector primitives and collection.

:func:`connector` / :func:`enumerator` / :func:`loader` decorators produce
:class:`Connector` instances.  :class:`Connectors` is an immutable composable collection.

Typed exceptions live in :mod:`parsimony.errors`.
"""

from __future__ import annotations

__all__ = [
    "Connector",
    "Connectors",
    "Namespace",
    "ResultCallback",
    "connector",
    "enumerator",
    "loader",
]

import functools
import inspect
import logging
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field, replace
from types import MappingProxyType
from typing import Any, Union, get_type_hints

import pandas as pd
from pydantic import BaseModel, GetJsonSchemaHandler
from pydantic_core import CoreSchema

from parsimony.catalog.models import normalize_code
from parsimony.errors import ParseError
from parsimony.result import ColumnRole, OutputConfig, Provenance, Result

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Namespace:
    """Optional metadata on a connector param: valid values come from this catalog namespace.

    Use with ``typing.Annotated``, e.g. ``Annotated[str, Namespace("fmp_symbols")]``.
    The string is the catalog ``namespace`` field (lowercase snake_case).
    """

    name: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "name", normalize_code(self.name))

    def __get_pydantic_core_schema__(self, source_type: Any, handler: Any) -> CoreSchema:
        return handler(source_type)  # type: ignore[no-any-return]

    def __get_pydantic_json_schema__(self, core_schema: CoreSchema, handler: GetJsonSchemaHandler) -> dict[str, Any]:
        json_schema = handler(core_schema)
        json_schema = handler.resolve_ref_schema(json_schema)
        json_schema["namespace"] = self.name
        return json_schema


def _mapping_proxy(d: dict[str, Any] | None) -> Mapping[str, Any]:
    return MappingProxyType(dict(d or {}))


def _resolve_type(spec: dict[str, Any]) -> str:
    if "type" in spec:
        return str(spec["type"])
    any_of = spec.get("anyOf", [])
    types = [s.get("type") for s in any_of if s.get("type") and s["type"] != "null"]
    return str(types[0]) if types else "any"


def _summarize_params(schema: Mapping[str, Any]) -> str:
    props = schema.get("properties", {})
    required = set(schema.get("required", []))
    parts: list[str] = []
    for name, spec in props.items():
        typ = _resolve_type(spec)
        suffix = "" if name in required else "?"
        parts.append(f"{name}{suffix}: {typ}")
    return ", ".join(parts)


def _parse_first_param_and_deps(
    fn: Callable[..., Any],
) -> tuple[str, type[BaseModel], frozenset[str], frozenset[str]]:
    """Return (params_arg_name, param_model_type, required_dep_names, optional_dep_names)."""
    hints = get_type_hints(fn, include_extras=True)
    sig = inspect.signature(fn)
    params_list = list(sig.parameters.values())
    if not params_list:
        raise TypeError(f"{fn.__name__}: connector must accept at least a params argument")
    first = params_list[0]
    if first.kind in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD):
        raise TypeError(f"{fn.__name__}: first parameter must be params, not *args/**kwargs")
    params_name = first.name
    ann = hints.get(params_name)
    if ann is None:
        raise TypeError(f"{fn.__name__}: first parameter {params_name!r} must be annotated with a Pydantic model")
    origin = getattr(ann, "__origin__", None)
    model_type = ann if isinstance(ann, type) and issubclass(ann, BaseModel) else None
    if model_type is None and origin is Union:
        args = getattr(ann, "__args__", ())
        for a in args:
            if isinstance(a, type) and issubclass(a, BaseModel):
                model_type = a
                break
    if model_type is None or not issubclass(model_type, BaseModel):
        raise TypeError(f"{fn.__name__}: first parameter must be annotated with a Pydantic BaseModel subclass")
    required_deps: list[str] = []
    optional_deps: list[str] = []
    for p in params_list[1:]:
        if p.kind == inspect.Parameter.KEYWORD_ONLY:
            if p.default is inspect.Parameter.empty:
                required_deps.append(p.name)
            else:
                optional_deps.append(p.name)
        elif p.kind == inspect.Parameter.POSITIONAL_OR_KEYWORD:
            raise TypeError(
                f"{fn.__name__}: only the first parameter may be positional; "
                f"dependencies must be keyword-only after '*'"
            )
    return params_name, model_type, frozenset(required_deps), frozenset(optional_deps)


def _validate_bind_deps(
    *,
    name: str,
    dep_names: frozenset[str],
    optional_dep_names: frozenset[str],
    deps: dict[str, Any],
) -> None:
    allowed = dep_names | optional_dep_names
    extra = frozenset(deps.keys()) - allowed
    if extra:
        raise TypeError(f"{name!r} received unexpected dependencies: {sorted(extra)}")


@dataclass(frozen=True)
class Connector:
    """Metadata + wrapped async function for a data connector (fetch/search/etc.).

    Callbacks are per-connector: use :meth:`with_callback` to attach post-fetch
    hooks that fire after every successful call. Callbacks are preserved through
    :meth:`bind_deps` and collection operations.
    """

    name: str
    description: str
    param_type: type[BaseModel]
    param_schema: Mapping[str, Any]
    fn: Callable[..., Any]
    dep_names: frozenset[str]
    optional_dep_names: frozenset[str]
    output_config: OutputConfig | None = None
    result_type: str = "dataframe"
    tags: tuple[str, ...] = ()
    properties: Mapping[str, Any] = field(default_factory=lambda: MappingProxyType({}))
    _callbacks: tuple[ResultCallback, ...] = field(default=(), repr=False)

    def with_callback(self, callback: ResultCallback) -> Connector:
        """Return a new :class:`Connector` with *callback* appended to its post-fetch hooks."""
        return replace(self, _callbacks=(*self._callbacks, callback))

    def bind_deps(self, **deps: Any) -> Connector:
        """Return a new :class:`Connector` with keyword-only dependencies pre-applied via :func:`functools.partial`."""
        _validate_bind_deps(
            name=self.name,
            dep_names=self.dep_names,
            optional_dep_names=self.optional_dep_names,
            deps=deps,
        )
        consumed = frozenset(deps.keys())
        return replace(
            self,
            fn=functools.partial(self.fn, **deps),
            dep_names=self.dep_names - consumed,
            optional_dep_names=self.optional_dep_names - consumed,
        )

    async def call_raw(self, params_model: BaseModel) -> Any:
        """Invoke underlying function with validated model (deps already bound in ``fn``)."""
        return await self.fn(params_model)

    def _wrap_result(self, raw: Any, params_model: BaseModel) -> Result:
        """Wrap a bare return value in a :class:`Result`, applying output_config if set.

        When the raw return is a DataFrame/Series with no ``output_config``,
        it stays a raw :class:`Result` with tabular ``data`` but no
        semantic table schema.

        Always stamps provenance with the connector name and description so
        downstream logging/UI shows which connector produced the result.
        """
        if isinstance(raw, Result):
            return raw
        provenance = Provenance(
            source=self.name,
            source_description=self.description,
            params=params_model.model_dump(mode="python"),
        )
        if self.output_config is not None and isinstance(raw, (pd.DataFrame, pd.Series)):
            return self.output_config.build_table_result(
                raw,
                provenance=provenance,
                params=params_model.model_dump(mode="python"),
            )
        if isinstance(raw, (pd.DataFrame, pd.Series)):
            return Result.from_dataframe(raw, provenance=provenance)
        return Result(data=raw, provenance=provenance)

    def _validate_params(
        self,
        params: BaseModel | None = None,
        **kwargs: Any,
    ) -> BaseModel:
        if params is not None and kwargs:
            raise TypeError("Pass either params=... or keyword arguments, not both")
        if kwargs:
            return self.param_type.model_validate(kwargs)
        if params is None:
            raise TypeError("Missing params")
        if isinstance(params, self.param_type):
            return params
        if isinstance(params, BaseModel):
            return self.param_type.model_validate(params.model_dump(mode="python"))
        raise TypeError(f"params must be {self.param_type.__name__} or None; got {type(params).__name__}")

    async def __call__(
        self,
        params: BaseModel | None = None,
        **kwargs: Any,
    ) -> Result:
        """Execute the connector with validated parameters.

        Call with keyword arguments validated by the params model::

            await conn(series_id="GDPC1")

        Or pass an already-built Pydantic params instance::

            await conn(FredFetchParams(series_id="GDPC1"))

        Raw ``dict`` is not accepted at the boundary; use kwargs or a typed model.

        If the underlying function returns a bare value (not a :class:`Result`),
        the framework wraps it automatically with auto-constructed provenance.
        Per-connector callbacks fire after a successful call.
        """
        if self.dep_names:
            raise TypeError(
                f"Connector {self.name!r} has unbound dependencies {sorted(self.dep_names)}; "
                "call bind_deps(**deps) before registration and execution."
            )
        model = self._validate_params(params, **kwargs)
        raw = await self.call_raw(model)
        try:
            result = self._wrap_result(raw, model)
        except (ValueError, TypeError) as exc:
            raise ParseError(self.name, str(exc)) from exc
        # Ensure provenance carries the connector name for UI display
        if result.provenance.source != self.name:
            result = result.model_copy(
                update={
                    "provenance": result.provenance.model_copy(
                        update={
                            "source": self.name,
                            "source_description": result.provenance.source_description or self.description,
                        }
                    )
                }
            )
        if self._callbacks:
            await _invoke_result_callbacks(self._callbacks, result)
        return result

    def describe(self) -> str:
        """Return a multi-line human- and LLM-readable description of this connector.

        Includes the full description, all parameters with types and namespace
        annotations, unbound dependencies, and the output schema if configured.
        Use ``__repr__`` for the compact one-liner; this method is for documentation
        and tool introspection.
        """
        lines: list[str] = []

        header = f"Connector: {self.name}"
        lines.append(header)
        lines.append("─" * len(header))
        lines.append("")
        lines.append(self.description)
        lines.append("")

        schema = dict(self.param_schema)
        props: dict[str, Any] = schema.get("properties", {})
        required: set[str] = set(schema.get("required", []))
        if props:
            lines.append("Parameters:")
            for fname, spec in props.items():
                typ = _resolve_type(spec)
                req_label = "required" if fname in required else "optional"
                line = f"  {fname}: {typ} ({req_label})"
                extras: list[str] = []
                ns = spec.get("namespace")
                if ns:
                    extras.append(f"namespace={ns!r}")
                fdesc = spec.get("description")
                if fdesc:
                    extras.append(fdesc)
                if extras:
                    line += "  —  " + ", ".join(extras)
                lines.append(line)
            lines.append("")

        req_deps = sorted(self.dep_names)
        opt_deps = sorted(self.optional_dep_names)
        if req_deps or opt_deps:
            lines.append("Dependencies (bind via bind_deps before calling):")
            for d in req_deps:
                lines.append(f"  {d} (required)")
            for d in opt_deps:
                lines.append(f"  {d} (optional)")
            lines.append("")

        if self.output_config is not None:
            lines.append("Output Schema:")
            cols = self.output_config.columns
            name_w = max((len(c.name) for c in cols), default=0) + 2
            for col in cols:
                role_str = col.role.value.upper()
                suffix = f"  namespace={col.namespace!r}" if col.namespace else ""
                lines.append(f"  {col.name:<{name_w}}{role_str:<10}{suffix}")
            lines.append("")

        if self.tags:
            lines.append(f"Tags: {', '.join(self.tags)}")
        if self.properties:
            lines.append(f"Properties: {dict(self.properties)}")

        return "\n".join(lines).rstrip()

    def to_llm(self) -> str:
        """Return a compact, token-efficient description for LLM system prompts.

        Full description with output columns appended, then structured parameter list.  No decorative
        separators or redundant labels — optimised for injection into a system prompt.
        """
        lines: list[str] = []

        # --- header: ### name [tags] ---
        tag_suffix = f" [{', '.join(self.tags)}]" if self.tags else ""
        lines.append(f"### {self.name}{tag_suffix}")

        # --- description (reflowed) + appended output columns ---
        desc = " ".join(self.description.split())  # collapse whitespace / indentation
        if self.output_config is not None:
            data_cols = [c.name for c in self.output_config.columns]
            if data_cols:
                desc += f" Returns: {', '.join(data_cols)}."
        if self.result_type != "dataframe":
            desc += f" → result.data is {self.result_type} (not a DataFrame)."
        lines.append(desc)

        # --- parameters as a compact list ---
        schema = dict(self.param_schema)
        props: dict[str, Any] = schema.get("properties", {})
        required: set[str] = set(schema.get("required", []))
        for fname, spec in props.items():
            typ = _resolve_type(spec)
            opt = "?" if fname not in required else ""
            ns = spec.get("namespace")
            ns_hint = f" [ns:{ns}]" if ns else ""
            fdesc = spec.get("description", "")
            desc_part = f" — {fdesc}" if fdesc else ""
            lines.append(f"- {fname}{opt}: {typ}{ns_hint}{desc_part}")

        return "\n".join(lines)

    def __repr__(self) -> str:
        params = _summarize_params(self.param_schema)
        desc = self.description
        if len(desc) > 80:
            desc = desc[:77] + "..."
        return f"Connector({self.name!r}, params=[{params}], desc={desc!r})"

    def __str__(self) -> str:
        return self.__repr__()


def connector(
    *,
    name: str | None = None,
    description: str | None = None,
    params: type[BaseModel] | None = None,
    output: OutputConfig | None = None,
    result_type: str = "dataframe",
    tags: list[str] | None = None,
    properties: dict[str, Any] | None = None,
    catalog: Any = None,
) -> Callable[[Callable[..., Any]], Connector]:
    """Decorate an async data connector.

    **Canonical usage** — infer metadata from the function; add ``output`` when needed::

        @connector()
        async def fred_search(params: FredSearchParams, *, api_key: str) -> Result:
            '''Keyword search for FRED economic time series.'''

        @connector(output=FETCH_OUTPUT)
        async def fred_fetch(params: FredFetchParams, *, api_key: str) -> Result:
            '''Fetch FRED time series observations by series_id.'''

    **Defaults:** ``name`` ← ``fn.__name__``; ``description`` ← stripped ``fn.__doc__`` (required:
    set a docstring or pass ``description=``); param model ← type of the first parameter.

    **Escape hatches:** ``name=`` / ``description=`` / ``params=`` when the implementation
    function is internal (e.g. ``_fetch``) or when catalog-facing text must differ from the
    docstring. ``tags`` and ``properties`` are optional registry metadata.

    The wrapped function must be ``async``. Dependencies (HTTP clients, API keys) are
    keyword-only after ``*`` and bound with :meth:`Connector.bind_deps`.

    The ``catalog=`` kwarg is rejected here — only :func:`enumerator` may declare a
    bundle catalog spec, since the wire format requires KEY+TITLE+METADATA shape.
    """
    if catalog is not None:
        # Encode the invariant in the type system (Dodds R3): only @enumerator
        # accepts catalog=. @connector raises immediately so plugin authors
        # don't drift toward "catalog on a fetch connector" patterns.
        from parsimony.bundles.errors import BundleSpecError

        raise BundleSpecError(
            "catalog= is only valid on @enumerator, not @connector. "
            "Move the spec onto the enumerator that produces the catalog rows.",
        )

    def decorator(fn: Callable[..., Any]) -> Connector:
        if not inspect.iscoroutinefunction(fn):
            raise TypeError(f"{fn.__name__}: connector function must be async")
        _params_name, inferred_type, dep_names, optional_dep_names = _parse_first_param_and_deps(fn)
        param_type = params if params is not None else inferred_type
        doc = (fn.__doc__ or "").strip()
        desc = description if description is not None else doc
        if not desc:
            raise ValueError(f"{fn.__name__}: add a docstring or pass description= (connector description is required)")
        nm = name if name is not None else fn.__name__
        schema = _mapping_proxy(param_type.model_json_schema())
        tag_tup = tuple(tags) if tags else ()
        return Connector(
            name=nm,
            description=desc,
            param_type=param_type,
            param_schema=schema,
            fn=fn,
            dep_names=dep_names,
            optional_dep_names=optional_dep_names,
            output_config=output,
            result_type=result_type,
            tags=tag_tup,
            properties=_mapping_proxy(properties),
        )

    return decorator


def _validate_enumerator_output(output: OutputConfig) -> None:
    """Raise if *output* is not valid for catalog enumeration via :func:`enumerator`."""
    cols = output.columns
    data_names = [c.name for c in cols if c.role == ColumnRole.DATA]
    if data_names:
        raise ValueError(
            f"Enumerator output must not include DATA columns; remove or reassign roles for: {data_names!r}"
        )
    key_cols = [c for c in cols if c.role == ColumnRole.KEY]
    if len(key_cols) != 1:
        raise ValueError(
            f"Enumerator output must define exactly one KEY column for catalog indexing; found {len(key_cols)}"
        )
    key = key_cols[0]
    if key.namespace is None or not str(key.namespace).strip():
        raise ValueError(
            "Enumerator KEY column must declare a non-empty namespace=... (required by Catalog.index_result)"
        )
    title_cols = [c for c in cols if c.role == ColumnRole.TITLE]
    if len(title_cols) != 1:
        raise ValueError(f"Enumerator output must define exactly one TITLE column; found {len(title_cols)}")


def _validate_loader_output(output: OutputConfig) -> None:
    """Raise if *output* is not valid for data loading via :func:`loader`."""
    cols = output.columns
    title_names = [c.name for c in cols if c.role == ColumnRole.TITLE]
    if title_names:
        raise ValueError(f"Loader output must not include TITLE columns; remove or reassign roles for: {title_names!r}")
    meta_names = [c.name for c in cols if c.role == ColumnRole.METADATA]
    if meta_names:
        raise ValueError(
            f"Loader output must not include METADATA columns; remove or reassign roles for: {meta_names!r}"
        )
    data_names = [c.name for c in cols if c.role == ColumnRole.DATA]
    if not data_names:
        raise ValueError("Loader output must include at least one DATA column")
    key_cols = [c for c in cols if c.role == ColumnRole.KEY]
    if len(key_cols) != 1:
        raise ValueError(f"Loader output must define exactly one KEY column for identity; found {len(key_cols)}")
    key = key_cols[0]
    if key.namespace is None or not str(key.namespace).strip():
        raise ValueError("Loader KEY column must declare a non-empty namespace=... (required by DataStore.load_result)")


def loader(
    *,
    output: OutputConfig,
    name: str | None = None,
    description: str | None = None,
    params: type[BaseModel] | None = None,
    tags: list[str] | None = None,
    properties: dict[str, Any] | None = None,
) -> Callable[[Callable[..., Any]], Connector]:
    """Decorate an async **loader** — same as :func:`connector`, with a stricter ``output`` contract.

    Use for connectors whose job is to persist observation-level DATA columns keyed by
    ``(namespace, code)`` from the KEY column, not catalog TITLE/METADATA. The returned value
    is still a :class:`Connector` (``bind_deps``, ``with_callback``, ``Connectors`` composition).

    **Validation:** ``output`` must have no TITLE or METADATA columns, at least one DATA column,
    exactly one KEY column, and that KEY must set ``namespace=...`` for
    :meth:`~parsimony.data_store.DataStore.load_result`.
    """

    _validate_loader_output(output)
    merged_tags = ["loader", *(tags or [])]
    return connector(
        name=name,
        description=description,
        params=params,
        output=output,
        tags=merged_tags,
        properties=properties,
    )


def enumerator(
    *,
    output: OutputConfig,
    name: str | None = None,
    description: str | None = None,
    params: type[BaseModel] | None = None,
    tags: list[str] | None = None,
    properties: dict[str, Any] | None = None,
    catalog: Any = None,
) -> Callable[[Callable[..., Any]], Connector]:
    """Decorate an async **enumerator** — same as :func:`connector`, with a stricter ``output`` contract.

    Use for connectors whose job is to list entities (key + title + metadata) for discovery
    indexing, not to return observation-level DATA columns. The returned value is still a
    :class:`Connector` (``bind_deps``, ``with_callback``, ``Connectors`` composition).

    **Validation:** ``output`` must have no :attr:`~parsimony.result.ColumnRole.DATA` columns,
    exactly one :attr:`~parsimony.result.ColumnRole.KEY` column, exactly one
    :attr:`~parsimony.result.ColumnRole.TITLE` column, and that KEY must set
    ``namespace=...`` for :meth:`~parsimony.catalog.catalog.Catalog.index_result`.

    **Catalog publishing.** Pass ``catalog=`` to declare that this enumerator's output
    should be packaged and published as an HF bundle. Accepts a typed
    :class:`~parsimony.bundles.spec.CatalogSpec` instance::

        @enumerator(
            output=TREASURY_ENUMERATE_OUTPUT,
            catalog=CatalogSpec.static(namespace="treasury"),
        )
        async def enumerate_treasury(params): ...

    For dynamic plans (multiple namespaces), construct
    ``CatalogSpec(plan=async_generator)`` directly. The spec is validated at
    decorator import time (so plugin authors get fast feedback on malformed
    declarations) and stored in ``properties["catalog"]`` for discovery by
    ``parsimony.bundles``.
    """

    _validate_enumerator_output(output)
    merged_tags = ["enumerator", *(tags or [])]
    merged_properties: dict[str, Any] = dict(properties or {})
    if catalog is not None:
        from parsimony.bundles.spec import from_decorator_kwargs

        # The connector's __module__ — needed for the plan-provenance check
        # (rejects cross-plugin plan substitution). Frame inspection: caller
        # is the @enumerator(...) call site, which lives in the plugin's
        # connector module.
        frame = inspect.currentframe()
        connector_module = frame.f_back.f_globals.get("__name__", "") if frame and frame.f_back else ""
        merged_properties["catalog"] = from_decorator_kwargs(catalog, connector_module=connector_module)

    # Bypass the @connector catalog= guard by calling its decorator factory directly
    # without forwarding catalog= (we've already validated and inlined it into properties).
    return connector(
        name=name,
        description=description,
        params=params,
        output=output,
        tags=merged_tags,
        properties=merged_properties,
    )


ResultCallback = Callable[[Result], Any]
"""Post-fetch hook: ``(result)``. May return ``None`` or an awaitable."""


async def _invoke_result_callbacks(
    callbacks: tuple[ResultCallback, ...],
    result: Result,
) -> None:
    for cb in callbacks:
        try:
            ret = cb(result)
            if inspect.isawaitable(ret):
                await ret
        except Exception:
            logger.exception("Result callback %r failed; data was fetched successfully", cb)


class Connectors:
    """Immutable, composable collection of :class:`Connector` instances.

    Lookup by name: ``connectors["fred_fetch"]`` or ``connectors.get("fred_fetch")``.
    ``name in connectors`` is supported for membership checks.

    :meth:`with_callback` adds a callback to every connector in the collection
    (chainable). For per-connector hooks, call :meth:`Connector.with_callback`
    before composing into a collection.
    """

    def __init__(self, items: Sequence[Connector]) -> None:
        self._items: tuple[Connector, ...] = tuple(items)
        seen: set[str] = set()
        dupes: set[str] = set()
        for c in self._items:
            if c.name in seen:
                dupes.add(c.name)
            seen.add(c.name)
        if dupes:
            raise ValueError(f"Duplicate connector names: {sorted(dupes)}")

    def with_callback(self, callback: ResultCallback) -> Connectors:
        """Return a new collection where every connector has *callback* appended."""
        return Connectors([c.with_callback(callback) for c in self._items])

    def bind_deps(self, **deps: Any) -> Connectors:
        """Pre-apply ``deps`` to every connector (same keys for each)."""
        return Connectors([c.bind_deps(**deps) for c in self._items])

    def __iter__(self) -> Iterator[Connector]:
        return iter(self._items)

    def __len__(self) -> int:
        return len(self._items)

    def get(self, name: str) -> Connector | None:
        """Return connector *name* if present, else ``None``."""
        for c in self._items:
            if c.name == name:
                return c
        return None

    def __getitem__(self, name: str) -> Connector:
        conn = self.get(name)
        if conn is not None:
            return conn
        available = sorted(c.name for c in self._items)
        raise KeyError(f"No connector {name!r}. Available: {available}")

    def __contains__(self, name: object) -> bool:
        if not isinstance(name, str):
            return False
        return any(c.name == name for c in self._items)

    def names(self) -> list[str]:
        """Sorted connector names."""
        return sorted(c.name for c in self._items)

    def __add__(self, other: Connectors) -> Connectors:
        return Connectors(list(self._items) + list(other._items))

    def describe(self) -> str:
        """Return a table-of-contents summary of all connectors in this collection.

        Shows name and one-line description for each connector. To get full
        detail on an individual connector, call ``connectors["name"].describe()``.
        """
        if not self._items:
            return "Connectors (empty)"
        lines: list[str] = [f"Connectors ({len(self._items)}):"]
        name_w = max(len(c.name) for c in self._items) + 2
        for i, c in enumerate(self._items, 1):
            desc = c.description.splitlines()[0]  # first line only
            if len(desc) > 72:
                desc = desc[:69] + "..."
            lines.append(f"  {i:2}. {c.name:<{name_w}} {desc}")
        return "\n".join(lines)

    _CODE_HEADER = (
        "\n# Data connectors (code execution)\n"
        "\n"
        "These connectors are available via `client` in the code executor. "
        "They return full datasets as DataFrames — the data stays in the "
        "execution environment, not in the conversation context.\n"
        "\n"
        "## How to use\n"
        '- `result = await client["name"](param=value)` — returns Result '
        "with .data and .provenance (source metadata).\n"
        "- .data is usually a DataFrame; some connectors return text (noted in their description).\n"
        "- Keyword arguments must match the connector's typed parameters.\n"
        '- `client.filter(name="query")` narrows by name or description.\n'
        "- **ONLY use connectors listed below. Do NOT invent connector names.**\n"
    )

    _MCP_HEADER = (
        "\n# Parsimony — financial data discovery tools\n"
        "\n"
        "These MCP tools search and discover data. They return compact, "
        "context-friendly results — metadata, listings, search matches — "
        "not bulk datasets. For bulk retrieval, write and execute a Python "
        "script:\n"
        "```python\n"
        "from parsimony import client\n"
        "result = await client['fred_fetch'](series_id='UNRATE')\n"
        "df = result.data  # pandas DataFrame\n"
        "```\n"
        "\n"
        "After discovering data with MCP tools, always execute the fetch — "
        "do not just suggest code.\n"
        "\n"
        "Workflow: discover (MCP tool) → fetch and execute (client) → analyze.\n"
        "For SDMX: list_datasets → dsd → codelist → series_keys → fetch.\n"
    )

    def to_llm(self, context: str = "code") -> str:
        """Return an LLM-ready prompt section describing all connectors.

        Compact format inspired by parsimony's ``enhanced_description``
        pattern: full descriptions with output columns appended, structured
        parameter lists, no decorative separators.  Designed to be injected
        into an agent's system prompt.

        Parameters
        ----------
        context:
            ``"code"`` (default) — header explains ``client["name"](...)`` usage
            for code-execution agents.
            ``"mcp"`` — header explains MCP discovery workflow and directs bulk
            retrieval to code execution.
        """
        parts: list[str] = []

        if context == "mcp":
            parts.append(self._MCP_HEADER)
        else:
            parts.append(self._CODE_HEADER)

        if not self._items:
            parts.append("No connectors available.\n")
        else:
            label = "Tools" if context == "mcp" else "Connectors"
            parts.append(f"## {label} ({len(self._items)})\n")
            for c in self._items:
                parts.append(c.to_llm())
                parts.append("")  # single blank line separator

        return "\n".join(parts)

    def __repr__(self) -> str:
        names = [c.name for c in self._items]
        return f"Connectors({names!r})"

    def filter(
        self,
        *,
        name: str | None = None,
        tags: Sequence[str] | None = None,
        **properties: Any,
    ) -> Connectors:
        """Return connectors matching substring ``name`` and/or all ``tags`` and property key/values."""
        out: list[Connector] = []
        for c in self._items:
            if name is not None and name.strip():
                n = name.lower()
                if n not in c.name.lower() and n not in c.description.lower():
                    continue
            if tags is not None:
                tag_set = set(tags)
                if not tag_set.issubset(set(c.tags)):
                    continue
            skip = False
            for k, v in properties.items():
                if c.properties.get(k) != v:
                    skip = True
                    break
            if skip:
                continue
            out.append(c)
        return Connectors(out)
