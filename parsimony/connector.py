"""Connector primitives and collection.

:func:`connector` / :func:`enumerator` / :func:`loader` decorators produce
:class:`Connector` instances. :class:`Connectors` is an immutable composable
collection.

Typed exceptions live in :mod:`parsimony.errors`.
"""

from __future__ import annotations

__all__ = [
    "Connector",
    "Connectors",
    "ResultCallback",
    "connector",
    "enumerator",
    "loader",
]

import functools
import inspect
import logging
import os
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field, replace
from types import MappingProxyType
from typing import Any, Union, get_type_hints

import pandas as pd
from pydantic import BaseModel

from parsimony.errors import ParseError, UnauthorizedError
from parsimony.result import ColumnRole, OutputConfig, Provenance, Result

logger = logging.getLogger(__name__)


ResultCallback = Callable[[Result], Any]
"""Post-fetch **observer**: ``(result) -> None | Awaitable``.

**Observer semantics — exceptions are logged and swallowed.** The connector
has already produced a valid :class:`Result`; a downstream side-effect
failure (telemetry, audit log, notification, agent summary) must not
corrupt the caller's view. If you need fail-closed persistence (e.g. the
caller must not see a successful :class:`Result` when a write fails), call
the persistence function directly from the connector or wrap the call
site — do not rely on a post-hook.
"""


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
            # Observer semantics — see ResultCallback docstring.
            logger.exception("Result observer %r failed; data was fetched successfully", cb)


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------


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


def _namespace_hint_from_annotation(ann: Any) -> str | None:
    """Extract a namespace hint from ``Annotated[str, "ns:<name>"]`` metadata.

    Replaces the old ``Namespace`` annotation class with a plain string
    sentinel. Returns ``None`` when no ``"ns:"``-prefixed metadata is present.
    """
    metadata = getattr(ann, "__metadata__", None)
    if not metadata:
        return None
    for m in metadata:
        if isinstance(m, str) and m.startswith("ns:"):
            return m[3:] or None
    return None


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


def _validate_bind_kwargs(
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


def _namespace_hints_from_fn(fn: Callable[..., Any]) -> dict[str, str]:
    """Return a mapping ``{param_name: namespace}`` from ``Annotated`` metadata.

    Reads ``typing.get_type_hints(fn, include_extras=True)`` and looks for
    ``"ns:<name>"`` strings in the metadata tuple (the new-shape successor to
    ``Namespace("<name>")``).
    """
    hints = get_type_hints(fn, include_extras=True)
    out: dict[str, str] = {}
    for name, ann in hints.items():
        ns = _namespace_hint_from_annotation(ann)
        if ns is not None:
            out[name] = ns
    return out


# ---------------------------------------------------------------------------
# Connector dataclass
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Connector:
    """Metadata + wrapped async function for a data connector (fetch/search/etc.).

    Callbacks are per-connector: use :meth:`with_callback` to attach post-fetch
    hooks that fire after every successful call. Callbacks are preserved through
    :meth:`bind` and collection operations.

    ``env_map`` declares the decorator-level mapping from dep name to env-var
    name; :meth:`Connectors.bind_env` resolves it against ``os.environ``.
    ``bound`` is ``False`` on clones produced by :meth:`Connectors.bind_env`
    when a required env var was missing — calling such a connector raises
    :class:`UnauthorizedError` immediately, before param validation.
    """

    name: str
    description: str
    param_type: type[BaseModel]
    param_schema: Mapping[str, Any]
    fn: Callable[..., Any]
    dep_names: frozenset[str]
    optional_dep_names: frozenset[str]
    output_config: OutputConfig | None = None
    tags: tuple[str, ...] = ()
    properties: Mapping[str, Any] = field(default_factory=lambda: MappingProxyType({}))
    namespace_hints: Mapping[str, str] = field(default_factory=lambda: MappingProxyType({}))
    env_map: Mapping[str, str] = field(default_factory=lambda: MappingProxyType({}))
    bound: bool = True
    _callbacks: tuple[ResultCallback, ...] = field(default=(), repr=False)

    def with_callback(self, callback: ResultCallback) -> Connector:
        """Return a new :class:`Connector` with *callback* appended to its post-fetch hooks."""
        return replace(self, _callbacks=(*self._callbacks, callback))

    def bind(self, **deps: Any) -> Connector:
        """Return a new :class:`Connector` with keyword-only dependencies pre-applied."""
        _validate_bind_kwargs(
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
        """Wrap a bare return value in a :class:`Result`, applying output_config if set."""
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
        """Execute the connector with validated parameters."""
        if not self.bound:
            env_vars = sorted(self.env_map.values())
            detail = ", ".join(env_vars) if env_vars else "required credentials"
            raise UnauthorizedError(
                provider=self.name,
                message=f"{detail} is not set",
            )
        if self.dep_names:
            raise TypeError(
                f"Connector {self.name!r} has unbound dependencies {sorted(self.dep_names)}; "
                "call bind(**deps) before registration and execution."
            )
        model = self._validate_params(params, **kwargs)
        raw = await self.call_raw(model)
        try:
            result = self._wrap_result(raw, model)
        except ValueError as exc:
            raise ParseError(self.name, str(exc)) from exc
        if self._callbacks:
            await _invoke_result_callbacks(self._callbacks, result)
        return result

    def describe(self) -> str:
        return describe_connector(self)

    def to_llm(self) -> str:
        return llm_card(self)

    def __repr__(self) -> str:
        return _connector_repr(self)


# ---------------------------------------------------------------------------
# Presentation projections
# ---------------------------------------------------------------------------


def describe_connector(c: Connector) -> str:
    """Multi-line human- and LLM-readable description of *c*."""
    lines: list[str] = []
    header = f"Connector: {c.name}"
    lines.append(header)
    lines.append("─" * len(header))
    lines.append("")
    lines.append(c.description)
    lines.append("")

    schema = dict(c.param_schema)
    props: dict[str, Any] = schema.get("properties", {})
    required: set[str] = set(schema.get("required", []))
    if props:
        lines.append("Parameters:")
        for fname, spec in props.items():
            typ = _resolve_type(spec)
            req_label = "required" if fname in required else "optional"
            line = f"  {fname}: {typ} ({req_label})"
            extras: list[str] = []
            ns = c.namespace_hints.get(fname)
            if ns:
                extras.append(f"namespace={ns!r}")
            fdesc = spec.get("description")
            if fdesc:
                extras.append(fdesc)
            if extras:
                line += "  —  " + ", ".join(extras)
            lines.append(line)
        lines.append("")

    req_deps = sorted(c.dep_names)
    opt_deps = sorted(c.optional_dep_names)
    if req_deps or opt_deps:
        lines.append("Dependencies (bind via bind(**deps) before calling):")
        for d in req_deps:
            lines.append(f"  {d} (required)")
        for d in opt_deps:
            lines.append(f"  {d} (optional)")
        lines.append("")

    if c.output_config is not None:
        lines.append("Output Schema:")
        cols = c.output_config.columns
        name_w = max((len(col.name) for col in cols), default=0) + 2
        for col in cols:
            role_str = col.role.value.upper()
            suffix = f"  namespace={col.namespace!r}" if col.namespace else ""
            lines.append(f"  {col.name:<{name_w}}{role_str:<10}{suffix}")
        lines.append("")

    if c.tags:
        lines.append(f"Tags: {', '.join(c.tags)}")
    if c.properties:
        lines.append(f"Properties: {dict(c.properties)}")

    return "\n".join(lines).rstrip()


def llm_card(c: Connector) -> str:
    """Compact token-efficient description of *c* for LLM system prompts."""
    lines: list[str] = []
    tag_suffix = f" [{', '.join(c.tags)}]" if c.tags else ""
    lines.append(f"### {c.name}{tag_suffix}")

    desc = " ".join(c.description.split())
    if c.output_config is not None:
        data_cols = [col.name for col in c.output_config.columns]
        if data_cols:
            desc += f" Returns: {', '.join(data_cols)}."
    lines.append(desc)

    schema = dict(c.param_schema)
    props: dict[str, Any] = schema.get("properties", {})
    required: set[str] = set(schema.get("required", []))
    for fname, spec in props.items():
        typ = _resolve_type(spec)
        opt = "?" if fname not in required else ""
        ns = c.namespace_hints.get(fname)
        ns_hint = f" [ns:{ns}]" if ns else ""
        fdesc = spec.get("description", "")
        desc_part = f" — {fdesc}" if fdesc else ""
        lines.append(f"- {fname}{opt}: {typ}{ns_hint}{desc_part}")

    return "\n".join(lines)


def _connector_repr(c: Connector) -> str:
    params = _summarize_params(c.param_schema)
    desc = c.description
    if len(desc) > 80:
        desc = desc[:77] + "..."
    return f"Connector({c.name!r}, params=[{params}], desc={desc!r})"


# ---------------------------------------------------------------------------
# Decorator factories
# ---------------------------------------------------------------------------


def connector(
    *,
    env: dict[str, str] | None = None,
    name: str | None = None,
    description: str | None = None,
    params: type[BaseModel] | None = None,
    output: OutputConfig | None = None,
    tags: list[str] | None = None,
    properties: dict[str, Any] | None = None,
) -> Callable[[Callable[..., Any]], Connector]:
    """Decorate an async data connector.

    **Canonical usage** — infer metadata from the function; add ``output`` when needed::

        @connector(env={"api_key": "FRED_API_KEY"})
        async def fred_search(params: FredSearchParams, *, api_key: str) -> Result:
            '''Keyword search for FRED economic time series.'''

    **Defaults:** ``name`` ← ``fn.__name__``; ``description`` ← stripped
    ``fn.__doc__`` (required); param model ← type of the first parameter.

    The ``env`` mapping names environment-variable backings for keyword-only
    deps. Consumers resolve these through :meth:`Connectors.bind_env`; plugins
    can still be bound manually via :meth:`Connector.bind`.
    """

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
        ns_hints = _mapping_proxy(_param_namespace_hints(param_type))
        env_proxy: Mapping[str, str] = MappingProxyType(dict(env or {}))
        return Connector(
            name=nm,
            description=desc,
            param_type=param_type,
            param_schema=schema,
            fn=fn,
            dep_names=dep_names,
            optional_dep_names=optional_dep_names,
            output_config=output,
            tags=tag_tup,
            properties=_mapping_proxy(properties),
            namespace_hints=ns_hints,
            env_map=env_proxy,
        )

    return decorator


def _param_namespace_hints(model: type[BaseModel]) -> dict[str, str]:
    """Extract ``"ns:<name>"`` sentinels from field metadata on *model*.

    Plugin authors write ``Annotated[str, "ns:fred"]`` on their param-model
    fields; this surfaces that hint to the describe() / to_llm() projections.
    """
    out: dict[str, str] = {}
    for field_name, field_info in model.model_fields.items():
        for md in field_info.metadata:
            if isinstance(md, str) and md.startswith("ns:"):
                hint = md[3:]
                if hint:
                    out[field_name] = hint
                break
    return out


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
    env: dict[str, str] | None = None,
    name: str | None = None,
    description: str | None = None,
    params: type[BaseModel] | None = None,
    tags: list[str] | None = None,
    properties: dict[str, Any] | None = None,
) -> Callable[[Callable[..., Any]], Connector]:
    """Decorate an async **loader** — stricter ``output`` contract than :func:`connector`.

    **Validation:** ``output`` must have no TITLE or METADATA columns, at
    least one DATA column, exactly one KEY column, and that KEY must set
    ``namespace=...`` for :meth:`~parsimony.stores.InMemoryDataStore.load_result`.
    """

    _validate_loader_output(output)
    merged_tags = ["loader", *(tags or [])]
    return connector(
        env=env,
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
    env: dict[str, str] | None = None,
    name: str | None = None,
    description: str | None = None,
    params: type[BaseModel] | None = None,
    tags: list[str] | None = None,
    properties: dict[str, Any] | None = None,
) -> Callable[[Callable[..., Any]], Connector]:
    """Decorate an async **enumerator** — stricter ``output`` than :func:`connector`.

    **Validation:** ``output`` must have no :attr:`~parsimony.result.ColumnRole.DATA`
    columns, exactly one KEY column, and exactly one TITLE column. The KEY
    column's ``namespace=`` may be omitted — the catalog supplies its own
    ``name`` as the default at indexing time.

    **Catalog publishing.** Publish this enumerator's output by exporting
    ``CATALOGS = [("namespace", <this_enumerator>)]`` (or an async factory)
    on the plugin module. See :func:`parsimony.publish.publish` for details.
    """

    _validate_enumerator_output(output)
    merged_tags = ["enumerator", *(tags or [])]
    return connector(
        env=env,
        name=name,
        description=description,
        params=params,
        output=output,
        tags=merged_tags,
        properties=dict(properties or {}),
    )


# ---------------------------------------------------------------------------
# Connectors collection
# ---------------------------------------------------------------------------


class Connectors:
    """Immutable, composable collection of :class:`Connector` instances.

    Lookup by name: ``connectors["fred_fetch"]`` or ``connectors.get("fred_fetch")``.
    ``name in connectors`` is supported for membership checks.

    Public verbs:

    * :meth:`merge` — combine N collections (duplicate names raise).
    * :meth:`bind` — pre-apply non-env dependencies.
    * :meth:`bind_env` — resolve ``env_map`` declarations against ``os.environ``.
    * :meth:`filter` — substring/tag/property filter, or predicate.
    * :meth:`replace` — swap one entry.
    * :attr:`unbound` — connector names missing required env vars.
    * :meth:`env_vars` — declared env-var names across all connectors.
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

    def bind(self, **deps: Any) -> Connectors:
        """Pre-apply ``deps`` to every connector (same keys for each).

        Each connector silently ignores deps not in its own
        ``dep_names | optional_dep_names`` — you can bind a shared resource
        (e.g. a DB handle) across a heterogeneous collection.
        """
        out: list[Connector] = []
        for c in self._items:
            allowed = c.dep_names | c.optional_dep_names
            scoped = {k: v for k, v in deps.items() if k in allowed}
            out.append(c.bind(**scoped) if scoped else c)
        return Connectors(out)

    @classmethod
    def merge(cls, *others: Connectors) -> Connectors:
        """Combine ``others`` into a new collection. Duplicate names raise ``ValueError``.

        Accepts zero arguments (returns an empty collection) for use as the
        identity element in ``Connectors.merge(*collections)`` where
        ``collections`` may be empty.
        """
        items: list[Connector] = []
        for coll in others:
            if not isinstance(coll, Connectors):
                raise TypeError(
                    f"Connectors.merge arguments must be Connectors; got {type(coll).__name__}"
                )
            items.extend(coll._items)
        return cls(items)

    def bind_env(
        self,
        overrides: Mapping[str, str] | None = None,
    ) -> Connectors:
        """Resolve each connector's ``env_map`` against ``os.environ | overrides``.

        For each connector, walks ``env_map`` in declaration order. Values found
        in the merged environment are bound via :meth:`Connector.bind`. A
        connector is marked ``bound=False`` and stays in the collection when at
        least one required env var is missing — calling it raises
        :class:`UnauthorizedError` (see :attr:`Connector.bound`).
        """
        env: dict[str, str] = dict(os.environ)
        if overrides:
            env.update(overrides)

        out: list[Connector] = []
        for c in self._items:
            if not c.env_map:
                out.append(c)
                continue

            required = c.dep_names
            resolved: dict[str, Any] = {}
            missing_required = False
            for dep_name, env_var in c.env_map.items():
                value = env.get(env_var, "")
                if not value:
                    if dep_name in required:
                        missing_required = True
                    continue
                resolved[dep_name] = value

            if missing_required:
                out.append(replace(c, bound=False))
                continue

            out.append(c.bind(**resolved) if resolved else c)
        return Connectors(out)

    @property
    def unbound(self) -> tuple[str, ...]:
        """Names of connectors where at least one required env var is unresolved."""
        return tuple(c.name for c in self._items if not c.bound)

    def env_vars(self) -> frozenset[str]:
        """Union of all env-var names declared across every connector's ``env_map``."""
        vars_: set[str] = set()
        for c in self._items:
            vars_.update(c.env_map.values())
        return frozenset(vars_)

    def replace(self, name: str, connector: Connector) -> Connectors:
        """Return a new collection with the entry named ``name`` swapped for ``connector``."""
        if not any(c.name == name for c in self._items):
            available = sorted(c.name for c in self._items)
            raise KeyError(f"No connector {name!r}. Available: {available}")
        out = [connector if c.name == name else c for c in self._items]
        return Connectors(out)

    def __iter__(self) -> Iterator[Connector]:
        return iter(self._items)

    def __len__(self) -> int:
        return len(self._items)

    def get(self, name: str) -> Connector | None:
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
        return sorted(c.name for c in self._items)

    def describe(self) -> str:
        if not self._items:
            return "Connectors (empty)"
        lines: list[str] = [f"Connectors ({len(self._items)}):"]
        name_w = max(len(c.name) for c in self._items) + 2
        for i, c in enumerate(self._items, 1):
            desc = c.description.splitlines()[0]
            if len(desc) > 72:
                desc = desc[:69] + "..."
            lines.append(f"  {i:2}. {c.name:<{name_w}} {desc}")
        return "\n".join(lines)

    def to_llm(self, *, header: str = "", heading: str = "Connectors") -> str:
        """Return an LLM-ready prompt section describing all connectors."""
        if not self._items and not header:
            return ""

        parts: list[str] = []
        if header:
            parts.append(header)
        if self._items:
            parts.append(f"## {heading} ({len(self._items)})\n")
            for c in self._items:
                parts.append(c.to_llm())
                parts.append("")

        return "\n".join(parts)

    def __repr__(self) -> str:
        names = [c.name for c in self._items]
        return f"Connectors({names!r})"

    def filter(
        self,
        predicate: Callable[[Connector], bool] | None = None,
        *,
        name: str | None = None,
        tags: Sequence[str] | None = None,
        **properties: Any,
    ) -> Connectors:
        """Return a filtered view.

        When ``predicate`` is supplied, it overrides every other filter and
        retains only connectors for which the predicate returns truthy.
        Otherwise, returns connectors matching the substring ``name`` (checked
        against name and description), the full ``tags`` subset, and every
        property k/v match.
        """
        if predicate is not None:
            return Connectors([c for c in self._items if predicate(c)])

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
