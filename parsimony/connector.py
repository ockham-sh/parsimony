"""Connector primitives and collection.

A connector is a small synchronous Python callable plus metadata. Function
parameters are the connector parameters; binding fixes parameter values and
returns a new connector with a smaller exposed surface.

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

import inspect
import logging
import types as _types
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from types import MappingProxyType
from typing import Any, Union, get_args, get_origin, get_type_hints, overload

import pandas as pd
from pydantic import BaseModel

from parsimony.errors import ParseError
from parsimony.namespace import Namespace
from parsimony.result import ColumnRole, OutputConfig, Provenance, Result, TabularResult

logger = logging.getLogger(__name__)

ResultCallback = Callable[[Result], None]
"""Post-fetch **observer**: ``(result) -> None``.

**Observer semantics — exceptions are logged and swallowed.** The connector
has already produced a valid :class:`Result`; a downstream side-effect
failure (telemetry, audit log, notification, agent summary) must not
corrupt the caller's view. If you need fail-closed persistence (e.g. the
caller must not see a successful :class:`Result` when a write fails), call
the persistence function directly from the connector or wrap the call
site — do not rely on a post-hook.
"""


def _invoke_result_callbacks(
    callbacks: tuple[ResultCallback, ...],
    result: Result,
) -> None:
    for cb in callbacks:
        try:
            cb(result)
        except Exception:
            # Observer semantics — see ResultCallback docstring.
            logger.exception("Result observer %r failed; data was fetched successfully", cb)


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------


def _mapping_proxy(d: dict[str, Any] | None) -> Mapping[str, Any]:
    return MappingProxyType(dict(d or {}))


def _param_type_label(param: inspect.Parameter, hints: Mapping[str, Any]) -> str:
    ann = hints.get(param.name, param.annotation)
    if ann is inspect.Parameter.empty:
        return "any"
    ann = _strip_annotated(ann)
    if hasattr(ann, "__name__"):
        return str(ann.__name__)
    return str(ann).replace("typing.", "")


def _exposed_param_rows(c: Connector) -> list[tuple[str, str, bool]]:
    try:
        hints = get_type_hints(c.fn, include_extras=True)
    except Exception:  # noqa: BLE001 - local annotations may be unavailable
        hints = {}
    rows: list[tuple[str, str, bool]] = []
    for name, param in c.exposed_signature.parameters.items():
        if param.kind in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD):
            continue
        required = param.default is inspect.Parameter.empty
        rows.append((name, _param_type_label(param, hints), required))
    return rows


def _summarize_params(c: Connector) -> str:
    parts = [f"{name}{'' if required else '?'}: {typ}" for name, typ, required in _exposed_param_rows(c)]
    return ", ".join(parts)


def _namespace_hint_from_annotation(ann: Any) -> str | None:
    metadata = getattr(ann, "__metadata__", None)
    if not metadata:
        return None
    for m in metadata:
        if isinstance(m, Namespace):
            return m.name or None
        if isinstance(m, str) and m.startswith("ns:"):
            return m[3:] or None
    return None


def _namespace_hints_from_signature(fn: Callable[..., Any], signature: inspect.Signature) -> dict[str, str]:
    try:
        hints = get_type_hints(fn, include_extras=True)
    except Exception:  # noqa: BLE001 - local annotations may be unavailable at decoration time
        hints = getattr(fn, "__annotations__", {})
    out: dict[str, str] = {}
    for name in signature.parameters:
        ann = hints.get(name)
        if ann is None:
            continue
        ns = _namespace_hint_from_annotation(ann)
        if ns is not None:
            out[name] = ns
    return out


def _strip_annotated(ann: Any) -> Any:
    # Annotated[X, ...] carries its metadata as __metadata__; stripping yields X.
    # We use hasattr rather than a string comparison on get_origin(...) because
    # str(get_origin(Annotated[X, ...])) is "<class 'typing.Annotated'>" not
    # "typing.Annotated" in CPython 3.11+.
    if hasattr(ann, "__metadata__"):
        args = get_args(ann)
        if args:
            return args[0]
    return ann


def _json_type_for_annotation(ann: Any) -> dict[str, Any]:
    """Map a Python annotation to a minimal JSON Schema fragment."""
    ann = _strip_annotated(ann)
    if ann is inspect.Parameter.empty or ann is Any:
        return {}
    if ann is str:
        return {"type": "string"}
    if ann is int:
        return {"type": "integer"}
    if ann is float:
        return {"type": "number"}
    if ann is bool:
        return {"type": "boolean"}
    origin = get_origin(ann)
    if origin is not None:
        args = get_args(ann)
        if origin is list or str(origin) in {"list", "typing.List"}:
            item = _json_type_for_annotation(args[0]) if args else {}
            return {"type": "array", "items": item or {"type": "string"}}
        if origin is dict or str(origin) in {"dict", "typing.Dict"}:
            return {"type": "object"}
        # Handle Union[X, None] (typing.Optional) and X | None (PEP 604).
        # get_origin returns typing.Union for the former and types.UnionType
        # for the latter; both need to be checked explicitly.
        is_union = origin is Union or (hasattr(_types, "UnionType") and origin is _types.UnionType)
        if is_union:
            non_none = [a for a in args if a is not type(None)]
            if len(non_none) == 1:
                return _json_type_for_annotation(non_none[0])
    if isinstance(ann, type) and issubclass(ann, BaseModel):
        return ann.model_json_schema()
    return {"type": "string"}


def _param_schema_from_connector(c: Connector) -> dict[str, Any]:
    """Build a JSON Schema for the connector's exposed call surface."""
    exposed = c.exposed_signature
    params = [
        p
        for p in exposed.parameters.values()
        if p.kind not in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD) and p.name not in c.secrets
    ]
    if len(params) == 1:
        ann = get_type_hints(c.fn, include_extras=True).get(params[0].name, params[0].annotation)
        ann = _strip_annotated(ann)
        if isinstance(ann, type) and issubclass(ann, BaseModel):
            return ann.model_json_schema()

    try:
        hints = get_type_hints(c.fn, include_extras=True)
    except Exception:  # noqa: BLE001 - local annotations may be unavailable
        hints = {}

    properties: dict[str, Any] = {}
    required: list[str] = []
    for param in params:
        ann = hints.get(param.name, param.annotation)
        spec = _json_type_for_annotation(ann)
        if param.default is not inspect.Parameter.empty and param.default is not None:
            spec.setdefault("default", param.default)
        properties[param.name] = spec
        if param.default is inspect.Parameter.empty:
            required.append(param.name)

    schema: dict[str, Any] = {"type": "object", "properties": properties}
    if required:
        schema["required"] = required
    return schema


def _public_signature(signature: inspect.Signature, bound: Mapping[str, Any]) -> inspect.Signature:
    params = [p for name, p in signature.parameters.items() if name not in bound]
    return signature.replace(parameters=params)


def _validate_secrets(signature: inspect.Signature, secrets: tuple[str, ...]) -> None:
    """Raise if any name in *secrets* is not a parameter of *signature*."""
    if not secrets:
        return
    param_names = set(signature.parameters)
    unknown = sorted(set(secrets) - param_names)
    if unknown:
        raise ValueError(f"secrets references unknown parameters: {unknown}")


# ---------------------------------------------------------------------------
# Connector dataclass
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Connector:
    """Metadata + wrapped synchronous callable for a data connector.

    A connector's exposed parameters are the callable's current unbound
    parameters. :meth:`bind` returns a new connector with selected parameters
    fixed; those fixed values are not part of the new connector's public call
    surface and are not recorded as call-time provenance params.
    """

    name: str
    description: str
    fn: Callable[..., Any]
    signature: inspect.Signature
    output_config: OutputConfig | None = None
    tags: tuple[str, ...] = ()
    properties: Mapping[str, Any] = field(default_factory=lambda: MappingProxyType({}))
    namespace_hints: Mapping[str, str] = field(default_factory=lambda: MappingProxyType({}))
    bound_arguments: Mapping[str, Any] = field(default_factory=lambda: MappingProxyType({}), repr=False)
    secrets: tuple[str, ...] = ()
    role: str | None = None
    _callbacks: tuple[ResultCallback, ...] = field(default=(), repr=False)

    @property
    def exposed_signature(self) -> inspect.Signature:
        """Signature currently visible to callers after binding."""
        return _public_signature(self.signature, self.bound_arguments)

    @property
    def param_schema(self) -> Mapping[str, Any]:
        """JSON Schema for the connector's exposed parameters (MCP / tool adapters)."""
        return MappingProxyType(_param_schema_from_connector(self))

    def with_callback(self, callback: ResultCallback) -> Connector:
        """Return a new :class:`Connector` with *callback* appended to its post-fetch hooks."""
        return replace(self, _callbacks=(*self._callbacks, callback))

    def bind(self, **kwargs: Any) -> Connector:
        """Return a new connector with parameters fixed by name."""
        if not kwargs:
            return self
        known = set(self.signature.parameters)
        already_bound = set(self.bound_arguments)
        extra = sorted(set(kwargs) - known)
        if extra:
            raise TypeError(f"{self.name!r} received unexpected bind arguments: {extra}")
        duplicate = sorted(set(kwargs) & already_bound)
        if duplicate:
            raise TypeError(f"{self.name!r} received already-bound arguments: {duplicate}")
        merged = {**self.bound_arguments, **kwargs}
        return replace(self, bound_arguments=_mapping_proxy(merged))

    def call_raw(self, **kwargs: Any) -> Any:
        """Invoke the underlying function with merged bound and call-time arguments."""
        return self.fn(**kwargs)

    def _wrap_result(self, raw: Any, call_params: Mapping[str, Any]) -> Result:
        """Wrap a connector return value in a :class:`Result` with framework-built provenance."""
        if isinstance(raw, tuple):
            raise TypeError(
                f"connector {self.name!r}: must return raw data, not (data, properties) tuples; "
                "put provider facts in DataFrame columns."
            )

        if isinstance(raw, (Result, TabularResult)):
            raise TypeError(
                f"connector {self.name!r}: must return raw data, not Result/TabularResult; "
                "the framework builds the execution envelope."
            )

        safe_call_params = {k: v for k, v in call_params.items() if k not in self.secrets}
        provenance = Provenance.model_construct(
            source=self.name,
            source_description=self.description,
            fetched_at=datetime.now(UTC),
            params=dict(safe_call_params),
            properties={},
        )

        if self.output_config is not None and isinstance(raw, (pd.DataFrame, pd.Series)):
            merge_unmapped = "enumerator" not in self.tags
            result = self.output_config.build_table_result(raw, merge_unmapped_as_data=merge_unmapped)
            if "enumerator" in self.tags:
                _validate_enumerator_dataframe(result.data, self.output_config)
            return result.model_copy(update={"provenance": provenance})
        if isinstance(raw, (pd.DataFrame, pd.Series)):
            return TabularResult(data=pd.DataFrame(raw), provenance=provenance)
        return Result(data=raw, provenance=provenance)

    def _bind_call(self, args: tuple[Any, ...], kwargs: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
        exposed = self.exposed_signature
        try:
            call_bound = exposed.bind(*args, **kwargs)
        except TypeError as exc:
            raise TypeError(f"Invalid parameters for connector {self.name!r}: {exc}") from exc
        call_bound.apply_defaults()
        call_params = dict(call_bound.arguments)
        all_params = {**self.bound_arguments, **dict(call_bound.arguments)}
        try:
            self.signature.bind(**all_params)
        except TypeError as exc:
            raise TypeError(f"Invalid bound parameters for connector {self.name!r}: {exc}") from exc
        return all_params, call_params

    def __call__(self, *args: Any, **kwargs: Any) -> Result:
        """Execute the connector with call-time parameters."""
        all_params, call_params = self._bind_call(args, kwargs)
        raw = self.call_raw(**all_params)
        try:
            result = self._wrap_result(raw, call_params)
        except ValueError as exc:
            raise ParseError(self.name, str(exc)) from exc
        if self._callbacks:
            _invoke_result_callbacks(self._callbacks, result)
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

    param_rows = _exposed_param_rows(c)
    if param_rows:
        lines.append("Parameters:")
        for fname, typ, required in param_rows:
            req_label = "required" if required else "optional"
            line = f"  {fname}: {typ} ({req_label})"
            ns = c.namespace_hints.get(fname)
            if ns:
                line += f"  —  namespace={ns!r}"
            lines.append(line)
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

    for fname, typ, required in _exposed_param_rows(c):
        opt = "?" if not required else ""
        ns = c.namespace_hints.get(fname)
        ns_hint = f" [ns:{ns}]" if ns else ""
        lines.append(f"- {fname}{opt}: {typ}{ns_hint}")

    return "\n".join(lines)


def _connector_repr(c: Connector) -> str:
    params = _summarize_params(c)
    desc = c.description
    if len(desc) > 80:
        desc = desc[:77] + "..."
    return f"Connector({c.name!r}, params=[{params}], desc={desc!r})"


# ---------------------------------------------------------------------------
# Decorator factories
# ---------------------------------------------------------------------------


@overload
def connector(fn: Callable[..., Any]) -> Connector: ...


@overload
def connector(
    fn: None = None,
    *,
    name: str | None = None,
    description: str | None = None,
    output: OutputConfig | None = None,
    tags: list[str] | None = None,
    properties: dict[str, Any] | None = None,
    secrets: tuple[str, ...] = (),
    role: str | None = None,
) -> Callable[[Callable[..., Any]], Connector]: ...


def connector(
    fn: Callable[..., Any] | None = None,
    *,
    name: str | None = None,
    description: str | None = None,
    output: OutputConfig | None = None,
    tags: list[str] | None = None,
    properties: dict[str, Any] | None = None,
    secrets: tuple[str, ...] = (),
    role: str | None = None,
) -> Callable[[Callable[..., Any]], Connector] | Connector:
    """Decorate a synchronous data connector.

    Defaults: ``name`` ← ``fn.__name__``; ``description`` ← stripped
    ``fn.__doc__`` (required). The callable signature is the connector's
    parameter surface.
    """

    def decorator(inner: Callable[..., Any]) -> Connector:
        if inspect.iscoroutinefunction(inner):
            raise TypeError(
                f"{inner.__name__}: connector function must be synchronous; "
                "async connectors are not supported in this release"
            )
        sig = inspect.signature(inner)
        doc = (inner.__doc__ or "").strip()
        desc = description if description is not None else doc
        if not desc:
            raise ValueError(
                f"{inner.__name__}: add a docstring or pass description= (connector description is required)"
            )
        nm = name if name is not None else inner.__name__
        tag_tup = tuple(tags) if tags else ()
        secret_tup = tuple(secrets)
        _validate_secrets(sig, secret_tup)
        ns_hints = _mapping_proxy(_namespace_hints_from_signature(inner, sig))
        return Connector(
            name=nm,
            description=desc,
            fn=inner,
            signature=sig,
            output_config=output,
            tags=tag_tup,
            properties=_mapping_proxy(properties),
            namespace_hints=ns_hints,
            secrets=secret_tup,
            role=role,
        )

    if fn is not None:
        return decorator(fn)
    return decorator


def _validate_enumerator_output(output: OutputConfig) -> None:
    """Raise if *output* is not a strict entity-discovery schema."""
    cols = output.columns
    key_cols = [c for c in cols if c.role == ColumnRole.KEY]
    if len(key_cols) != 1:
        raise ValueError(f"Enumerator output must define exactly one KEY column; found {len(key_cols)}")
    key = key_cols[0]
    if key.namespace is None or not str(key.namespace).strip():
        raise ValueError("Enumerator KEY column must declare a non-empty namespace=...")
    if key.namespace == "__row__":
        ns_cols = [c.name for c in cols if c.role == ColumnRole.METADATA and c.name not in ("*",)]
        if "entity_namespace" not in ns_cols:
            raise ValueError('Enumerator with namespace="__row__" requires entity_namespace METADATA column')
    title_cols = [c for c in cols if c.role == ColumnRole.TITLE]
    if not title_cols:
        raise ValueError("Enumerator output must include at least one TITLE column")
    data_cols = [c.name for c in cols if c.role == ColumnRole.DATA]
    if data_cols:
        raise ValueError(f"Enumerator output must not include DATA columns; remove: {data_cols!r}")
    invalid = [c.name for c in cols if c.role not in (ColumnRole.KEY, ColumnRole.TITLE, ColumnRole.METADATA)]
    if invalid:
        raise ValueError(f"Enumerator output has invalid column roles: {invalid!r}")


def _validate_enumerator_dataframe(df: pd.DataFrame, output: OutputConfig) -> None:
    """Raise if *df* columns do not match the declared enumerator schema."""
    declared = {c.name for c in output.columns if c.name != "*"}
    actual = set(df.columns)
    missing = sorted(declared - actual)
    if missing:
        raise ValueError(f"Enumerator DataFrame missing declared columns: {missing}")
    extra = sorted(actual - declared)
    if extra:
        raise ValueError(f"Enumerator DataFrame has undeclared columns: {extra}")


def _validate_enumerator_return(fn: Callable[..., Any]) -> None:
    """Raise if *fn* does not annotate ``pd.DataFrame`` return."""
    try:
        hints = get_type_hints(fn, include_extras=True)
    except Exception:  # noqa: BLE001 - local annotations may be unavailable
        hints = getattr(fn, "__annotations__", {})
    ann = hints.get("return")
    if ann is None:
        raise ValueError(f"{fn.__name__}: enumerator must annotate return type pd.DataFrame")
    return_str = str(_strip_annotated(ann))
    if "DataFrame" not in return_str and "Series" not in return_str:
        raise ValueError(f"{fn.__name__}: enumerator return must be pd.DataFrame")
    if "Entity" in return_str or "list[" in return_str:
        raise ValueError(f"{fn.__name__}: enumerator must not return list[Entity]")


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
    tags: list[str] | None = None,
    properties: dict[str, Any] | None = None,
    secrets: tuple[str, ...] = (),
) -> Callable[[Callable[..., Any]], Connector]:
    """Decorate a synchronous **loader** — stricter ``output`` contract than :func:`connector`."""

    _validate_loader_output(output)
    merged_tags = ["loader", *(tags or [])]
    return connector(
        name=name,
        description=description,
        output=output,
        tags=merged_tags,
        properties=properties,
        secrets=secrets,
        role="loader",
    )


def enumerator(
    *,
    output: OutputConfig,
    name: str | None = None,
    description: str | None = None,
    tags: list[str] | None = None,
    properties: dict[str, Any] | None = None,
    secrets: tuple[str, ...] = (),
) -> Callable[[Callable[..., Any]], Connector]:
    """Decorate a synchronous **enumerator** returning a raw ``pd.DataFrame``."""

    _validate_enumerator_output(output)

    def decorator(inner: Callable[..., Any]) -> Connector:
        _validate_enumerator_return(inner)
        merged_tags = ["enumerator", *(tags or [])]
        return connector(
            name=name,
            description=description,
            output=output,
            tags=merged_tags,
            properties=dict(properties or {}),
            secrets=secrets,
            role="enumerator",
        )(inner)

    return decorator


# ---------------------------------------------------------------------------
# Connectors collection
# ---------------------------------------------------------------------------


class Connectors:
    """Immutable, composable collection of :class:`Connector` instances."""

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

    def bind(self, **kwargs: Any) -> Connectors:
        """Bind matching parameters across every connector in the collection."""
        if kwargs:
            all_params = {p for c in self._items for p in c.exposed_signature.parameters}
            unmatched = [k for k in kwargs if k not in all_params]
            if unmatched:
                logging.getLogger(__name__).warning(
                    "bind() keys matched no connector parameters: %s",
                    sorted(unmatched),
                )
        out: list[Connector] = []
        for c in self._items:
            allowed = set(c.exposed_signature.parameters)
            scoped = {k: v for k, v in kwargs.items() if k in allowed}
            out.append(c.bind(**scoped) if scoped else c)
        return Connectors(out)

    def __add__(self, other: Connectors) -> Connectors:
        if not isinstance(other, Connectors):
            return NotImplemented
        return Connectors([*self._items, *other._items])

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

    def bind_env(self, overrides: Mapping[str, str] | None = None) -> Connectors:
        """Compatibility shim: flat connectors resolve credentials at call time.

        Returns ``self`` unchanged. Hosts (MCP, agents) load ``.env`` into
        ``os.environ`` before calling; connector implementations use
        :func:`parsimony.transport.helpers.require_key` for env fallback.
        """
        _ = overrides
        return self

    def env_vars(self) -> frozenset[str]:
        """Declared env-var names for this collection (empty for flat connectors)."""
        return frozenset()

    @property
    def unbound(self) -> tuple[str, ...]:
        """Connector names missing required credentials (empty for flat connectors)."""
        return ()

    def filter(
        self,
        predicate: Callable[[Connector], bool] | None = None,
        *,
        tags: Sequence[str] | None = None,
    ) -> Connectors:
        """Return connectors matching *predicate* and/or *tags*."""
        tag_set = set(tags) if tags is not None else None

        def _match(c: Connector) -> bool:
            if tag_set is not None and not tag_set.issubset(set(c.tags)):
                return False
            return predicate(c) if predicate is not None else True

        return Connectors([c for c in self._items if _match(c)])

    def search(
        self,
        query: str,
        *,
        tags: Sequence[str] | None = None,
        **properties: Any,
    ) -> Connectors:
        """Substring match over connector name and description."""
        out: list[Connector] = []
        needle = query.strip().lower()
        if not needle:
            return Connectors(list(self._items))
        for c in self._items:
            if needle not in c.name.lower() and needle not in c.description.lower():
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
