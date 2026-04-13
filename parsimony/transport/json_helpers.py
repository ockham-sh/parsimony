"""JSON/tabular helpers for HTTP connectors."""

from __future__ import annotations

import json
from string import Formatter
from typing import Any

import pandas as pd


def _is_indexed_dict(value: dict) -> bool:
    if not value:
        return False
    return all(isinstance(k, str) and k.isdigit() for k in value)


def _is_date_keyed_dict(value: dict) -> bool:
    if not value:
        return False
    for k in value:
        if not isinstance(k, str):
            return False
        if pd.isna(pd.to_datetime(k, errors="coerce")):
            return False
    return True


def json_to_df(data: Any, prefix: str = "") -> pd.DataFrame:
    """Convert JSON to DataFrame; nested dict/list become TableRefs."""

    def _to_scalar(v: Any) -> Any:
        return (
            json.dumps(v, sort_keys=True, ensure_ascii=True)
            if isinstance(v, (dict, list))
            else v
        )

    def _sanitize(rows: list[dict]) -> list[dict]:
        return [{k: _to_scalar(v) for k, v in row.items()} for row in rows]

    if isinstance(data, list):
        if data and all(isinstance(item, dict) for item in data):
            return pd.DataFrame(_sanitize(data))
        return pd.DataFrame({"value": [_to_scalar(item) for item in data]})

    if isinstance(data, dict):
        if len(data) == 1:
            k, v = next(iter(data.items()))
            return json_to_df(v, prefix or k)

        if _is_indexed_dict(data):
            ordered = [data[k] for k in sorted(data.keys(), key=int)]
            if ordered and all(isinstance(item, dict) for item in ordered):
                return pd.DataFrame(_sanitize(ordered))
            return pd.DataFrame({"value": [_to_scalar(item) for item in ordered]})

        if _is_date_keyed_dict(data):
            rows = []
            for k in sorted(data.keys()):
                v = data[k]
                if isinstance(v, dict):
                    rows.append({"date": k, **v})
                else:
                    rows.append({"date": k, "value": _to_scalar(v)})
            if rows:
                return pd.DataFrame(_sanitize(rows))

        row: dict[str, Any] = {}
        for key, value in data.items():
            if isinstance(value, (dict, list)):
                sub = json_to_df(value, f"{prefix}::{key}" if prefix else key)
                ref = (
                    f"TableRef({prefix}::{key}, cols={'|'.join(map(str, sub.columns))})"
                    if prefix
                    else f"TableRef({key}, cols={'|'.join(map(str, sub.columns))})"
                )
                row[key] = ref
            else:
                row[key] = value
        return pd.DataFrame([row])

    return pd.DataFrame([{"value": _to_scalar(data)}])


def interpolate_path(path: str, all_params: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    """Interpolate path placeholders and return (rendered_path, request_params)."""
    formatter = Formatter()
    used_keys = {field_name for _, field_name, _, _ in formatter.parse(path) if field_name}
    path_params = {k: (v if v is not None else "") for k, v in all_params.items()}
    rendered_path = path.format(**path_params)
    request_params = {k: v for k, v in all_params.items() if k not in used_keys}
    return rendered_path, request_params
