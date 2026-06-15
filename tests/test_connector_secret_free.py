"""A connector's public surface is secret-free.

A host renders connectors into the agent's prompt via ``to_llm()`` (the
``<available_connectors>`` catalog the model reads to decide what to call). These
tests pin the invariant that rendering relies on: once a secret is bound, its
value never appears in any public surface, and the bound parameter drops out of
the exposed signature. Reading only public attributes therefore cannot leak a
credential.
"""

from __future__ import annotations

import pandas as pd

from parsimony.connector import connector

_SECRET = "sk-THIS-MUST-NOT-LEAK-1234567890"


@connector(secrets=("api_key",))
def keyed_fetch(series_id: str, api_key: str) -> pd.DataFrame:
    """Fetch a test series; needs an API key."""
    return pd.DataFrame({"date": ["2020-01-01"], "value": [1.0]})


def test_bound_secret_value_absent_from_card() -> None:
    bound = keyed_fetch.bind(api_key=_SECRET)
    assert _SECRET not in bound.to_llm()


def test_bound_secret_value_absent_from_describe() -> None:
    bound = keyed_fetch.bind(api_key=_SECRET)
    assert _SECRET not in bound.describe()


def test_bound_secret_param_dropped_from_exposed_signature() -> None:
    bound = keyed_fetch.bind(api_key=_SECRET)
    assert "api_key" not in bound.exposed_signature.parameters
    assert _SECRET not in str(bound.exposed_signature)


def test_name_is_secret_free() -> None:
    bound = keyed_fetch.bind(api_key=_SECRET)
    assert _SECRET not in bound.name
