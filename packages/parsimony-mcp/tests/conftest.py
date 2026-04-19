"""Shared fixtures for MCP tests — mock connectors that never hit real APIs."""

from __future__ import annotations

import pandas as pd
import pytest
from parsimony.connector import Connectors, connector
from parsimony.result import Column, ColumnRole, OutputConfig
from pydantic import BaseModel, Field


class SearchParams(BaseModel):
    query: str = Field(..., description="Search query")
    limit: int = Field(default=10, description="Max results")


class ProfileParams(BaseModel):
    ticker: str = Field(..., description="Ticker symbol")


SEARCH_OUTPUT = OutputConfig(
    columns=[
        Column(name="id", role=ColumnRole.KEY),
        Column(name="title", role=ColumnRole.TITLE),
    ],
)


@connector(output=SEARCH_OUTPUT, tags=["macro", "tool"])
async def mock_search(params: SearchParams) -> pd.DataFrame:
    """Search for mock series by keyword."""
    return pd.DataFrame(
        {
            "id": ["A", "B", "C"],
            "title": [f"Series about {params.query}"] * 3,
        }
    ).head(params.limit)


@connector(tags=["equity", "tool"])
async def mock_profile(params: ProfileParams) -> pd.DataFrame:
    """Look up a mock company profile."""
    return pd.DataFrame(
        {
            "ticker": [params.ticker],
            "name": [f"Mock Corp ({params.ticker})"],
            "sector": ["Technology"],
        }
    )


@connector(tags=["macro"])
async def mock_fetch(params: SearchParams) -> pd.DataFrame:
    """Fetch mock time series (NOT tagged as tool)."""
    return pd.DataFrame(
        {
            "date": ["2020-01-01", "2020-02-01"],
            "value": [1.0, 2.0],
        }
    )


@pytest.fixture()
def tool_connectors() -> Connectors:
    """Only connectors tagged 'tool' — what the MCP server would expose."""
    all_conns = Connectors([mock_search, mock_profile, mock_fetch])
    return all_conns.filter(tags=["tool"])


@pytest.fixture()
def all_connectors() -> Connectors:
    """All connectors including non-tool ones."""
    return Connectors([mock_search, mock_profile, mock_fetch])
