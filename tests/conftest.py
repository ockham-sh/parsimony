"""Shared test fixtures for parsimony test suite.

The new :mod:`parsimony.discover` is stateless — no cache — so the legacy
autouse fixture that cleared the per-process cache between tests is gone.
This file is kept so pytest still recognises the directory as a test package
and for future fixtures.
"""

from __future__ import annotations
