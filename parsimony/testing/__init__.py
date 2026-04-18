"""Plugin testing utilities for parsimony.

Re-exports the public conformance entry point :func:`assert_plugin_valid`
so plugin authors can write a single-line conformance test.
"""

from __future__ import annotations

from parsimony.testing.conformance import ConformanceError, assert_plugin_valid

__all__ = ["ConformanceError", "assert_plugin_valid"]
