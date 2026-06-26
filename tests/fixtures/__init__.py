"""Synthetic fixtures for kernel tests.

The kernel contains no provider-specific code — the freeze
(``DESIGN-distribution-model.md`` §5) forbids it — so any kernel test
that needs an example connector module imports a synthetic provider
from this package.
"""
