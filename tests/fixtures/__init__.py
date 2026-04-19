"""Synthetic fixtures for kernel tests.

These modules replace the previous use of in-tree connector modules
(``parsimony.connectors.treasury`` etc.) as test fixtures. The kernel no
longer contains provider-specific code — the freeze
(``DESIGN-distribution-model.md`` §5) forbids it — so any kernel test
that needs an example connector module imports from here instead.
"""
