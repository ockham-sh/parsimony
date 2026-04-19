"""Module entry point for ``python -m parsimony.cli``."""

from __future__ import annotations

import sys

from parsimony.cli import main

if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
