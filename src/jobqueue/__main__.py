"""Entrypoint module enabling execution via python -m jobqueue."""

from __future__ import annotations

import sys

from jobqueue.cli import main

if __name__ == "__main__":
    sys.exit(main())
