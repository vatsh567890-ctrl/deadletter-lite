"""Entrypoint module enabling execution via python -m deadletter_lite."""

from __future__ import annotations

import sys

from deadletter_lite.cli import main

if __name__ == "__main__":
    sys.exit(main())
