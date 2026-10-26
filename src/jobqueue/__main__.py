"""Backward-compatibility entrypoint for python -m jobqueue."""

from __future__ import annotations

import sys

from deadletter_lite.cli import main

if __name__ == "__main__":
    sys.exit(main())
