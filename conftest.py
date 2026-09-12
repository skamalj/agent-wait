"""Root conftest.

Puts two things on `sys.path`: the core test helpers (`tests/core/helpers.py`), and the
example agent that the integration and end-to-end suites drive.
"""

from __future__ import annotations

import sys
from pathlib import Path

_ROOT = Path(__file__).parent
for _path in (_ROOT / "tests" / "core", _ROOT / "examples"):
    if str(_path) not in sys.path:
        sys.path.insert(0, str(_path))
