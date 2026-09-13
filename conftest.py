"""Root conftest.

Puts three things on `sys.path`: the core test helpers (`tests/core/helpers.py`), the
Strands scripted model (`tests/strands_agents/mocked_model.py`), and the example agent
that the integration and end-to-end suites drive.
"""

from __future__ import annotations

import sys
from pathlib import Path

_ROOT = Path(__file__).parent
for _path in (_ROOT / "tests" / "core", _ROOT / "tests" / "strands_agents", _ROOT / "examples"):
    if str(_path) not in sys.path:
        sys.path.insert(0, str(_path))
