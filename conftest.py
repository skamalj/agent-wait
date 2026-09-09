"""Root conftest.

It puts two things on `sys.path`: the shared conformance suite, and the example
agent that the integration and end-to-end suites drive. The rules in
REQUIREMENTS section 10 (and 18.2) are written once and run against every store -- the two built
into the core here, and `DynamoWaitStore` over moto from the AWS package. A rule that
only ever ran against the in-memory store would not have been tested.
"""

from __future__ import annotations

import sys
from pathlib import Path

_ROOT = Path(__file__).parent
for _path in (_ROOT / "packages" / "agent-wait" / "tests", _ROOT / "examples"):
    if str(_path) not in sys.path:
        sys.path.insert(0, str(_path))
