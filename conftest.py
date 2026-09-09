"""Root conftest.

Its one job is to put the shared conformance suite on `sys.path`. The twelve rules in
REQUIREMENTS section 10 are written once and run against every store -- the two built
into the core here, and `DynamoWaitStore` over moto from the AWS package. A rule that
only ever ran against the in-memory store would not have been tested.
"""

from __future__ import annotations

import sys
from pathlib import Path

_SHARED = Path(__file__).parent / "packages" / "agent-wait" / "tests"
if str(_SHARED) not in sys.path:
    sys.path.insert(0, str(_SHARED))
