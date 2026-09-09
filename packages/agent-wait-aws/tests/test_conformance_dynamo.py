"""Every conformance rule again -- this time against DynamoDB.

Same class, third store. If a rule holds in memory and in SQLite but not here, the
DynamoDB implementation is wrong; the rule is not up for discussion.
"""

from __future__ import annotations

from agent_wait import WaitStore
from conformance import WaitStoreConformance

from conftest import make_dynamo_store


class TestDynamoStoreConformance(WaitStoreConformance):
    @staticmethod
    def make_store() -> WaitStore:
        return make_dynamo_store()
