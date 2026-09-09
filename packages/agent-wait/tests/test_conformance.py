"""Run every conformance rule against both stores built into the core.

`agent-wait-aws` runs the same class against `DynamoWaitStore` on moto. Three
implementations, one set of guarantees.
"""

from __future__ import annotations

from agent_wait import InMemoryWaitStore, SqliteWaitStore, WaitStore
from conformance import StubAdapterSanity, WaitStoreConformance


class TestInMemoryStoreConformance(WaitStoreConformance):
    @staticmethod
    def make_store() -> WaitStore:
        return InMemoryWaitStore()


class TestSqliteStoreConformance(WaitStoreConformance):
    @staticmethod
    def make_store() -> WaitStore:
        return SqliteWaitStore(":memory:")


class TestStubAdapterSanity(StubAdapterSanity):
    pass
