"""DynamoDB specifics: the single-table layout, the indexes, and the type mapping.

The conformance suite already proves the *semantics*. What is left is everything
particular to this store -- that the item shapes are what the CDK stack provisions for,
that floats survive the Decimal round trip, and that the table's other tenants never leak
into a `Wait`.
"""

from __future__ import annotations

import boto3
import pytest
from agent_wait import WaitPolicy
from agent_wait.model import Wait
from agent_wait_aws import DynamoWaitStore
from agent_wait_aws.store_dynamo import NEVER

from conftest import REGION, create_wait_table, make_dynamo_store


def a_wait(**overrides: object) -> Wait:
    fields: dict[str, object] = {
        "wait_id": "01JWAIT0000000000000000AA",
        "thread_id": "order-4471",
        "framework": "langgraph",
        "interrupt_id": "int-1",
        "checkpoint_id": "ckpt-1",
        "idempotency_key": "idem-1",
        "question": {"kind": "refund_approval", "amount": 41000},
        "binding": "ab" * 32,
        "policy": WaitPolicy(timeout="PT2H", allowed_actions=("approve", "reject")),
        "created_at": 1_760_000_000.5,
        "updated_at": 1_760_000_000.5,
        "expires_at": 1_760_007_200.5,
    }
    fields.update(overrides)
    return Wait(**fields)  # type: ignore[arg-type]


# --------------------------------------------------------------------------- layout
def test_the_item_uses_the_documented_keys_and_indexes() -> None:
    name = create_wait_table()
    store = DynamoWaitStore(name, region_name=REGION)
    wait = a_wait()
    store.create(wait)

    table = boto3.resource("dynamodb", region_name=REGION).Table(name)
    item = table.get_item(Key={"pk": f"WAIT#{wait.wait_id}", "sk": "#"})["Item"]

    assert item["pk"] == f"WAIT#{wait.wait_id}"
    assert item["sk"] == "#"
    assert item["gsi1pk"] == "order-4471"
    assert item["gsi1sk"] == wait.wait_id
    assert item["gsi2pk"] == "pending"
    assert float(item["gsi2sk"]) == wait.expires_at


def test_the_idempotency_marker_is_its_own_item() -> None:
    name = create_wait_table()
    store = DynamoWaitStore(name, region_name=REGION)
    wait = a_wait()
    store.create(wait)

    table = boto3.resource("dynamodb", region_name=REGION).Table(name)
    marker = table.get_item(Key={"pk": "IDEM#idem-1", "sk": "#"})["Item"]

    assert marker["wait_id"] == wait.wait_id


def test_create_is_atomic_in_both_directions() -> None:
    """A second create with the same key returns the original and writes nothing new."""
    store = make_dynamo_store()
    first, created_first = store.create(a_wait())
    second, created_second = store.create(a_wait(wait_id="01JDIFFERENT000000000000A"))

    assert created_first is True
    assert created_second is False
    assert second.wait_id == first.wait_id
    assert len(store.find()) == 1


def test_a_wait_with_no_timeout_gets_the_never_sort_key() -> None:
    """It must still be visible to the sweeper's index scan, but never look due."""
    name = create_wait_table()
    store = DynamoWaitStore(name, region_name=REGION)
    store.create(a_wait(policy=WaitPolicy(), expires_at=None))

    table = boto3.resource("dynamodb", region_name=REGION).Table(name)
    item = table.get_item(Key={"pk": "WAIT#01JWAIT0000000000000000AA", "sk": "#"})["Item"]

    assert float(item["gsi2sk"]) == NEVER
    assert store.find(status="pending", notified=False) != []
    assert store.find(status="pending", due_before=2_000_000_000.0) == []


# --------------------------------------------------------------------------- types
def test_floats_survive_the_decimal_round_trip() -> None:
    store = make_dynamo_store()
    store.create(a_wait())

    loaded = store.get("01JWAIT0000000000000000AA")

    assert loaded is not None
    assert loaded.created_at == 1_760_000_000.5
    assert loaded.expires_at == 1_760_007_200.5
    assert isinstance(loaded.created_at, float)


def test_json_fields_round_trip() -> None:
    store = make_dynamo_store()
    policy = WaitPolicy(
        timeout="P3D",
        default={"action": "reject", "nested": {"deep": [1, 2, 3]}},
        allowed_actions=("approve", "reject"),
        tags={"approver_group": "finance"},
        correlation={"provider": "vendor", "id": "77"},
    )
    store.create(a_wait(policy=policy, question={"unicode": "मराठी", "n": 1.25}))

    loaded = store.get("01JWAIT0000000000000000AA")

    assert loaded is not None
    assert loaded.policy == policy
    assert loaded.question == {"unicode": "मराठी", "n": 1.25}


def test_transition_writes_json_fields_as_json() -> None:
    store = make_dynamo_store()
    wait = a_wait()
    store.create(wait)

    assert store.transition(
        wait.wait_id, expect="pending", to="answered", answer={"action": "approve", "note": "ok"}
    )

    loaded = store.get(wait.wait_id)
    assert loaded is not None
    assert loaded.answer == {"action": "approve", "note": "ok"}
    assert loaded.status == "answered"
    assert loaded.version == 1


def test_transition_keeps_gsi2pk_in_step_with_status() -> None:
    """Otherwise the sweeper's index would keep handing back settled waits."""
    name = create_wait_table()
    store = DynamoWaitStore(name, region_name=REGION)
    wait = a_wait()
    store.create(wait)
    store.transition(wait.wait_id, expect="pending", to="answered")

    table = boto3.resource("dynamodb", region_name=REGION).Table(name)
    item = table.get_item(Key={"pk": f"WAIT#{wait.wait_id}", "sk": "#"})["Item"]

    assert item["gsi2pk"] == "answered"
    assert store.find(status="pending") == []
    assert [w.wait_id for w in store.find(status="answered")] == [wait.wait_id]


# --------------------------------------------------------------------------- isolation
def test_the_other_tenants_never_leak_into_a_wait() -> None:
    """Leases, markers, applied messages and parked answers share the table. An
    unindexed `find()` must not try to read any of them back as a Wait."""
    store = make_dynamo_store()
    store.create(a_wait())
    store.record_applied("order-4471", "evt-1")
    store.acquire_lease("order-4471", "worker-a", expires_at=1_760_001_000.0, now=1_760_000_000.0)
    store.park_answer("some-key", {"answer_id": "a"})

    found = store.find()

    assert [w.wait_id for w in found] == ["01JWAIT0000000000000000AA"]


def test_set_fields_on_a_missing_wait_is_silent() -> None:
    store = make_dynamo_store()
    store.set_fields("no-such-wait", notified_at=1.0)  # must not raise
    assert store.get("no-such-wait") is None


def test_transition_on_a_missing_wait_is_false() -> None:
    store = make_dynamo_store()
    assert store.transition("no-such-wait", expect="pending", to="answered") is False


def test_set_fields_with_nothing_to_set_is_a_no_op() -> None:
    store = make_dynamo_store()
    wait = a_wait()
    store.create(wait)
    store.set_fields(wait.wait_id)
    loaded = store.get(wait.wait_id)
    assert loaded is not None and loaded.version == 0


# --------------------------------------------------------------------------- leases
def test_a_lease_is_not_releasable_by_someone_else() -> None:
    store = make_dynamo_store()
    store.acquire_lease("t", "worker-a", expires_at=2000.0, now=1000.0)

    store.release_lease("t", "worker-b")

    lease = store.get_lease("t")
    assert lease is not None and lease.owner == "worker-a"


def test_refreshing_someone_elses_lease_fails() -> None:
    store = make_dynamo_store()
    store.acquire_lease("t", "worker-a", expires_at=2000.0, now=1000.0)

    assert store.refresh_lease("t", "worker-b", expires_at=9000.0) is False
    assert store.refresh_lease("t", "worker-a", expires_at=9000.0) is True

    lease = store.get_lease("t")
    assert lease is not None and lease.expires_at == 9000.0


def test_limit_is_respected() -> None:
    store = make_dynamo_store()
    for index in range(5):
        store.create(
            a_wait(
                wait_id=f"01JWAIT000000000000000{index:02d}",
                idempotency_key=f"idem-{index}",
                created_at=1_760_000_000.0 + index,
            )
        )

    assert len(store.find(thread_id="order-4471", limit=2)) == 2
    assert len(store.find(status="pending", limit=3)) == 3


@pytest.mark.parametrize("bad_status", ["answered", "resumed"])
def test_find_by_status_only_returns_that_status(bad_status: str) -> None:
    store = make_dynamo_store()
    wait = a_wait()
    store.create(wait)
    store.transition(wait.wait_id, expect="pending", to=bad_status)  # type: ignore[arg-type]

    assert store.find(status="pending") == []
    assert len(store.find(status=bad_status)) == 1  # type: ignore[arg-type]


# --------------------------------------------------------------------------- pagination
def test_find_follows_every_page() -> None:
    """DynamoDB caps a response at 1 MB, so one call is a page, not an answer. A wait the
    sweeper never sees is a thread parked forever."""
    store = make_dynamo_store()
    for index in range(12):
        store.create(
            a_wait(
                wait_id=f"01JWAIT000000000000000{index:03d}",
                idempotency_key=f"idem-{index}",
                created_at=1_760_000_000.0 + index,
            )
        )

    real_query = store.table.query
    pages: list[int] = []

    def one_item_at_a_time(**kwargs: object) -> dict[str, object]:
        response = real_query(**{**kwargs, "Limit": 1})
        pages.append(1)
        return response

    store.table.query = one_item_at_a_time  # type: ignore[method-assign]
    found = store.find(thread_id="order-4471")

    assert len(found) == 12, f"only {len(found)} of 12 waits survived pagination"
    assert len(pages) > 1, "the test did not actually force more than one page"


def test_find_stops_once_the_limit_is_satisfied() -> None:
    store = make_dynamo_store()
    for index in range(6):
        store.create(a_wait(wait_id=f"01JWAIT00000000000000A{index:02d}", idempotency_key=f"k-{index}"))

    assert len(store.find(thread_id="order-4471", limit=2)) == 2
