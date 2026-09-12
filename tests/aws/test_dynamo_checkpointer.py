"""The example app's DynamoDB checkpointer.

It is example infrastructure rather than library code, but it is load-bearing for the
end-to-end run, so it gets tested like everything else. The property that matters is the
last one: a graph parked by one saver instance must resume through a *different* one,
because on Lambda those are different processes on different machines.
"""

from __future__ import annotations

import boto3
import pytest
from langgraph.checkpoint.memory import InMemorySaver
from refund_agent.dynamo_checkpointer import (
    CHECKPOINT_TABLE_ATTRIBUTES,
    CHECKPOINT_TABLE_KEY_SCHEMA,
    DynamoDBSaver,
)
from refund_agent.graph import PAYMENTS_CALLED, build_graph, reset_side_effects

from conftest import REGION

TABLE = "agent-wait-checkpoints"


@pytest.fixture()
def saver() -> DynamoDBSaver:
    boto3.client("dynamodb", region_name=REGION).create_table(
        TableName=TABLE,
        KeySchema=CHECKPOINT_TABLE_KEY_SCHEMA,
        AttributeDefinitions=CHECKPOINT_TABLE_ATTRIBUTES,
        BillingMode="PAY_PER_REQUEST",
    )
    reset_side_effects()
    yield DynamoDBSaver(TABLE, region_name=REGION)
    reset_side_effects()


def config(thread_id: str = "order-4471") -> dict[str, object]:
    return {"configurable": {"thread_id": thread_id}}


def test_a_graph_runs_to_completion_through_it(saver: DynamoDBSaver) -> None:
    graph = build_graph(saver)

    final = graph.invoke({"order_id": "order-1", "amount": 900}, config("order-1"))

    assert final["status"] == "refunded"
    assert PAYMENTS_CALLED == ["order-1"]


def test_an_interrupt_is_persisted_and_readable(saver: DynamoDBSaver) -> None:
    graph = build_graph(saver)
    raised = graph.invoke({"order_id": "order-4471", "amount": 41000}, config())["__interrupt__"][0]

    state = graph.get_state(config())

    assert state.config["configurable"]["checkpoint_id"]
    assert [i.id for task in state.tasks for i in task.interrupts] == [raised.id]


def test_the_checkpoint_id_is_stable_across_reads(saver: DynamoDBSaver) -> None:
    """The idempotency key depends on it, so this is rule 1's foundation."""
    graph = build_graph(saver)
    graph.invoke({"order_id": "order-4471", "amount": 41000}, config())

    first = graph.get_state(config()).config["configurable"]["checkpoint_id"]
    graph.invoke(None, config())

    assert graph.get_state(config()).config["configurable"]["checkpoint_id"] == first


def test_a_thread_parked_by_one_process_resumes_in_another(saver: DynamoDBSaver) -> None:
    """The whole reason this file exists.

    Two savers, two graphs, sharing nothing but the table -- which is what two Lambda
    invocations on two machines actually are.
    """
    from langgraph.types import Command

    parking_graph = build_graph(saver)
    raised = parking_graph.invoke({"order_id": "order-4471", "amount": 41000}, config())["__interrupt__"][0]
    assert PAYMENTS_CALLED == []

    del parking_graph, saver  # that container is gone
    resuming_graph = build_graph(DynamoDBSaver(TABLE, region_name=REGION))

    final = resuming_graph.invoke(Command(resume={raised.id: {"action": "approve"}}), config())

    assert final["status"] == "refunded"
    assert PAYMENTS_CALLED == ["order-4471"]


def test_a_rejection_survives_the_handover_too(saver: DynamoDBSaver) -> None:
    from langgraph.types import Command

    raised = build_graph(saver).invoke({"order_id": "order-4471", "amount": 41000}, config())[
        "__interrupt__"
    ][0]

    final = build_graph(DynamoDBSaver(TABLE, region_name=REGION)).invoke(
        Command(resume={raised.id: {"action": "reject", "reason": "duplicate"}}), config()
    )

    assert final["status"] == "rejected"
    assert PAYMENTS_CALLED == []


def test_history_is_listed_newest_first(saver: DynamoDBSaver) -> None:
    graph = build_graph(saver)
    graph.invoke({"order_id": "order-1", "amount": 900}, config("order-1"))

    history = list(graph.get_state_history(config("order-1")))

    assert len(history) > 1
    ids = [h.config["configurable"]["checkpoint_id"] for h in history]
    assert ids == sorted(ids, reverse=True)


def test_threads_do_not_see_each_other(saver: DynamoDBSaver) -> None:
    graph = build_graph(saver)
    graph.invoke({"order_id": "order-a", "amount": 41000}, config("order-a"))

    assert graph.get_state(config("order-b")).values == {}


def test_delete_thread(saver: DynamoDBSaver) -> None:
    graph = build_graph(saver)
    graph.invoke({"order_id": "order-a", "amount": 41000}, config("order-a"))

    saver.delete_thread("order-a")

    assert graph.get_state(config("order-a")).values == {}


def test_it_behaves_like_the_in_memory_saver(saver: DynamoDBSaver) -> None:
    """Same graph, same input, two savers, identical observable outcome."""
    from langgraph.types import Command

    outcomes = []
    for checkpointer in (saver, InMemorySaver()):
        reset_side_effects()
        graph = build_graph(checkpointer)
        raised = graph.invoke({"order_id": "order-x", "amount": 41000}, config("order-x"))["__interrupt__"][0]
        final = graph.invoke(Command(resume={raised.id: {"action": "approve"}}), config("order-x"))
        outcomes.append((final["status"], list(PAYMENTS_CALLED)))

    assert outcomes[0] == outcomes[1] == ("refunded", ["order-x"])
