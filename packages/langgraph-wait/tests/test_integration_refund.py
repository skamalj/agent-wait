"""The refund agent end to end: a real graph, a real checkpointer, real interrupts.

Only the cloud is faked. `Host` below is what a host writes: the one `if` that tells an
answer from a start, the `invoke()`, and `publish_interrupts()` after it. Nothing is
checked on the way in -- LangGraph is asked to resume, and what it does with a duplicate
is asserted here as a fact rather than guarded against.

`PAYMENTS_CALLED` is the point of all of it.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from typing import Any

import pytest
from agent_wait import InMemoryAnnounce, WaitEnvelope
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.types import Command
from langgraph_wait import publish_interrupts
from refund_agent.graph import PAYMENTS_CALLED, build_graph, reset_side_effects

START_MESSAGE = {"thread_id": "order-4471", "input": {"order_id": "order-4471", "amount": 41000}}


@pytest.fixture(autouse=True)
def clean_side_effects() -> Iterator[None]:
    reset_side_effects()
    yield
    reset_side_effects()


class Host:
    """Everything the caller owns. Deliberately short."""

    def __init__(self, graph: Any) -> None:
        self.graph = graph
        self.inbox = InMemoryAnnounce()

    def config(self, thread_id: str) -> dict[str, Any]:
        return {"configurable": {"thread_id": thread_id}}

    def deliver(self, message: dict[str, Any]) -> Any:
        """The router. `question_id` present means answer; that is the whole rule."""
        thread_id = message["thread_id"]
        if "question_id" in message:
            value: Any = Command(resume={message["question_id"]: message["answer"]})
        else:
            value = message["input"]
        result = self.graph.invoke(value, self.config(thread_id))
        publish_interrupts(result, thread_id, [self.inbox])
        return result

    # -- consumer-side helpers ---------------------------------------------------
    def latest(self) -> WaitEnvelope:
        return self.inbox.events[-1][1]

    def reply(self, envelope: WaitEnvelope, answer: Any) -> dict[str, Any]:
        """Exactly what a consumer does: copy `reply_with`, fill in `answer`."""
        return {**dict(envelope.reply_with), "answer": answer}

    def state(self, thread_id: str) -> Any:
        return self.graph.get_state(self.config(thread_id)).values


def sqlite_saver() -> Any:
    from langgraph.checkpoint.sqlite import SqliteSaver

    return SqliteSaver(sqlite3.connect(":memory:", check_same_thread=False))


@pytest.fixture(params=["memory", "sqlite"])
def host(request: pytest.FixtureRequest) -> Host:
    saver = InMemorySaver() if request.param == "memory" else sqlite_saver()
    return Host(build_graph(saver))


# ============================================================== the happy path
def test_a_small_refund_never_asks_anyone(host: Host) -> None:
    host.deliver({"thread_id": "order-1", "input": {"order_id": "order-1", "amount": 900}})

    assert host.inbox.events == []
    assert PAYMENTS_CALLED == ["order-1"]


def test_a_large_refund_parks_and_publishes(host: Host) -> None:
    host.deliver(START_MESSAGE)

    envelope = host.latest()
    assert envelope.type == "wait.created"
    assert envelope.question["function"] == "review"
    assert envelope.question["args"]["state"]["amount"] == 41000
    assert envelope.source == {"function": "review"}
    assert envelope.allowed_actions == ("approve", "reject")
    assert envelope.tags == {"approver_group": "finance"}
    assert envelope.expires_at is not None
    assert envelope.reply_with["question_id"] == envelope.question_id
    assert PAYMENTS_CALLED == [], "nothing irreversible before a human answers"


def test_approval_resumes_the_graph_and_refunds_once(host: Host) -> None:
    host.deliver(START_MESSAGE)

    host.deliver(host.reply(host.latest(), {"action": "approve", "note": "within budget"}))

    assert PAYMENTS_CALLED == ["order-4471"]
    assert host.state("order-4471")["decision"] == {"action": "approve", "note": "within budget"}, "verbatim"


def test_rejection_skips_the_refund(host: Host) -> None:
    host.deliver(START_MESSAGE)

    host.deliver(host.reply(host.latest(), {"action": "reject", "reason": "duplicate claim"}))

    assert PAYMENTS_CALLED == []
    assert host.state("order-4471")["status"] == "rejected"


# ============================================== duplicates: LangGraph's behaviour, asserted
def test_the_same_answer_delivered_twice_refunds_once(host: Host) -> None:
    """No check in the host. The second `Command(resume=...)` lands on a thread that has
    already moved past the interrupt, and LangGraph runs nothing."""
    host.deliver(START_MESSAGE)
    approval = host.reply(host.latest(), {"action": "approve"})

    host.deliver(approval)
    host.deliver(approval)

    assert PAYMENTS_CALLED == ["order-4471"]


def test_a_second_different_decision_after_the_first_changes_nothing(host: Host) -> None:
    host.deliver(START_MESSAGE)
    envelope = host.latest()

    host.deliver(host.reply(envelope, {"action": "approve"}))
    host.deliver(host.reply(envelope, {"action": "reject"}))

    assert PAYMENTS_CALLED == ["order-4471"]
    assert host.state("order-4471")["decision"] == {"action": "approve"}, "the first decision stands"


def test_the_default_is_just_an_answer_the_consumer_sends(host: Host) -> None:
    """Timeouts are the consumer's. When they decide the deadline has passed, they send
    the envelope's `default` as the answer, like any other answer."""
    host.deliver(START_MESSAGE)
    envelope = host.latest()

    host.deliver(host.reply(envelope, envelope.default))

    assert host.state("order-4471")["decision"] == {"action": "reject", "reason": "no response within P3D"}
    assert PAYMENTS_CALLED == []


# ============================================================== parallel and subgraph
def test_two_parallel_approvals_resume_independently() -> None:
    from parallel_graphs import build_parallel_graph

    host = Host(build_parallel_graph())
    host.deliver({"thread_id": "batch-1", "input": {}})
    first, second = (e for _, e in host.inbox.events)

    host.deliver(host.reply(first, {"action": "approve", "note": "within budget"}))
    host.deliver(host.reply(second, {"action": "reject", "note": "within budget"}))

    assert host.state("batch-1") == {
        "a": {"action": "approve", "note": "within budget"},
        "b": {"action": "reject", "note": "within budget"},
    }


def test_a_subgraph_interrupt_parks_and_resumes() -> None:
    from parallel_graphs import build_subgraph_graph

    host = Host(build_subgraph_graph())
    host.deliver({"thread_id": "nested-1", "input": {}})
    envelope = host.latest()
    assert envelope.question["function"] == "inner_node"

    host.deliver(host.reply(envelope, {"action": "approve", "note": "within budget"}))

    assert host.state("nested-1")["v"] == {"action": "approve", "note": "within budget"}
