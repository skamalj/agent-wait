"""The four scenarios of REQUIREMENTS section 13, against a real LangGraph graph.

Everything here is real except the cloud: a real graph, a real checkpointer, real
interrupts, real tokens, the real `dispatch()`/`register()` pair. Only SQS and Lambda are
replaced, by `LocalAgent`, which crashes exactly where the scenarios say it does.

The assertion that matters in every one of them is the same:
`PAYMENTS_CALLED == ["order-4471"]`. Once. Not twice, not zero.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from typing import Any

import pytest
from agent_wait import FailingAnnounce, Ignore, InMemoryWaitStore, SqliteWaitStore
from driver import Died, LocalAgent
from langgraph.checkpoint.memory import InMemorySaver
from refund_agent.graph import PAYMENTS_CALLED, build_graph, reset_side_effects

START_MESSAGE = {
    "thread_id": "order-4471",
    "input": {"order_id": "order-4471", "amount": 41000},
    "message_id": "evt-return-4471",
}


@pytest.fixture(autouse=True)
def clean_side_effects() -> Iterator[None]:
    reset_side_effects()
    yield
    reset_side_effects()


def sqlite_saver() -> Any:
    from langgraph.checkpoint.sqlite import SqliteSaver

    return SqliteSaver(sqlite3.connect(":memory:", check_same_thread=False))


@pytest.fixture(params=["memory", "sqlite"])
def agent(request: pytest.FixtureRequest) -> LocalAgent:
    """The whole suite runs twice: once entirely in memory, once with a real SQLite
    checkpointer and a real SQLite wait store, which is the closest local analogue of
    the deployed shape."""
    if request.param == "memory":
        return LocalAgent(graph=build_graph(InMemorySaver()), store=InMemoryWaitStore())
    return LocalAgent(graph=build_graph(sqlite_saver()), store=SqliteWaitStore(":memory:"))


# ============================================================== the happy path
def test_a_small_refund_never_asks_anyone(agent: LocalAgent) -> None:
    """The wait machinery costs nothing when the graph does not pause."""
    result = agent.deliver(
        {"thread_id": "order-1", "input": {"order_id": "order-1", "amount": 900}, "message_id": "m"}
    )

    assert result.envelopes == []
    assert PAYMENTS_CALLED == ["order-1"]
    assert agent.announce.events == []


def test_a_large_refund_parks_and_announces(agent: LocalAgent) -> None:
    result = agent.deliver(START_MESSAGE)

    assert len(result.envelopes) == 1
    envelope = result.envelopes[0]
    assert envelope.type == "wait.created"
    assert envelope.question["kind"] == "refund_approval"
    assert envelope.question["amount"] == 41000
    assert envelope.allowed_actions == ("approve", "reject")
    assert envelope.tags == {"approver_group": "finance"}
    assert envelope.token.startswith("aw1.")
    assert PAYMENTS_CALLED == [], "nothing irreversible before a human answers"


def test_approval_resumes_the_graph_and_refunds_once(agent: LocalAgent) -> None:
    agent.deliver(START_MESSAGE)

    agent.deliver(agent.answer("approve", answer_id="click-9f1"))

    assert PAYMENTS_CALLED == ["order-4471"]
    assert agent.wait().status == "resumed"
    assert agent.graph.get_state(agent.adapter.config_for("order-4471")).values["status"] == "refunded"


def test_rejection_skips_the_refund(agent: LocalAgent) -> None:
    agent.deliver(START_MESSAGE)

    agent.deliver(agent.answer("reject", answer_id="click-9f1", payload={"reason": "duplicate claim"}))

    assert PAYMENTS_CALLED == []
    assert agent.graph.get_state(agent.adapter.config_for("order-4471")).values["status"] == "rejected"


# ============================================================== scenario A
def test_scenario_a_crash_before_register_then_double_click(agent: LocalAgent) -> None:
    """Crash after the checkpoint but before `register()`; SQS redelivers; the wait is
    created once. Then: the same click twice, a different decision, a forged token, and
    a timer that arrives too late. One refund."""
    with pytest.raises(Died):
        agent.deliver(START_MESSAGE, die_after_invoke=True)
    assert agent.store.find(thread_id="order-4471") == [], "the crash was before any write"

    # -- the redelivery
    result = agent.deliver(START_MESSAGE)
    assert len(result.envelopes) == 1
    assert len(agent.store.find(thread_id="order-4471")) == 1
    assert len(agent.envelopes("created")) == 1

    # -- a third delivery must change nothing
    agent.deliver(START_MESSAGE)
    assert len(agent.store.find(thread_id="order-4471")) == 1
    assert len(agent.envelopes("created")) == 1

    # -- approve
    approved = agent.deliver(agent.answer("approve", answer_id="click-9f1"))
    assert not isinstance(approved, Ignore)
    assert PAYMENTS_CALLED == ["order-4471"]

    # -- the same click again: success, not a conflict
    again = agent.deliver(agent.answer("approve", answer_id="click-9f1"))
    assert isinstance(again, Ignore) and again.reason == "duplicate"

    # -- a different decision arriving late
    reject = agent.deliver(agent.answer("reject", answer_id="click-different"))
    assert isinstance(reject, Ignore) and reject.reason == "already_answered"

    # -- a tampered token
    token = agent.token()
    forged = token[:-1] + ("A" if token[-1] != "A" else "B")
    tampered = agent.deliver(agent.answer("approve", answer_id="evil", token=forged))
    assert isinstance(tampered, Ignore) and tampered.reason == "token_invalid"

    # -- the timer, three days late
    agent.clock.advance(3 * 86400)
    late = agent.deliver(agent.timeout_message())
    assert isinstance(late, Ignore) and late.reason == "already_answered"

    assert PAYMENTS_CALLED == ["order-4471"], "exactly one refund, after all of that"


# ============================================================== scenario B
def test_scenario_b_timeout_applies_the_default(agent: LocalAgent) -> None:
    """Nobody answers. The schedule fires, delivers `action: timeout` to the agent's own
    entry point, and the declared default is applied."""
    agent.deliver(START_MESSAGE)
    agent.clock.advance(3 * 86400)

    agent.deliver(agent.timeout_message())

    assert PAYMENTS_CALLED == []
    state = agent.graph.get_state(agent.adapter.config_for("order-4471"))
    assert state.values["status"] == "rejected"
    # Section 18.1 as amended: the graph sees the default its author wrote.
    assert state.values["decision"] == {
        "action": "reject",
        "reason": "no response within P3D",
    }
    # The record, meanwhile, says a timer did it.
    assert agent.wait().action == "timeout"
    assert agent.wait().actor == "system:timer"
    assert agent.wait().status == "resumed"
    assert "expired" in agent.announce.transitions()

    late = agent.deliver(agent.answer("approve", answer_id="click-too-late"))
    assert isinstance(late, Ignore) and late.reason == "already_answered"
    assert PAYMENTS_CALLED == []


def test_scenario_b_a_second_timer_firing_is_harmless(agent: LocalAgent) -> None:
    agent.deliver(START_MESSAGE)
    agent.clock.advance(3 * 86400)
    agent.deliver(agent.timeout_message())

    repeat = agent.deliver(agent.timeout_message())

    assert isinstance(repeat, Ignore) and repeat.reason == "duplicate"


# ============================================================== scenario C
def test_scenario_c_crash_after_resume_before_ack(agent: LocalAgent) -> None:
    """The refund happened and then the handler died before acking. SQS redelivers the
    approval. The refund must not happen again."""
    agent.deliver(START_MESSAGE)

    approval = agent.answer("approve", answer_id="click-9f1")
    with pytest.raises(Died):
        agent.deliver(approval, die_after_invoke=True)
    assert PAYMENTS_CALLED == ["order-4471"], "the side effect did happen"

    redelivered = agent.deliver(approval)

    assert isinstance(redelivered, Ignore)
    assert redelivered.reason == "duplicate"
    assert PAYMENTS_CALLED == ["order-4471"], "and did not happen twice"


def test_scenario_c_register_settles_the_wait_on_the_next_run(agent: LocalAgent) -> None:
    """The crash left the wait `answered` rather than `resumed`. The next message on the
    thread finalises it -- the bookkeeping catches up on its own."""
    agent.deliver(START_MESSAGE)
    approval = agent.answer("approve", answer_id="click-9f1")
    with pytest.raises(Died):
        agent.deliver(approval, die_after_invoke=True)
    assert agent.wait().status == "answered"

    agent.deliver(approval)
    agent.runtime.register([], agent.adapter.config_for("order-4471"), "order-4471")

    assert agent.wait().status == "resumed"
    assert "resumed" in agent.announce.transitions()


# ============================================================== scenario D
def test_scenario_d_sweeper_completes_a_half_finished_register(agent: LocalAgent) -> None:
    """The process died between the DynamoDB write and the announce, so nobody knows the
    wait exists and no timer will fire. The sweeper repairs it."""
    healthy = agent.announce
    agent.runtime.announce.adapters = [FailingAnnounce()]

    agent.deliver(START_MESSAGE)
    wait = agent.store.find(thread_id="order-4471")[0]
    assert wait.notified_at is None, "an unannounced wait is invisible to the world"

    agent.runtime.announce.adapters = [healthy]
    counts = agent.runtime.sweep()

    assert counts["announced"] == 1
    assert len(healthy.of("created")) == 1
    assert agent.store.get(wait.wait_id).notified_at is not None

    # and it is answerable, which is the whole point of repairing it
    agent.deliver(agent.answer("approve", answer_id="click-9f1"))
    assert PAYMENTS_CALLED == ["order-4471"]


# ============================================================== parallel and subgraph
def test_two_parallel_approvals_resume_independently(agent: LocalAgent) -> None:
    """Rule 11, end to end: two waits, two tokens, and answering one leaves the other
    exactly where it was."""
    from parallel_graphs import build_parallel_graph

    parallel = LocalAgent(graph=build_parallel_graph(), store=InMemoryWaitStore())
    result = parallel.deliver({"thread_id": "batch-1", "input": {}, "message_id": "m"})
    assert len(result.envelopes) == 2

    first, second = result.envelopes
    outcome = parallel.deliver(parallel.answer("approve", answer_id="click-a", index=0))

    assert not isinstance(outcome, Ignore)
    assert parallel.store.get(first.wait_id).status in ("answered", "resumed")
    still_open = parallel.store.get(second.wait_id)
    assert still_open.status == "pending"
    assert still_open.notified_at is not None

    parallel.deliver(parallel.answer("reject", answer_id="click-b", index=1))
    assert parallel.graph.get_state(parallel.adapter.config_for("batch-1")).values == {
        "a": {"action": "approve", "note": "within budget"},
        "b": {"action": "reject", "note": "within budget"},
    }


def test_a_subgraph_interrupt_parks_and_resumes(agent: LocalAgent) -> None:
    from parallel_graphs import build_subgraph_graph

    nested = LocalAgent(graph=build_subgraph_graph(), store=InMemoryWaitStore())
    result = nested.deliver({"thread_id": "nested-1", "input": {}, "message_id": "m"})

    assert len(result.envelopes) == 1
    assert result.envelopes[0].question == {"kind": "inner_approval"}

    nested.deliver(nested.answer("approve", answer_id="click-1"))

    assert nested.store.get(result.envelopes[0].wait_id).status == "resumed"
    assert nested.graph.get_state(nested.adapter.config_for("nested-1")).values["v"] == {
        "action": "approve",
        "note": "within budget",
    }
