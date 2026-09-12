"""`@wait`: a function becomes interruptible. Both modes, on nodes and on tools, on real
graphs with a real checkpointer."""

from __future__ import annotations

from typing import Any, TypedDict

import pytest
from langchain_core.tools import tool
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.graph import END, START, StateGraph
from langgraph.types import Command

from agent_wait import InMemoryAnnounce, WaitPolicy, question_id_for
from agent_wait.langgraph import publish_interrupts, wait

FINANCE = WaitPolicy(timeout="P1D", default={"action": "reject"}, tags={"approver_group": "finance"})
CALLS: list[Any] = []


def cfg(thread_id: str) -> dict[str, Any]:
    return {"configurable": {"thread_id": thread_id}}


class S(TypedDict, total=False):
    out: Any


def one_node_graph(fn: Any) -> Any:
    g = StateGraph(S)
    g.add_node("n", fn)
    g.add_edge(START, "n")
    g.add_edge("n", END)
    return g.compile(checkpointer=InMemorySaver())


def park(graph: Any, thread_id: str = "t") -> Any:
    """Invoke, publish, return the one envelope."""
    (envelope,) = publish_interrupts(graph.invoke({}, cfg(thread_id)), thread_id, [InMemoryAnnounce()])
    return envelope


def answer(graph: Any, envelope: Any, value: Any) -> Any:
    return graph.invoke(Command(resume={envelope.question_id: value}), cfg(envelope.thread_id))


# ============================================================ a tool: runs only on approve
@wait(FINANCE)
def issue_refund(order_id: str, amount: int) -> str:
    CALLS.append(("refund", order_id, amount))
    return f"refunded {amount} on {order_id}"


def refund_graph() -> Any:
    return one_node_graph(lambda s: {"out": issue_refund(order_id="o1", amount=41000)})


def test_calling_a_wait_function_parks_the_graph_and_publishes_the_call() -> None:
    CALLS.clear()

    envelope = park(refund_graph())

    assert CALLS == [], "the body did not run"
    assert envelope.question == {"function": "issue_refund", "args": {"order_id": "o1", "amount": 41000}}
    assert envelope.source == {"function": "issue_refund"}
    assert envelope.tags == {"approver_group": "finance"}
    assert envelope.default == {"action": "reject"}
    assert envelope.expires_at is not None


def test_approve_runs_the_body() -> None:
    CALLS.clear()
    graph = refund_graph()

    result = answer(graph, park(graph), {"action": "approve"})

    assert CALLS == [("refund", "o1", 41000)]
    assert result["out"] == "refunded 41000 on o1"


def test_approve_with_edited_args_runs_with_them() -> None:
    CALLS.clear()
    graph = refund_graph()

    answer(graph, park(graph), {"action": "approve", "args": {"amount": 100}})

    assert CALLS == [("refund", "o1", 100)], "the human's amount, the original order id"


def test_anything_but_approve_returns_the_answer_in_place_of_the_body() -> None:
    CALLS.clear()
    graph = refund_graph()

    result = answer(graph, park(graph), {"action": "reject", "reason": "dup"})

    assert CALLS == []
    assert result["out"] == {"action": "reject", "reason": "dup"}, "verbatim, so a model sees why"


# ============================================================ a node: always runs, gets the answer
@wait(FINANCE)
def review(state: S, decision: Any = None) -> S:
    CALLS.append(("review", decision))
    return {"out": decision}


def test_a_function_with_a_decision_parameter_always_runs_and_receives_the_answer() -> None:
    CALLS.clear()
    graph = one_node_graph(review)
    envelope = park(graph)
    assert envelope.question == {"function": "review", "args": {"state": {}}}, "`decision` is not an arg"

    result = answer(graph, envelope, {"action": "reject", "note": "no"})

    assert CALLS == [("review", {"action": "reject", "note": "no"})], "ran even on reject"
    assert result["out"] == {"action": "reject", "note": "no"}


def test_the_decision_parameter_name_is_configurable() -> None:
    CALLS.clear()

    @wait(FINANCE, decision="verdict")
    def judge(state: S, verdict: Any = None) -> S:
        CALLS.append(verdict)
        return {"out": verdict}

    graph = one_node_graph(judge)
    answer(graph, park(graph), "guilty")

    assert CALLS == ["guilty"]


# ============================================================ mode="async"
ASYNC_INBOX = InMemoryAnnounce()


@wait(FINANCE, mode="async", announce=[ASYNC_INBOX])
def big_transfer(account: str, amount: int) -> str:
    CALLS.append(("transfer", account, amount))
    return "transferred"


def transfer_graph() -> Any:
    g = StateGraph(S)
    g.add_node("transfer", lambda s: {"out": big_transfer(account="acc-9", amount=5000)})
    g.add_node("after", lambda s: {"out": {**s["out"], "then": "carried on"}})
    g.add_edge(START, "transfer")
    g.add_edge("transfer", "after")
    g.add_edge("after", END)
    return g.compile(checkpointer=InMemorySaver())


def test_async_publishes_from_the_decorator_and_the_graph_carries_on() -> None:
    """No publish_interrupts() anywhere: the decorator announced from inside the call."""
    CALLS.clear()
    ASYNC_INBOX.clear()

    result = transfer_graph().invoke({}, cfg("t"))

    assert CALLS == [], "the body did not run"
    assert "__interrupt__" not in result, "nothing parked"
    assert result["out"]["status"] == "pending_approval"
    assert result["out"]["then"] == "carried on", "the next node ran in the same invoke"
    (_, envelope) = ASYNC_INBOX.events[0]
    assert envelope.question == {"function": "big_transfer", "args": {"account": "acc-9", "amount": 5000}}
    assert envelope.question_id == result["out"]["question_id"]
    assert envelope.thread_id == "t"
    assert envelope.source == {"function": "big_transfer"}


def test_async_question_ids_are_deterministic_per_thread_and_call() -> None:
    ASYNC_INBOX.clear()
    graph = transfer_graph()

    graph.invoke({}, cfg("t"))
    graph.invoke({}, cfg("t"))
    other = transfer_graph().invoke({}, cfg("t2"))

    ids = {e.question_id for _, e in ASYNC_INBOX.events[:2]}
    assert len(ids) == 1, "the same call on the same thread is the same question"
    assert ids == {question_id_for("t", "big_transfer", {"account": "acc-9", "amount": 5000})}
    assert other["out"]["question_id"] not in ids, "a different thread is a different question"


def test_async_requires_announcers_at_decoration_time() -> None:
    """Not at call time, three days later inside a Lambda."""
    with pytest.raises(ValueError, match="announce"):
        wait(FINANCE, mode="async")(lambda: None)


def test_async_leaves_nothing_for_publish_interrupts() -> None:
    ASYNC_INBOX.clear()
    result = transfer_graph().invoke({}, cfg("t"))

    assert publish_interrupts(result, "t", [InMemoryAnnounce()]) == []
    assert len(ASYNC_INBOX.events) == 1, "the decorator already published it"


# ============================================================ under @tool
def test_wait_composes_under_langchain_tool() -> None:
    """`@tool` sees the wrapped function's signature and docstring via functools.wraps."""

    @tool
    @wait(FINANCE)
    def send_gift(to: str, amount: int) -> str:
        """Send a gift card."""
        CALLS.append(("gift", to, amount))
        return "sent"

    assert send_gift.name == "send_gift"
    assert send_gift.description == "Send a gift card."
    assert set(send_gift.args) == {"to", "amount"}

    CALLS.clear()
    graph = one_node_graph(lambda s: {"out": send_gift.invoke({"to": "x", "amount": 5})})
    envelope = park(graph)
    assert envelope.question == {"function": "send_gift", "args": {"to": "x", "amount": 5}}
    answer(graph, envelope, {"action": "approve"})
    assert CALLS == [("gift", "x", 5)]
