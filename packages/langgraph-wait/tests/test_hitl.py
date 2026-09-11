"""`@hitl`: both modes on real graphs, and the middleware batch shape.

Three ways a tool can need a human, one decorator, one `publish_interrupts()` after
the run for all of them.
"""

from __future__ import annotations

from typing import Any, TypedDict

from agent_wait import InMemoryAnnounce, WaitPolicy
from langchain.agents import create_agent
from langchain.agents.middleware import HumanInTheLoopMiddleware
from langchain_core.language_models.fake_chat_models import GenericFakeChatModel
from langchain_core.messages import AIMessage
from langchain_core.tools import tool
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.graph import END, START, StateGraph
from langgraph.types import Command
from langgraph_wait import hitl, policy_for, publish_interrupts, question_id_for, questions_in

FINANCE = WaitPolicy(timeout="P1D", default={"action": "reject"}, tags={"approver_group": "finance"})
CALLS: list[tuple[str, Any]] = []


def cfg(thread_id: str, **extra: Any) -> dict[str, Any]:
    return {"configurable": {"thread_id": thread_id, **extra}}


class S(TypedDict, total=False):
    out: Any


# ============================================================ mode="interrupt"
@hitl(FINANCE)
def issue_refund(order_id: str, amount: int) -> str:
    CALLS.append(("refund", (order_id, amount)))
    return f"refunded {amount} on {order_id}"


def refund_graph() -> Any:
    g = StateGraph(S)
    g.add_node("refund", lambda s: {"out": issue_refund(order_id="o1", amount=41000)})
    g.add_edge(START, "refund")
    g.add_edge("refund", END)
    return g.compile(checkpointer=InMemorySaver())


def test_interrupt_mode_parks_and_publishes_the_call() -> None:
    CALLS.clear()
    graph = refund_graph()

    result = graph.invoke({}, cfg("t"))
    (envelope,) = publish_interrupts(result, "t", [InMemoryAnnounce()])

    assert CALLS == [], "the tool did not run"
    assert envelope.question == {"tool": "issue_refund", "args": {"order_id": "o1", "amount": 41000}}
    assert envelope.source == {"tool": "issue_refund"}
    assert envelope.tags == {"approver_group": "finance"}
    assert envelope.default == {"action": "reject"}


def test_interrupt_mode_runs_the_tool_on_approve() -> None:
    CALLS.clear()
    graph = refund_graph()
    (envelope,) = publish_interrupts(graph.invoke({}, cfg("t")), "t", [InMemoryAnnounce()])

    result = graph.invoke(Command(resume={envelope.question_id: {"action": "approve"}}), cfg("t"))

    assert CALLS == [("refund", ("o1", 41000))]
    assert result["out"] == "refunded 41000 on o1"


def test_interrupt_mode_runs_with_edited_args() -> None:
    CALLS.clear()
    graph = refund_graph()
    (envelope,) = publish_interrupts(graph.invoke({}, cfg("t")), "t", [InMemoryAnnounce()])

    graph.invoke(
        Command(resume={envelope.question_id: {"action": "approve", "args": {"amount": 100}}}), cfg("t")
    )

    assert CALLS == [("refund", ("o1", 100))], "the human's amount, the original order id"


def test_interrupt_mode_returns_the_decision_instead_of_running_on_reject() -> None:
    CALLS.clear()
    graph = refund_graph()
    (envelope,) = publish_interrupts(graph.invoke({}, cfg("t")), "t", [InMemoryAnnounce()])

    result = graph.invoke(
        Command(resume={envelope.question_id: {"action": "reject", "reason": "dup"}}), cfg("t")
    )

    assert CALLS == []
    assert result["out"] == {"status": "not_executed", "action": "reject", "reason": "dup"}


def test_the_policy_is_registered_by_tool_name() -> None:
    assert policy_for("issue_refund") == FINANCE
    assert policy_for("never_decorated") is None


# ============================================================ mode="async"
ASYNC_INBOX = InMemoryAnnounce()


@hitl(FINANCE, mode="async", announce=[ASYNC_INBOX])
def big_transfer(account: str, amount: int) -> str:
    CALLS.append(("transfer", (account, amount)))
    return "transferred"


def transfer_graph() -> Any:
    g = StateGraph(S)
    g.add_node("transfer", lambda s: {"out": big_transfer(account="acc-9", amount=5000)})
    g.add_node("after", lambda s: {"out": {**s["out"], "then": "carried on"}})
    g.add_edge(START, "transfer")
    g.add_edge("transfer", "after")
    g.add_edge("after", END)
    return g.compile(checkpointer=InMemorySaver())


def test_async_mode_publishes_and_the_graph_carries_on() -> None:
    """No publish_interrupts() anywhere: the decorator announced from inside the tool."""
    CALLS.clear()
    memory = ASYNC_INBOX
    memory.clear()

    result = transfer_graph().invoke({}, cfg("t"))

    assert CALLS == [], "the tool did not run"
    assert "__interrupt__" not in result, "nothing parked"
    assert result["out"]["status"] == "pending_approval"
    assert result["out"]["then"] == "carried on", "the node after it ran in the same invoke"
    (transition, envelope) = memory.events[0]
    assert transition == "created"
    assert envelope.question == {"tool": "big_transfer", "args": {"account": "acc-9", "amount": 5000}}
    assert envelope.question_id == result["out"]["question_id"]
    assert envelope.thread_id == "t"
    assert envelope.source == {"tool": "big_transfer"}


def test_async_question_ids_are_deterministic() -> None:
    """The same call on the same thread is the same question; a consumer sees one."""
    memory = ASYNC_INBOX
    memory.clear()
    graph = transfer_graph()

    graph.invoke({}, cfg("t"))
    graph.invoke({}, cfg("t"))

    ids = {e.question_id for _, e in memory.events}
    assert len(memory.events) == 2 and len(ids) == 1
    assert ids == {question_id_for("t", "big_transfer", {"account": "acc-9", "amount": 5000})}
    other = transfer_graph().invoke({}, cfg("t2"))
    assert other["out"]["question_id"] not in ids, "a different thread is a different question"


def test_async_mode_requires_announcers_at_decoration_time() -> None:
    """Not at call time, three days later inside a Lambda."""
    import pytest

    with pytest.raises(ValueError, match="announce"):
        hitl(FINANCE, mode="async")(lambda: None)


def test_async_mode_publishes_nothing_through_publish_interrupts() -> None:
    """There is no __interrupt__ to find, so the after-run call is a harmless no-op."""
    ASYNC_INBOX.clear()
    result = transfer_graph().invoke({}, cfg("t"))

    assert publish_interrupts(result, "t", [InMemoryAnnounce()]) == []
    assert len(ASYNC_INBOX.events) == 1, "the decorator already published it"


# ============================================================ HumanInTheLoopMiddleware
@tool
def issue_refund_tool(order_id: str, amount: int) -> str:
    """Refund an order."""
    CALLS.append(("refund", (order_id, amount)))
    return "refunded"


@tool
def send_email(to: str) -> str:
    """Send an email."""
    CALLS.append(("email", to))
    return "sent"


class FakeToolCaller(GenericFakeChatModel):
    def bind_tools(self, tools: Any, **kwargs: Any) -> Any:
        return self


def middleware_agent() -> Any:
    hitl(FINANCE)(issue_refund_tool.func)  # register the policy under the tool's name
    model = FakeToolCaller(
        messages=iter(
            [
                AIMessage(
                    content="",
                    tool_calls=[
                        {
                            "name": "issue_refund_tool",
                            "args": {"order_id": "o1", "amount": 41000},
                            "id": "c1",
                        },
                        {"name": "send_email", "args": {"to": "x@y"}, "id": "c2"},
                    ],
                ),
                AIMessage(content="done"),
            ]
        )
    )
    return create_agent(
        model,
        tools=[issue_refund_tool, send_email],
        middleware=[
            HumanInTheLoopMiddleware(
                interrupt_on={
                    "issue_refund_tool": True,
                    "send_email": {"allowed_decisions": ["approve", "reject"]},
                }
            )
        ],
        checkpointer=InMemorySaver(),
    )


def test_middleware_batches_two_tool_calls_into_one_question() -> None:
    """The batch is one question -- which is also the shape that sidesteps the
    same-id-in-one-ToolNode problem (langgraph #6626)."""
    CALLS.clear()
    agent = middleware_agent()

    result = agent.invoke({"messages": [("user", "refund o1")]}, cfg("t"))
    (q,) = questions_in(result)

    assert [a["name"] for a in q.question["actions"]] == ["issue_refund_tool", "send_email"]
    assert q.question["actions"][0]["args"] == {"order_id": "o1", "amount": 41000}
    assert [r["action_name"] for r in q.question["review"]] == ["issue_refund_tool", "send_email"]
    assert q.source == {"tools": ["issue_refund_tool", "send_email"], "via": "HumanInTheLoopMiddleware"}
    assert q.policy == FINANCE, "the @hitl policy for the first tool in the batch"
    assert q.question_id == result["__interrupt__"][0].id


def test_middleware_decisions_go_back_in_batch_order() -> None:
    CALLS.clear()
    agent = middleware_agent()
    (envelope,) = publish_interrupts(
        agent.invoke({"messages": [("user", "go")]}, cfg("t")), "t", [InMemoryAnnounce()]
    )

    answer = {"decisions": [{"type": "approve"}, {"type": "reject", "message": "not now"}]}
    result = agent.invoke(Command(resume={envelope.question_id: answer}), cfg("t"))

    assert CALLS == [("refund", ("o1", 41000))], "approved ran, rejected did not"
    assert "__interrupt__" not in result
