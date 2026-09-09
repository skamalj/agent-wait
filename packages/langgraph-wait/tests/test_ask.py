"""`ask()` and the `__wait__` envelope it rides in."""

from __future__ import annotations

from typing import Any, TypedDict

from agent_wait import WaitPolicy
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.graph import END, START, StateGraph
from langgraph.types import Command, interrupt
from langgraph_wait import ask, unwrap


class S(TypedDict, total=False):
    decision: dict[str, Any]


def graph_with(node: Any) -> Any:
    graph = StateGraph(S)
    graph.add_node("n", node)
    graph.add_edge(START, "n")
    graph.add_edge("n", END)
    return graph.compile(checkpointer=InMemorySaver())


POLICY = WaitPolicy(
    timeout="P3D",
    default={"action": "reject"},
    allowed_actions=("approve", "reject"),
    tags={"approver_group": "finance"},
)


def test_ask_carries_the_policy_inside_the_interrupt_value() -> None:
    """Section 16.2: the interrupt payload is the only channel there is."""
    graph = graph_with(lambda state: {"decision": ask({"amount": 41000}, POLICY)})
    config = {"configurable": {"thread_id": "t"}}

    raised = graph.invoke({}, config)["__interrupt__"][0]

    assert raised.value["question"] == {"amount": 41000}
    assert raised.value["__wait__"] == POLICY.to_dict()


def test_ask_returns_the_resume_value() -> None:
    graph = graph_with(lambda state: {"decision": ask({"amount": 41000}, POLICY)})
    config = {"configurable": {"thread_id": "t"}}
    raised = graph.invoke({}, config)["__interrupt__"][0]

    final = graph.invoke(Command(resume={raised.id: {"action": "approve", "note": "ok"}}), config)

    assert final["decision"] == {"action": "approve", "note": "ok"}


def test_ask_without_a_policy_uses_the_default() -> None:
    graph = graph_with(lambda state: {"decision": ask({"amount": 1})})
    config = {"configurable": {"thread_id": "t"}}

    raised = graph.invoke({}, config)["__interrupt__"][0]

    assert raised.value["__wait__"] == WaitPolicy().to_dict()


def test_unwrap_round_trips_what_ask_produced() -> None:
    question, policy = unwrap({"question": {"amount": 1}, "__wait__": POLICY.to_dict()})

    assert question == {"amount": 1}
    assert policy == POLICY


def test_a_bare_interrupt_still_works() -> None:
    """A graph that already calls `interrupt()` gains durable waits with no edit at all
    (section 5)."""
    graph = graph_with(lambda state: {"decision": interrupt({"raw": True})})
    config = {"configurable": {"thread_id": "t"}}

    raised = graph.invoke({}, config)["__interrupt__"][0]
    question, policy = unwrap(raised.value)

    assert question == {"raw": True}
    assert policy == WaitPolicy()
    assert policy.allowed_actions == ("resume",)


def test_unwrap_tolerates_a_scalar_interrupt_value() -> None:
    question, policy = unwrap("please confirm")

    assert question == "please confirm"
    assert policy == WaitPolicy()
