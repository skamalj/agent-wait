"""`publish_interrupts()` against real `graph.invoke()` results.

Everything comes from what LangGraph returned -- no graph handle, no `get_state()`.
"""

from __future__ import annotations

from typing import Any, TypedDict

from langgraph.checkpoint.memory import InMemorySaver
from langgraph.graph import END, START, StateGraph
from langgraph.types import Command, interrupt

from agent_wait import InMemoryAnnounce, WaitPolicy
from agent_wait.langgraph import publish_interrupts, questions_in, wait

POLICY = WaitPolicy(timeout="PT2H", default={"action": "reject"}, allowed_actions=("approve", "reject"))


class S(TypedDict, total=False):
    a: Any
    b: Any


def graph_with(*nodes: tuple[str, Any]) -> Any:
    g = StateGraph(S)
    for name, fn in nodes:
        g.add_node(name, fn)
        g.add_edge(START, name)
        g.add_edge(name, END)
    return g.compile(checkpointer=InMemorySaver())


def cfg(thread_id: str) -> dict[str, Any]:
    return {"configurable": {"thread_id": thread_id}}


@wait(POLICY)
def approve_a(state: S, decision: Any = None) -> S:
    return {"a": decision}


@wait(POLICY)
def approve_b(state: S, decision: Any = None) -> S:
    return {"b": decision}


# ------------------------------------------------------------------ the @wait shape
def test_a_wait_interrupt_becomes_one_envelope_with_its_policy() -> None:
    graph = graph_with(("review", approve_a))
    memory = InMemoryAnnounce()

    result = graph.invoke({}, cfg("t"))
    sent = publish_interrupts(result, "t", [memory])

    (envelope,) = sent
    assert envelope.question == {"function": "approve_a", "args": {"state": {}}}
    assert envelope.source == {"function": "approve_a"}
    assert envelope.allowed_actions == ("approve", "reject")
    assert envelope.default == {"action": "reject"}
    assert envelope.expires_at is not None
    assert envelope.question_id == result["__interrupt__"][0].id
    assert envelope.reply_with == {"thread_id": "t", "question_id": envelope.question_id, "answer": None}
    assert [e.question_id for _, e in memory.events] == [envelope.question_id]


# ------------------------------------------------------------------ a bare interrupt
def test_a_bare_interrupt_is_published_with_the_default_policy() -> None:
    graph = graph_with(("n", lambda s: {"a": interrupt({"raw": True})}))

    (envelope,) = publish_interrupts(graph.invoke({}, cfg("t")), "t", [InMemoryAnnounce()])

    assert envelope.question == {"raw": True}
    assert envelope.allowed_actions == ("resume",)
    assert envelope.expires_at is None
    assert envelope.source is None


# ------------------------------------------------------------------ nothing parked
def test_a_run_that_finishes_publishes_nothing() -> None:
    graph = graph_with(("n", lambda s: {"a": 1}))
    memory = InMemoryAnnounce()

    assert publish_interrupts(graph.invoke({}, cfg("t")), "t", [memory]) == []
    assert memory.events == []


def test_garbage_in_publishes_nothing() -> None:
    assert publish_interrupts(None, "t", []) == []
    assert publish_interrupts("a string", "t", []) == []
    assert publish_interrupts({"__interrupt__": []}, "t", []) == []
    assert questions_in({}) == []


# ------------------------------------------------------------------ several at once
def test_parallel_nodes_give_one_envelope_each() -> None:
    graph = graph_with(("na", approve_a), ("nb", approve_b))

    sent = publish_interrupts(graph.invoke({}, cfg("t")), "t", [InMemoryAnnounce()])

    assert sorted(e.question["function"] for e in sent) == ["approve_a", "approve_b"]
    assert len({e.question_id for e in sent}) == 2, "distinct interrupts, distinct ids"


# ------------------------------------------------------------------ stream chunks
def test_a_stream_chunk_has_the_same_shape() -> None:
    graph = graph_with(("review", approve_a))
    memory = InMemoryAnnounce()

    sent: list[Any] = []
    for chunk in graph.stream({}, cfg("t")):
        sent += publish_interrupts(chunk, "t", [memory])

    assert len(sent) == 1


# ------------------------------------------------------------------ the round trip
def test_the_answer_goes_back_verbatim_through_the_stub() -> None:
    """What the envelope hands out is enough to resume with; nothing else is needed."""
    graph = graph_with(("review", approve_a))
    (envelope,) = publish_interrupts(graph.invoke({}, cfg("t")), "t", [InMemoryAnnounce()])

    reply = {**dict(envelope.reply_with), "answer": {"action": "approve", "note": "ok"}}
    result = graph.invoke(Command(resume={reply["question_id"]: reply["answer"]}), cfg(reply["thread_id"]))

    assert result["a"] == {"action": "approve", "note": "ok"}
    assert publish_interrupts(result, "t", [InMemoryAnnounce()]) == [], "nothing left parked"
