"""The spike REQUIREMENTS section 9.2 asks for: what does LangGraph actually do?

The design assumes two things about interrupts. If either is false, the idempotency key
is not stable and rule 1 collapses. So they are asserted, not assumed -- and the third
group of tests pins a *bug* we have to work around, so that a future LangGraph fix
arrives as a failing test rather than as silence.

Findings against langgraph 1.2.11 are written up in `adapter.py`'s module docstring and
in TEST_REPORT.md.
"""

from __future__ import annotations

from typing import Any, TypedDict

import pytest
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.graph import END, START, StateGraph
from langgraph.types import Command, interrupt

pytestmark = pytest.mark.spike


def checkpoint_id(graph: Any, config: dict[str, Any]) -> str:
    return graph.get_state(config).config["configurable"]["checkpoint_id"]


# --------------------------------------------------------------------------- fixtures
class Simple(TypedDict, total=False):
    amount: int
    decision: dict[str, Any]


def simple_graph() -> Any:
    def review(state: Simple) -> Simple:
        return {"decision": interrupt({"amount": state["amount"]})}

    graph = StateGraph(Simple)
    graph.add_node("review", review)
    graph.add_edge(START, "review")
    graph.add_edge("review", END)
    return graph.compile(checkpointer=InMemorySaver())


class Parallel(TypedDict, total=False):
    a: dict[str, Any]
    b: dict[str, Any]


def parallel_graph(log: list[str]) -> Any:
    def node_a(state: Parallel) -> Parallel:
        decision = interrupt({"which": "a"})
        log.append("a")
        return {"a": decision}

    def node_b(state: Parallel) -> Parallel:
        decision = interrupt({"which": "b"})
        log.append("b")
        return {"b": decision}

    graph = StateGraph(Parallel)
    graph.add_node("na", node_a)
    graph.add_node("nb", node_b)
    graph.add_edge(START, "na")
    graph.add_edge(START, "nb")
    graph.add_edge("na", END)
    graph.add_edge("nb", END)
    return graph.compile(checkpointer=InMemorySaver())


# --------------------------------------------------------------------------- 1. ids
def test_interrupt_id_is_stable_across_reinvocation() -> None:
    """The idempotency key is sha256(thread|interrupt|checkpoint). If the id moved, a
    redelivery would create a second wait and rule 1 would be unimplementable."""
    graph = simple_graph()
    config = {"configurable": {"thread_id": "t1"}}

    first = graph.invoke({"amount": 41000}, config)["__interrupt__"][0]
    second = graph.invoke(None, config)["__interrupt__"][0]

    assert first.id == second.id


def test_checkpoint_id_is_stable_while_parked() -> None:
    graph = simple_graph()
    config = {"configurable": {"thread_id": "t1"}}
    graph.invoke({"amount": 41000}, config)

    before = checkpoint_id(graph, config)
    graph.invoke(None, config)

    assert checkpoint_id(graph, config) == before
    assert before, "checkpoint_id must be present on a parked thread"


def test_interrupt_value_survives_the_round_trip() -> None:
    graph = simple_graph()
    config = {"configurable": {"thread_id": "t1"}}

    raised = graph.invoke({"amount": 41000}, config)["__interrupt__"][0]

    assert raised.value == {"amount": 41000}


# --------------------------------------------------------------------------- 2. tasks
def test_get_state_tasks_report_the_pending_interrupt() -> None:
    graph = simple_graph()
    config = {"configurable": {"thread_id": "t1"}}
    raised = graph.invoke({"amount": 41000}, config)["__interrupt__"][0]

    tasks = graph.get_state(config).tasks

    assert [i.id for task in tasks for i in task.interrupts] == [raised.id]
    assert all(task.result is None for task in tasks), "a parked task has no result yet"


def test_tasks_empty_once_the_thread_finishes() -> None:
    graph = simple_graph()
    config = {"configurable": {"thread_id": "t1"}}
    raised = graph.invoke({"amount": 41000}, config)["__interrupt__"][0]

    graph.invoke(Command(resume={raised.id: {"action": "approve"}}), config)

    assert graph.get_state(config).tasks == ()


# --------------------------------------------------------------------------- 3. parallel
def test_two_parallel_interrupts_get_distinct_ids() -> None:
    log: list[str] = []
    graph = parallel_graph(log)
    config = {"configurable": {"thread_id": "p1"}}

    raised = graph.invoke({}, config)["__interrupt__"]

    assert len(raised) == 2
    assert raised[0].id != raised[1].id
    assert {i.value["which"] for i in raised} == {"a", "b"}
    assert log == [], "neither node ran past its interrupt"


def test_resuming_one_parallel_interrupt_leaves_the_other_parked() -> None:
    """Rule 11, at the framework level. Dict-keyed resume is what makes this work; a
    bare `Command(resume=value)` would answer both."""
    log: list[str] = []
    graph = parallel_graph(log)
    config = {"configurable": {"thread_id": "p1"}}
    first, second = graph.invoke({}, config)["__interrupt__"]

    out = graph.invoke(Command(resume={first.id: {"ok": 1}}), config)

    assert log == ["a"]
    assert [i.id for i in out["__interrupt__"]] == [second.id]
    assert out["a"] == {"ok": 1}
    assert "b" not in out


def test_known_bug_tasks_over_report_after_a_partial_parallel_resume() -> None:
    """langgraph #4796 / #6792, reproduced.

    After resuming one of two parallel interrupts, `get_state().tasks` still lists the
    *finished* task's interrupt id. A `still_pending()` built on `task.interrupts` alone
    therefore returns True for an interrupt the graph has already moved past.

    `task.result` is the discriminator, and `LangGraphAdapter.still_pending()` uses it.
    If this test ever fails, LangGraph has fixed the bug and the workaround can go.
    """
    log: list[str] = []
    graph = parallel_graph(log)
    config = {"configurable": {"thread_id": "p1"}}
    first, second = graph.invoke({}, config)["__interrupt__"]
    graph.invoke(Command(resume={first.id: {"ok": 1}}), config)

    tasks = {task.name: task for task in graph.get_state(config).tasks}

    reported = {i.id for task in tasks.values() for i in task.interrupts}
    assert first.id in reported, "the finished task still advertises its interrupt"
    assert second.id in reported

    assert tasks["na"].result == {"a": {"ok": 1}}, "but the finished task has a result"
    assert tasks["nb"].result is None, "and the parked one does not"
    assert graph.get_state(config).next == ("nb",)


def test_replaying_a_resume_does_not_repeat_the_side_effect() -> None:
    """Good news, and worth pinning: LangGraph will not re-run a node whose resume it has
    already applied. Our own status compare-and-set is the first line of defence; this is
    the second."""
    log: list[str] = []
    graph = parallel_graph(log)
    config = {"configurable": {"thread_id": "p1"}}
    first, _ = graph.invoke({}, config)["__interrupt__"]
    graph.invoke(Command(resume={first.id: {"ok": 1}}), config)

    graph.invoke(Command(resume={first.id: {"ok": 1}}), config)

    assert log == ["a"], "the node ran once despite the resume being applied twice"


# --------------------------------------------------------------------------- 4. subgraphs
class Inner(TypedDict, total=False):
    v: dict[str, Any]


def subgraph_graph() -> Any:
    def inner_node(state: Inner) -> Inner:
        return {"v": interrupt({"inner": True})}

    inner = StateGraph(Inner)
    inner.add_node("inner_node", inner_node)
    inner.add_edge(START, "inner_node")
    inner.add_edge("inner_node", END)

    outer = StateGraph(Inner)
    outer.add_node("sub", inner.compile())
    outer.add_edge(START, "sub")
    outer.add_edge("sub", END)
    return outer.compile(checkpointer=InMemorySaver())


def test_a_subgraph_interrupt_surfaces_on_the_parent() -> None:
    """It surfaces on the parent's `__interrupt__`, with a stable id, and the parent's
    `get_state()` reports it against the subgraph node's task. `subgraphs=True` is not
    needed, which is what lets one adapter handle both cases."""
    graph = subgraph_graph()
    config = {"configurable": {"thread_id": "s1"}}

    raised = graph.invoke({}, config)["__interrupt__"]

    assert len(raised) == 1
    assert raised[0].value == {"inner": True}
    tasks = graph.get_state(config).tasks
    assert [task.name for task in tasks] == ["sub"]
    assert [i.id for task in tasks for i in task.interrupts] == [raised[0].id]
    assert checkpoint_id(graph, config)


def test_a_subgraph_interrupt_resumes_through_the_parent() -> None:
    graph = subgraph_graph()
    config = {"configurable": {"thread_id": "s1"}}
    raised = graph.invoke({}, config)["__interrupt__"][0]

    final = graph.invoke(Command(resume={raised.id: {"done": True}}), config)

    assert final["v"] == {"done": True}
    assert graph.get_state(config).tasks == ()
