"""`LangGraphAdapter`: three methods, and the workaround for #4796/#6792.

`pending()` is the one with substance. Everything the publisher says about a thread comes
from it, so if it over-reports, the world is told about a question the graph has already
answered -- and somebody clicks approve on a refund that already went out.
"""

from __future__ import annotations

from typing import Any, TypedDict

import pytest
from agent_wait import WaitPolicy
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.graph import END, START, StateGraph
from langgraph.types import Command, interrupt
from langgraph_wait import LangGraphAdapter, ask


class S(TypedDict, total=False):
    a: dict[str, Any]
    b: dict[str, Any]


POLICY = WaitPolicy(timeout="PT2H", default={"action": "reject"}, allowed_actions=("approve", "reject"))


def parallel_graph() -> Any:
    graph = StateGraph(S)
    graph.add_node("na", lambda state: {"a": ask({"which": "a"}, POLICY)})
    graph.add_node("nb", lambda state: {"b": ask({"which": "b"}, POLICY)})
    graph.add_edge(START, "na")
    graph.add_edge(START, "nb")
    graph.add_edge("na", END)
    graph.add_edge("nb", END)
    return graph.compile(checkpointer=InMemorySaver())


def single_graph() -> Any:
    graph = StateGraph(S)
    graph.add_node("na", lambda state: {"a": ask({"which": "a"}, POLICY)})
    graph.add_edge(START, "na")
    graph.add_edge("na", END)
    return graph.compile(checkpointer=InMemorySaver())


@pytest.fixture()
def adapter() -> LangGraphAdapter:
    return LangGraphAdapter(single_graph())


# ------------------------------------------------------------------ the basics
def test_a_thread_that_has_never_run_is_parked_on_nothing(adapter: LangGraphAdapter) -> None:
    assert adapter.pending("never-seen") == []


def test_pending_carries_the_question_and_the_policy(adapter: LangGraphAdapter) -> None:
    """Both come back out of the interrupt value, which is the only channel there is."""
    adapter.invoke({}, adapter.config_for("t"))

    (parked,) = adapter.pending("t")
    assert parked.question == {"which": "a"}
    assert parked.policy == POLICY
    assert parked.interrupt_id


def test_a_bare_interrupt_is_read_as_a_question_with_the_default_policy() -> None:
    """A graph that has never heard of this library still gets published."""
    graph = StateGraph(S)
    graph.add_node("na", lambda state: {"a": interrupt({"raw": True})})
    graph.add_edge(START, "na")
    graph.add_edge("na", END)
    adapter = LangGraphAdapter(graph.compile(checkpointer=InMemorySaver()))

    adapter.invoke({}, adapter.config_for("t"))

    (parked,) = adapter.pending("t")
    assert parked.question == {"raw": True}
    assert parked.policy == WaitPolicy()


def test_asked_at_comes_from_the_checkpoint_not_the_wall_clock(adapter: LangGraphAdapter) -> None:
    """It has to be stable across reads, or `expires_at` walks forward on every
    republish."""
    adapter.invoke({}, adapter.config_for("t"))

    first = adapter.pending("t")[0].asked_at
    second = adapter.pending("t")[0].asked_at

    assert first is not None
    assert first == second


def test_resuming_clears_it(adapter: LangGraphAdapter) -> None:
    adapter.invoke({}, adapter.config_for("t"))
    (parked,) = adapter.pending("t")

    adapter.invoke(Command(resume={parked.interrupt_id: {"action": "approve"}}), adapter.config_for("t"))

    assert adapter.pending("t") == []


# --------------------------------------------------- the over-reporting bug
def test_interrupt_ids_are_stable_across_reads(adapter: LangGraphAdapter) -> None:
    """What makes `dedupe_key` a key. If these churned, every republish after a crash
    would look to a consumer like a brand new question."""
    adapter.invoke({}, adapter.config_for("t"))

    assert adapter.pending("t")[0].interrupt_id == adapter.pending("t")[0].interrupt_id


def test_a_resumed_parallel_sibling_is_not_reported_as_pending() -> None:
    """langgraph #4796 / #6792, and the reason `pending()` filters on `task.result`.

    Two interrupts in one superstep; resume one. LangGraph *still* lists the finished
    task's interrupt id under `tasks[*].interrupts`. Reading that list naively would
    leave the answered question published as open forever -- and would offer a second
    approval for a node that has already run.
    """
    adapter = LangGraphAdapter(parallel_graph())
    config = adapter.config_for("batch")
    adapter.invoke({}, config)
    first, second = sorted(adapter.pending("batch"), key=lambda p: p.question["which"])

    adapter.invoke(Command(resume={first.interrupt_id: {"action": "approve"}}), config)

    still_open = adapter.pending("batch")
    assert [p.interrupt_id for p in still_open] == [second.interrupt_id]

    # The raw LangGraph view still disagrees -- which is the whole point of the filter.
    raw = {str(i.id) for task in adapter.graph.get_state(config).tasks for i in (task.interrupts or ())}
    assert first.interrupt_id in raw, (
        "langgraph stopped over-reporting; the filter in pending() may now be removable"
    )


def test_a_subgraph_interrupt_surfaces_on_the_parent() -> None:
    """Verified rather than assumed: `subgraphs=True` is not needed, and the id is
    stable enough to resume through the parent."""
    from parallel_graphs import build_subgraph_graph

    adapter = LangGraphAdapter(build_subgraph_graph())
    config = adapter.config_for("nested")
    adapter.invoke({}, config)

    (parked,) = adapter.pending("nested")
    assert parked.question == {"kind": "inner_approval"}

    adapter.invoke(Command(resume={parked.interrupt_id: {"action": "approve"}}), config)
    assert adapter.pending("nested") == []


def test_an_unparseable_checkpoint_timestamp_degrades_to_no_anchor() -> None:
    """`created_at` is LangGraph's field, in LangGraph's format. If a future version
    changes it, `expires_at` should fall back to publish time rather than crash the run --
    a drifting deadline beats no question at all."""
    from langgraph_wait.adapter import _epoch

    assert _epoch("2026-09-10T13:36:40.790853+00:00") is not None
    assert _epoch("last Tuesday") is None
    assert _epoch(None) is None
    assert _epoch(1_760_000_000) is None
