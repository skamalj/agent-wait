"""Two graphs that exercise the awkward shapes: parallel interrupts and a subgraph.

They live beside the tests rather than in `examples/` because they are not an example of
anything -- they exist to make rule 11 and the subgraph findings testable end to end.
"""

from __future__ import annotations

from typing import Any, TypedDict

from agent_wait import WaitPolicy
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.graph import END, START, StateGraph
from langgraph_wait import hitl

POLICY = WaitPolicy(
    timeout="PT2H",
    default={"action": "reject"},
    allowed_actions=("approve", "reject"),
)


class Batch(TypedDict, total=False):
    a: dict[str, Any]
    b: dict[str, Any]


def build_parallel_graph() -> Any:
    """Two nodes, one superstep, two independent approvals."""

    @hitl(POLICY)
    def na(state: Batch, decision: Any = None) -> Batch:
        return {"a": decision}

    @hitl(POLICY)
    def nb(state: Batch, decision: Any = None) -> Batch:
        return {"b": decision}

    graph = StateGraph(Batch)
    graph.add_node("na", na)
    graph.add_node("nb", nb)
    graph.add_edge(START, "na")
    graph.add_edge(START, "nb")
    graph.add_edge("na", END)
    graph.add_edge("nb", END)
    return graph.compile(checkpointer=InMemorySaver())


class Nested(TypedDict, total=False):
    v: dict[str, Any]


def build_subgraph_graph() -> Any:
    """The interrupt is raised two levels down; the parent is where it surfaces."""

    @hitl(POLICY)
    def inner_node(state: Nested, decision: Any = None) -> Nested:
        return {"v": decision}

    inner = StateGraph(Nested)
    inner.add_node("inner_node", inner_node)
    inner.add_edge(START, "inner_node")
    inner.add_edge("inner_node", END)

    outer = StateGraph(Nested)
    outer.add_node("sub", inner.compile())
    outer.add_edge(START, "sub")
    outer.add_edge("sub", END)
    return outer.compile(checkpointer=InMemorySaver())
