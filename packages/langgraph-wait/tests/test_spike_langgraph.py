"""The spike: what does LangGraph actually do?

The design assumes two things about interrupts. If either is false, the idempotency key
is not stable and rule 1 collapses. So they are asserted, not assumed -- and the third
group of tests pins a *bug* we have to work around, so that a future LangGraph fix
arrives as a failing test rather than as silence.

Findings against langgraph 1.2.11 are written up in `adapter.py`'s module docstring and
in TEST_REPORT.md.
"""

from __future__ import annotations

from collections.abc import Iterator
from importlib.metadata import version
from pathlib import Path
from typing import Any, TypedDict

import pytest
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.graph import END, START, StateGraph
from langgraph.types import Command, interrupt

pytestmark = pytest.mark.spike

# ---------------------------------------------------------------- verbatim recording
# The project manager asked for the observed behaviour of parallel and subgraph
# interrupts to be recorded verbatim in the report, pass or fail -- these are the items
# most likely to differ from the design. So the spike writes down what it actually saw,
# and the fixture's teardown runs whether the assertions held or not.

OBSERVATIONS: list[str] = []
OBSERVATIONS_PATH = Path(__file__).resolve().parents[3] / "reports" / "langgraph-spike-observations.txt"


def record(line: str = "") -> None:
    OBSERVATIONS.append(line)


_LABELS: dict[str, str] = {}


def short(value: object) -> str:
    """Label an interrupt or checkpoint id stably, in first-seen order.

    The raw ids are 32 random hex characters, regenerated every run. Printing them makes
    the recorded file churn on every test run -- a meaningless diff in the working tree,
    and a quote in TEST_REPORT that goes stale the moment anyone runs the suite.

    What the observation is actually *about* is which task advertises which interrupt and
    what each one's `result` says. That is preserved exactly; only the opaque identifier
    is replaced by a stable label, so the same behaviour records as the same bytes.
    """
    text = str(value)
    if len(text) <= 12:
        return text
    return _LABELS.setdefault(text, f"<id-{chr(ord('A') + len(_LABELS))}>")


def _tasks(graph: Any, config: dict[str, Any]) -> list[tuple[str, list[str], Any]]:
    """`(task name, the interrupt ids it advertises, its result)` -- the three fields the
    whole parallel-interrupt question turns on."""
    return [
        (task.name, [short(i.id) for i in task.interrupts], task.result)
        for task in graph.get_state(config).tasks
    ]


@pytest.fixture(scope="module", autouse=True)
def write_observations() -> Iterator[None]:
    OBSERVATIONS.clear()
    _LABELS.clear()
    record(f"langgraph {version('langgraph')} · langgraph-checkpoint {version('langgraph-checkpoint')}")
    record("Recorded by packages/langgraph-wait/tests/test_spike_langgraph.py, verbatim.")
    record("Interrupt and checkpoint ids are 32 random hex chars, regenerated every run;")
    record("they are labelled <id-A>, <id-B>, ... in first-seen order so that identical")
    record("behaviour records as identical bytes. Everything else is as observed.")
    record()
    yield
    OBSERVATIONS_PATH.parent.mkdir(parents=True, exist_ok=True)
    OBSERVATIONS_PATH.write_text("\n".join(OBSERVATIONS) + "\n", encoding="utf-8")


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

    record("== PARALLEL: two interrupts in one superstep ==")
    record(f"  result['__interrupt__']  = {[(short(i.id), i.value) for i in raised]}")
    record(f"  get_state().tasks        = {_tasks(graph, config)}")
    record(f"  get_state().next         = {graph.get_state(config).next}")
    record(f"  checkpoint_id            = {short(checkpoint_id(graph, config))}")
    record(f"  side-effect log          = {log}")
    record()

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

    record("== PARALLEL: after resuming ONE of the two ==")
    record(f"  resumed                  = {short(first.id)} with {{'ok': 1}}")
    record(f"  result keys              = {sorted(out)}")
    record(f"  result['__interrupt__']  = {[short(i.id) for i in out.get('__interrupt__', [])]}")
    record(f"  the other interrupt      = {short(second.id)}")
    record(f"  state values             = {graph.get_state(config).values}")
    record(f"  side-effect log          = {log}")
    record()

    assert log == ["a"]
    assert [i.id for i in out["__interrupt__"]] == [second.id]
    assert out["a"] == {"ok": 1}
    assert "b" not in out


def test_known_bug_tasks_over_report_after_a_partial_parallel_resume() -> None:
    """langgraph #4796 / #6792, reproduced.

    After resuming one of two parallel interrupts, `get_state().tasks` still lists the
    *finished* task's interrupt id. A `pending()` built on `task.interrupts` alone
    therefore returns True for an interrupt the graph has already moved past.

    `task.result` is the discriminator, and `LangGraphAdapter.pending()` uses it.
    If this test ever fails, LangGraph has fixed the bug and the workaround can go.
    """
    log: list[str] = []
    graph = parallel_graph(log)
    config = {"configurable": {"thread_id": "p1"}}
    first, second = graph.invoke({}, config)["__interrupt__"]
    graph.invoke(Command(resume={first.id: {"ok": 1}}), config)

    tasks = {task.name: task for task in graph.get_state(config).tasks}

    record("== PARALLEL: the #4796/#6792 bug, as observed ==")
    record(
        f"  resumed                  = {short(first.id)} (node 'na'); still parked = {short(second.id)} (node 'nb')"
    )
    for name, task in tasks.items():
        record(
            f"  task {name!r:6} interrupts={[short(i.id) for i in task.interrupts]} "
            f"result={task.result!r} error={task.error!r}"
        )
    record(f"  get_state().next         = {graph.get_state(config).next}")
    record("  -> tasks[*].interrupts over-reports: 'na' has finished and still lists its id.")
    record("  -> task.result is the discriminator, and is what pending() reads.")
    record()

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

    record("== PARALLEL: replaying an already-applied resume ==")
    record(f"  applied {short(first.id)} twice; side-effect log = {log}")
    record("  -> LangGraph does not re-run a node whose resume it already applied.")
    record()

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
    with_subgraphs = graph.get_state(config, subgraphs=True)

    record("== SUBGRAPH: an interrupt raised two levels down ==")
    record(f"  result['__interrupt__']  = {[(short(i.id), i.value) for i in raised]}")
    record(f"  get_state().tasks        = {_tasks(graph, config)}")
    record(
        f"  get_state(subgraphs=True) = "
        f"{[(t.name, [short(i.id) for i in t.interrupts]) for t in with_subgraphs.tasks]}"
    )
    record(f"  get_state().next         = {graph.get_state(config).next}")
    record(f"  checkpoint_id            = {short(checkpoint_id(graph, config))}")
    record("  -> it surfaces on the PARENT's __interrupt__, against the subgraph node's task.")
    record("  -> subgraphs=True was not needed, so one adapter handles both shapes.")
    record()

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

    record("== SUBGRAPH: resuming it through the parent ==")
    record(f"  resumed {short(raised.id)} via the parent graph")
    record(f"  final state values       = {final}")
    record(f"  get_state().tasks        = {_tasks(graph, config)}")
    record("  -> resuming by id through the parent works; no subgraph-specific path needed.")
    record()

    assert final["v"] == {"done": True}
    assert graph.get_state(config).tasks == ()


# ================================================== two interrupting tools, one ToolNode
def toolnode_graph() -> Any:
    """Two tools that each call `interrupt()`, dispatched together by one `ToolNode`.

    This is the shape langgraph #6624 and #6626 are about, and it is *not* the shape the
    parallel tests above use (those are two nodes). It is the shape a tool-calling agent
    produces naturally, which is why it is recorded here rather than assumed to behave
    like the node case.
    """
    from langchain_core.messages import AIMessage
    from langchain_core.tools import tool
    from langgraph.graph import MessagesState
    from langgraph.prebuilt import ToolNode

    @tool
    def approve_a(x: str) -> str:
        """needs approval"""
        return str(interrupt({"which": "a", "x": x}))

    @tool
    def approve_b(x: str) -> str:
        """needs approval"""
        return str(interrupt({"which": "b", "x": x}))

    def fake_llm(state: Any) -> Any:
        calls = [
            {"name": "approve_a", "args": {"x": "1"}, "id": "call_a"},
            {"name": "approve_b", "args": {"x": "2"}, "id": "call_b"},
        ]
        return {"messages": [AIMessage(content="", tool_calls=calls)]}

    graph = StateGraph(MessagesState)
    graph.add_node("llm", fake_llm)
    graph.add_node("tools", ToolNode([approve_a, approve_b]))
    graph.add_edge(START, "llm")
    graph.add_edge("llm", "tools")
    graph.add_edge("tools", END)
    return graph.compile(checkpointer=InMemorySaver())


def test_known_limitation_two_interrupting_tools_in_one_toolnode_share_an_id() -> None:
    """langgraph #6624 + #6626, as observed. This is a shape agent-wait does NOT support.

    Two things happen, and both break the assumptions the rest of this library rests on:

    1. Only ONE interrupt surfaces per invoke. The second tool's question appears only
       after the first is resumed (#6624).
    2. When it does, it carries THE SAME interrupt id as the first (#6626) -- a different
       question under an identical id -- and the task reports `result={}` while it is
       still genuinely parked.

    For agent-wait that means `dedupe_key` would make a consumer discard the second
    question as a duplicate of the first, and `pending()` would read `result={}` as
    finished. The rule this pins is therefore: **one `interrupt()` per node.** Put each
    approval-requiring tool in its own node, which is also what #6208's workaround and the
    "double execution" write-ups arrive at independently.

    Asserted so that a LangGraph release which fixes it fails this test and the docs get
    updated, rather than the limitation quietly persisting in prose.
    """
    graph = toolnode_graph()
    config = {"configurable": {"thread_id": "tn1"}}

    first_run = graph.invoke({"messages": []}, config)
    first = first_run["__interrupt__"]
    second_run = graph.invoke(Command(resume={first[0].id: "ok-a"}), config)
    second = second_run["__interrupt__"]
    parked_tasks = _tasks(graph, config)
    final = graph.invoke(Command(resume={second[0].id: "ok-b"}), config)

    record("== TOOLNODE: two tools that both interrupt, dispatched by one ToolNode ==")
    record(f"  first invoke raised      = {[(short(i.id), i.value) for i in first]}")
    record(f"  after resuming it        = {[(short(i.id), i.value) for i in second]}")
    record(f"  same id for both?        = {second[0].id == first[0].id}")
    record(f"  tasks while parked on b  = {parked_tasks}")
    record(f"  after resuming b         = {len(final.get('__interrupt__', []))} interrupts left")
    record("  -> #6624: only one interrupt surfaces per invoke, not two.")
    record("  -> #6626: the second carries the SAME id as the first. A different question,")
    record("     an identical id. dedupe_key would drop it; pending() would read result={}")
    record("     as finished. agent-wait's rule: one interrupt() per node.")
    record()

    assert len(first) == 1, "if two now surface, #6624 is fixed -- update the docs"
    assert first[0].value["which"] == "a"
    assert second[0].value["which"] == "b"
    assert second[0].id == first[0].id, "if ids now differ, #6626 is fixed -- update the docs"
    assert parked_tasks[0][2] is not None, "result is not None while still parked on b"
    assert final.get("__interrupt__", []) == []
