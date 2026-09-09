"""`LangGraphAdapter`: the five methods, including the workaround for #4796/#6792."""

from __future__ import annotations

from typing import Any, TypedDict

import pytest
from agent_wait import Wait, WaitPolicy
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.graph import END, START, StateGraph
from langgraph.types import Command
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


def make_wait(adapter: LangGraphAdapter, interrupt_id: str, thread_id: str) -> Wait:
    return Wait(
        wait_id="01JW",
        thread_id=thread_id,
        framework=adapter.name,
        interrupt_id=interrupt_id,
        checkpoint_id="ckpt",
        idempotency_key="k",
        question={},
        binding="b" * 64,
        policy=POLICY,
    )


# --------------------------------------------------------------------------- extract
def test_extract_reads_question_policy_and_checkpoint() -> None:
    graph = single_graph()
    adapter = LangGraphAdapter(graph)
    config = adapter.config_for("t")
    result = graph.invoke({}, config)

    pending = adapter.extract(result, config)

    assert len(pending) == 1
    assert pending[0].question == {"which": "a"}
    assert pending[0].policy == POLICY
    assert pending[0].interrupt_id == result["__interrupt__"][0].id
    assert pending[0].checkpoint_id == graph.get_state(config).config["configurable"]["checkpoint_id"]


def test_extract_returns_nothing_for_a_completed_run() -> None:
    graph = single_graph()
    adapter = LangGraphAdapter(graph)
    config = adapter.config_for("t")
    raised = graph.invoke({}, config)["__interrupt__"][0]

    final = graph.invoke(Command(resume={raised.id: {"action": "approve"}}), config)

    assert adapter.extract(final, config) == []


@pytest.mark.parametrize("result", [None, {}, {"__interrupt__": []}, "not a dict", 42])
def test_extract_is_unbothered_by_odd_results(result: Any) -> None:
    adapter = LangGraphAdapter(single_graph())
    assert adapter.extract(result, adapter.config_for("t")) == []


def test_extract_finds_both_parallel_interrupts() -> None:
    graph = parallel_graph()
    adapter = LangGraphAdapter(graph)
    config = adapter.config_for("p")

    pending = adapter.extract(graph.invoke({}, config), config)

    assert len(pending) == 2
    assert {p.question["which"] for p in pending} == {"a", "b"}
    assert len({p.interrupt_id for p in pending}) == 2
    assert len({p.checkpoint_id for p in pending}) == 1, "one superstep, one checkpoint"


# --------------------------------------------------------------------------- build_resume
def test_build_resume_is_dict_keyed() -> None:
    """A bare `Command(resume=value)` would hand the same answer to every parked
    interrupt on the thread -- one click approving two refunds."""
    adapter = LangGraphAdapter(single_graph())
    wait = make_wait(adapter, "int-1", "t")

    command = adapter.build_resume(wait, {"action": "approve"})

    assert isinstance(command, Command)
    assert command.resume == {"int-1": {"action": "approve"}}


# --------------------------------------------------------------------------- still_pending
def test_still_pending_is_true_while_parked() -> None:
    graph = single_graph()
    adapter = LangGraphAdapter(graph)
    config = adapter.config_for("t")
    raised = graph.invoke({}, config)["__interrupt__"][0]

    assert adapter.still_pending(make_wait(adapter, raised.id, "t")) is True


def test_still_pending_is_false_once_the_thread_finishes() -> None:
    graph = single_graph()
    adapter = LangGraphAdapter(graph)
    config = adapter.config_for("t")
    raised = graph.invoke({}, config)["__interrupt__"][0]

    graph.invoke(Command(resume={raised.id: {"action": "approve"}}), config)

    assert adapter.still_pending(make_wait(adapter, raised.id, "t")) is False


def test_still_pending_is_false_for_an_unknown_interrupt() -> None:
    graph = single_graph()
    adapter = LangGraphAdapter(graph)
    graph.invoke({}, adapter.config_for("t"))

    assert adapter.still_pending(make_wait(adapter, "never-existed", "t")) is False


def test_still_pending_survives_the_partial_parallel_resume_bug() -> None:
    """The regression this adapter exists to absorb.

    After resuming one of two parallel interrupts, `tasks[*].interrupts` still lists the
    finished one (langgraph #4796/#6792). Reading that naively would report the resumed
    interrupt as still parked; `task.result` is what tells the truth.
    """
    graph = parallel_graph()
    adapter = LangGraphAdapter(graph)
    config = adapter.config_for("p")
    first, second = graph.invoke({}, config)["__interrupt__"]
    graph.invoke(Command(resume={first.id: {"action": "approve"}}), config)

    naive = {i.id for task in graph.get_state(config).tasks for i in task.interrupts}
    assert first.id in naive, "precondition: LangGraph still over-reports"

    assert adapter.still_pending(make_wait(adapter, first.id, "p")) is False
    assert adapter.still_pending(make_wait(adapter, second.id, "p")) is True


def test_checkpoint_id_is_empty_for_a_thread_that_never_ran() -> None:
    adapter = LangGraphAdapter(single_graph())
    assert adapter.checkpoint_id(adapter.config_for("never")) == ""


# --------------------------------------------------------------------------- has_checkpoint
def test_has_checkpoint_is_false_before_the_thread_runs() -> None:
    """Section 18.5. This is the signal that tells "the first run died before persisting
    anything" apart from "an ordinary redelivery"."""
    adapter = LangGraphAdapter(single_graph())

    assert adapter.has_checkpoint("never-ran") is False


def test_has_checkpoint_is_true_once_the_thread_is_parked() -> None:
    graph = single_graph()
    adapter = LangGraphAdapter(graph)
    graph.invoke({}, adapter.config_for("t"))

    assert adapter.has_checkpoint("t") is True


def test_has_checkpoint_stays_true_after_the_thread_finishes() -> None:
    graph = single_graph()
    adapter = LangGraphAdapter(graph)
    raised = graph.invoke({}, adapter.config_for("t"))["__interrupt__"][0]
    graph.invoke(Command(resume={raised.id: {"action": "approve"}}), adapter.config_for("t"))

    assert adapter.has_checkpoint("t") is True


def test_has_checkpoint_does_not_leak_between_threads() -> None:
    graph = single_graph()
    adapter = LangGraphAdapter(graph)
    graph.invoke({}, adapter.config_for("t"))

    assert adapter.has_checkpoint("t") is True
    assert adapter.has_checkpoint("other") is False
