"""`@wait` and `publish_interrupts` on real Strands agents (a scripted model, no network).

The model is scripted: first turn calls the tool, the next turn answers. Every test goes
through `agent(...)` and the real tool, interrupt and session machinery, so what is
asserted is what a user gets.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from mocked_model import MockedModelProvider
from strands import Agent, tool
from strands.session.file_session_manager import FileSessionManager
from strands.types.tools import ToolContext

from agent_wait import InMemoryAnnounce, WaitPolicy, question_id_for
from agent_wait.strands import publish_interrupts, wait

FINANCE = WaitPolicy(timeout="P1D", default={"action": "reject"}, tags={"approver_group": "finance"})
PAID: list[dict[str, Any]] = []


def scripted(name: str, args: dict[str, Any], *, tool_use_id: str = "tu-1") -> MockedModelProvider:
    """Call `name(args)` once; then say done."""
    return MockedModelProvider(
        [
            {
                "role": "assistant",
                "content": [{"toolUse": {"name": name, "toolUseId": tool_use_id, "input": args}}],
            },
            {"role": "assistant", "content": [{"text": "done"}]},
        ]
    )


def refund_tool(**wait_kwargs: Any) -> Any:
    @tool(context=True)
    @wait(FINANCE, **wait_kwargs)
    def issue_refund(order_id: str, amount: int, tool_context: ToolContext) -> str:
        PAID.append({"order_id": order_id, "amount": amount})
        return f"refunded {amount}"

    return issue_refund


def refund_agent(**kwargs: Any) -> Agent:
    return Agent(
        model=scripted("issue_refund", {"order_id": "o-1", "amount": 250}),
        tools=[refund_tool()],
        callback_handler=None,
        **kwargs,
    )


def answer(agent: Agent, question_id: str, value: Any) -> Any:
    """The host's resume: the framework's own call, the answer verbatim."""
    return agent([{"interruptResponse": {"interruptId": question_id, "response": value}}])


@pytest.fixture(autouse=True)
def _reset() -> None:
    PAID.clear()


# ------------------------------------------------------------------ park and publish
def test_a_wait_tool_parks_the_run_and_publishes_the_call() -> None:
    agent = refund_agent()
    inbox = InMemoryAnnounce()

    result = agent("refund o-1")
    assert result.stop_reason == "interrupt"
    assert PAID == []

    [envelope] = publish_interrupts(result, agent.session_id, [inbox])
    [interrupt] = result.interrupts
    assert envelope.question_id == interrupt.id
    assert envelope.question_id.startswith("v1:tool_call:tu-1:")  # per tool call
    assert envelope.thread_id == agent.session_id
    assert envelope.question == {
        "function": "issue_refund",
        "args": {"order_id": "o-1", "amount": 250},
    }  # tool_context hidden
    assert envelope.source == {"function": "issue_refund"}
    assert envelope.tags == {"approver_group": "finance"}
    assert envelope.default == {"action": "reject"}
    assert envelope.expires_at is not None


def test_a_finished_run_publishes_nothing() -> None:
    agent = Agent(
        model=MockedModelProvider([{"role": "assistant", "content": [{"text": "hi"}]}]), callback_handler=None
    )
    inbox = InMemoryAnnounce()
    assert publish_interrupts(agent("x"), agent.session_id, [inbox]) == []
    assert inbox.events == []


# ------------------------------------------------------------------ the answer comes back
def test_approve_runs_the_body_once_with_original_args() -> None:
    agent = refund_agent()
    parked = agent("refund o-1")
    [envelope] = publish_interrupts(parked, agent.session_id, [InMemoryAnnounce()])

    done = answer(agent, envelope.question_id, {"action": "approve", "by": "finance"})
    assert done.stop_reason == "end_turn"
    assert PAID == [{"order_id": "o-1", "amount": 250}]


def test_approve_with_edited_args_runs_the_body_with_them() -> None:
    agent = refund_agent()
    parked = agent("refund o-1")
    [interrupt] = parked.interrupts
    answer(agent, interrupt.id, {"action": "approve", "args": {"amount": 100}})
    assert PAID == [{"order_id": "o-1", "amount": 100}]


def test_anything_but_approve_does_not_run_the_body_and_is_the_tool_result() -> None:
    agent = refund_agent()
    parked = agent("refund o-1")
    [interrupt] = parked.interrupts
    done = answer(agent, interrupt.id, {"action": "reject", "reason": "over limit"})
    assert PAID == []
    assert done.stop_reason == "end_turn"
    # The rejection went back to the model as the tool's result.
    tool_results = [
        block["toolResult"]
        for message in agent.messages
        for block in message["content"]
        if "toolResult" in block
    ]
    assert len(tool_results) == 1
    assert "over limit" in str(tool_results[0]["content"])


def test_a_decision_parameter_always_runs_and_receives_the_answer() -> None:
    seen: list[Any] = []

    @tool(context=True)
    @wait(FINANCE, decision="verdict")
    def review(order_id: str, tool_context: ToolContext, verdict: Any = None) -> str:
        seen.append(verdict)
        return f"reviewed {order_id}"

    agent = Agent(model=scripted("review", {"order_id": "o-1"}), tools=[review], callback_handler=None)
    parked = agent("review o-1")
    [envelope] = publish_interrupts(parked, agent.session_id, [InMemoryAnnounce()])
    assert envelope.question == {"function": "review", "args": {"order_id": "o-1"}}  # verdict hidden

    answer(agent, envelope.question_id, {"action": "reject", "note": "no"})
    assert seen == [{"action": "reject", "note": "no"}]


# ------------------------------------------------------------------ what the host owns
def test_the_parked_question_survives_a_new_process_through_a_session_manager(tmp_path: Path) -> None:
    """Durability is the session manager's: a fresh Agent on the same session resumes."""
    first = refund_agent(session_manager=FileSessionManager(session_id="s-1", storage_dir=str(tmp_path)))
    parked = first("refund o-1")
    [envelope] = publish_interrupts(parked, first.session_id, [InMemoryAnnounce()])
    assert envelope.thread_id == "s-1"
    del first

    # A new process: new Agent, new tool object, a model that only has the final turn left.
    second = Agent(
        model=MockedModelProvider([{"role": "assistant", "content": [{"text": "done"}]}]),
        tools=[refund_tool()],
        callback_handler=None,
        session_manager=FileSessionManager(session_id="s-1", storage_dir=str(tmp_path)),
    )
    done = answer(second, envelope.question_id, {"action": "approve"})
    assert PAID == [{"order_id": "o-1", "amount": 250}]
    assert done.stop_reason == "end_turn"


def test_a_parked_run_refuses_a_plain_prompt_and_an_unknown_answer() -> None:
    """Pinned so the doc lines stay true."""
    agent = refund_agent()
    agent("refund o-1")
    with pytest.raises(TypeError, match="interruptResponse"):
        agent("something else")
    with pytest.raises(KeyError, match="no interrupt found"):
        answer(agent, "v1:tool_call:nope:x", {"action": "approve"})
    assert PAID == []


def test_a_duplicate_answer_is_refused_not_ignored() -> None:
    """Pinned so the doc line stays true: unlike LangGraph, a second answer after the run
    has moved on is an error, so the host checks `stop_reason` (or its own record) first."""
    agent = refund_agent()
    parked = agent("refund o-1")
    [interrupt] = parked.interrupts
    answer(agent, interrupt.id, {"action": "approve"})
    with pytest.raises(ValueError, match="not in interrupt state"):
        answer(agent, interrupt.id, {"action": "approve"})
    assert PAID == [{"order_id": "o-1", "amount": 250}]


# ------------------------------------------------------------------ tools that never heard of us
def test_a_bare_tool_context_interrupt_is_published_with_the_default_policy() -> None:
    @tool(context=True)
    def delete_file(path: str, tool_context: ToolContext) -> str:
        approval = tool_context.interrupt("for_delete", reason={"path": path})
        return f"deleted ({approval})"

    agent = Agent(
        model=scripted("delete_file", {"path": "/etc/hosts"}), tools=[delete_file], callback_handler=None
    )
    [envelope] = publish_interrupts(agent("rm"), agent.session_id, [InMemoryAnnounce()])
    assert envelope.question == {"path": "/etc/hosts"}
    assert envelope.source is None
    assert envelope.allowed_actions == ("resume",)


# ------------------------------------------------------------------ async mode
def test_async_publishes_from_inside_the_tool_and_the_run_carries_on() -> None:
    inbox = InMemoryAnnounce()
    agent = Agent(
        model=scripted("issue_refund", {"order_id": "o-1", "amount": 250}),
        tools=[refund_tool(mode="async", announce=[inbox])],
        callback_handler=None,
    )
    result = agent("refund o-1")
    assert result.stop_reason == "end_turn"
    assert PAID == []
    qid = question_id_for(agent.session_id, "issue_refund", {"order_id": "o-1", "amount": 250})
    [envelope] = inbox.of("created")
    assert (envelope.thread_id, envelope.question_id) == (agent.session_id, qid)
    assert publish_interrupts(result, agent.session_id, [inbox]) == []


# ------------------------------------------------------------------ the tool_context rule
def test_a_wait_tool_without_a_tool_context_is_refused_up_front() -> None:
    @tool
    @wait(FINANCE)
    def issue_refund(order_id: str, amount: int) -> str:
        return "refunded"

    agent = Agent(
        model=scripted("issue_refund", {"order_id": "o-1", "amount": 1}),
        tools=[issue_refund],
        callback_handler=None,
    )
    result = agent("refund")
    # Strands turns a tool exception into an error tool result rather than raising.
    errors = [
        block["toolResult"]
        for message in agent.messages
        for block in message["content"]
        if "toolResult" in block and block["toolResult"].get("status") == "error"
    ]
    assert errors and "tool_context" in str(errors[0]["content"])
    assert result.stop_reason == "end_turn"
