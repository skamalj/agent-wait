"""`@wait` and `publish_interrupts` on real Pydantic AI agents (`FunctionModel`, no network).

The model is scripted: first turn calls the tool, the turn after a tool return answers
with the tool's result. Every test goes through `agent.run_sync()` and the real
deferred-tool machinery -- `ApprovalRequired`, `DeferredToolRequests`,
`DeferredToolResults`, the approved re-run -- so what is asserted is what a user gets.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pytest
from pydantic_ai import (
    Agent,
    CallDeferred,
    DeferredToolRequests,
    DeferredToolResults,
    RunContext,
    ToolApproved,
    ToolDenied,
)
from pydantic_ai.exceptions import UserError
from pydantic_ai.messages import ModelMessage, ModelResponse, TextPart, ToolCallPart, ToolReturnPart
from pydantic_ai.models.function import AgentInfo, FunctionModel

from agent_wait import InMemoryAnnounce, WaitPolicy, question_id_for
from agent_wait.pydantic_ai import publish_interrupts, wait

FINANCE = WaitPolicy(timeout="P1D", default={"action": "reject"}, tags={"approver_group": "finance"})
PAID: list[dict[str, Any]] = []


def scripted(tool: str, args: dict[str, Any]) -> FunctionModel:
    """Call `tool(args)` once; after its return, answer with the return value."""

    def model(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        for message in reversed(messages):
            for part in getattr(message, "parts", []):
                if isinstance(part, ToolReturnPart):
                    return ModelResponse(parts=[TextPart(f"done: {part.content}")])
        return ModelResponse(parts=[ToolCallPart(tool_name=tool, args=args)])

    return FunctionModel(model)


@dataclass
class Deps:
    thread_id: str


def refund_agent(**tool_kwargs: Any) -> Agent[Deps, str | DeferredToolRequests]:
    agent: Agent[Deps, str | DeferredToolRequests] = Agent(
        scripted("issue_refund", {"order_id": "o-1", "amount": 250}),
        deps_type=Deps,
        output_type=[str, DeferredToolRequests],
    )

    @agent.tool
    @wait(FINANCE, **tool_kwargs)
    def issue_refund(ctx: RunContext[Deps], order_id: str, amount: int) -> str:
        PAID.append({"order_id": order_id, "amount": amount})
        return f"refunded {amount}"

    return agent


@pytest.fixture(autouse=True)
def _reset() -> None:
    PAID.clear()


# ------------------------------------------------------------------ park and publish
def test_a_wait_tool_parks_the_run_and_publishes_the_call() -> None:
    agent = refund_agent()
    inbox = InMemoryAnnounce()

    result = agent.run_sync("refund o-1", deps=Deps("t-1"))
    assert isinstance(result.output, DeferredToolRequests)
    assert PAID == []

    [envelope] = publish_interrupts(result, "t-1", [inbox])
    [call] = result.output.approvals
    assert envelope.question_id == call.tool_call_id
    assert envelope.thread_id == "t-1"
    assert envelope.question == {
        "function": "issue_refund",
        "args": {"order_id": "o-1", "amount": 250},
    }  # ctx hidden
    assert envelope.source == {"function": "issue_refund"}
    assert envelope.tags == {"approver_group": "finance"}
    assert envelope.default == {"action": "reject"}
    assert envelope.expires_at is not None


def test_a_finished_run_publishes_nothing() -> None:
    agent: Agent[None, str] = Agent(FunctionModel(lambda m, i: ModelResponse(parts=[TextPart("hi")])))
    inbox = InMemoryAnnounce()
    assert publish_interrupts(agent.run_sync("x"), "t-1", [inbox]) == []
    assert inbox.events == []


# ------------------------------------------------------------------ the answer comes back
def _resume(agent: Agent[Deps, Any], parked: Any, answer: Any, *, deps: Deps = Deps("t-1")) -> Any:
    """The host's resume, as the docs recommend it: approve or deny, answer in metadata."""
    [call] = parked.output.approvals
    approved = isinstance(answer, dict) and answer.get("action") == "approve"
    results = DeferredToolResults(
        approvals={call.tool_call_id: ToolApproved() if approved else ToolDenied(str(answer))},
        metadata={call.tool_call_id: answer} if isinstance(answer, dict) else {},
    )
    return agent.run_sync(
        None, deps=deps, message_history=parked.all_messages(), deferred_tool_results=results
    )


def test_approve_runs_the_body_once_with_original_args() -> None:
    agent = refund_agent()
    parked = agent.run_sync("refund o-1", deps=Deps("t-1"))
    done = _resume(agent, parked, {"action": "approve", "by": "finance"})
    assert done.output == "done: refunded 250"
    assert PAID == [{"order_id": "o-1", "amount": 250}]


def test_approve_with_edited_args_runs_the_body_with_them() -> None:
    agent = refund_agent()
    parked = agent.run_sync("refund o-1", deps=Deps("t-1"))
    done = _resume(agent, parked, {"action": "approve", "args": {"amount": 100}})
    assert done.output == "done: refunded 100"
    assert PAID == [{"order_id": "o-1", "amount": 100}]


def test_anything_but_approve_does_not_run_the_body_and_the_model_sees_why() -> None:
    agent = refund_agent()
    parked = agent.run_sync("refund o-1", deps=Deps("t-1"))
    done = _resume(agent, parked, {"action": "reject", "reason": "over limit"})
    assert PAID == []
    assert "over limit" in done.output


def test_a_decision_parameter_always_runs_and_receives_the_answer() -> None:
    seen: list[Any] = []
    agent: Agent[Deps, str | DeferredToolRequests] = Agent(
        scripted("review", {"order_id": "o-1"}), deps_type=Deps, output_type=[str, DeferredToolRequests]
    )

    @agent.tool
    @wait(FINANCE, decision="verdict")
    def review(ctx: RunContext[Deps], order_id: str, verdict: Any = None) -> str:
        seen.append(verdict)
        return f"reviewed {order_id}: {verdict['action']}"

    parked = agent.run_sync("review o-1", deps=Deps("t-1"))
    [envelope] = publish_interrupts(parked, "t-1", [InMemoryAnnounce()])
    assert envelope.question == {"function": "review", "args": {"order_id": "o-1"}}  # verdict hidden

    # On Pydantic AI only an approved call is re-run, so the answer that reaches the
    # function is an approval -- with whatever else the approver attached.
    done = _resume(agent, parked, {"action": "approve", "note": "ok"})
    assert seen == [{"action": "approve", "note": "ok"}]
    assert done.output == "done: reviewed o-1: approve"


def test_approval_without_metadata_is_a_plain_approve() -> None:
    agent = refund_agent()
    parked = agent.run_sync("refund o-1", deps=Deps("t-1"))
    [call] = parked.output.approvals
    done = agent.run_sync(
        None,
        deps=Deps("t-1"),
        message_history=parked.all_messages(),
        deferred_tool_results=DeferredToolResults(approvals={call.tool_call_id: True}),
    )
    assert done.output == "done: refunded 250"
    assert PAID == [{"order_id": "o-1", "amount": 250}]


# ------------------------------------------------------------------ what the host owns
def test_the_host_owns_idempotency() -> None:
    """Pinned so the doc line stays true: a duplicate answer is refused only when the
    history passed on resume already holds the tool's return."""
    agent = refund_agent()
    parked = agent.run_sync("refund o-1", deps=Deps("t-1"))
    [call] = parked.output.approvals
    results = DeferredToolResults(approvals={call.tool_call_id: True})

    done = agent.run_sync(
        None, deps=Deps("t-1"), message_history=parked.all_messages(), deferred_tool_results=results
    )
    assert len(PAID) == 1

    # From the post-run history: refused, nothing runs.
    with pytest.raises(UserError):
        agent.run_sync(
            None, deps=Deps("t-1"), message_history=done.all_messages(), deferred_tool_results=results
        )
    assert len(PAID) == 1

    # From the pre-resume history: the framework cannot know, and the tool runs again.
    agent.run_sync(
        None, deps=Deps("t-1"), message_history=parked.all_messages(), deferred_tool_results=results
    )
    assert len(PAID) == 2


# ------------------------------------------------------------------ tools that never heard of us
def test_a_requires_approval_tool_is_published_with_the_default_policy() -> None:
    agent: Agent[None, str | DeferredToolRequests] = Agent(
        scripted("delete_file", {"path": "/etc/hosts"}), output_type=[str, DeferredToolRequests]
    )

    @agent.tool_plain(requires_approval=True)
    def delete_file(path: str) -> str:
        return "deleted"

    [envelope] = publish_interrupts(agent.run_sync("rm"), "t-1", [InMemoryAnnounce()])
    assert envelope.question == {"function": "delete_file", "args": {"path": "/etc/hosts"}}
    assert envelope.source is None
    assert envelope.allowed_actions == ("resume",)  # WaitPolicy() default, same as a bare LangGraph interrupt
    assert envelope.expires_at is None


def test_a_call_deferred_tool_is_published_as_a_wait_for_the_outside_world() -> None:
    agent: Agent[None, str | DeferredToolRequests] = Agent(
        scripted("run_kyc", {"customer": "c-9"}), output_type=[str, DeferredToolRequests]
    )

    @agent.tool_plain
    def run_kyc(customer: str) -> str:
        raise CallDeferred(metadata={"job": "kyc-42"})

    result = agent.run_sync("kyc")
    [envelope] = publish_interrupts(result, "t-1", [InMemoryAnnounce()])
    [call] = result.output.calls
    assert envelope.question_id == call.tool_call_id
    assert envelope.question == {
        "function": "run_kyc",
        "args": {"customer": "c-9"},
        "metadata": {"job": "kyc-42"},
    }


# ------------------------------------------------------------------ async mode
def test_async_publishes_from_inside_the_tool_and_the_run_carries_on() -> None:
    inbox = InMemoryAnnounce()
    agent = refund_agent(mode="async", announce=[inbox])

    result = agent.run_sync("refund o-1", deps=Deps("t-7"))
    assert PAID == []
    assert not isinstance(result.output, DeferredToolRequests)
    qid = question_id_for("t-7", "issue_refund", {"order_id": "o-1", "amount": 250})
    assert qid in result.output and "pending_approval" in result.output
    [envelope] = inbox.of("created")
    assert (envelope.thread_id, envelope.question_id) == ("t-7", qid)
    assert publish_interrupts(result, "t-7", [inbox]) == []


def test_async_needs_a_thread_id_in_deps() -> None:
    agent: Agent[None, str | DeferredToolRequests] = Agent(
        scripted("issue_refund", {"order_id": "o-1", "amount": 1}), output_type=[str, DeferredToolRequests]
    )

    @agent.tool
    @wait(FINANCE, mode="async", announce=[InMemoryAnnounce()])
    def issue_refund(ctx: RunContext[None], order_id: str, amount: int) -> str:
        return "refunded"

    with pytest.raises(RuntimeError, match="thread_id"):
        agent.run_sync("refund")


# ------------------------------------------------------------------ the ctx rule
def test_a_wait_tool_without_a_run_context_is_refused_up_front() -> None:
    agent: Agent[None, str | DeferredToolRequests] = Agent(
        scripted("issue_refund", {"order_id": "o-1", "amount": 1}), output_type=[str, DeferredToolRequests]
    )

    @agent.tool_plain
    @wait(FINANCE)
    def issue_refund(order_id: str, amount: int) -> str:
        return "refunded"

    with pytest.raises(TypeError, match="ctx: RunContext"):
        agent.run_sync("refund")
