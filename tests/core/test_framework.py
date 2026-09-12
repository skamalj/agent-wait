"""`@wait` and `publish_interrupts` against a stub `Framework`. No LangGraph in this file.

This is the contract a framework subpackage has to meet: park on a value and hand the
answer back; list `(id, value)` from a result; know the current thread. Everything the
user sees is built over those three methods, so if this passes, a new implementor only
has to get those three right.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import pytest

from agent_wait import (
    Framework,
    InMemoryAnnounce,
    WaitPolicy,
    make_publish_interrupts,
    make_wait,
    question_id_for,
)
from agent_wait.wait import WAIT_KEY, pack, unpack

FINANCE = WaitPolicy(timeout="P1D", default={"action": "reject"}, tags={"approver_group": "finance"})


class Parked(Exception):
    """The stub's way of stopping the run: raise, carrying the parked value."""

    def __init__(self, value: Mapping[str, Any]) -> None:
        super().__init__("parked")
        self.value = value


class StubFramework(Framework):
    """A hand-driven framework. `answer` is what the next `interrupt()` returns; `None`
    means "park", which the stub does by raising `Parked`."""

    name = "stub"
    hidden_params = ("ctx",)

    def __init__(self) -> None:
        self.answer: Any = None
        self.parked: list[Mapping[str, Any]] = []
        self.thread_id = "thread-1"

    def interrupt(self, value: Mapping[str, Any], call_args: Mapping[str, Any]) -> Any:
        self.parked.append(value)
        if self.answer is None:
            raise Parked(value)
        return self.answer

    def interrupts_in(self, result: Any) -> list[tuple[str, Any]]:
        if isinstance(result, Mapping):
            return list(result.get("parked", []))  # pyright: ignore[reportUnknownMemberType, reportUnknownArgumentType]
        return []

    def current_thread_id(self, call_args: Mapping[str, Any]) -> str:
        return self.thread_id


@pytest.fixture
def fw() -> StubFramework:
    return StubFramework()


# ------------------------------------------------------------------ interrupt mode
def test_calling_a_wait_function_parks_on_the_function_and_its_args(fw: StubFramework) -> None:
    wait = make_wait(fw)

    @wait(FINANCE)
    def issue_refund(order_id: str, amount: int, ctx: object = None) -> str:
        return f"refunded {amount}"

    with pytest.raises(Parked) as parked:
        issue_refund("o-1", 250, ctx=object())

    question, policy, source = unpack(parked.value.value)
    assert question == {"function": "issue_refund", "args": {"order_id": "o-1", "amount": 250}}  # ctx hidden
    assert policy == FINANCE
    assert source == {"function": "issue_refund"}


def test_approve_runs_the_body_with_original_or_edited_args(fw: StubFramework) -> None:
    wait = make_wait(fw)

    @wait(FINANCE)
    def issue_refund(order_id: str, amount: int) -> str:
        return f"refunded {amount}"

    fw.answer = {"action": "approve"}
    assert issue_refund("o-1", 250) == "refunded 250"
    fw.answer = {"action": "approve", "args": {"amount": 100}}
    assert issue_refund("o-1", 250) == "refunded 100"


def test_anything_but_approve_is_returned_instead_of_running(fw: StubFramework) -> None:
    wait = make_wait(fw)
    ran: list[str] = []

    @wait(FINANCE)
    def issue_refund(order_id: str) -> str:
        ran.append(order_id)
        return "refunded"

    fw.answer = {"action": "reject", "reason": "over limit"}
    assert issue_refund("o-1") == {"action": "reject", "reason": "over limit"}
    fw.answer = "no"
    assert issue_refund("o-1") == "no"
    assert ran == []


def test_a_decision_parameter_always_runs_and_receives_the_answer(fw: StubFramework) -> None:
    wait = make_wait(fw)

    @wait(FINANCE, decision="verdict")
    def review(order_id: str, verdict: Any = None) -> dict[str, Any]:
        return {"order_id": order_id, "verdict": verdict}

    fw.answer = {"action": "reject"}
    assert review("o-1") == {"order_id": "o-1", "verdict": {"action": "reject"}}

    fw.answer = None
    with pytest.raises(Parked) as parked:
        review("o-1")
    assert unpack(parked.value.value)[0] == {
        "function": "review",
        "args": {"order_id": "o-1"},
    }  # verdict hidden


def test_the_decorated_function_is_tagged(fw: StubFramework) -> None:
    wait = make_wait(fw)

    @wait(FINANCE)
    def f() -> None: ...

    assert f.__agent_wait__ == {"framework": "stub", "mode": "interrupt", "policy": FINANCE}  # type: ignore[attr-defined]


# ------------------------------------------------------------------ async mode
def test_async_publishes_from_inside_the_call_and_returns_pending(fw: StubFramework) -> None:
    wait = make_wait(fw)
    inbox = InMemoryAnnounce()

    @wait(FINANCE, mode="async", announce=[inbox])
    def issue_refund(order_id: str, amount: int) -> str:
        raise AssertionError("the body must not run")

    result = issue_refund("o-1", 250)
    qid = question_id_for("thread-1", "issue_refund", {"order_id": "o-1", "amount": 250})
    assert result == {"status": "pending_approval", "question_id": qid, "function": "issue_refund"}
    assert fw.parked == []
    [envelope] = inbox.of("created")
    assert envelope.question_id == qid
    assert envelope.question == {"function": "issue_refund", "args": {"order_id": "o-1", "amount": 250}}
    assert envelope.source == {"function": "issue_refund"}
    assert envelope.tags == {"approver_group": "finance"}


def test_async_needs_announcers_at_decoration(fw: StubFramework) -> None:
    wait = make_wait(fw)
    with pytest.raises(ValueError, match="announce"):
        wait(FINANCE, mode="async")(lambda: None)


# ------------------------------------------------------------------ publish_interrupts
def test_publish_interrupts_reads_only_what_the_framework_returns(fw: StubFramework) -> None:
    publish_interrupts = make_publish_interrupts(fw)
    inbox = InMemoryAnnounce()
    packed = pack({"function": "issue_refund", "args": {"amount": 1}}, FINANCE, {"function": "issue_refund"})

    assert publish_interrupts({"messages": []}, "thread-1", [inbox]) == []
    assert inbox.events == []

    [envelope] = publish_interrupts({"parked": [("int-1", packed)]}, "thread-1", [inbox])
    assert envelope.question_id == "int-1"
    assert envelope.thread_id == "thread-1"
    assert envelope.question == {"function": "issue_refund", "args": {"amount": 1}}
    assert envelope.expires_at is not None  # from FINANCE.timeout
    assert envelope.default == {"action": "reject"}


def test_a_bare_interrupt_value_is_a_question_with_the_default_policy(fw: StubFramework) -> None:
    publish_interrupts = make_publish_interrupts(fw)
    inbox = InMemoryAnnounce()
    [envelope] = publish_interrupts({"parked": [("int-2", "is this ok?")]}, "thread-1", [inbox])
    assert envelope.question == "is this ok?"
    assert envelope.source is None
    assert WAIT_KEY not in str(envelope.question)


def test_questions_in_is_exposed_for_hosts_that_want_the_list(fw: StubFramework) -> None:
    publish_interrupts = make_publish_interrupts(fw)
    [q] = publish_interrupts.questions_in({"parked": [("int-3", pack("q", FINANCE, None))]})  # type: ignore[attr-defined]
    assert (q.question_id, q.question, q.policy) == ("int-3", "q", FINANCE)


# ------------------------------------------------------------------ the extras guard
def test_framework_subpackage_import_error_names_the_extra(monkeypatch: pytest.MonkeyPatch) -> None:
    import builtins
    import importlib
    import sys

    real_import = builtins.__import__

    def no_langgraph(name: str, *args: Any, **kwargs: Any) -> Any:
        if name == "langgraph" or name.startswith("langgraph."):
            raise ImportError(name)
        return real_import(name, *args, **kwargs)

    monkeypatch.delitem(sys.modules, "agent_wait.langgraph", raising=False)
    monkeypatch.setattr(builtins, "__import__", no_langgraph)
    with pytest.raises(ImportError, match=r"agent-wait\[langgraph\]"):
        importlib.import_module("agent_wait.langgraph")
