"""`@hitl` -- make a function interruptible.

    @tool
    @hitl(WaitPolicy(timeout="P1D", default={"action": "reject"}, tags={"approver_group": "finance"}))
    def issue_refund(order_id: str, amount: int) -> str:
        ...

That is the whole declaration. Calling the decorated function inside a run raises a
LangGraph interrupt with `{"function": "issue_refund", "args": {...}}` as the question;
`publish_interrupts()` after the run announces it; the answer comes back as
`Command(resume={question_id: answer})`, and:

    {"action": "approve"}                    -> the body runs with its original args
    {"action": "approve", "args": {...}}     -> the body runs with these args instead
    anything else                            -> the body does NOT run; the answer is
                                                returned in its place, so a model sees why

## Getting the answer itself: the `decision` parameter

A tool usually only needs to know whether to run. A node usually needs the answer. If
the decorated function has a parameter named `decision` (rename it with `decision=`),
it **always** runs and receives the answer there, approve or not:

    @hitl(FINANCE)
    def review(state, decision=None):
        return {"decision": decision}        # verbatim: {"action": "approve", "note": "ok"}

## `mode="async"` -- the thread does not park

    @tool
    @hitl(FINANCE, mode="async", announce=[SnsAnnounce(topic), DynamoDbAnnounce(table)])
    def issue_refund(order_id: str, amount: int) -> str: ...

The decorator *is* the publisher. The call announces the question through the announcers
given here and returns `{"status": "pending_approval", "question_id": ...}` without
running the body. Nothing is parked; nothing is called after the run; the graph carries
on. The decision arrives later as a **new message** and the graph acts on it however you
designed it. This is the mode for a single-thread channel like WhatsApp, where the
approver is not the person on the thread. LangGraph remembers nothing about the
question; whatever record you keep (a `DynamoDbAnnounce` row is one) is the only one.

`question_id` is `sha256(thread | function | args)`, so the same call on the same thread
is the same question and a consumer sees one, not two.

## Not compatible with `HumanInTheLoopMiddleware`

LangChain's `create_agent` middleware interrupts *before* a tool is called, in its own
shape, with its own resume format. Putting `@hitl` on a tool the middleware also
intercepts interrupts twice -- once by the middleware, then again inside the tool when it
finally runs. Use one or the other. This library is the other.

## One `interrupt()` per node

Two `@hitl` functions dispatched by the same `ToolNode` share an interrupt id on
langgraph 1.2.x (#6626); only one surfaces per run (#6624). Give each interrupting tool
its own node.
"""

from __future__ import annotations

import functools
import hashlib
import inspect
from collections.abc import Callable, Mapping, Sequence
from typing import Any, Literal

from agent_wait import AnnounceAdapter, Question, WaitPolicy, canonical_json, publish

from ._interrupt import raise_question

Mode = Literal["interrupt", "async"]


def question_id_for(thread_id: str, function: str, args: Mapping[str, Any]) -> str:
    """Deterministic: the same call on the same thread is the same question."""
    return hashlib.sha256(f"{thread_id}|{function}|{canonical_json(dict(args))}".encode()).hexdigest()[:32]


def _thread_id() -> str:
    from langgraph.config import get_config

    thread_id = get_config().get("configurable", {}).get("thread_id")
    if not thread_id:
        raise RuntimeError("@hitl(mode='async') needs a thread_id in the run config")
    return str(thread_id)


def hitl(
    policy: WaitPolicy | None = None,
    *,
    mode: Mode = "interrupt",
    announce: Sequence[AnnounceAdapter] | None = None,
    decision: str = "decision",
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Make a function interruptible. See the module docstring."""
    resolved = policy or WaitPolicy(allowed_actions=("approve", "reject"))
    if mode == "async" and not announce:
        raise ValueError("@hitl(mode='async') publishes from inside the function, so it needs announce=[...]")
    announcers: Sequence[AnnounceAdapter] = list(announce or ())

    def decorate(fn: Callable[..., Any]) -> Callable[..., Any]:
        name = fn.__name__
        signature = inspect.signature(fn)
        wants_decision = decision in signature.parameters
        source = {"function": name}

        @functools.wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            bound = signature.bind_partial(*args, **kwargs)
            bound.apply_defaults()
            call_args: dict[str, Any] = {k: v for k, v in bound.arguments.items() if k != decision}
            question = {"function": name, "args": call_args}

            if mode == "async":
                thread_id = _thread_id()
                qid = question_id_for(thread_id, name, call_args)
                publish([Question(qid, question, resolved, source=source)], thread_id, announcers)
                return {"status": "pending_approval", "question_id": qid, "function": name}

            answer = raise_question(question, resolved, source)
            if wants_decision:
                return fn(**call_args, **{decision: answer})
            if isinstance(answer, Mapping) and answer.get("action") == "approve":
                edited = answer.get("args")
                merged = {**call_args, **dict(edited)} if isinstance(edited, Mapping) else call_args  # pyright: ignore[reportUnknownArgumentType]
                return fn(**merged)
            return answer

        wrapper.__hitl__ = {"mode": mode, "policy": resolved}  # type: ignore[attr-defined]
        return wrapper

    return decorate
