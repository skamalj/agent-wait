"""`@hitl` -- mark a tool as needing a human, in one of two ways.

    @tool
    @hitl(WaitPolicy(timeout="P1D", default={"action": "reject"}, tags={"approver_group": "finance"}))
    def issue_refund(order_id: str, amount: int) -> str:
        ...

Apply it *under* `@tool`, on the function. It registers the policy against the tool's
name (so `publish_interrupts` can find it for middleware-batched interrupts too) and
wraps the call in one of two behaviours.

## `mode="interrupt"` (default) -- the thread parks

The wrapper calls `ask()` with `{"tool": name, "args": {...}}` before running the tool.
The graph stops; `publish_interrupts()` after the run announces it; the answer comes back
as `Command(resume={question_id: decision})` and the tool runs, or does not:

    {"action": "approve"}                      -> the tool runs with its original args
    {"action": "approve", "args": {...}}       -> the tool runs with these args instead
    anything else                              -> the tool does NOT run; the decision is
                                                  returned as the tool's result, so the
                                                  model sees why

One `interrupt()` per node still applies (langgraph #6626): two `@hitl` tools dispatched
by the same `ToolNode` share an interrupt id. Use `HumanInTheLoopMiddleware`, which
batches them into one interrupt, or give each its own node.

## `mode="async"` -- the thread does not park

    @tool
    @hitl(FINANCE, mode="async", announce=[SnsAnnounce(topic), DynamoDbAnnounce(table)])
    def issue_refund(order_id: str, amount: int) -> str: ...

The decorator *is* the publisher. When the tool is called it announces the question
through the announcers given here and returns `{"status": "pending_approval",
"question_id": ...}` without running the tool body. The graph carries on; the model sees
that the action is pending; there is no `__interrupt__` and nothing to call after the run.
The decision arrives later as a **new message** to the agent, carrying the `question_id`
-- how, and what the graph does with it, is yours.

Nothing is parked, so nothing in LangGraph remembers the question. Whatever record you
keep (a `DynamoDbAnnounce` table is one) is the only one. This is the right mode when the
person answering is not the person on the thread -- a customer chatting on WhatsApp while
finance approves in a dashboard -- and it should be a visible choice, not a default.

`question_id` is derived from the thread, the tool and its arguments, so the same call
on the same thread produces the same id: a consumer sees one question, not two. Pass
`question_id=` to override. The thread id comes from the run config, where LangGraph
keeps it.
"""

from __future__ import annotations

import functools
import hashlib
import inspect
import json
from collections.abc import Callable, Mapping, Sequence
from typing import Any, Literal

from agent_wait import AnnounceAdapter, Question, WaitPolicy, canonical_json, publish

from .ask import ask

Mode = Literal["interrupt", "async"]

_REGISTRY: dict[str, WaitPolicy] = {}


def policy_for(tool_name: str) -> WaitPolicy | None:
    """The policy `@hitl` registered for a tool, or None."""
    return _REGISTRY.get(tool_name)


def question_id_for(thread_id: str, tool: str, args: Mapping[str, Any]) -> str:
    """Deterministic: the same call on the same thread is the same question."""
    return hashlib.sha256(f"{thread_id}|{tool}|{canonical_json(dict(args))}".encode()).hexdigest()[:32]


def _thread_id() -> str:
    from langgraph.config import get_config

    configurable = get_config().get("configurable", {})
    thread_id = configurable.get("thread_id")
    if not thread_id:
        raise RuntimeError("@hitl(mode='async') needs a thread_id in the run config")
    return str(thread_id)


def hitl(
    policy: WaitPolicy | None = None,
    *,
    mode: Mode = "interrupt",
    announce: Sequence[AnnounceAdapter] | None = None,
    question_id: Callable[[str, str, Mapping[str, Any]], str] | None = None,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    resolved = policy or WaitPolicy(allowed_actions=("approve", "reject"))
    if mode == "async" and not announce:
        raise ValueError("@hitl(mode='async') publishes from inside the tool, so it needs announce=[...]")
    announcers: Sequence[AnnounceAdapter] = list(announce or ())

    def decorate(fn: Callable[..., Any]) -> Callable[..., Any]:
        name = fn.__name__
        _REGISTRY[name] = resolved
        signature = inspect.signature(fn)

        @functools.wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            bound = signature.bind_partial(*args, **kwargs)
            bound.apply_defaults()
            call_args: dict[str, Any] = dict(bound.arguments)
            question = {"tool": name, "args": call_args}

            if mode == "interrupt":
                decision = ask(question, resolved, source={"tool": name})
                return _apply(decision, fn, call_args)

            thread_id = _thread_id()
            qid = (question_id or question_id_for)(thread_id, name, call_args)
            publish(
                [Question(question_id=qid, question=question, policy=resolved, source={"tool": name})],
                thread_id,
                announcers,
            )
            return {"status": "pending_approval", "question_id": qid, "tool": name}

        wrapper.__hitl__ = {"mode": mode, "policy": resolved}  # type: ignore[attr-defined]
        return wrapper

    return decorate


def _apply(decision: Any, fn: Callable[..., Any], call_args: Mapping[str, Any]) -> Any:
    """Run the tool if the decision says so; otherwise hand the decision back as the result."""
    if isinstance(decision, Mapping) and decision.get("action") == "approve":
        edited = decision.get("args")
        merged = {**call_args, **dict(edited)} if isinstance(edited, Mapping) else dict(call_args)  # pyright: ignore[reportUnknownArgumentType]
        return fn(**merged)
    if isinstance(decision, Mapping):
        return {"status": "not_executed", **dict(decision)}  # pyright: ignore[reportUnknownArgumentType]
    return {
        "status": "not_executed",
        "decision": decision
        if isinstance(decision, str | int | float | bool | type(None))
        else json.loads(json.dumps(decision, default=str)),
    }
