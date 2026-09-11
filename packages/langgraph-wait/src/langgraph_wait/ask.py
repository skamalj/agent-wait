"""`ask()` -- the one line a graph author writes.

    decision = ask({"kind": "refund_approval", "amount": amount},
                   policy=WaitPolicy(timeout="P3D", allowed_actions=("approve", "reject")))

It is a thin wrapper over `langgraph.types.interrupt()`, and thin is the point: the node
still pauses exactly the way LangGraph pauses, the checkpointer still does its job, and
nothing about the graph changes. What `ask()` adds is the *policy* -- how long this
question should stand, what to assume if nobody answers, which answers are meaningful --
carried inside the interrupt value under `__wait__`.

That key is the only channel available. An interrupt has one payload and LangGraph does
not offer a side channel for metadata, so the policy rides with the question and
`unwrap()` peels it back off.

A plain `interrupt(value)` with no `ask()` still works. It is read as a question with the
default policy, so a graph that already interrupts gets published without any edit at all.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from agent_wait import WaitPolicy, check_question_size
from langgraph.types import interrupt

WAIT_KEY = "__wait__"
SOURCE_KEY = "__source__"
QUESTION_KEY = "question"


def ask(question: Any, policy: WaitPolicy | None = None, *, source: Mapping[str, Any] | None = None) -> Any:
    """Park the graph on `question` and return the answer when it arrives.

    On the first pass this raises through `interrupt()` and the node does not return. On
    resume it returns the answer message's `answer` field, verbatim -- whatever the
    consumer put there. The library does not wrap it, merge into it or add an action to
    it; what was sent is what the node sees.
    """
    check_question_size(question)
    resolved = policy or WaitPolicy()
    value: dict[str, Any] = {QUESTION_KEY: question, WAIT_KEY: resolved.to_dict()}
    if source:
        value[SOURCE_KEY] = dict(source)
    return interrupt(value)


def unwrap(value: Any) -> tuple[Any, WaitPolicy]:
    """Split an interrupt value back into `(question, policy)`.

    Handles both shapes: one raised by `ask()`, and a bare `interrupt(value)` from a
    graph that has never heard of this library.
    """
    if isinstance(value, dict) and WAIT_KEY in value:
        return value.get(QUESTION_KEY), WaitPolicy.from_dict(value.get(WAIT_KEY))
    return value, WaitPolicy()
