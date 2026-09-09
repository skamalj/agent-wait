"""`ask()` -- the one line a graph author writes.

    decision = ask({"kind": "refund_approval", "amount": amount},
                   policy=WaitPolicy(timeout="P3D", default={"action": "reject"},
                                     allowed_actions=("approve", "reject")))

It is a thin wrapper over `langgraph.types.interrupt()`, and thin is the point: the node
still pauses exactly the way LangGraph pauses, the checkpointer still does its job, and
nothing about the graph changes. What `ask()` adds is the *policy* -- how long this
question may go unanswered, what happens if nobody answers, which answers are even
legal -- carried inside the interrupt value under `__wait__`.

That key is the only channel available. An interrupt has one payload and LangGraph does
not offer a side channel for metadata, so the policy rides with the question and
`LangGraphAdapter.extract()` peels it back off (REQUIREMENTS section 16.2).

A plain `interrupt(value)` with no `ask()` still works. It is read as a wait with the
default policy and `question = value`, so a graph that already interrupts gains durable
waits without any edit at all.
"""

from __future__ import annotations

from typing import Any

from agent_wait import WaitPolicy
from langgraph.types import interrupt

WAIT_KEY = "__wait__"
QUESTION_KEY = "question"


def ask(question: Any, policy: WaitPolicy | None = None) -> Any:
    """Park the graph on `question` and return the answer when it arrives.

    On the first pass this raises through `interrupt()` and the node does not return.
    On resume it returns the answer envelope's `payload` merged with its `action` --
    so `{"action": "approve", "note": "within budget"}` for an approval, or the policy's
    `default` if the wait timed out.
    """
    resolved = policy or WaitPolicy()
    return interrupt({QUESTION_KEY: question, WAIT_KEY: resolved.to_dict()})


def unwrap(value: Any) -> tuple[Any, WaitPolicy]:
    """Split an interrupt value back into `(question, policy)`.

    Handles both shapes: one raised by `ask()`, and a bare `interrupt(value)` from a
    graph that has never heard of this library.
    """
    if isinstance(value, dict) and WAIT_KEY in value:
        return value.get(QUESTION_KEY), WaitPolicy.from_dict(value.get(WAIT_KEY))
    return value, WaitPolicy()
