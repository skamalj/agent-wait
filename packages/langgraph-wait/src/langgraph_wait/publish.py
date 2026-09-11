"""`publish_interrupts()` -- announce what `graph.invoke()` just parked on.

    result = graph.invoke(value, config)
    publish_interrupts(result, thread_id, announce=[SnsAnnounce(topic_arn)])

When a `@hitl` function is called (or any node calls `interrupt()`), LangGraph returns
the run's output with an `__interrupt__` entry: a sequence of `Interrupt` objects, each
with `.id` and `.value` and nothing else (`ns`, `when`, `resumable` were removed in
langgraph 0.6). This reads that entry and hands one envelope per question to the
announcers. No `__interrupt__`, nothing happens. `stream()` yields the same shape as a
chunk, so it works on a chunk.

A bare `interrupt(value)` from a node that never heard of this library is published too,
with `value` as the question and the default policy.

Nothing here runs the graph, reads its state, or resumes anything.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from agent_wait import AnnounceAdapter, Clock, EntryPoint, Question, WaitEnvelope, publish

from ._interrupt import unwrap

INTERRUPT_KEY = "__interrupt__"


def questions_in(result: Any) -> list[Question]:
    """The questions a run returned, from its `__interrupt__`. `[]` if it did not park."""
    if not isinstance(result, Mapping):
        return []
    raw = result.get(INTERRUPT_KEY)  # pyright: ignore[reportUnknownMemberType]
    if not raw:
        return []
    out: list[Question] = []
    for item in raw:  # pyright: ignore[reportUnknownVariableType]
        question, policy, source = unwrap(getattr(item, "value", None))
        out.append(Question(str(getattr(item, "id", "")), question, policy, source=source))
    return out


def publish_interrupts(
    result: Any,
    thread_id: str,
    announce: Sequence[AnnounceAdapter],
    *,
    reply_to: EntryPoint | None = None,
    clock: Clock | None = None,
) -> list[WaitEnvelope]:
    """Announce every question in `result`. Returns the envelopes sent; `[]` if none."""
    questions = questions_in(result)
    if not questions:
        return []
    return publish(questions, thread_id, announce, reply_to=reply_to, clock=clock)
