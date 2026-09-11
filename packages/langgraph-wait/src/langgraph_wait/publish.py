"""`publish_interrupts()` -- announce what `graph.invoke()` just parked on.

    result = graph.invoke(value, config)
    publish_interrupts(result, thread_id, announce=[SnsAnnounce(topic_arn)])

When a node calls `interrupt()`, LangGraph returns the run's output with an
`__interrupt__` entry: a sequence of `Interrupt` objects, each with `.id` and `.value`
(nothing else -- `ns`, `when`, `resumable` were removed in langgraph 0.6). This reads
that entry and hands one envelope per question to the announcers. No `__interrupt__`,
nothing happens. `stream()` yields the same shape as a chunk, so it works on a chunk.

## Three interrupt shapes are understood

1. **`ask()`** -- `.value` is `{"question": ..., "__wait__": policy}`. One question.
2. **A bare `interrupt(value)`** -- one question, `value` verbatim, default policy.
3. **`HumanInTheLoopMiddleware`** (`langchain.agents`) -- `.value` is an `HITLRequest`:
   `{"action_requests": [{name, args, description}], "review_configs": [...]}`. The
   middleware batches every tool call needing review into ONE interrupt, so this is one
   question whose payload is the whole batch, and whose answer is the middleware's
   `HITLResponse`: `{"decisions": [{type: approve|reject|edit|respond, ...}, ...]}` in
   batch order. The policy comes from `@hitl` on the first tool in the batch, if any.

Nothing here runs the graph, reads its state, or resumes anything. The answer's shape
and its return path are yours; the envelope's `reply_with` stub carries `question_id`,
and `Command(resume={question_id: answer})` is how LangGraph takes it.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from agent_wait import AnnounceAdapter, Clock, EntryPoint, Question, WaitEnvelope, WaitPolicy, publish

from .ask import unwrap
from .hitl import policy_for

INTERRUPT_KEY = "__interrupt__"


def _is_hitl_request(value: Any) -> bool:
    return isinstance(value, Mapping) and "action_requests" in value and "review_configs" in value


def _from_hitl_request(interrupt_id: str, value: Mapping[str, Any]) -> Question:
    actions: list[Mapping[str, Any]] = list(value.get("action_requests") or [])
    names = [str(a.get("name", "")) for a in actions]
    policy = policy_for(names[0]) if names else None
    return Question(
        question_id=interrupt_id,
        question={"actions": actions, "review": list(value.get("review_configs") or [])},
        policy=policy or WaitPolicy(allowed_actions=("approve", "reject", "edit", "respond")),
        source={"tools": names, "via": "HumanInTheLoopMiddleware"},
    )


def questions_in(result: Any) -> list[Question]:
    """The questions a run returned, from its `__interrupt__`. `[]` if it did not park."""
    if not isinstance(result, Mapping):
        return []
    raw = result.get(INTERRUPT_KEY)  # pyright: ignore[reportUnknownMemberType]
    if not raw:
        return []
    out: list[Question] = []
    for item in raw:  # pyright: ignore[reportUnknownVariableType]
        interrupt_id = str(getattr(item, "id", ""))
        value = getattr(item, "value", None)
        if _is_hitl_request(value):
            out.append(_from_hitl_request(interrupt_id, value))
            continue
        question, policy = unwrap(value)
        source = None
        if isinstance(value, Mapping) and isinstance(value.get("__source__"), Mapping):
            source = dict(value["__source__"])  # pyright: ignore[reportUnknownArgumentType]
        out.append(Question(question_id=interrupt_id, question=question, policy=policy, source=source))
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
