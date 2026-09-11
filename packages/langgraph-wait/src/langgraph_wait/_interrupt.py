"""The interrupt value `@hitl` raises, and how `publish_interrupts` reads it back.

Private. An `Interrupt` has one payload and LangGraph offers no side channel, so the
policy and the source ride inside the value next to the question:

    {"question": {...}, "__wait__": {policy}, "__source__": {...}}

`unwrap()` reverses it. A value without `__wait__` -- a bare `interrupt(x)` from a graph
that has never heard of this library -- is read as a question with the default policy.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from agent_wait import WaitPolicy, check_question_size
from langgraph.types import interrupt

WAIT_KEY = "__wait__"
SOURCE_KEY = "__source__"
QUESTION_KEY = "question"


def raise_question(question: Any, policy: WaitPolicy, source: Mapping[str, Any] | None) -> Any:
    """Park the graph on `question`; return the answer, verbatim, on resume."""
    check_question_size(question)
    value: dict[str, Any] = {QUESTION_KEY: question, WAIT_KEY: policy.to_dict()}
    if source:
        value[SOURCE_KEY] = dict(source)
    return interrupt(value)


def unwrap(value: Any) -> tuple[Any, WaitPolicy, Mapping[str, Any] | None]:
    """`(question, policy, source)` from an interrupt value of either shape."""
    if isinstance(value, Mapping) and WAIT_KEY in value:
        source = value.get(SOURCE_KEY)
        return (
            value.get(QUESTION_KEY),
            WaitPolicy.from_dict(value.get(WAIT_KEY)),
            dict(source) if isinstance(source, Mapping) else None,  # pyright: ignore[reportUnknownArgumentType]
        )
    return value, WaitPolicy(), None
