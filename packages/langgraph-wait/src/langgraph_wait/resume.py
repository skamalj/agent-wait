"""`resume_command()` -- turn an answer message into the thing you pass to `invoke()`.

A pure function. It makes no decisions, reads nothing and writes nothing; it exists so
that the one LangGraph-specific detail in the inbound message format -- that a resume is
keyed by interrupt id, so that parallel interrupts resume independently -- is written
down once instead of in every consumer.

    def handle(message):
        if is_answer(message):
            return agent.invoke(resume_command(message), message["thread_id"])
        return agent.invoke(message["input"], message["thread_id"])

`Command(resume=value)` -- the un-keyed form -- hands the same value to *every* parked
interrupt on the thread. With two approvals outstanding that is one click approving both,
which is why this builds the dict form even when there is only one.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from langgraph.types import Command


def is_answer(message: Mapping[str, Any]) -> bool:
    """`interrupt_id` present means resume; absent means start. That is the whole rule.

    It holds because the publisher ships a filled-in `reply_with` stub in every envelope
    and the consumer echoes it back, so the key is there by construction rather than by
    the consumer remembering to add it.
    """
    return bool(message.get("interrupt_id"))


def resume_command(message: Mapping[str, Any]) -> Command:
    """Build the resume for one answer message. Raises `KeyError` if it is a start."""
    interrupt_id = message["interrupt_id"]
    return Command(resume={str(interrupt_id): message.get("answer")})
