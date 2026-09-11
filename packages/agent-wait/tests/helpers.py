"""Shared factories for the core tests. No framework here; `Question` is the input."""

from __future__ import annotations

from typing import Any

from agent_wait import FakeClock, InMemoryAnnounce, Question, WaitEnvelope, WaitPolicy, publish

ASKED_AT = 1_760_000_000.0  # 2025-10-09T08:53:20Z


def pending(
    question_id: str = "int-1",
    *,
    question: Any = None,
    policy: WaitPolicy | None = None,
    asked_at: float | None = ASKED_AT,
) -> Question:
    return Question(
        question_id=question_id,
        question=question if question is not None else {"kind": "approval", "id": question_id},
        policy=policy or WaitPolicy(allowed_actions=("approve", "reject")),
        asked_at=asked_at,
    )


def publish_one(
    interrupt: Question | None = None,
    *,
    thread_id: str = "order-4471",
    announce: list[Any] | None = None,
    clock: FakeClock | None = None,
) -> tuple[list[WaitEnvelope], InMemoryAnnounce]:
    """Publish one interrupt through an in-memory announcer (plus any others given)."""
    memory = InMemoryAnnounce()
    envelopes = publish([interrupt or pending()], thread_id, [memory, *(announce or [])], clock=clock)
    return envelopes, memory
