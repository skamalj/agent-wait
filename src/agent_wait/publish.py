"""`publish()` -- turn interrupts into envelopes and hand them to the announcers.

Framework-agnostic: it takes `Question`s. `agent_wait.langgraph.publish_interrupts()`
is the one-liner that pulls them out of what `graph.invoke()` returned and calls this.

There is deliberately nothing else here. No invoke, no routing, no record of what was
published. Publishing the same interrupt twice produces two envelopes with the same
`dedupe_key`, which is how a consumer knows they are one question.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence

from .announce.base import AnnounceAdapter
from .announce.composite import CompositeAnnounce
from .model import Clock, EntryPoint, Question, SystemClock, WaitEnvelope, iso, new_ulid

_log = logging.getLogger("agent_wait.publish")


def build_envelope(
    thread_id: str,
    question: Question,
    *,
    reply_to: EntryPoint | None = None,
    clock: Clock | None = None,
) -> WaitEnvelope:
    """One envelope for one parked question. Pure; announces nothing."""
    now = (clock or SystemClock()).now()
    policy = question.policy
    timeout = policy.timeout_seconds
    # `expires_at` is measured from when the question was asked if the caller knows
    # that (`Question.asked_at`), otherwise from now. Publishing the same
    # interrupt again later therefore gives a later deadline unless `asked_at` is set.
    asked_at = question.asked_at if question.asked_at is not None else now
    return WaitEnvelope(
        type="wait.created",
        event_id=new_ulid(clock),
        thread_id=thread_id,
        question_id=question.question_id,
        question=question.question,
        allowed_actions=policy.allowed_actions,
        expires_at=iso(asked_at + timeout) if timeout else None,
        reply_to=reply_to.to_dict() if reply_to is not None else None,
        # A filled-in stub. The consumer replaces `answer` and posts this back; how it
        # gets back, and what happens then, is the host's business.
        reply_with={"thread_id": thread_id, "question_id": question.question_id, "answer": None},
        default=policy.default,
        answer_ttl=policy.answer_ttl,
        source=question.source,
        correlation=policy.correlation,
        tags=policy.tags,
    )


def publish(
    pending: Sequence[Question],
    thread_id: str,
    announce: Sequence[AnnounceAdapter],
    *,
    reply_to: EntryPoint | None = None,
    clock: Clock | None = None,
) -> list[WaitEnvelope]:
    """Announce every pending interrupt. Returns the envelopes that were sent.

    Announcer failures are contained per adapter and logged; this never raises for
    them. It returns the envelopes regardless, so a caller that wants its own record of
    what went out can keep one.
    """
    fanout = CompositeAnnounce(announce)
    envelopes = [build_envelope(thread_id, p, reply_to=reply_to, clock=clock) for p in pending]
    for envelope in envelopes:
        _log.debug("publishing %s", envelope.dedupe_key)
        fanout.announce(envelope, "created")
    return envelopes
