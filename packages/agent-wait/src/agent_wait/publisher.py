"""`WaitPublisher` -- run the graph, and tell the world about anything it parked on.

That is the entire library. There is one call:

    agent.invoke(payload, thread_id="order-4471")

It runs the graph, works out which questions opened and which closed, and publishes an
envelope for each. Nothing else happens: no store, no tokens, no timers, no inbound
message handling. Whoever receives the envelope decides what to do with it, and calls
`invoke()` again with the answer.

## How "opened" and "closed" are worked out

By diffing the framework's own state, not by tracking anything ourselves:

    before = {what the thread was parked on}
    result = graph.invoke(...)
    after  = {what the thread is parked on now}

    created = after - before      # new questions
    resumed = before - after      # questions the graph has moved past

Two `get_state()` reads per invoke, and the framework stays the only source of truth.

## Recovering a lost announce

If the process dies between the invoke and the announce, the graph is parked and nobody
was told. `republish()` is the repair: it publishes `created` for whatever the thread is
parked on right now, with the same interrupt ids and therefore the same `dedupe_key`, so
a consumer that already saw the question discards it.

The caller has to know when to use it, and the rule is short enough to inline:

    if agent.pending(thread_id):   # a redelivered start for a thread already parked
        agent.republish(thread_id)
    else:
        agent.invoke(message["input"], thread_id)

**Do not re-invoke a parked thread with its original input.** LangGraph will treat it as
a fresh turn, ask the question a second time, and give it a new interrupt id -- so the
consumer sees two questions and the deduplication that repairs everything else cannot
help. This is the one thing v0.1's store bought that the caller now has to remember, and
it is why `pending()` is part of the public surface.

## What happens if the graph raises

The exception propagates and nothing is published, because nothing is known: a run that
raised has not necessarily parked or unparked anything. There is no bookkeeping left
half-done, which is the advantage of keeping none.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from typing import Any, Protocol, runtime_checkable

from .announce.base import AnnounceAdapter
from .announce.composite import CompositeAnnounce
from .model import (
    Clock,
    EntryPoint,
    PendingInterrupt,
    SystemClock,
    Transition,
    WaitEnvelope,
    iso,
    new_ulid,
)

_log = logging.getLogger("agent_wait.publisher")


@runtime_checkable
class FrameworkAdapter(Protocol):
    """Everything framework-specific, in three methods.

    `langgraph_wait.LangGraphAdapter` is the one implementation; the protocol exists so
    the core can be tested without LangGraph and so a second framework is a day's work
    rather than a fork.
    """

    name: str

    def config_for(self, thread_id: str) -> dict[str, Any]:
        """The framework's handle for one conversation."""
        ...

    def invoke(self, value: Any, config: Any) -> Any:
        """Run the graph. `value` is whatever the caller passed -- fresh input on a
        start, a resume command on an answer. The adapter does not inspect it."""
        ...

    def pending(self, thread_id: str) -> list[PendingInterrupt]:
        """What this thread is parked on *right now*, read from the checkpoint.

        Must return `[]` for a thread that has never run. Must not over-report: an
        interrupt the graph has already moved past does not belong here, however the
        framework's own API chooses to report it.
        """
        ...


class WaitPublisher:
    """Wraps a graph. Runs it, and publishes what it parks on."""

    def __init__(
        self,
        adapter: FrameworkAdapter,
        *,
        announce: Sequence[AnnounceAdapter],
        reply_to: EntryPoint | None = None,
        clock: Clock | None = None,
    ) -> None:
        """`reply_to` is a hint for consumers, and nothing more.

        This library builds no return leg. It does not listen anywhere, does not receive
        answers and does not verify them -- so it has no need to know where the agent
        lives. If you *do* build a return leg and want the envelope to tell consumers
        where it is, pass an `EntryPoint` and it is published verbatim. Leave it out and
        `reply_to` is `null`, which is fine for any consumer that already knows.
        """
        self.adapter = adapter
        self.announce = CompositeAnnounce(announce)
        self.reply_to = reply_to
        self.clock: Clock = clock or SystemClock()

    # ------------------------------------------------------------------ the one call
    def invoke(self, value: Any, thread_id: str, *, config: Any = None) -> Any:
        """Run the graph for `thread_id` and publish whatever changed.

        Returns the framework's own result, unchanged -- the wrapper is not a filter.
        """
        resolved = config if config is not None else self.adapter.config_for(thread_id)

        before = {p.interrupt_id: p for p in self.adapter.pending(thread_id)}
        result = self.adapter.invoke(value, resolved)
        after = {p.interrupt_id: p for p in self.adapter.pending(thread_id)}

        for interrupt_id, opened in after.items():
            if interrupt_id not in before:
                self._publish(thread_id, opened, "created")
        for interrupt_id, closed in before.items():
            if interrupt_id not in after:
                self._publish(thread_id, closed, "resumed")

        return result

    def republish(self, thread_id: str) -> list[PendingInterrupt]:
        """Announce `created` again for everything this thread is parked on.

        The repair for an announce that was lost -- a crash before it went out, a broker
        that was down, an adapter added after the question was asked. Safe to call at any
        time and as often as you like: the interrupt ids are stable, so every republish
        carries the same `dedupe_key` and a consumer that already has the question
        discards it.

        Runs the graph not at all.
        """
        parked = self.adapter.pending(thread_id)
        for interrupt in parked:
            self._publish(thread_id, interrupt, "created")
        return parked

    # ------------------------------------------------------------------ reading back
    def pending(self, thread_id: str) -> list[PendingInterrupt]:
        """What this thread is parked on. Read-only; publishes nothing.

        This is the honest answer to "is this answer I just received still live, or did
        somebody beat me to it?" -- which is a question you now own, and this is the
        tool for it.
        """
        return self.adapter.pending(thread_id)

    # ------------------------------------------------------------------ the envelope
    def envelope_for(
        self, thread_id: str, interrupt: PendingInterrupt, transition: Transition
    ) -> WaitEnvelope:
        policy = interrupt.policy
        timeout = policy.timeout_seconds
        # Anchored to the checkpoint, not to now -- see `PendingInterrupt.asked_at`.
        asked_at = interrupt.asked_at if interrupt.asked_at is not None else self.clock.now()
        return WaitEnvelope(
            type=f"wait.{transition}",
            event_id=new_ulid(self.clock),
            thread_id=thread_id,
            interrupt_id=interrupt.interrupt_id,
            question=interrupt.question,
            allowed_actions=policy.allowed_actions,
            expires_at=iso(asked_at + timeout) if timeout else None,
            reply_to=self.reply_to.to_dict() if self.reply_to is not None else None,
            # A filled-in stub, not a description of one. The consumer replaces `answer`
            # and posts this back to `reply_to`; the presence of `interrupt_id` is what
            # makes it a resume rather than a start.
            reply_with={"thread_id": thread_id, "interrupt_id": interrupt.interrupt_id, "answer": None},
            default=policy.default,
            correlation=policy.correlation,
            tags=policy.tags,
        )

    def _publish(self, thread_id: str, interrupt: PendingInterrupt, transition: Transition) -> None:
        envelope = self.envelope_for(thread_id, interrupt, transition)
        _log.debug("publishing %s for %s", envelope.type, envelope.interrupt_id)
        self.announce.announce(envelope, transition)
