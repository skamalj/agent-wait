"""`CompositeAnnounce` -- fan out to several adapters, isolate their failures.

This is where "must not raise" is actually enforced rather than merely requested of
adapter authors. A third-party adapter that breaks its side of the contract is contained
here, and the publisher never learns about it.

There is no retry and no reporting of what got through. A failed announce is recovered
the same way everything else is: re-invoke the thread, get the same interrupt ids back,
republish. The consumer discards the duplicate on `dedupe_key`.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence

from ..model import Transition, WaitEnvelope
from .base import AnnounceAdapter

_log = logging.getLogger("agent_wait.announce")


class CompositeAnnounce:
    name = "composite"

    def __init__(self, adapters: Sequence[AnnounceAdapter]) -> None:
        self.adapters = list(adapters)

    def supports(self, transition: Transition) -> bool:
        return any(a.supports(transition) for a in self.adapters)

    def announce(self, envelope: WaitEnvelope, transition: Transition) -> None:
        for adapter in self.adapters:
            name = getattr(adapter, "name", "?")
            try:
                if not adapter.supports(transition):
                    continue
                adapter.announce(envelope, transition)
            except Exception:
                # A contract violation by the adapter. Contained here, on purpose: if
                # Slack is down, the graph still parked, and the run still completes.
                _log.exception(
                    "announce adapter %s raised on %s for interrupt %s",
                    name,
                    transition,
                    envelope.interrupt_id,
                )
