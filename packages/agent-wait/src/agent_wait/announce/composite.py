"""`CompositeAnnounce` -- fan out to several adapters, isolate their failures.

This is where the "must not raise" rule is actually enforced, rather than merely
requested of adapter authors. A third-party adapter that breaks its side of the contract
is contained here, and the runtime never learns about it.

`any_succeeded()` reports whether at least one adapter accepted the envelope, which is
what the runtime uses to decide whether to stamp `notified_at` -- if nothing got
through, the sweeper should try again.
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
        self.announce_reporting(envelope, transition)

    def announce_reporting(self, envelope: WaitEnvelope, transition: Transition) -> bool:
        """Same as `announce()`, but returns whether any adapter accepted the envelope.

        An adapter that does not support the transition is skipped and does not count as
        a success; if *no* adapter supports it, that counts as success, because there is
        nothing left to retry.
        """
        attempted = 0
        succeeded = 0
        for adapter in self.adapters:
            try:
                if not adapter.supports(transition):
                    continue
            except Exception:
                _log.exception("announce adapter %s failed in supports()", getattr(adapter, "name", "?"))
                continue
            attempted += 1
            try:
                adapter.announce(envelope, transition)
                succeeded += 1
            except Exception:
                # Contract violation by the adapter. Contained here, on purpose.
                _log.exception(
                    "announce adapter %s raised on %s for wait %s",
                    getattr(adapter, "name", "?"),
                    transition,
                    envelope.wait_id,
                )
        return succeeded > 0 or attempted == 0
