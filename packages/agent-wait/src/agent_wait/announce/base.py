"""The `AnnounceAdapter` port -- the only thing users are expected to plug into.

An announce adapter tells the world that a wait exists or changed. That is *all* it
does. It does not decide anything, it is not consulted, and its return value is
discarded. The contract is one line long and the whole design leans on it:

    **`announce()` must not raise into the caller.**

If Slack is down, the refund still parks. If the SNS topic was deleted, the run still
completes. A failed `created` announce leaves `notified_at` unset and the sweeper
retries it later (rule 10) -- which is why the runtime, not the adapter, owns that flag.

The timeout is an announce adapter too. `SchedulerAnnounce` "delivers" the wait to the
future by creating a one-shot schedule; when it fires, the answer arrives at the agent's
own entry point like any other. That is the whole reason there is no timer Lambda.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from ..model import Transition, WaitEnvelope


@runtime_checkable
class AnnounceAdapter(Protocol):
    name: str

    def announce(self, envelope: WaitEnvelope, transition: Transition) -> None:
        """Fire-and-forget. Log failures; never raise."""
        ...

    def supports(self, transition: Transition) -> bool:
        """Adapters that only care about some transitions say so here, so the runtime
        can skip them cheaply."""
        ...
