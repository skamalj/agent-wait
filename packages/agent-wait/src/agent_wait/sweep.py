"""The sweeper -- the answer to "what if the process died between two writes?".

`register()` does three things that are not one atomic act: it creates the wait, it
announces it, and (through `SchedulerAnnounce`) it arms the timeout. A crash between any
two of them leaves a wait that exists but that nobody knows about and no timer will ever
fire for. Left alone, that wait is a thread parked forever.

The sweeper is the periodic repair pass, and it is deliberately dull:

* a pending wait with `notified_at` unset never got announced -- announce it now, which
  also creates the schedule, because the timeout *is* an announce adapter (scenario D);
* a pending wait whose `expires_at` has passed had its schedule lost -- re-announce, and
  `SchedulerAnnounce` will deliver the timeout straight to the entry point rather than
  try to schedule something in the past.

It runs as a one-minute EventBridge rule against a tiny Lambda. Note what it does *not*
need: the graph, a checkpointer, or any knowledge of the framework. It only ever moves
waits back onto the announce path, and lets `dispatch()` at the real entry point do the
deciding. That is what keeps it safe to run every minute, forever.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover
    from .runtime import WaitRuntime

_log = logging.getLogger("agent_wait.sweep")

OVERDUE_GRACE_SECONDS = 60.0


def sweep(runtime: WaitRuntime, *, limit: int = 100, grace: float = OVERDUE_GRACE_SECONDS) -> dict[str, int]:
    """Returns counts: `{"scanned", "announced", "overdue"}`. Safe to call concurrently
    with anything else: every action it takes is idempotent."""
    now = runtime.clock.now()
    counts = {"scanned": 0, "announced": 0, "overdue": 0}

    unnotified = runtime.store.find(status="pending", notified=False, limit=limit)
    for wait in unnotified:
        counts["scanned"] += 1
        envelope = runtime.envelope_for(wait, "created")
        if runtime.announce.announce_reporting(envelope, "created"):
            runtime.store.set_fields(wait.wait_id, notified_at=now, updated_at=now)
            counts["announced"] += 1
            _log.info("sweeper completed setup for wait %s", wait.wait_id)

    overdue = runtime.store.find(status="pending", due_before=now - grace, limit=limit)
    for wait in overdue:
        if wait.notified_at is None:
            continue  # just handled above
        counts["scanned"] += 1
        counts["overdue"] += 1
        _log.info("sweeper re-announcing overdue wait %s; its schedule appears to be lost", wait.wait_id)
        runtime.announce.announce_reporting(runtime.envelope_for(wait, "created"), "created")

    return counts
