"""`LogAnnounce` -- a structured log line per transition. The default in tests.

Deliberately careful about what it prints. The question may hold anything the agent was
working on -- a customer's address, the body of a claim -- so it does not go to INFO
(CLAUDE.md). At DEBUG it is included, because by then someone has opted in.
"""

from __future__ import annotations

import json
import logging

from ..model import Transition, WaitEnvelope


class LogAnnounce:
    name = "log"

    def __init__(self, logger: logging.Logger | None = None, *, level: int = logging.INFO) -> None:
        self._log = logger or logging.getLogger("agent_wait.announce")
        self._level = level

    def supports(self, transition: Transition) -> bool:
        return True

    def announce(self, envelope: WaitEnvelope, transition: Transition) -> None:
        try:
            record = {
                "event": envelope.type,
                "event_id": envelope.event_id,
                "thread_id": envelope.thread_id,
                "interrupt_id": envelope.interrupt_id,
                "allowed_actions": list(envelope.allowed_actions),
                "expires_at": envelope.expires_at,
                "tags": dict(envelope.tags),
            }
            self._log.log(self._level, "agent-wait %s", json.dumps(record, default=str))
            if self._log.isEnabledFor(logging.DEBUG):
                self._log.debug(
                    "agent-wait question for %s: %s",
                    envelope.interrupt_id,
                    json.dumps(envelope.question, default=str),
                )
        except Exception:  # pragma: no cover - an adapter may never break the run
            self._log.exception("LogAnnounce failed for interrupt %s", envelope.interrupt_id)
