"""`LogAnnounce` -- a structured log line per transition. The default in tests.

Deliberately careful about what it prints. The question may hold anything the agent was
working on -- a customer's address, the body of a claim -- so it does not go to INFO
(CLAUDE.md). At DEBUG it is included, because by then someone has opted in.
"""

from __future__ import annotations

import json
import logging

from ..model import Transition, WaitEnvelope
from .base import BaseAnnounce


class LogAnnounce(BaseAnnounce):
    name = "log"

    def __init__(
        self,
        logger: logging.Logger | None = None,
        *,
        level: int = logging.INFO,
        only: tuple[Transition, ...] | None = None,
    ) -> None:
        super().__init__(only=only)
        self._out = logger or logging.getLogger("agent_wait.announce")
        self._level = level

    def deliver(self, envelope: WaitEnvelope, transition: Transition) -> None:
        record = {
            "event": envelope.type,
            "event_id": envelope.event_id,
            "thread_id": envelope.thread_id,
            "interrupt_id": envelope.interrupt_id,
            "allowed_actions": list(envelope.allowed_actions),
            "expires_at": envelope.expires_at,
            "tags": dict(envelope.tags),
        }
        self._out.log(self._level, "agent-wait %s", json.dumps(record, default=str))
        if self._out.isEnabledFor(logging.DEBUG):
            self._out.debug(
                "agent-wait question for %s: %s",
                envelope.interrupt_id,
                json.dumps(envelope.question, default=str),
            )
