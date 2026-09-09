"""`LogAnnounce` -- a structured log line per transition. The default in tests.

Deliberately careful about what it prints. A wait envelope carries a live token and the
question, which may hold anything the agent was working on. Neither goes to INFO
(CLAUDE.md; REQUIREMENTS section 12). At DEBUG the question is included, because by then
someone has opted in; the token never is, at any level.
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
                "event": f"wait.{transition}",
                "event_id": envelope.event_id,
                "wait_id": envelope.wait_id,
                "thread_id": envelope.thread_id,
                "allowed_actions": list(envelope.allowed_actions),
                "expires_at": envelope.expires_at,
                "tags": dict(envelope.tags),
                "transition_detail": dict(envelope.transition_detail),
            }
            self._log.log(self._level, "agent-wait %s", json.dumps(record, default=str))
            if self._log.isEnabledFor(logging.DEBUG):
                self._log.debug(
                    "agent-wait question for %s: %s",
                    envelope.wait_id,
                    json.dumps(envelope.question, default=str),
                )
        except Exception:  # pragma: no cover - an adapter may never break the run
            self._log.exception("LogAnnounce failed for wait %s", envelope.wait_id)
