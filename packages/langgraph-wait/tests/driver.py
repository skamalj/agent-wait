"""A local host: the same nine lines as `examples/refund_agent/handler.py`, minus AWS.

The example's `route()` cannot be imported here -- it builds a DynamoDB checkpointer and
an SNS client at module scope -- so the routing logic is repeated. That duplication is
deliberate and it is small; if the two ever disagree, the example is the one that is
wrong, because it is the one people copy.

`LocalHost` adds one thing the real handler does not have: `die_after_invoke`, which
raises *between* the graph returning and the publisher announcing. That is the crash
window with no store behind it any more, so it is the one worth being able to reproduce.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from agent_wait import EntryPoint, InMemoryAnnounce, WaitPublisher
from langgraph_wait import LangGraphAdapter, is_answer, resume_command


class Died(RuntimeError):
    """The process, at the least convenient moment."""


class _Crashing(LangGraphAdapter):
    """A graph that returns and then the host dies before anything is published."""

    die_after_invoke = False

    def invoke(self, value: Any, config: Any) -> Any:
        result = super().invoke(value, config)
        if self.die_after_invoke:
            raise Died("after invoke, before announce")
        return result


class LocalHost:
    def __init__(self, graph: Any) -> None:
        self.adapter = _Crashing(graph)
        self.announce = InMemoryAnnounce()
        self.agent = WaitPublisher(
            self.adapter,
            announce=[self.announce],
            reply_to=EntryPoint("sqs", "https://sqs.example/agent-inbox"),
        )

    # -- the router, mirroring examples/refund_agent/handler.py ----------------
    def deliver(self, message: Mapping[str, Any], *, die_after_invoke: bool = False) -> Any:
        self.adapter.die_after_invoke = die_after_invoke
        try:
            thread_id = message["thread_id"]
            if is_answer(message):
                if not self.is_still_open(thread_id, str(message["interrupt_id"])):
                    return "ignored: already closed"
                return self.agent.invoke(resume_command(message), thread_id)
            if self.agent.pending(thread_id):
                self.agent.republish(thread_id)
                return "republished: already parked"
            return self.agent.invoke(message.get("input"), thread_id)
        finally:
            self.adapter.die_after_invoke = False

    def is_still_open(self, thread_id: str, interrupt_id: str) -> bool:
        return any(p.interrupt_id == interrupt_id for p in self.agent.pending(thread_id))

    # -- what a consumer would do ---------------------------------------------
    def reply(self, envelope: Any, answer: Any) -> dict[str, Any]:
        """Take the `reply_with` stub off an envelope and fill in the answer, exactly as
        a UI would."""
        return {**dict(envelope.reply_with), "answer": answer}

    def open_envelopes(self) -> list[Any]:
        return self.announce.of("created")
