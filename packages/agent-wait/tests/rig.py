"""A framework that is not LangGraph, so the core can be tested without one.

`StubAdapter` is a hand-driven graph: you say what a thread is parked on and what the
next invoke does to it. That makes the awkward cases -- a crash between the invoke and
the announce, two parallel questions, a run that closes one and opens another -- ordinary
three-line tests instead of LangGraph choreography.

The LangGraph behaviour those cases stand in for is verified for real in
`packages/langgraph-wait/tests/`.
"""

from __future__ import annotations

from typing import Any

from agent_wait import (
    EntryPoint,
    FakeClock,
    InMemoryAnnounce,
    PendingInterrupt,
    Transition,
    WaitPolicy,
    WaitPublisher,
)


class StubAdapter:
    name = "stub"

    def __init__(self) -> None:
        self.parked: dict[str, list[PendingInterrupt]] = {}
        self.calls: list[tuple[Any, Any]] = []
        self.on_invoke: Any = None
        self.raises: Exception | None = None

    # -- the protocol ---------------------------------------------------------
    def config_for(self, thread_id: str) -> dict[str, Any]:
        return {"configurable": {"thread_id": thread_id}}

    def invoke(self, value: Any, config: Any) -> Any:
        self.calls.append((value, config))
        if self.raises is not None:
            raise self.raises
        if self.on_invoke is not None:
            self.on_invoke()
        return {"ok": True}

    def pending(self, thread_id: str) -> list[PendingInterrupt]:
        return list(self.parked.get(thread_id, []))

    # -- driving it -----------------------------------------------------------
    def park(
        self,
        thread_id: str,
        *interrupt_ids: str,
        policy: WaitPolicy | None = None,
        question: Any = None,
    ) -> None:
        self.parked[thread_id] = [
            PendingInterrupt(
                interrupt_id=iid,
                question=question if question is not None else {"kind": "approval", "id": iid},
                policy=policy or WaitPolicy(allowed_actions=("approve", "reject")),
                asked_at=1_760_000_000.0,
            )
            for iid in interrupt_ids
        ]

    def unpark(self, thread_id: str, interrupt_id: str) -> None:
        self.parked[thread_id] = [p for p in self.parked.get(thread_id, []) if p.interrupt_id != interrupt_id]


class Rig:
    """A publisher wired to a stub adapter and a recording announcer."""

    def __init__(self) -> None:
        self.adapter = StubAdapter()
        self.announce = InMemoryAnnounce()
        self.clock = FakeClock()
        self.agent = WaitPublisher(
            self.adapter,
            announce=[self.announce],
            reply_to=EntryPoint("sqs", "https://sqs.example/inbox"),
            clock=self.clock,
        )

    def invoke_that(self, change: Any, value: Any = None, thread_id: str = "t") -> Any:
        """Run one invoke whose effect on the parked set is `change`."""
        self.adapter.on_invoke = change
        try:
            return self.agent.invoke(value, thread_id)
        finally:
            self.adapter.on_invoke = None

    def ids(self, transition: Transition) -> list[str]:
        return [e.interrupt_id for e in self.announce.of(transition)]
