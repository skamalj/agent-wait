"""`InMemoryAnnounce` -- collects envelopes so tests can assert on what the world saw.

`FailingAnnounce` raises on demand, so the suite can check the run is unharmed and the
other adapters still got their envelope.
"""

from __future__ import annotations

from ..model import Transition, WaitEnvelope
from .base import BaseAnnounce


class InMemoryAnnounce(BaseAnnounce):
    name = "memory"

    def __init__(self, *, only: tuple[Transition, ...] | None = None) -> None:
        super().__init__(only=only)
        self.events: list[tuple[Transition, WaitEnvelope]] = []

    def deliver(self, envelope: WaitEnvelope, transition: Transition) -> None:
        self.events.append((transition, envelope))

    # -- test helpers --------------------------------------------------------
    def of(self, transition: Transition) -> list[WaitEnvelope]:
        return [e for t, e in self.events if t == transition]

    def transitions(self) -> list[Transition]:
        return [t for t, _ in self.events]

    def last(self) -> WaitEnvelope | None:
        return self.events[-1][1] if self.events else None

    def clear(self) -> None:
        self.events.clear()


class FailingAnnounce:
    """Raises on every announce. Used to prove the isolation in `CompositeAnnounce`."""

    name = "failing"

    def __init__(self, message: str = "announce backend is down") -> None:
        self.calls = 0
        self._message = message

    def supports(self, transition: Transition) -> bool:
        return True

    def announce(self, envelope: WaitEnvelope, transition: Transition) -> None:
        self.calls += 1
        raise RuntimeError(self._message)
