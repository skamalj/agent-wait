"""`BaseAnnounce`: the contract implemented once, for anyone who subclasses it."""

from __future__ import annotations

import logging

import pytest
from agent_wait import AnnounceAdapter, BaseAnnounce, Transition, WaitEnvelope, WaitPublisher
from rig import Rig, StubAdapter


class Exploding(BaseAnnounce):
    """A third-party adapter written by someone who did not read the contract."""

    name = "exploding"

    def deliver(self, envelope: WaitEnvelope, transition: Transition) -> None:
        raise ConnectionError("redis is down")


class Recording(BaseAnnounce):
    name = "recording"

    def __init__(self, **kw: object) -> None:
        super().__init__(**kw)  # type: ignore[arg-type]
        self.seen: list[str] = []

    def deliver(self, envelope: WaitEnvelope, transition: Transition) -> None:
        self.seen.append(transition)


def test_deliver_is_all_a_subclass_has_to_write() -> None:
    adapter = Recording()

    assert isinstance(adapter, AnnounceAdapter), "the base satisfies the protocol"
    assert adapter.supports("created") and adapter.supports("resumed")


def test_a_raising_deliver_becomes_a_log_line_not_an_exception(caplog: pytest.LogCaptureFixture) -> None:
    """The one rule, enforced by the base rather than remembered by the author."""
    rig = Rig()
    rig.agent.announce.adapters = [Exploding()]

    with caplog.at_level(logging.ERROR, logger="agent_wait.announce.exploding"):
        rig.invoke_that(lambda: rig.adapter.park("t", "int-1"))  # must not raise

    assert "Exploding failed for interrupt int-1" in caplog.text
    assert "redis is down" in caplog.text


def test_only_filters_without_the_subclass_doing_anything() -> None:
    adapter = Recording(only=("created",))
    rig = Rig()
    rig.agent.announce.adapters = [adapter]
    rig.adapter.park("t", "int-1")

    rig.invoke_that(lambda: rig.adapter.unpark("t", "int-1"))  # a resume

    assert adapter.seen == []
    assert not adapter.supports("resumed")


def test_subclassing_is_optional() -> None:
    """The publisher accepts the protocol, not the base class. Duck typing still works,
    and `CompositeAnnounce` still contains a failure from an adapter that did not inherit."""

    class Bare:
        name = "bare"
        seen: list[str] = []

        def supports(self, transition: Transition) -> bool:
            return True

        def announce(self, envelope: WaitEnvelope, transition: Transition) -> None:
            self.seen.append(transition)
            raise RuntimeError("still contained")

    adapter = StubAdapter()
    agent = WaitPublisher(adapter, announce=[Bare()])
    adapter.on_invoke = lambda: adapter.park("t", "int-1")

    assert agent.invoke(None, "t") == {"ok": True}
    assert Bare.seen == ["created"]
