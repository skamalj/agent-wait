"""`LogAnnounce`, and the one thing it must never do.

A wait envelope carries a live credential and the question the agent asked -- which may
be a customer's name, an amount, or anything else the graph was working on. CLAUDE.md and
REQUIREMENTS section 12 both say it must not reach the logs. This is where that is
enforced rather than intended.
"""

from __future__ import annotations

import logging

import pytest
from agent_wait import InMemoryWaitStore, LogAnnounce
from rig import Rig


@pytest.fixture()
def rig() -> Rig:
    return Rig(store=InMemoryWaitStore())


def test_the_token_never_reaches_the_logs(rig: Rig, caplog: pytest.LogCaptureFixture) -> None:
    """At any level, including DEBUG. A token in CloudWatch is a token anyone with read
    access to CloudWatch can use to approve a refund."""
    rig.runtime.announce.adapters = [LogAnnounce()]

    with caplog.at_level(logging.DEBUG, logger="agent_wait.announce"):
        wait = rig.park()
        rig.answer(wait, "approve", answer_id="click-1")

    token = rig.token_for(wait)
    assert token not in caplog.text
    assert "aw1." not in caplog.text


def test_the_question_is_not_logged_at_info(rig: Rig, caplog: pytest.LogCaptureFixture) -> None:
    rig.runtime.announce.adapters = [LogAnnounce()]

    with caplog.at_level(logging.INFO, logger="agent_wait.announce"):
        rig.park()

    assert "refund_approval" not in caplog.text
    assert "41000" not in caplog.text


def test_the_question_is_available_at_debug(rig: Rig, caplog: pytest.LogCaptureFixture) -> None:
    """By then somebody has deliberately turned it on."""
    rig.runtime.announce.adapters = [LogAnnounce()]

    with caplog.at_level(logging.DEBUG, logger="agent_wait.announce"):
        rig.park()

    assert "refund_approval" in caplog.text


def test_the_useful_fields_are_logged(rig: Rig, caplog: pytest.LogCaptureFixture) -> None:
    """Operationally this has to be worth reading: what happened, to which wait, when."""
    rig.runtime.announce.adapters = [LogAnnounce()]

    with caplog.at_level(logging.INFO, logger="agent_wait.announce"):
        wait = rig.park()

    assert "wait.created" in caplog.text
    assert wait.wait_id in caplog.text
    assert "order-4471" in caplog.text
    assert "approver_group" in caplog.text


def test_it_logs_every_transition(rig: Rig, caplog: pytest.LogCaptureFixture) -> None:
    rig.runtime.announce.adapters = [LogAnnounce()]

    with caplog.at_level(logging.INFO, logger="agent_wait.announce"):
        wait = rig.park()
        rig.answer(wait, "approve", answer_id="click-1")
        rig.adapter.advance_past(wait.interrupt_id)
        rig.finish()

    assert "wait.created" in caplog.text
    assert "wait.answered" in caplog.text
    assert "wait.resumed" in caplog.text


def test_a_broken_logger_cannot_break_the_run(rig: Rig) -> None:
    """The adapter contract applies to the built-in adapters too."""

    class ExplodingLogger(logging.Logger):
        def log(self, *args: object, **kwargs: object) -> None:
            raise RuntimeError("the logging backend is on fire")

        def isEnabledFor(self, level: int) -> bool:
            return False

    rig.runtime.announce.adapters = [LogAnnounce(ExplodingLogger("boom"))]

    wait = rig.park()  # must not raise

    assert rig.reload(wait).status == "pending"


def test_supports_every_transition() -> None:
    adapter = LogAnnounce()
    for transition in ("created", "answered", "expired", "resumed", "cancelled"):
        assert adapter.supports(transition) is True  # type: ignore[arg-type]
