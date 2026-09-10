"""`LogAnnounce`, and the one thing it must never do.

An envelope carries the question the agent asked -- which may be a customer's name, an
amount, or anything else the graph was working on. CLAUDE.md says it must not reach the
logs at INFO. This is where that is enforced rather than intended.
"""

from __future__ import annotations

import logging

import pytest
from agent_wait import LogAnnounce, WaitPolicy
from rig import Rig


@pytest.fixture()
def rig() -> Rig:
    r = Rig()
    r.agent.announce.adapters = [LogAnnounce()]
    return r


def park(rig: Rig) -> None:
    """Run one invoke that leaves the thread parked on a refund approval."""
    rig.invoke_that(
        lambda: rig.adapter.park(
            "order-4471",
            "int-1",
            policy=WaitPolicy(allowed_actions=("approve", "reject"), tags={"group": "finance"}),
            question={"kind": "refund_approval", "amount": 41000},
        ),
        thread_id="order-4471",
    )


def test_the_question_is_not_logged_at_info(rig: Rig, caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level(logging.INFO, logger="agent_wait.announce"):
        park(rig)

    assert "refund_approval" not in caplog.text
    assert "41000" not in caplog.text


def test_the_question_is_available_at_debug(rig: Rig, caplog: pytest.LogCaptureFixture) -> None:
    """By then somebody has deliberately turned it on."""
    with caplog.at_level(logging.DEBUG, logger="agent_wait.announce"):
        park(rig)

    assert "refund_approval" in caplog.text


def test_the_useful_fields_are_logged(rig: Rig, caplog: pytest.LogCaptureFixture) -> None:
    """Operationally this has to be worth reading: what happened, to which interrupt."""
    with caplog.at_level(logging.INFO, logger="agent_wait.announce"):
        park(rig)

    assert "wait.created" in caplog.text
    assert "order-4471" in caplog.text
    assert "int-1" in caplog.text
    assert "finance" in caplog.text


def test_a_logger_that_explodes_does_not_take_the_run_with_it() -> None:
    """The adapter contract is absolute, and it applies to our own adapters too."""

    class Exploding(logging.Logger):
        def log(self, *args: object, **kwargs: object) -> None:
            raise RuntimeError("logging backend is down")

    rig = Rig()
    rig.agent.announce.adapters = [LogAnnounce(Exploding("boom"))]

    park(rig)  # must not raise
