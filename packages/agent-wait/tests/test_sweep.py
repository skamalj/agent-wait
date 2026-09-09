"""The sweeper: the repair pass for crashes between the create and the announce."""

from __future__ import annotations

import pytest
from agent_wait import FailingAnnounce, InMemoryWaitStore, sweep
from rig import Rig, approval_policy


@pytest.fixture()
def rig() -> Rig:
    return Rig(store=InMemoryWaitStore())


def test_a_healthy_system_gives_the_sweeper_nothing_to_do(rig: Rig) -> None:
    rig.park()
    assert rig.runtime.sweep() == {"scanned": 0, "announced": 0, "overdue": 0}


def test_the_sweeper_completes_a_half_finished_register(rig: Rig) -> None:
    """Scenario D."""
    rig.runtime.announce.adapters = [FailingAnnounce()]
    wait = rig.park()
    assert rig.reload(wait).notified_at is None

    rig.runtime.announce.adapters = [rig.announce]
    counts = rig.runtime.sweep()

    assert counts == {"scanned": 1, "announced": 1, "overdue": 0}
    assert len(rig.announce.of("created")) == 1
    assert rig.reload(wait).notified_at is not None


def test_the_sweeper_re_announces_an_overdue_wait(rig: Rig) -> None:
    """The wait was announced, but its schedule was lost. Announcing again is how the
    timeout gets re-armed -- because the timeout is an announce adapter."""
    wait = rig.park()
    rig.announce.clear()
    rig.clock.advance(7200 + 120)

    counts = rig.runtime.sweep()

    assert counts["overdue"] == 1
    assert len(rig.announce.of("created")) == 1
    assert rig.reload(wait).status == "pending", "the sweeper decides nothing itself"


def test_the_sweeper_respects_the_grace_period(rig: Rig) -> None:
    rig.park()
    rig.clock.advance(7200 + 10)  # past expiry, inside the grace window

    assert rig.runtime.sweep()["overdue"] == 0


def test_the_sweeper_ignores_settled_waits(rig: Rig) -> None:
    wait = rig.park()
    rig.answer(wait, "approve", answer_id="a")
    rig.announce.clear()
    rig.clock.advance(30 * 86400)

    assert rig.runtime.sweep() == {"scanned": 0, "announced": 0, "overdue": 0}
    assert rig.announce.events == []


def test_the_sweeper_leaves_waits_without_a_timeout_alone(rig: Rig) -> None:
    rig.park(policy=approval_policy(timeout=None))
    rig.clock.advance(365 * 86400)

    assert rig.runtime.sweep()["overdue"] == 0


def test_sweep_is_also_a_function(rig: Rig) -> None:
    rig.park()
    assert sweep(rig.runtime) == {"scanned": 0, "announced": 0, "overdue": 0}


def test_the_sweeper_does_not_thrash_when_announce_is_still_down(rig: Rig) -> None:
    rig.runtime.announce.adapters = [FailingAnnounce()]
    wait = rig.park()

    assert rig.runtime.sweep()["announced"] == 0
    assert rig.reload(wait).notified_at is None, "still unnotified, so still on the worklist"
