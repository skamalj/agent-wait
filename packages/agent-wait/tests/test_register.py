"""`register()` against the in-memory store and in-memory announce (section 12, unit)."""

from __future__ import annotations

import pytest
from agent_wait import FailingAnnounce, InMemoryAnnounce, InMemoryWaitStore, Resume, WaitPolicy
from rig import Rig, approval_policy


@pytest.fixture()
def rig() -> Rig:
    return Rig(store=InMemoryWaitStore())


def test_no_interrupts_returns_nothing_and_releases_the_lease(rig: Rig) -> None:
    rig.runtime.dispatch({"thread_id": "t", "input": {}, "message_id": "m"})

    result = rig.finish("t")

    assert result.envelopes == []
    assert result.immediate_resume is None
    assert rig.store.get_lease("t") is None


def test_one_interrupt_becomes_one_wait_and_one_envelope(rig: Rig) -> None:
    result = rig.run_register([rig.interrupt()])

    assert len(result.envelopes) == 1
    wait = rig.store.get(result.envelopes[0].wait_id)
    assert wait is not None
    assert wait.status == "pending"
    assert wait.framework == "stub"
    assert wait.interrupt_id == "int-1"
    assert wait.checkpoint_id == "ckpt-1"
    assert wait.notified_at == rig.clock.now()
    assert wait.expires_at == rig.clock.now() + 7200


def test_expires_at_is_null_without_a_timeout(rig: Rig) -> None:
    wait = rig.park(policy=WaitPolicy(allowed_actions=("approve",)))

    assert wait.expires_at is None
    assert rig.announce.of("created")[0].expires_at is None


def test_a_failed_announce_leaves_notified_at_null_for_the_sweeper(rig: Rig) -> None:
    rig.runtime.announce.adapters = [FailingAnnounce()]

    wait = rig.park()

    assert rig.reload(wait).notified_at is None


def test_a_partly_failed_announce_still_counts_as_notified(rig: Rig) -> None:
    """One healthy adapter is enough. Re-announcing to the world because Slack was down
    would spam every other consumer."""
    healthy = InMemoryAnnounce()
    rig.runtime.announce.adapters = [FailingAnnounce(), healthy]

    wait = rig.park()

    assert rig.reload(wait).notified_at is not None
    assert len(healthy.of("created")) == 1


def test_register_with_no_announce_adapters_at_all(rig: Rig) -> None:
    """Nothing to retry means nothing to hold open."""
    rig.runtime.announce.adapters = []

    wait = rig.park()

    assert rig.reload(wait).notified_at is not None


def test_immediate_resume_holds_the_lease_for_the_next_invoke(rig: Rig) -> None:
    wait = rig.park()
    rig.store.park_answer(wait.wait_id, rig.answer_payload(wait, "approve", answer_id="early"))

    result = rig.run_register([rig.interrupt()])

    assert isinstance(result.immediate_resume, Resume)
    lease = rig.store.get_lease("order-4471")
    assert lease is not None and lease.owner == rig.runtime.owner


def test_a_parked_answer_that_is_not_resumable_is_reported_not_raised(rig: Rig) -> None:
    wait = rig.park()
    rig.store.park_answer(wait.wait_id, rig.answer_payload(wait, "not_an_allowed_action", answer_id="early"))

    result = rig.run_register([rig.interrupt()])

    assert result.immediate_resume is None
    assert rig.reload(wait).status == "pending"


def test_correlated_answers_are_parked_and_found_by_correlation(rig: Rig) -> None:
    policy = approval_policy(correlation={"provider": "vendor-api", "id": "job-77"})
    wait = rig.park(policy=policy)
    rig.store.park_answer("vendor-api:job-77", rig.answer_payload(wait, "approve", answer_id="cb-1"))

    result = rig.run_register([rig.interrupt(policy=policy)])

    assert isinstance(result.immediate_resume, Resume)
    assert rig.reload(wait).status == "answered"


def test_finalize_only_touches_waits_the_thread_has_passed(rig: Rig) -> None:
    answered = rig.park(interrupt_id="int-a")
    still_open = rig.park(interrupt_id="int-b", checkpoint_id="ckpt-b")
    rig.answer(answered, "approve", answer_id="a")
    rig.answer(still_open, "approve", answer_id="b")
    rig.adapter.advance_past("int-a")  # only this one moved on

    rig.finish()

    assert rig.reload(answered).status == "resumed"
    assert rig.reload(still_open).status == "answered"


def test_register_is_a_no_op_for_a_thread_with_no_waits(rig: Rig) -> None:
    assert rig.finish("never-seen").envelopes == []
