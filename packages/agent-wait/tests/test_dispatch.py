"""The `dispatch()` decision table -- every `Ignore.reason`, in one place.

`dispatch()` is the only thing standing between a queue full of stale, forged, duplicated
and out-of-order messages and a graph that issues refunds. Its whole job is to say "no"
correctly, so the table of nos is the test.
"""

from __future__ import annotations

import pytest
from agent_wait import Ignore, InMemoryWaitStore, Resume, Start
from rig import Rig, approval_policy


@pytest.fixture()
def rig() -> Rig:
    return Rig(store=InMemoryWaitStore())


# ------------------------------------------------------------------ starts
def test_a_payload_without_a_token_is_a_start(rig: Rig) -> None:
    outcome = rig.runtime.dispatch({"thread_id": "t", "input": {"amount": 1}, "message_id": "m1"})

    assert isinstance(outcome, Start)
    assert outcome.thread_id == "t"
    assert outcome.input == {"amount": 1}
    assert outcome.config == {"configurable": {"thread_id": "t"}}


def test_a_start_without_a_message_id_is_taken_at_face_value(rig: Rig) -> None:
    first = rig.runtime.dispatch({"thread_id": "t", "input": {"amount": 1}})
    rig.finish("t")
    second = rig.runtime.dispatch({"thread_id": "t", "input": {"amount": 1}})

    assert isinstance(first, Start) and isinstance(second, Start)
    assert second.input == {"amount": 1}, "with nothing to dedupe on, we cannot suppress it"


# ------------------------------------------------------------------ the nos
def test_unknown_payload_no_thread_id(rig: Rig) -> None:
    outcome = rig.runtime.dispatch({"input": {"amount": 1}})
    assert isinstance(outcome, Ignore) and outcome.reason == "unknown_payload"


def test_unknown_payload_not_a_mapping(rig: Rig) -> None:
    outcome = rig.runtime.dispatch(["not", "a", "mapping"])  # type: ignore[arg-type]
    assert isinstance(outcome, Ignore) and outcome.reason == "unknown_payload"


def test_unknown_payload_missing_answer_id(rig: Rig) -> None:
    """`answer_id` is required (section 7.2) -- without it we cannot tell a double click
    from a second decision."""
    wait = rig.park()
    outcome = rig.runtime.dispatch({"token": rig.token_for(wait), "action": "approve"})

    assert isinstance(outcome, Ignore) and outcome.reason == "unknown_payload"
    assert rig.reload(wait).status == "pending"


def test_token_invalid(rig: Rig) -> None:
    outcome = rig.runtime.dispatch({"token": "aw1.k1.x.1.y.z.bad", "action": "approve", "answer_id": "a"})
    assert isinstance(outcome, Ignore) and outcome.reason == "token_invalid"


def test_token_not_a_string(rig: Rig) -> None:
    outcome = rig.runtime.dispatch({"token": 42, "action": "approve", "answer_id": "a"})
    assert isinstance(outcome, Ignore) and outcome.reason == "token_invalid"


def test_expired(rig: Rig) -> None:
    wait = rig.park()
    token = rig.token_for(wait)
    rig.clock.advance(8 * 86400)

    outcome = rig.runtime.dispatch({"token": token, "action": "approve", "answer_id": "a"})
    assert isinstance(outcome, Ignore) and outcome.reason == "expired"


def test_parked(rig: Rig) -> None:
    wait = rig.park()
    token = rig.token_for(wait)
    fresh = Rig(store=InMemoryWaitStore(), clock=rig.clock)  # a store that never saw this wait

    outcome = fresh.runtime.dispatch({"token": token, "action": "approve", "answer_id": "a"})

    assert isinstance(outcome, Ignore) and outcome.reason == "parked"
    assert fresh.store.take_parked_answer(wait.wait_id) is not None


def test_action_not_allowed(rig: Rig) -> None:
    wait = rig.park(policy=approval_policy(allowed_actions=("approve",)))

    outcome = rig.answer(wait, "reject", answer_id="a")

    assert isinstance(outcome, Ignore) and outcome.reason == "action_not_allowed"
    assert rig.reload(wait).status == "pending"


def test_action_defaults_to_resume(rig: Rig) -> None:
    wait = rig.park(policy=approval_policy(allowed_actions=("resume",)))

    outcome = rig.runtime.dispatch(
        {"token": rig.token_for(wait), "answer_id": "a", "payload": {"note": "ok"}}
    )

    assert isinstance(outcome, Resume)
    assert outcome.command == {"resume_map": {"int-1": {"action": "resume", "note": "ok"}}}


def test_duplicate_and_already_answered(rig: Rig) -> None:
    wait = rig.park()
    rig.answer(wait, "approve", answer_id="click-1")

    same = rig.answer(wait, "approve", answer_id="click-1")
    different = rig.answer(wait, "reject", answer_id="click-2")

    assert isinstance(same, Ignore) and same.reason == "duplicate"
    assert isinstance(different, Ignore) and different.reason == "already_answered"


def test_not_pending(rig: Rig) -> None:
    wait = rig.park()
    rig.adapter.advance_past(wait.interrupt_id)  # the graph already moved on

    outcome = rig.answer(wait, "approve", answer_id="a")

    assert isinstance(outcome, Ignore) and outcome.reason == "not_pending"
    assert rig.reload(wait).status == "resumed"


def test_lease_held(rig: Rig) -> None:
    wait = rig.park()
    other = Rig(store=rig.store, clock=rig.clock)
    other.runtime.owner = "worker-b"
    other.runtime.dispatch({"thread_id": "order-4471", "input": {}, "message_id": "m"})

    outcome = rig.answer(wait, "approve", answer_id="a")

    assert isinstance(outcome, Ignore) and outcome.reason == "lease_held"
    assert outcome.should_retry is True


def test_every_ignore_reason_is_reachable() -> None:
    """A reason nobody can produce is a lie in the type."""
    from typing import get_args

    from agent_wait.model import IgnoreReason

    covered = {
        "duplicate",
        "already_answered",
        "token_invalid",
        "expired",
        "not_pending",
        "action_not_allowed",
        "binding_mismatch",
        "parked",
        "unknown_payload",
        "lease_held",
        "failed",
    }
    assert set(get_args(IgnoreReason)) == covered


# ------------------------------------------------------------------ resume values
def test_resume_value_is_the_payload_plus_the_action(rig: Rig) -> None:
    wait = rig.park()

    outcome = rig.answer(wait, "approve", answer_id="a", payload={"note": "within budget"})

    assert isinstance(outcome, Resume)
    assert outcome.command == {"resume_map": {"int-1": {"action": "approve", "note": "within budget"}}}
    assert outcome.wait.wait_id == wait.wait_id
    assert outcome.config == {"configurable": {"thread_id": "order-4471"}}


def test_resume_value_tolerates_a_non_mapping_payload(rig: Rig) -> None:
    wait = rig.park()

    outcome = rig.answer(wait, "approve", answer_id="a", payload="just a string")

    assert isinstance(outcome, Resume)
    assert outcome.command == {"resume_map": {"int-1": {"action": "approve", "payload": "just a string"}}}


def test_resume_value_when_there_is_no_payload(rig: Rig) -> None:
    wait = rig.park()

    outcome = rig.runtime.dispatch({"token": rig.token_for(wait), "action": "reject", "answer_id": "a"})

    assert isinstance(outcome, Resume)
    assert outcome.command == {"resume_map": {"int-1": {"action": "reject"}}}


def test_actor_is_recorded_but_never_trusted(rig: Rig) -> None:
    """Section 16.5: `actor` is informational. It is stored and announced, and it grants
    nothing -- authentication is the entry point's business."""
    wait = rig.park()

    rig.answer(wait, "approve", answer_id="a", actor="anyone-at-all")

    assert rig.reload(wait).actor == "anyone-at-all"


# ------------------------------------------------- section 18.1: the merge order
def test_a_payload_cannot_override_the_envelopes_action(rig: Rig) -> None:
    """The reason §18.1 fixes the merge order.

    Under `{"action": action, **payload}` a sender could smuggle
    `{"payload": {"action": "approve"}}` past a wait that only permits `reject`: the
    allowed-actions check sees `reject` and passes, and the graph then reads `approve`.
    The envelope's action must win.
    """
    wait = rig.park(policy=approval_policy(allowed_actions=("reject",)))

    outcome = rig.answer(wait, "reject", answer_id="a", payload={"action": "approve", "note": "hi"})

    assert isinstance(outcome, Resume)
    assert outcome.command == {"resume_map": {"int-1": {"action": "reject", "note": "hi"}}}, (
        "the smuggled action must not survive the merge"
    )
    assert rig.reload(wait).action == "reject"


def test_a_timeout_default_cannot_override_the_timeout_action(rig: Rig) -> None:
    """Same rule on the timeout side: a mapping default gets `action: "timeout"` last."""
    wait = rig.park(policy=approval_policy(default={"action": "approve", "reason": "assumed"}))

    outcome = rig.timeout(wait)

    assert isinstance(outcome, Resume)
    assert outcome.command == {"resume_map": {"int-1": {"action": "timeout", "reason": "assumed"}}}, (
        "a default claiming 'approve' must not make a timeout look like an approval"
    )


def test_a_non_mapping_timeout_default_passes_through_unchanged(rig: Rig) -> None:
    """§18.1: the author asked for that exact value; wrapping it would be a surprise."""
    for default in ("rejected", 42, ["a", "b"], None):
        rig = Rig(store=InMemoryWaitStore())
        wait = rig.park(policy=approval_policy(default=default))

        outcome = rig.timeout(wait)

        assert isinstance(outcome, Resume)
        assert outcome.command == {"resume_map": {"int-1": default}}


def test_the_other_default_keys_survive(rig: Rig) -> None:
    wait = rig.park(policy=approval_policy(default={"reason": "no response", "by": "policy"}))

    outcome = rig.timeout(wait)

    assert isinstance(outcome, Resume)
    assert outcome.command == {
        "resume_map": {"int-1": {"action": "timeout", "reason": "no response", "by": "policy"}}
    }
