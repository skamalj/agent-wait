"""`publish()` and `build_envelope()`: interrupts in, envelopes out, announcers told."""

from __future__ import annotations

from helpers import ASKED_AT, pending, publish_one

from agent_wait import (
    EntryPoint,
    FailingAnnounce,
    FakeClock,
    InMemoryAnnounce,
    WaitPolicy,
    build_envelope,
    publish,
)


def test_one_envelope_per_interrupt_reaches_every_announcer() -> None:
    a, b = InMemoryAnnounce(), InMemoryAnnounce()

    sent = publish([pending("int-a"), pending("int-b")], "t", [a, b])

    assert [e.question_id for e in sent] == ["int-a", "int-b"]
    assert [e.question_id for _, e in a.events] == ["int-a", "int-b"]
    assert [e.question_id for _, e in b.events] == ["int-a", "int-b"]
    assert {t for t, _ in a.events} == {"created"}


def test_nothing_pending_publishes_nothing() -> None:
    memory = InMemoryAnnounce()

    assert publish([], "t", [memory]) == []
    assert memory.events == []


def test_the_envelope_carries_what_the_consumer_needs() -> None:
    (envelope,), _ = publish_one(
        pending(
            "int-1",
            question={"kind": "refund_approval", "amount": 41000},
            policy=WaitPolicy(
                timeout="P3D",
                default={"action": "reject"},
                allowed_actions=("approve", "reject"),
                tags={"approver_group": "finance"},
            ),
        )
    )

    assert envelope.type == "wait.created"
    assert envelope.thread_id == "order-4471"
    assert envelope.question_id == "int-1"
    assert envelope.question == {"kind": "refund_approval", "amount": 41000}
    assert envelope.allowed_actions == ("approve", "reject")
    assert envelope.default == {"action": "reject"}
    assert envelope.tags == {"approver_group": "finance"}
    assert envelope.reply_with == {"thread_id": "order-4471", "question_id": "int-1", "answer": None}


def test_reply_to_is_optional_and_null_when_absent() -> None:
    without = build_envelope("t", pending())
    with_ = build_envelope("t", pending(), reply_to=EntryPoint("sqs", "https://q"))

    assert without.reply_to is None
    assert without.to_dict()["reply_to"] is None
    assert with_.reply_to == {"kind": "sqs", "url": "https://q"}


# ------------------------------------------------------------------ deadlines
def test_expires_at_is_measured_from_asked_at_when_known() -> None:
    envelope = build_envelope("t", pending(policy=WaitPolicy(timeout="P3D"), asked_at=ASKED_AT))

    assert envelope.expires_at == "2025-10-12T08:53:20Z"


def test_expires_at_is_measured_from_now_when_asked_at_is_unknown() -> None:
    """`Interrupt` carries no timestamp, so a caller building from `__interrupt__` alone
    gets a deadline measured from the publish. Documented; not hidden."""
    clock = FakeClock(ASKED_AT)
    envelope = build_envelope("t", pending(policy=WaitPolicy(timeout="PT1H"), asked_at=None), clock=clock)

    assert envelope.expires_at == "2025-10-09T09:53:20Z"


def test_no_timeout_means_no_expiry() -> None:
    assert build_envelope("t", pending(policy=WaitPolicy())).expires_at is None


# ------------------------------------------------------------------ identity
def test_publishing_twice_gives_the_same_dedupe_key_and_a_fresh_event_id() -> None:
    """A consumer dedupes on `dedupe_key`; `event_id` is per publish and is not the key."""
    first = build_envelope("t", pending("int-1"))
    second = build_envelope("t", pending("int-1"))

    assert first.dedupe_key == second.dedupe_key == "wait.created:int-1"
    assert first.event_id != second.event_id


# ------------------------------------------------------------------ isolation
def test_a_broken_announcer_does_not_stop_the_others_or_raise() -> None:
    broken = FailingAnnounce()
    healthy = InMemoryAnnounce()

    sent = publish([pending()], "t", [broken, healthy])

    assert len(sent) == 1, "the envelope is still returned"
    assert broken.calls == 1
    assert [e.question_id for _, e in healthy.events] == ["int-1"]
