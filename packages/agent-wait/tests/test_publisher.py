"""`WaitPublisher.invoke()` -- what gets published, and when.

The publisher keeps no records, so everything it says is derived from a before/after diff
of the framework's own state. These tests are that diff, case by case.
"""

from __future__ import annotations

import pytest
from agent_wait import EntryPoint, FailingAnnounce, InMemoryAnnounce, WaitPolicy, WaitPublisher
from rig import Rig, StubAdapter


@pytest.fixture()
def rig() -> Rig:
    return Rig()


# ------------------------------------------------------------------ opening
def test_a_run_that_parks_publishes_created(rig: Rig) -> None:
    rig.invoke_that(lambda: rig.adapter.park("t", "int-1"), value={"amount": 41000})

    assert rig.announce.transitions() == ["created"]
    envelope = rig.announce.last()
    assert envelope is not None
    assert envelope.type == "wait.created"
    assert envelope.interrupt_id == "int-1"
    assert envelope.thread_id == "t"
    assert envelope.question == {"kind": "approval", "id": "int-1"}
    assert envelope.allowed_actions == ("approve", "reject")


def test_a_run_that_parks_on_nothing_publishes_nothing(rig: Rig) -> None:
    rig.agent.invoke({"amount": 900}, "t")

    assert rig.announce.events == []


def test_the_frameworks_result_is_returned_unchanged(rig: Rig) -> None:
    """The publisher is a wrapper, not a filter."""
    assert rig.agent.invoke(None, "t") == {"ok": True}


def test_the_value_reaches_the_framework_untouched(rig: Rig) -> None:
    sentinel = object()
    rig.agent.invoke(sentinel, "t")

    value, config = rig.adapter.calls[0]
    assert value is sentinel
    assert config == {"configurable": {"thread_id": "t"}}


# ------------------------------------------------------------------ closing
def test_a_run_that_clears_an_interrupt_publishes_resumed(rig: Rig) -> None:
    rig.adapter.park("t", "int-1")
    rig.announce.clear()

    rig.invoke_that(lambda: rig.adapter.unpark("t", "int-1"))

    assert rig.announce.transitions() == ["resumed"]
    assert rig.ids("resumed") == ["int-1"]


def test_one_run_can_close_one_question_and_open_another(rig: Rig) -> None:
    """A graph that answers an approval and immediately asks a follow-up."""
    rig.adapter.park("t", "int-1")

    rig.invoke_that(lambda: rig.adapter.park("t", "int-2"))

    assert sorted(rig.announce.transitions()) == ["created", "resumed"]
    assert rig.ids("created") == ["int-2"]
    assert rig.ids("resumed") == ["int-1"]


# ------------------------------------------------------------------ parallel
def test_parallel_questions_are_published_separately(rig: Rig) -> None:
    rig.invoke_that(lambda: rig.adapter.park("t", "int-a", "int-b"))

    assert sorted(rig.ids("created")) == ["int-a", "int-b"]
    targets = {e.reply_with["interrupt_id"] for e in rig.announce.of("created")}
    assert targets == {"int-a", "int-b"}, "each envelope must resume its own interrupt"


def test_answering_one_parallel_question_does_not_touch_the_other(rig: Rig) -> None:
    rig.adapter.park("t", "int-a", "int-b")
    rig.announce.clear()

    rig.invoke_that(lambda: rig.adapter.unpark("t", "int-a"))

    assert rig.ids("resumed") == ["int-a"]
    assert rig.announce.of("created") == []


# ------------------------------------------------------------ the crash window
def test_republishing_after_a_lost_announce_repeats_the_same_dedupe_key(rig: Rig) -> None:
    """The one recovery mechanism there is.

    A crash between the invoke and the announce leaves a thread parked that nobody was
    told about. There is no store and no sweeper to notice; what fixes it is the start
    message being redelivered, the thread re-invoking, and the *same* interrupt id coming
    back -- so the consumer sees a duplicate rather than a second question.
    """
    rig.adapter.park("t", "int-1")

    first = rig.agent.envelope_for("t", rig.adapter.pending("t")[0], "created")
    rig.clock.advance(30)  # the redelivery is not instant
    second = rig.agent.envelope_for("t", rig.adapter.pending("t")[0], "created")

    assert first.dedupe_key == second.dedupe_key
    assert first.event_id != second.event_id, "event_id is per-publish; it is not the key"


def test_the_deadline_does_not_walk_forward_on_republish(rig: Rig) -> None:
    """`expires_at` is anchored to the checkpoint, not to the clock at publish time.

    Otherwise every redelivery would push the deadline out by however long the redelivery
    took, and a wait retried often enough would never expire -- the exact failure a
    timeout is there to prevent.
    """
    rig.adapter.park("t", "int-1", policy=WaitPolicy(timeout="PT1H"))

    first = rig.agent.envelope_for("t", rig.adapter.pending("t")[0], "created")
    rig.clock.advance(3600)
    second = rig.agent.envelope_for("t", rig.adapter.pending("t")[0], "created")

    assert first.expires_at == second.expires_at


def test_a_graph_that_raises_publishes_nothing(rig: Rig) -> None:
    """Nothing is known, so nothing is said -- and there is no half-written record to
    repair afterwards, which is the advantage of keeping none."""
    rig.adapter.park("t", "int-1")
    rig.announce.clear()
    rig.adapter.raises = RuntimeError("the model timed out")

    with pytest.raises(RuntimeError, match="the model timed out"):
        rig.agent.invoke(None, "t")

    assert rig.announce.events == []


# ------------------------------------------------------------------ isolation
def test_a_broken_announcer_cannot_break_the_run() -> None:
    broken = FailingAnnounce()
    healthy = InMemoryAnnounce()
    adapter = StubAdapter()
    agent = WaitPublisher(
        adapter,
        announce=[broken, healthy],
        reply_to=EntryPoint("sqs", "https://sqs.example/inbox"),
    )
    adapter.on_invoke = lambda: adapter.park("t", "int-1")

    assert agent.invoke(None, "t") == {"ok": True}
    assert broken.calls == 1
    assert healthy.transitions() == ["created"], "the healthy adapter still got it"


def test_an_adapter_can_opt_out_of_a_transition(rig: Rig) -> None:
    """A UI that draws new questions and does not care about closed ones."""
    only_new = InMemoryAnnounce(only=("created",))
    rig.agent.announce.adapters.append(only_new)
    rig.adapter.park("t", "int-1")

    rig.invoke_that(lambda: rig.adapter.unpark("t", "int-1"))

    assert rig.announce.transitions() == ["resumed"]
    assert only_new.events == []


# ------------------------------------------------------------------ reading back
def test_pending_reports_what_the_thread_is_parked_on(rig: Rig) -> None:
    """The tool for the question v0.2 hands back to the caller: is this answer still
    live, or did somebody beat me to it?"""
    rig.adapter.park("t", "int-a", "int-b")

    assert [p.interrupt_id for p in rig.agent.pending("t")] == ["int-a", "int-b"]

    rig.adapter.unpark("t", "int-a")
    assert [p.interrupt_id for p in rig.agent.pending("t")] == ["int-b"]
    assert rig.agent.pending("never-run") == []


def test_pending_publishes_nothing(rig: Rig) -> None:
    rig.adapter.park("t", "int-1")
    rig.agent.pending("t")

    assert rig.announce.events == []
