"""The refund agent end to end: a real graph, a real checkpointer, real interrupts.

Only the cloud is faked. What these tests are really checking is the v0.2 bargain -- that
the library publishes correctly, and that the guarantees it handed back to the caller can
in fact be met by a caller doing something reasonable. Where a v0.1 test asserted "the
library refused this", the v0.2 test asserts "the router refused this, using `pending()`",
and says so.

`PAYMENTS_CALLED` is the point of all of it.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from typing import Any

import pytest
from driver import Died, LocalHost
from langgraph.checkpoint.memory import InMemorySaver
from refund_agent.graph import PAYMENTS_CALLED, build_graph, reset_side_effects

START_MESSAGE = {
    "thread_id": "order-4471",
    "input": {"order_id": "order-4471", "amount": 41000},
}


@pytest.fixture(autouse=True)
def clean_side_effects() -> Iterator[None]:
    reset_side_effects()
    yield
    reset_side_effects()


def sqlite_saver() -> Any:
    from langgraph.checkpoint.sqlite import SqliteSaver

    return SqliteSaver(sqlite3.connect(":memory:", check_same_thread=False))


@pytest.fixture(params=["memory", "sqlite"])
def host(request: pytest.FixtureRequest) -> LocalHost:
    """Twice: once entirely in memory, once against a real SQLite checkpointer, which is
    the closest local analogue of a deployed checkpointer."""
    saver = InMemorySaver() if request.param == "memory" else sqlite_saver()
    return LocalHost(build_graph(saver))


# ============================================================== the happy path
def test_a_small_refund_never_asks_anyone(host: LocalHost) -> None:
    """The publisher costs nothing when the graph does not pause."""
    host.deliver({"thread_id": "order-1", "input": {"order_id": "order-1", "amount": 900}})

    assert host.announce.events == []
    assert PAYMENTS_CALLED == ["order-1"]


def test_a_large_refund_parks_and_publishes(host: LocalHost) -> None:
    host.deliver(START_MESSAGE)

    (envelope,) = host.open_envelopes()
    assert envelope.type == "wait.created"
    assert envelope.question["kind"] == "refund_approval"
    assert envelope.question["amount"] == 41000
    assert envelope.allowed_actions == ("approve", "reject")
    assert envelope.tags == {"approver_group": "finance"}
    assert envelope.expires_at is not None
    assert PAYMENTS_CALLED == [], "nothing irreversible before a human answers"


def test_the_envelope_carries_everything_needed_to_answer_it(host: LocalHost) -> None:
    """A consumer should never need a second lookup, and never need to construct the
    reply itself."""
    host.deliver(START_MESSAGE)
    (envelope,) = host.open_envelopes()

    assert envelope.reply_to == {"kind": "sqs", "url": "https://sqs.example/agent-inbox"}
    assert envelope.reply_with["thread_id"] == "order-4471"
    assert envelope.reply_with["interrupt_id"] == envelope.interrupt_id
    assert "answer" in envelope.reply_with


def test_approval_resumes_the_graph_and_refunds_once(host: LocalHost) -> None:
    host.deliver(START_MESSAGE)
    (envelope,) = host.open_envelopes()

    host.deliver(host.reply(envelope, {"action": "approve", "note": "within budget"}))

    assert PAYMENTS_CALLED == ["order-4471"]
    assert host.agent.pending("order-4471") == []
    assert host.announce.transitions() == ["created", "resumed"], "the ticket was closed"


def test_the_answer_reaches_the_node_verbatim(host: LocalHost) -> None:
    """No merging, no injected action field. What the consumer sent is what `ask()`
    returns -- which is the simplification that removed a whole class of v0.1 rule."""
    host.deliver(START_MESSAGE)
    (envelope,) = host.open_envelopes()

    host.deliver(host.reply(envelope, {"action": "approve", "note": "within budget"}))

    state = host.agent.adapter.graph.get_state(host.adapter.config_for("order-4471"))
    assert state.values["decision"] == {"action": "approve", "note": "within budget"}


def test_rejection_skips_the_refund(host: LocalHost) -> None:
    host.deliver(START_MESSAGE)
    (envelope,) = host.open_envelopes()

    host.deliver(host.reply(envelope, {"action": "reject", "reason": "duplicate claim"}))

    assert PAYMENTS_CALLED == []
    state = host.agent.adapter.graph.get_state(host.adapter.config_for("order-4471"))
    assert state.values["status"] == "rejected"


# ============================================== the crash between run and announce
def test_a_crash_before_the_announce_is_repaired_by_the_redelivery(host: LocalHost) -> None:
    """The failure v0.1 needed a store and a sweeper for.

    The graph parked, the process died before anything was published, and there is now no
    record anywhere that a question exists. What repairs it is SQS redelivering the start
    message: the router sees the thread is already parked, and republishes rather than
    starting a fresh turn -- same interrupt id, same `dedupe_key`, so a consumer that
    somehow did see the first one discards the repeat.

    Re-invoking with the original input instead would ask the question a *second* time
    under a new id, which is a duplicate no consumer could detect. That is the rule v0.1
    got from its store of applied message ids, and the reason `pending()` is public.
    """
    with pytest.raises(Died):
        host.deliver(START_MESSAGE, die_after_invoke=True)
    assert host.announce.events == [], "nobody was told"
    assert host.agent.pending("order-4471"), "but the graph really is parked"

    host.deliver(START_MESSAGE)

    (envelope,) = host.open_envelopes()
    assert envelope.dedupe_key == f"wait.created:{host.agent.pending('order-4471')[0].interrupt_id}"

    # And a third delivery republishes the same key rather than opening a second question.
    host.deliver(START_MESSAGE)
    keys = {e.dedupe_key for e in host.open_envelopes()}
    assert len(keys) == 1, "three deliveries, one question"
    assert len(host.open_envelopes()) == 2, "published twice, deduplicable on the key"
    assert PAYMENTS_CALLED == []


def test_a_crash_after_the_refund_does_not_refund_twice(host: LocalHost) -> None:
    """The side effect happened and the host died before acking. SQS redelivers the
    approval. `pending()` is what stops it: the thread is no longer parked on that
    interrupt, so the router drops the message."""
    host.deliver(START_MESSAGE)
    (envelope,) = host.open_envelopes()
    approval = host.reply(envelope, {"action": "approve"})

    with pytest.raises(Died):
        host.deliver(approval, die_after_invoke=True)
    assert PAYMENTS_CALLED == ["order-4471"], "the side effect did happen"

    assert host.deliver(approval) == "ignored: already closed"
    assert PAYMENTS_CALLED == ["order-4471"], "and did not happen twice"


# ====================================================== the caller's own guards
def test_the_same_click_twice_is_dropped_by_the_router(host: LocalHost) -> None:
    """v0.1 called this `duplicate` and refused it in `dispatch()`. In v0.2 it is the
    router's `is_still_open()` check -- same outcome, different owner."""
    host.deliver(START_MESSAGE)
    (envelope,) = host.open_envelopes()
    approval = host.reply(envelope, {"action": "approve"})

    host.deliver(approval)
    assert host.deliver(approval) == "ignored: already closed"

    assert PAYMENTS_CALLED == ["order-4471"]


def test_a_second_different_decision_arriving_late_is_dropped(host: LocalHost) -> None:
    host.deliver(START_MESSAGE)
    (envelope,) = host.open_envelopes()

    host.deliver(host.reply(envelope, {"action": "approve"}))
    late = host.deliver(host.reply(envelope, {"action": "reject"}))

    assert late == "ignored: already closed"
    assert PAYMENTS_CALLED == ["order-4471"]


def test_an_answer_for_an_unknown_interrupt_is_dropped(host: LocalHost) -> None:
    host.deliver(START_MESSAGE)

    outcome = host.deliver(
        {"thread_id": "order-4471", "interrupt_id": "not-a-real-id", "answer": {"action": "approve"}}
    )

    assert outcome == "ignored: already closed"
    assert PAYMENTS_CALLED == []


# ============================================================== the timeout
def test_the_timeout_is_the_consumers_to_enforce(host: LocalHost) -> None:
    """v0.2 publishes the deadline and the default, and does nothing else.

    This is what "the world enforces it" looks like in practice: eleven lines, and they
    live wherever you already run scheduled work. The graph gets exactly the default its
    author declared, because nothing in between rewrote it.
    """
    host.deliver(START_MESSAGE)
    (envelope,) = host.open_envelopes()

    def sweep_expired(now: str) -> None:
        for open_envelope in host.open_envelopes():
            due = open_envelope.expires_at and open_envelope.expires_at <= now
            if due and host.is_still_open(open_envelope.thread_id, open_envelope.interrupt_id):
                host.deliver(host.reply(open_envelope, open_envelope.default))

    sweep_expired("2000-01-01T00:00:00Z")
    assert PAYMENTS_CALLED == [], "not due yet, and nothing happened"

    sweep_expired("2999-01-01T00:00:00Z")

    state = host.agent.adapter.graph.get_state(host.adapter.config_for("order-4471"))
    assert state.values["decision"] == {
        "action": "reject",
        "reason": "no response within P3D",
    }, "the author's default, verbatim"
    assert PAYMENTS_CALLED == []
    assert envelope.expires_at is not None


# ============================================================== parallel and subgraph
def test_two_parallel_approvals_resume_independently() -> None:
    """Two questions, two envelopes, and answering one leaves the other exactly where it
    was -- including still being published as open."""
    from parallel_graphs import build_parallel_graph

    host = LocalHost(build_parallel_graph())
    host.deliver({"thread_id": "batch-1", "input": {}})
    assert len(host.open_envelopes()) == 2

    first, second = host.open_envelopes()
    host.deliver(host.reply(first, {"action": "approve", "note": "within budget"}))

    assert host.announce.of("resumed")[0].interrupt_id == first.interrupt_id
    assert host.is_still_open("batch-1", second.interrupt_id)

    host.deliver(host.reply(second, {"action": "reject", "note": "within budget"}))
    values = host.agent.adapter.graph.get_state(host.adapter.config_for("batch-1")).values
    assert values == {
        "a": {"action": "approve", "note": "within budget"},
        "b": {"action": "reject", "note": "within budget"},
    }


def test_a_subgraph_interrupt_parks_and_resumes() -> None:
    from parallel_graphs import build_subgraph_graph

    host = LocalHost(build_subgraph_graph())
    host.deliver({"thread_id": "nested-1", "input": {}})

    (envelope,) = host.open_envelopes()
    assert envelope.question == {"kind": "inner_approval"}

    host.deliver(host.reply(envelope, {"action": "approve", "note": "within budget"}))

    assert host.agent.pending("nested-1") == []
    values = host.agent.adapter.graph.get_state(host.adapter.config_for("nested-1")).values
    assert values["v"] == {"action": "approve", "note": "within budget"}
