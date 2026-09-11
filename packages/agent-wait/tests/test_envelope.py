"""The wire contract.

`docs/message-formats.md` promises a specific JSON document. This file is what keeps that
promise honest: if a field is renamed or dropped, the doc is wrong and a consumer breaks,
so the shape is asserted key by key rather than by round-tripping a dataclass.
"""

from __future__ import annotations

import json

import pytest
from agent_wait import EntryPoint, QuestionTooLarge, WaitPolicy, canonical_json, new_ulid
from rig import Rig


def test_created_envelope_matches_the_documented_shape() -> None:
    rig = Rig()
    rig.adapter.park(
        "order-4471",
        "int-1",
        policy=WaitPolicy(
            timeout="P3D",
            default={"action": "reject"},
            allowed_actions=("approve", "reject"),
            tags={"approver_group": "finance"},
        ),
    )

    envelope = rig.agent.envelope_for("order-4471", rig.adapter.pending("order-4471")[0], "created")
    body = json.loads(envelope.to_json())

    assert set(body) == {
        "type",
        "event_id",
        "thread_id",
        "interrupt_id",
        "question",
        "allowed_actions",
        "expires_at",
        "default",
        "reply_to",
        "reply_with",
        "correlation",
        "tags",
    }, "the documented field set changed; docs/message-formats.md must change with it"
    assert body["type"] == "wait.created"
    assert body["thread_id"] == "order-4471"
    assert body["interrupt_id"] == "int-1"
    assert body["allowed_actions"] == ["approve", "reject"]
    assert body["default"] == {"action": "reject"}
    assert body["tags"] == {"approver_group": "finance"}
    assert body["reply_to"] == {"kind": "sqs", "url": "https://sqs.example/inbox"}


def test_reply_with_is_a_filled_in_stub_not_a_description_of_one() -> None:
    """The consumer copies it, sets `answer`, and posts it back. That is what guarantees
    `interrupt_id` is present on the way in -- so the start/resume rule always works."""
    rig = Rig()
    rig.adapter.park("order-4471", "int-1")

    envelope = rig.agent.envelope_for("order-4471", rig.adapter.pending("order-4471")[0], "created")

    assert envelope.reply_with == {
        "thread_id": "order-4471",
        "interrupt_id": "int-1",
        "answer": None,
    }


def test_a_wait_with_no_timeout_has_no_expiry() -> None:
    rig = Rig()
    rig.adapter.park("t", "int-1", policy=WaitPolicy())

    envelope = rig.agent.envelope_for("t", rig.adapter.pending("t")[0], "created")

    assert envelope.expires_at is None


def test_expires_at_is_an_instant_not_a_duration() -> None:
    """A consumer should never have to parse `P3D`. It gets an absolute time."""
    rig = Rig()
    rig.adapter.park("t", "int-1", policy=WaitPolicy(timeout="P3D"))

    envelope = rig.agent.envelope_for("t", rig.adapter.pending("t")[0], "created")

    assert envelope.expires_at == "2025-10-12T08:53:20Z", "asked_at 1_760_000_000 plus three days"


def test_dedupe_key_separates_the_two_transitions() -> None:
    """`created` and `resumed` for the same interrupt are different events; a consumer
    that deduped on the interrupt id alone would drop the close."""
    rig = Rig()
    rig.adapter.park("t", "int-1")
    interrupt = rig.adapter.pending("t")[0]

    created = rig.agent.envelope_for("t", interrupt, "created")
    resumed = rig.agent.envelope_for("t", interrupt, "resumed")

    assert created.dedupe_key == "wait.created:int-1"
    assert resumed.dedupe_key == "wait.resumed:int-1"


def test_entry_point_names_its_address_by_kind() -> None:
    assert EntryPoint("sqs", "https://q").to_dict() == {"kind": "sqs", "url": "https://q"}
    assert EntryPoint("lambda", "arn:fn").to_dict() == {"kind": "lambda", "arn": "arn:fn"}
    assert EntryPoint("http", "https://h/hook").to_dict() == {"kind": "http", "url": "https://h/hook"}


# ------------------------------------------------------------------ helpers
def test_a_ulid_is_sortable_by_time() -> None:
    from agent_wait import FakeClock

    clock = FakeClock()
    first = new_ulid(clock)
    clock.advance(1)
    second = new_ulid(clock)

    assert len(first) == 26
    assert first < second


def test_canonical_json_is_byte_stable_across_key_order() -> None:
    assert canonical_json({"b": 1, "a": 2}) == canonical_json({"a": 2, "b": 1})


def test_an_oversized_question_is_refused_where_it_is_asked() -> None:
    """Not at publish time. A question too big for SNS would otherwise park the graph on
    something nobody can ever be told about."""
    from agent_wait import check_question_size

    with pytest.raises(QuestionTooLarge, match="204800"):
        check_question_size({"blob": "x" * 300_000})


def test_iso_passes_none_through() -> None:
    """A wait with no timeout has no expiry, and `None` has to survive the formatting."""
    from agent_wait import iso

    assert iso(None) is None
    assert iso(1_760_000_000.0) == "2025-10-09T08:53:20Z"
