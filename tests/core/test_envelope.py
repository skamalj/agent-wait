"""The wire contract.

`docs/message-formats.md` promises a specific JSON document. This file keeps that promise
honest: if a field is renamed or dropped, the doc is wrong and a consumer breaks, so the
shape is asserted key by key rather than by round-tripping a dataclass.
"""

from __future__ import annotations

import json

import pytest
from helpers import pending

from agent_wait import (
    EntryPoint,
    FakeClock,
    Question,
    QuestionTooLarge,
    WaitPolicy,
    build_envelope,
    canonical_json,
    check_question_size,
    iso,
    new_ulid,
)


def test_created_envelope_matches_the_documented_shape() -> None:
    envelope = build_envelope(
        "order-4471",
        pending(
            "int-1",
            policy=WaitPolicy(
                timeout="P3D",
                default={"action": "reject"},
                answer_ttl="PT15M",
                allowed_actions=("approve", "reject"),
                tags={"approver_group": "finance"},
            ),
        ),
        reply_to=EntryPoint("sqs", "https://sqs.example/inbox"),
    )
    body = json.loads(envelope.to_json())

    assert set(body) == {
        "type",
        "event_id",
        "thread_id",
        "question_id",
        "question",
        "allowed_actions",
        "expires_at",
        "default",
        "answer_ttl",
        "source",
        "reply_to",
        "reply_with",
        "correlation",
        "tags",
    }, "the documented field set changed; docs/message-formats.md must change with it"
    assert body["type"] == "wait.created"
    assert body["thread_id"] == "order-4471"
    assert body["question_id"] == "int-1"
    assert body["allowed_actions"] == ["approve", "reject"]
    assert body["default"] == {"action": "reject"}
    assert body["answer_ttl"] == "PT15M"
    assert body["tags"] == {"approver_group": "finance"}
    assert body["reply_to"] == {"kind": "sqs", "url": "https://sqs.example/inbox"}
    assert body["reply_with"] == {"thread_id": "order-4471", "question_id": "int-1", "answer": None}


def test_source_is_published_when_given() -> None:
    q = Question("q1", {"k": 1}, WaitPolicy(), source={"tool": "issue_refund"})

    assert build_envelope("t", q).to_dict()["source"] == {"tool": "issue_refund"}
    assert build_envelope("t", pending()).to_dict()["source"] is None


def test_expires_at_is_an_instant_not_a_duration() -> None:
    """A consumer should never have to parse `P3D`. It gets an absolute time."""
    envelope = build_envelope("t", pending(policy=WaitPolicy(timeout="P3D")))

    assert envelope.expires_at == "2025-10-12T08:53:20Z", "asked_at 1_760_000_000 plus three days"


def test_dedupe_key_is_type_and_question_id() -> None:
    assert build_envelope("t", pending("int-1")).dedupe_key == "wait.created:int-1"


def test_entry_point_names_its_address_by_kind() -> None:
    assert EntryPoint("sqs", "https://q").to_dict() == {"kind": "sqs", "url": "https://q"}
    assert EntryPoint("lambda", "arn:fn").to_dict() == {"kind": "lambda", "arn": "arn:fn"}
    assert EntryPoint("http", "https://h/hook").to_dict() == {"kind": "http", "url": "https://h/hook"}


# ------------------------------------------------------------------ helpers
def test_a_ulid_is_sortable_by_time() -> None:
    clock = FakeClock()
    first = new_ulid(clock)
    clock.advance(1)
    second = new_ulid(clock)

    assert len(first) == 26
    assert first < second


def test_canonical_json_is_byte_stable_across_key_order() -> None:
    assert canonical_json({"b": 1, "a": 2}) == canonical_json({"a": 2, "b": 1})


def test_iso_passes_none_through() -> None:
    assert iso(None) is None
    assert iso(1_760_000_000.0) == "2025-10-09T08:53:20Z"


def test_an_oversized_question_is_refused_where_it_is_asked() -> None:
    """Not at publish time. A question too big for SNS would otherwise park the graph on
    something nobody can ever be told about."""
    with pytest.raises(QuestionTooLarge, match="204800"):
        check_question_size({"blob": "x" * 300_000})
