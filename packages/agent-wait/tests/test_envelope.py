"""The wire contract: envelopes, ids, hashes, size limits (REQUIREMENTS section 7)."""

from __future__ import annotations

import json

import pytest
from agent_wait import (
    EntryPoint,
    FakeClock,
    InMemoryWaitStore,
    QuestionTooLarge,
    WaitEnvelope,
    binding_hash,
    canonical_json,
    idempotency_key,
    new_ulid,
)
from rig import Rig


def test_created_envelope_matches_the_documented_shape() -> None:
    rig = Rig(store=InMemoryWaitStore())
    wait = rig.park()

    envelope = rig.announce.of("created")[0]
    body = json.loads(envelope.to_json())

    assert body["type"] == "wait.created"
    assert body["wait_id"] == wait.wait_id
    assert body["thread_id"] == "order-4471"
    assert body["question"] == {"kind": "refund_approval", "order_id": "order-4471", "amount": 41000}
    assert body["allowed_actions"] == ["approve", "reject"]
    assert body["expires_at"] == "2025-10-09T10:53:20Z"
    assert body["reply_to"] == {
        "kind": "sqs",
        "url": "https://sqs.ap-south-1.amazonaws.com/x/agent-runs.fifo",
    }
    assert body["tags"] == {"approver_group": "finance"}
    assert body["token"].startswith("aw1.k1.")
    assert set(body) == {
        "type",
        "event_id",
        "wait_id",
        "thread_id",
        "question",
        "allowed_actions",
        "expires_at",
        "token",
        "reply_to",
        "correlation",
        "tags",
        "transition_detail",
    }


def test_envelope_round_trips() -> None:
    rig = Rig(store=InMemoryWaitStore())
    rig.park()
    original = rig.announce.of("created")[0]

    assert WaitEnvelope.from_dict(json.loads(original.to_json())) == original


def test_answered_envelope_carries_the_transition_detail() -> None:
    rig = Rig(store=InMemoryWaitStore())
    wait = rig.park()
    rig.answer(wait, "approve", answer_id="click-9f1", actor="priya@corp")

    envelope = rig.announce.of("answered")[0]

    assert envelope.type == "wait.answered"
    assert envelope.transition_detail == {
        "action": "approve",
        "actor": "priya@corp",
        "answered_at": "2025-10-09T08:53:20Z",
    }


def test_event_ids_are_unique_per_transition() -> None:
    rig = Rig(store=InMemoryWaitStore())
    wait = rig.park()
    rig.answer(wait, "approve")

    ids = [e.event_id for _, e in rig.announce.events]
    assert len(ids) == len(set(ids)), "consumers dedupe on event_id"


def test_correlation_is_null_when_unset() -> None:
    rig = Rig(store=InMemoryWaitStore())
    rig.park()
    assert json.loads(rig.announce.of("created")[0].to_json())["correlation"] is None


def test_entry_point_shapes() -> None:
    assert EntryPoint("sqs", "https://q").to_dict() == {"kind": "sqs", "url": "https://q"}
    assert EntryPoint("lambda", "arn:x").to_dict() == {"kind": "lambda", "arn": "arn:x"}
    assert EntryPoint("http", "https://h").to_dict() == {"kind": "http", "url": "https://h"}


def test_ulids_are_sortable_and_unique() -> None:
    clock = FakeClock()
    first = new_ulid(clock)
    clock.advance(1)
    second = new_ulid(clock)

    assert len(first) == 26
    assert first < second, "ULIDs must sort by time"
    assert len({new_ulid(clock) for _ in range(500)}) == 500


def test_canonical_json_is_order_independent() -> None:
    assert canonical_json({"b": 1, "a": 2}) == canonical_json({"a": 2, "b": 1})
    assert canonical_json({"a": [1, 2]}) == '{"a":[1,2]}'


def test_idempotency_key_and_binding_are_distinct_and_stable() -> None:
    key = idempotency_key("t", "i", "c")
    same = idempotency_key("t", "i", "c")
    other = idempotency_key("t", "i", "c2")
    binding = binding_hash({"q": 1}, "i", "c")

    assert key == same
    assert key != other
    assert key != binding
    assert binding != binding_hash({"q": 2}, "i", "c"), "the binding must track the question"


def test_a_question_over_200kb_fails_loudly() -> None:
    """Section 16.6: no silent truncation, no blob-by-reference in v0.1."""
    rig = Rig(store=InMemoryWaitStore())

    with pytest.raises(QuestionTooLarge, match="inline limit"):
        rig.park(question={"blob": "x" * (200 * 1024)})
