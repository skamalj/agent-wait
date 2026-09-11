"""The four AWS announce adapters, against moto.

Two things get checked for every one: that the envelope lands where it should with the
right routing metadata, and that a broken backend produces a log line rather than an
exception. The second is the contract, and it is the reason a Slack outage cannot fail a
refund that has already been decided.
"""

from __future__ import annotations

import json
from typing import Any

import boto3
import pytest
from agent_wait_aws import DynamoDbAnnounce, EventBridgeAnnounce, SnsAnnounce, SqsAnnounce

from conftest import REGION, Broken, create_approvals_table, envelope, make_fifo_queue, received


# ============================================================== SQS
def test_sqs_announce_sends_the_envelope_to_a_fifo_queue() -> None:
    url, _ = make_fifo_queue("announcements.fifo")
    SqsAnnounce(url, region_name=REGION).announce(envelope(), "created")

    [body] = received(url)

    assert body["type"] == "wait.created"
    assert body["interrupt_id"] == "a1b2c3d4e5f60718"
    assert body["question"]["amount"] == 41000
    assert body["reply_with"]["interrupt_id"] == "a1b2c3d4e5f60718"


def test_sqs_announce_dedupes_a_republished_question() -> None:
    """The republish path, at the transport.

    Two publishes of the same question carry different `event_id`s -- it is minted per
    publish -- but the same `dedupe_key`, and that is what goes to SQS. A crash-and-retry
    inside the deduplication window therefore delivers once.
    """
    url, _ = make_fifo_queue("announcements.fifo")
    adapter = SqsAnnounce(url, region_name=REGION)

    adapter.announce(envelope(event_id="01JEVENT00000000000000001"), "created")
    adapter.announce(envelope(event_id="01JEVENT00000000000000002"), "created")

    assert len(received(url)) == 1


def test_created_and_resumed_are_not_deduplicated_against_each_other() -> None:
    """Opening and closing the same question are different events; a consumer that never
    saw the close would show the button forever."""
    url, _ = make_fifo_queue("announcements.fifo")
    adapter = SqsAnnounce(url, region_name=REGION)

    adapter.announce(envelope(), "created")
    adapter.announce(envelope(type="wait.resumed"), "resumed")

    assert len(received(url)) == 2


def test_sqs_announce_omits_fifo_parameters_for_a_standard_queue() -> None:
    url = boto3.client("sqs", region_name=REGION).create_queue(QueueName="plain")["QueueUrl"]

    SqsAnnounce(url, region_name=REGION).announce(envelope(), "created")

    assert len(received(url)) == 1


def test_sqs_announce_accepts_a_custom_group_id() -> None:
    url, _ = make_fifo_queue("announcements.fifo")
    adapter = SqsAnnounce(url, group_id=lambda e: e.tags["approver_group"], region_name=REGION)

    adapter.announce(envelope(), "created")

    assert len(received(url)) == 1


def test_sqs_announce_swallows_a_broken_client() -> None:
    SqsAnnounce("https://q.fifo", client=Broken()).announce(envelope(), "created")


# ============================================================== SNS
def test_sns_announce_publishes_with_tags_as_message_attributes() -> None:
    sns = boto3.client("sns", region_name=REGION)
    sqs = boto3.client("sqs", region_name=REGION)
    topic_arn = sns.create_topic(Name="waits")["TopicArn"]
    # A standard queue: SNS refuses to deliver from a standard topic to a FIFO one.
    url = sqs.create_queue(QueueName="sns-target")["QueueUrl"]
    arn = sqs.get_queue_attributes(QueueUrl=url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
    sns.subscribe(TopicArn=topic_arn, Protocol="sqs", Endpoint=arn, Attributes={"RawMessageDelivery": "true"})

    SnsAnnounce(topic_arn, region_name=REGION).announce(envelope(), "created")

    [body] = received(url)
    assert body["interrupt_id"] == "a1b2c3d4e5f60718"


def test_sns_announce_skips_empty_tag_values() -> None:
    """SNS rejects an empty StringValue, and no filter policy can match one."""
    captured: dict[str, Any] = {}

    class Capture:
        def publish(self, **kwargs: Any) -> dict[str, Any]:
            captured.update(kwargs)
            return {}

    SnsAnnounce("arn:topic", client=Capture()).announce(
        envelope(tags={"approver_group": "finance", "note": ""}), "created"
    )

    assert set(captured["MessageAttributes"]) == {"transition", "thread_id", "approver_group"}
    assert captured["MessageAttributes"]["transition"]["StringValue"] == "created"


def test_sns_announce_swallows_a_broken_client() -> None:
    SnsAnnounce("arn:topic", client=Broken()).announce(envelope(), "created")


# ============================================================== EventBridge
def test_eventbridge_announce_uses_the_transition_as_the_detail_type() -> None:
    captured: dict[str, Any] = {}

    class Capture:
        def put_events(self, **kwargs: Any) -> dict[str, Any]:
            captured.update(kwargs)
            return {"FailedEntryCount": 0}

    EventBridgeAnnounce("agent-wait-bus", client=Capture()).announce(envelope(type="wait.resumed"), "resumed")

    [entry] = captured["Entries"]
    assert entry["DetailType"] == "wait.resumed"
    assert entry["Source"] == "agent-wait"
    assert entry["EventBusName"] == "agent-wait-bus"
    assert json.loads(entry["Detail"])["interrupt_id"] == "a1b2c3d4e5f60718"


def test_eventbridge_announce_notices_a_failed_entry(caplog: pytest.LogCaptureFixture) -> None:
    """PutEvents returns 200 with the failures in the body, so a naive call looks fine."""

    class PartialFailure:
        def put_events(self, **kwargs: Any) -> dict[str, Any]:
            return {"FailedEntryCount": 1, "Entries": [{"ErrorCode": "ThrottlingException"}]}

    with caplog.at_level("ERROR"):
        EventBridgeAnnounce("bus", client=PartialFailure()).announce(envelope(), "created")

    assert "ThrottlingException" in caplog.text


def test_eventbridge_announce_swallows_a_broken_client() -> None:
    EventBridgeAnnounce("bus", client=Broken()).announce(envelope(), "created")


# ============================================================== DynamoDB
def rows(table_name: str) -> list[dict[str, Any]]:
    return boto3.resource("dynamodb", region_name=REGION).Table(table_name).scan()["Items"]


def test_dynamodb_announce_writes_the_question_as_a_row() -> None:
    """The point of this adapter: an approvals UI can read the question directly, with no
    broker and no projection in between."""
    table = create_approvals_table()

    DynamoDbAnnounce(table, region_name=REGION).announce(envelope(), "created")

    [row] = rows(table)
    assert row["pk"] == "THREAD#order-4471"
    assert row["sk"] == "WAIT#a1b2c3d4e5f60718"
    assert row["status"] == "open"
    assert row["question"] == {"kind": "refund_approval", "amount": 41000}
    assert row["allowed_actions"] == ["approve", "reject"]
    assert row["reply_with"]["interrupt_id"] == "a1b2c3d4e5f60718"
    assert row["default"] == {"action": "reject"}


def test_open_questions_are_queryable_by_status() -> None:
    """The query an approvals UI actually runs, and the reason for the GSI. A sweep
    enforcing timeouts runs the same one with a range condition on `expires_at`."""
    from boto3.dynamodb.conditions import Key

    table_name = create_approvals_table()
    adapter = DynamoDbAnnounce(table_name, region_name=REGION)
    adapter.announce(envelope(), "created")
    adapter.announce(envelope(interrupt_id="second", thread_id="order-9"), "created")

    table = boto3.resource("dynamodb", region_name=REGION).Table(table_name)
    open_now = table.query(IndexName="by_status", KeyConditionExpression=Key("status").eq("open"))

    assert open_now["Count"] == 2


def test_resumed_closes_the_row_rather_than_deleting_it() -> None:
    """So there is still something to look at when someone asks why the button stopped
    working."""
    table = create_approvals_table()
    adapter = DynamoDbAnnounce(table, region_name=REGION)
    adapter.announce(envelope(), "created")

    adapter.announce(envelope(type="wait.resumed"), "resumed")

    [row] = rows(table)
    assert row["status"] == "closed"
    assert row["question"] == {"kind": "refund_approval", "amount": 41000}, "still readable"


def test_republishing_the_same_question_overwrites_its_own_row() -> None:
    """The recovery path again. Two publishes of one question must leave one row, or the
    approvals UI shows a duplicate that nobody can close."""
    table = create_approvals_table()
    adapter = DynamoDbAnnounce(table, region_name=REGION)

    adapter.announce(envelope(event_id="01JEVENT00000000000000001"), "created")
    adapter.announce(envelope(event_id="01JEVENT00000000000000002"), "created")

    assert len(rows(table)) == 1


def test_closing_a_question_this_table_never_saw_leaves_nothing_behind() -> None:
    """Otherwise a `resumed` from a thread announced before this adapter existed would
    put a phantom row in the approvals history."""
    table = create_approvals_table()

    DynamoDbAnnounce(table, region_name=REGION).announce(envelope(type="wait.resumed"), "resumed")

    assert rows(table) == []


def test_dynamodb_announce_swallows_a_broken_table() -> None:
    DynamoDbAnnounce("gone", table=Broken()).announce(envelope(), "created")


def test_a_ttl_is_written_when_one_is_configured() -> None:
    """An approvals table is a log of questions; most people want it to expire."""
    table = create_approvals_table()

    DynamoDbAnnounce(table, region_name=REGION, ttl_seconds=86_400).announce(envelope(), "created")

    [row] = rows(table)
    assert int(row["ttl"]) > 0
